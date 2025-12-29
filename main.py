from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import csv
import json
import os
from datetime import datetime
import asyncio

APP_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(APP_DIR, "data")
# primary input CSV path required by your spec
INPUT_CSV_PRIMARY = os.path.join(DATA_DIR, "input", "input.csv")
# fallback to previous location
INPUT_CSV_FALLBACK = os.path.join(DATA_DIR, "input.csv")

# output labeled CSV (append-only)
OUTPUT_DIR = os.path.join(DATA_DIR, "output")
LABELED_CSV = os.path.join(OUTPUT_DIR, "labeled_results.csv")

app = FastAPI(title="Book Success Labeling")

# mount static frontend
static_dir = os.path.join(APP_DIR, "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# simple in-memory lock for writes
write_lock = asyncio.Lock()


class LabelPayload(BaseModel):
    sample_index: int
    critical_success_label: str
    popular_success_label: str
    commercial_success_label: str
    annotator: Optional[str] = ""


def get_input_csv_path() -> Optional[str]:
    if os.path.exists(INPUT_CSV_PRIMARY):
        return INPUT_CSV_PRIMARY
    if os.path.exists(INPUT_CSV_FALLBACK):
        return INPUT_CSV_FALLBACK
    return None


def get_total_rows() -> int:
    path = get_input_csv_path()
    if not path:
        return 0
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        # count rows excluding header
        rows = list(reader)
        if not rows:
            return 0
        return max(0, len(rows) - 1)


def get_csv_fieldnames() -> List[str]:
    path = get_input_csv_path()
    if not path:
        return []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return reader.fieldnames or []


def read_row_by_index(idx: int) -> Optional[Dict[str, Any]]:
    path = get_input_csv_path()
    if not path:
        return None
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i == idx:
                return row
    return None


def read_labeled_indices() -> Dict[int, Dict[str, str]]:
    """Return a dict mapping sample_index -> record (annotator,timestamp,labels...)."""
    if not os.path.exists(LABELED_CSV):
        return {}
    result = {}
    with open(LABELED_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                idx = int(row.get('sample_index', '').strip())
            except Exception:
                continue
            result[idx] = row
    return result


async def append_label_csv(record: Dict[str, str]):
    async with write_lock:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        file_exists = os.path.exists(LABELED_CSV)
        with open(LABELED_CSV, 'a', newline='', encoding='utf-8') as f:
            fieldnames = ['sample_index', 'title', 'critical_success_label', 'popular_success_label', 'commercial_success_label', 'annotator', 'timestamp']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow({k: record.get(k, '') for k in fieldnames})


@app.get("/", response_class=HTMLResponse)
async def index():
    index_path = os.path.join(static_dir, "index.html")
    return FileResponse(index_path)


@app.get("/api/items")
async def list_items(start: Optional[int] = 0, end: Optional[int] = None, skip_labeled: bool = True, show_labeled: bool = False):
    """List items by zero-based index range [start, end].
    - If end is omitted, goes to the last row.
    - By default skip samples that already have labels. Set skip_labeled=False to include them.
    - If show_labeled=True, labeled metadata is included for labeled samples.
    """
    total = get_total_rows()
    if total == 0:
        return {"total_in_dataset": 0, "range_total": 0, "returned": 0, "items": []}

    if start is None or start < 0:
        start = 0
    if end is None or end >= total:
        end = total - 1
    if end < start:
        raise HTTPException(status_code=400, detail="end must be >= start")

    labeled = read_labeled_indices()
    items = []
    for idx in range(start, end + 1):
        if skip_labeled and idx in labeled:
            continue
        row = read_row_by_index(idx)
        if row is None:
            continue
        item = {"sample_index": idx, "row": row, "labeled": idx in labeled}
        if item["labeled"] and show_labeled:
            item["label"] = labeled.get(idx)
        items.append(item)

    return {"total_in_dataset": total, "range_total": end - start + 1, "returned": len(items), "items": items}


@app.get("/api/item/{sample_index}")
async def get_item(sample_index: int):
    total = get_total_rows()
    if sample_index < 0 or sample_index >= total:
        raise HTTPException(status_code=404, detail="Item not found")
    row = read_row_by_index(sample_index)
    if row is None:
        raise HTTPException(status_code=404, detail="Item not found")
    labeled = read_labeled_indices()
    result = {"sample_index": sample_index, "row": row, "labeled": sample_index in labeled}
    if result["labeled"]:
        result["label"] = labeled.get(sample_index)
    return result


@app.get("/api/progress")
async def progress(start: Optional[int] = 0, end: Optional[int] = None):
    total = get_total_rows()
    if total == 0:
        return {"total_in_dataset": 0, "range_total": 0, "labeled_in_range": 0}
    if start is None or start < 0:
        start = 0
    if end is None or end >= total:
        end = total - 1
    if end < start:
        raise HTTPException(status_code=400, detail="end must be >= start")
    labeled = read_labeled_indices()
    labeled_in_range = sum(1 for i in range(start, end + 1) if i in labeled)
    return {"total_in_dataset": total, "range_total": end - start + 1, "labeled_in_range": labeled_in_range}


@app.post("/api/label")
async def save_label(payload: LabelPayload):
    total = get_total_rows()
    if payload.sample_index < 0 or payload.sample_index >= total:
        raise HTTPException(status_code=400, detail="Invalid sample_index")

    # fetch the row snapshot to include title (if present) and make record self-contained
    row = read_row_by_index(payload.sample_index) or {}
    title = row.get('title') or row.get('Title') or ""

    record = {
        'sample_index': str(payload.sample_index),
        'title': title,
        'critical_success_label': payload.critical_success_label,
        'popular_success_label': payload.popular_success_label,
        'commercial_success_label': payload.commercial_success_label,
        'annotator': payload.annotator or "",
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }

    await append_label_csv(record)
    return {"status": "ok"}
