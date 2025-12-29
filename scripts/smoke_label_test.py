import csv
import os
import asyncio
from pathlib import Path
from datetime import datetime
import importlib

# import the backend module as `backend` to avoid name collisions
backend = importlib.import_module('main')

OUTPUT_CSV = Path(os.path.join(os.path.dirname(__file__), '..', 'data', 'output', 'labeled_results.csv')).resolve()


def find_first_unlabeled(limit=10):
    total = backend.get_total_rows()
    labeled = backend.read_labeled_indices()
    for i in range(min(limit, total)):
        if i not in labeled:
            return i
    return None


def append_label_direct(idx):
    row = backend.read_row_by_index(idx) or {}
    title = row.get('title') or row.get('Title') or ''
    record = {
        'sample_index': str(idx),
        'title': title,
        'critical_success_label': 'Moderate',
        'popular_success_label': 'High',
        'commercial_success_label': 'Moderate',
        'annotator': 'smoke',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    asyncio.run(backend.append_label_csv(record))


def run_test():
    idx = find_first_unlabeled(limit=10)
    if idx is None:
        print('No unlabeled items in the first 10 rows. Try a different range or set skip_labeled=false')
        return
    print('Selected sample index:', idx)
    append_label_direct(idx)

    if not OUTPUT_CSV.exists():
        print('Output CSV not found at', OUTPUT_CSV)
        return

    found = False
    with open(OUTPUT_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                if int(row.get('sample_index', '').strip()) == int(idx):
                    found = True
                    print('Found appended row for sample_index in output CSV.')
                    print(row)
                    break
            except Exception:
                continue

    if not found:
        print('Did not find the appended label row in output CSV. It may not have been written yet or indexing mismatch.')


if __name__ == '__main__':
    run_test()
