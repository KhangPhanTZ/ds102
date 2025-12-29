// Range-aware labeling frontend
const appState = {
  items: [],
  currentPointer: 0,
  rangeStart: 0,
  rangeEnd: 0
}

const el = id => document.getElementById(id)

async function fetchProgress(){
  const start = parseInt(el('start_index').value || 0)
  const end = parseInt(el('end_index').value || 0)
  const r = await fetch(`/api/progress?start=${start}&end=${end}`)
  if(!r.ok) return
  const j = await r.json()
  el('progress').innerText = `Labeled ${j.labeled_in_range} / ${j.range_total} (range)`
}

function clearGroups(){ el('groups').innerHTML = '' }

function buildGroup(title, entries){
  const div = document.createElement('div')
  div.className = 'group'
  const h = document.createElement('h4')
  h.innerText = title
  div.appendChild(h)
  const ul = document.createElement('ul')
  entries.forEach(e=>{
    const li = document.createElement('li')
    li.innerHTML = `<strong>${e.k}:</strong> ${e.v}`
    ul.appendChild(li)
  })
  div.appendChild(ul)
  return div
}

function renderCurrent(){
  clearGroups()
  const items = appState.items
  let currentPointer = appState.currentPointer
  if(items.length===0){
    el('sample_index').innerText = '-'
    el('title').innerText = 'No items in range or all skipped'
    return
  }
  if(currentPointer<0) currentPointer = 0
  if(currentPointer>=items.length) currentPointer = items.length-1
  appState.currentPointer = currentPointer
  const item = items[currentPointer]
  el('sample_index').innerText = item.sample_index
  const row = item.row || {}
  el('title').innerText = row.title || row.Title || '(no title)'

  const metadataKeys = ['title','Title','author','Author','publication_year','publicationYear','year','Year','Genre','genre','description','Description']
  const criticalKeys = ['is_expert','isExpert','critical_indicators','critical_flag']
  const popularKeys = ['rating','average_rating','ratings_count','n_votes','total_weeks','best_rank']
  const reviewKeys = ['review_text','review','summary']
  const commercialKeys = ['Units_Sold','Gross_Sales','Sale_Price','Sales_Rank','SalesRank','sales_rank']

  function collect(keys){
    const out = []
    keys.forEach(k=>{ if(row[k] !== undefined && row[k] !== null && String(row[k]).trim()!=='') out.push({k:k,v:row[k]}) })
    return out
  }

  const md = collect(metadataKeys)
  const cr = collect(criticalKeys)
  const pop = collect(popularKeys)
  const rv = collect(reviewKeys)
  const cm = collect(commercialKeys)

  if(md.length) el('groups').appendChild(buildGroup('Metadata', md))
  if(rv.length) el('groups').appendChild(buildGroup('Review & Expertise', rv))
  if(cr.length) el('groups').appendChild(buildGroup('Critical indicators', cr))
  if(pop.length) el('groups').appendChild(buildGroup('Popular indicators', pop))
  if(cm.length) el('groups').appendChild(buildGroup('Commercial indicators', cm))

  el('critical_success_label').value = ''
  el('popular_success_label').value = ''
  el('commercial_success_label').value = ''
}

async function loadRange(){
  appState.rangeStart = parseInt(el('start_index').value || 0)
  appState.rangeEnd = parseInt(el('end_index').value || 0)
  if(appState.rangeEnd < appState.rangeStart){ alert('End must be >= start'); return }
  const skip = el('skip_labeled').checked ? 'true' : 'false'
  const show = el('show_labeled').checked ? 'true' : 'false'
  const r = await fetch(`/api/items?start=${appState.rangeStart}&end=${appState.rangeEnd}&skip_labeled=${skip}&show_labeled=${show}`)
  if(!r.ok){ alert('Failed to load range'); return }
  const j = await r.json()
  appState.items = j.items || []
  appState.currentPointer = 0
  await fetchProgress()
  renderCurrent()
}

async function saveLabel(){
  const items = appState.items
  if(items.length===0) return
  const item = items[appState.currentPointer]
  const payload = {
    sample_index: item.sample_index,
    critical_success_label: el('critical_success_label').value,
    popular_success_label: el('popular_success_label').value,
    commercial_success_label: el('commercial_success_label').value,
    annotator: el('annotator').value || ''
  }
  if(!payload.critical_success_label && !payload.popular_success_label && !payload.commercial_success_label){
    if(!confirm('No labels selected. Save empty labels?')) return
  }
  const r = await fetch('/api/label',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)})
  if(!r.ok){ const t = await r.text(); alert('Save failed: '+t); return }
  appState.items.splice(appState.currentPointer,1)
  if(appState.currentPointer >= appState.items.length) appState.currentPointer = appState.items.length-1
  await fetchProgress()
  renderCurrent()
}

document.addEventListener('DOMContentLoaded', ()=>{
  el('load_range').addEventListener('click', async ()=>{ el('load_range').disabled=true; await loadRange(); el('load_range').disabled=false })
  el('prev').addEventListener('click', ()=>{ appState.currentPointer = Math.max(0, appState.currentPointer-1); renderCurrent() })
  el('next').addEventListener('click', ()=>{ appState.currentPointer = Math.min(appState.items.length-1, appState.currentPointer+1); renderCurrent() })
  el('save').addEventListener('click', async ()=>{ el('save').disabled = true; await saveLabel(); el('save').disabled=false })
  (async ()=>{
    const r = await fetch('/api/progress')
    if(r.ok){
      const j = await r.json()
      const total = j.total_in_dataset || j.total || 0
      el('end_index').value = Math.max(0, total-1)
      el('start_index').value = 0
      el('progress').innerText = `Labeled 0 / ${total} (dataset)`
    }
  })()
})
