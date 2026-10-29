'use strict';
const $ = s => document.querySelector(s);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const icon = name => `<svg aria-hidden="true"><use href="#${name}"/></svg>`;
// Refresh dynamic panes without dropping the user's keyboard position.
function replaceContent(element, html) {
  if (element.spillHTML === html) return;
  const active = document.activeElement;
  const key = element.contains(active) ? active.dataset.focusKey : null;
  const previous = key ? [...element.querySelectorAll('[data-focus-key]')].indexOf(active) : -1;
  const top = element.scrollTop, left = element.scrollLeft;
  element.innerHTML = html;
  element.spillHTML = html;
  if (key) {
    const targets = [...element.querySelectorAll('[data-focus-key]')].filter(el => !el.disabled);
    const target = targets.find(el => el.dataset.focusKey === key) || targets[Math.min(previous, targets.length-1)];
    (target || $('#search')).focus({preventScroll:true});
  }
  element.scrollTop = top; element.scrollLeft = left;
}
const bytes = n => {
  if (n == null) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(i ? 1 : 0)} ${units[i]}`;
};
const rate = n => n > 0 ? `${bytes(n)}/s` : '—';
const progress = t => t.metadata_pending ? null : t.completed ? 100 : Math.min(99.9, 100 * t.wanted_verified_bytes / (t.wanted_bytes || 1));
const stateClass = t => t.state;
const filters = [['all','grid','All transfers'],['downloading','down','Downloading'],['seeding','up','Seeding'],['paused','pause','Paused'],['queued','clock','Queued'],['error','alert','Needs attention']];
let torrents = [], selected = new Set(), focused = null, filter = 'all', query = '', sortKey = 'name', direction = 1, tab = 'overview';
let online = false, busy = false, refreshPromise;
let filePage = null, fileError = '', fileLoading = null, fileRequest = 0;
let fileOffset = 0, fileHistory = [], fileUpdated = 0;
function resetFiles() {
  filePage = null; fileError = ''; fileLoading = null;
  fileOffset = 0; fileHistory = []; fileUpdated = 0; fileRequest++;
}
const matches = (t, key) => key === 'all' || (key === 'error' ? !!t.last_error : stateClass(t) === key);
const visible = () => torrents.filter(t => matches(t, filter) && t.name.toLowerCase().includes(query.toLowerCase())).sort((a,b) => {
  const x = sortKey === 'size' ? a.total_bytes : sortKey === 'progress' ? progress(a) : a[sortKey];
  const y = sortKey === 'size' ? b.total_bytes : sortKey === 'progress' ? progress(b) : b[sortKey];
  return (typeof x === 'string' ? x.localeCompare(y) : (x ?? 0) - (y ?? 0)) * direction;
});
function updateTheme() {
  $('#theme-button').setAttribute('aria-pressed',String(document.documentElement.dataset.theme === 'dark'));
  $('meta[name="theme-color"]').content = getComputedStyle(document.documentElement).getPropertyValue('--paper').trim();
}
$('#theme-button').onclick = () => {
  const theme = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
  document.documentElement.dataset.theme = theme;
  localStorage.setItem('spill-theme',theme);
  updateTheme();
};
updateTheme();
let toastTimer;
function notify(message) {
  $('#toast').textContent = message; $('#toast').hidden = false;
  clearTimeout(toastTimer); toastTimer = setTimeout(() => $('#toast').hidden = true, 6000);
}
function connection(ok, message) {
  online = ok;
  $('#sidebar-status').textContent = ok ? 'Connected' : 'Disconnected';
  $('#connection-status').textContent = message;
  $('.status-badge').textContent = ok ? 'LIVE · 1s' : 'DISCONNECTED · STALE';
  $('.table-footer > span:last-child').textContent = ok ? 'Live daemon snapshot' : 'Showing last snapshot; retrying…';
  $('.bottom-bar .left > span:first-child').textContent = ok ? 'Connected' : 'Disconnected';
}
async function request(path, body, binary = false) {
  const response = await fetch(path, {
    method: body ? 'POST' : 'GET',
    headers: {'X-Spill-Token': token, ...(body ? {'Content-Type': binary ? 'application/x-bittorrent' : 'application/json'} : {})},
    body: body ? (binary ? body : JSON.stringify(body)) : undefined,
    signal: AbortSignal.timeout(body ? 35000 : 5000)
  });
  const value = await response.json().catch(() => ({error:`HTTP ${response.status}`}));
  if (!response.ok || value.code !== 0) throw new Error(value.error || (response.status === 403 ? 'Daemon restarted; reload this page.' : `HTTP ${response.status}`));
  return value.data;
}
const api = (op, fields = {}) => request('/api', {op, ...fields});
function transferRow(row) {
  const down = row.transfer?.download_bytes_per_second || 0;
  const remaining = (row.wanted_bytes || 0) - (row.wanted_verified_bytes || 0);
  return {...row, name:row.name || row.id, metadata_pending:row.total_bytes === null,
    down, up:row.transfer?.upload_bytes_per_second || 0, peers:row.transfer?.active_peers || 0,
    eta:remaining && down ? duration(1000*remaining/down) : '—'};
}
function refresh() {
  if (refreshPromise) return refreshPromise;
  refreshPromise = (async () => {
    try {
      const value = await api('list');
      torrents = value.torrents.map(transferRow);
      const ids = new Set(torrents.map(t => t.id));
      selected = new Set([...selected].filter(id => ids.has(id)));
      if (!ids.has(focused)) focused = null;
      connection(true, 'Updated ' + new Date().toLocaleTimeString());
    } catch (error) { connection(false, error.message); }
    finally { render(); }
  })().finally(() => refreshPromise = null);
  return refreshPromise;
}
async function poll() {
  if (!busy) { await refresh(); await refreshDetail(); }
  setTimeout(poll, 1000);
}
async function refreshFiles(force = false) {
  if (!focused || !online || tab !== 'files') return;
  const id = focused, offset = fileOffset, key = id+':'+offset;
  if (fileLoading === key || (!force && filePage && Date.now()-fileUpdated < 5000)) return;
  const request = ++fileRequest;
  fileLoading = key;
  try {
    const value = await api('files', {id});
    const end = Math.min(offset+100, value.files.length);
    const page = {id, offset, files:value.files.slice(offset,end),
      total_files:value.files.length, next_offset:end < value.files.length ? end : null};
    if (focused === id && fileOffset === offset && request === fileRequest) {
      filePage = page; fileError = ''; fileUpdated = Date.now();
    }
  } catch (error) {
    if (focused === id && fileOffset === offset && request === fileRequest) {
      fileError = error.message; fileUpdated = Date.now();
    }
  } finally {
    if (request === fileRequest) { fileLoading = null; renderDetail(); }
  }
}
async function refreshDetail() {
  if (tab === 'files') await refreshFiles();
}
function policy(t) {
  return [['trackers','TRK'],['dht','DHT'],['web_seeds','WEB']].map(([key,label]) => `<span class="source-bit ${t.settings[key] ? '' : 'off'}">${label}</span>`).join('');
}
function render() {
  replaceContent($('#navigation'), filters.map(([key,ic,label]) => `<button data-filter="${key}" data-focus-key="filter-${key}" aria-label="${label}" class="${key === filter ? 'active' : ''}" ${key === filter ? 'aria-current="page"' : ''}>${icon(ic)}<span class="label">${label}</span><span class="count">${torrents.filter(t => matches(t,key)).length}</span></button>`).join(''));
  const rows = visible();
  $('#list-title').textContent = `${filters.find(f => f[0] === filter)[2]} · ${rows.length}`;
  replaceContent($('#torrents'), rows.map(t => {
    const p = progress(t);
    return `<tr data-id="${esc(t.id)}" data-focus-key="row-${esc(t.id)}" class="${selected.has(t.id) ? 'selected ' : ''}${stateClass(t)}" tabindex="0" aria-label="${esc(t.name)}, ${esc(t.state)}">
      <td><input type="checkbox" data-check data-focus-key="select-${esc(t.id)}" aria-label="Select ${esc(t.name)}" ${selected.has(t.id) ? 'checked' : ''}></td>
      <td><div class="torrent-name"><span class="file-icon">${icon('file')}</span><div class="name-text"><strong title="${esc(t.name)}">${esc(t.name)}</strong><small>${esc(t.id.slice(0,12))}</small></div></div></td>
      <td class="mono">${t.metadata_pending ? '—' : bytes(t.total_bytes)}</td>
      <td><div class="progress-wrap"><div class="progress-meta"><span>${p == null ? '—' : p.toFixed(1)+'%'}</span></div><div class="progress-track"><i style="width:${p || 0}%"></i></div></div></td>
      <td><span class="status ${stateClass(t)}" title="${esc(t.last_error || t.tracker_message || '')}">${esc(t.state)}</span></td>
      <td class="mono total" title="${esc(t.downloaded_bytes)} bytes, lifetime">${bytes(t.downloaded_bytes)}</td><td class="mono total" title="${esc(t.uploaded_bytes)} bytes, lifetime">${bytes(t.uploaded_bytes)}</td>
      <td class="mono speed-down">${rate(t.down)}</td><td class="mono speed-up">${rate(t.up)}</td><td class="mono">${t.peers}</td><td class="mono">${esc(t.eta)}</td>
      <td><span class="discovery-bits" title="Allowed by policy; not activity telemetry">${policy(t)}</span></td><td><button class="icon-button" data-focus-key="inspect-${esc(t.id)}" aria-label="Inspect ${esc(t.name)}">${icon('dots')}</button></td></tr>`;
  }).join(''));
  $('#empty').hidden = rows.length > 0;
  $('#empty').textContent = online ? 'No transfers match. Add a torrent to get started.' : 'Waiting for daemon. Start spill serve and check the socket path.';
  const hiddenSelected = selected.size - rows.filter(t => selected.has(t.id)).length;
  $('#visible-count').textContent = `${rows.length} transfers · ${selected.size} selected${hiddenSelected ? ` (${hiddenSelected} hidden)` : ''}`;
  $('#select-all').checked = rows.length > 0 && rows.every(t => selected.has(t.id));
  $('#select-all').indeterminate = rows.some(t => selected.has(t.id)) && !$('#select-all').checked;
  ['pause','resume','recheck','reannounce','remove'].forEach(a => $('#'+a+'-button').disabled = !online || busy || !selected.size);
  $('#add-button').disabled = !online || busy;
  $('#limits-button').disabled = !online || busy || !focused;
  $('#seeding-button').disabled = !online || busy || !focused;
  $('#seeding-form button[type="submit"]').disabled = !online || busy;
  $('#limits-form button[type="submit"]').disabled = !online || busy;
  $('#confirm-remove').disabled = busy || !online;
  $('#add-form button[type="submit"]').disabled = busy || !online;
  $('#download-stat').textContent = rate(torrents.reduce((s,t) => s+t.down,0));
  $('#upload-stat').textContent = rate(torrents.reduce((s,t) => s+t.up,0));
  $('#peers-stat').textContent = torrents.reduce((s,t) => s+t.peers,0);
  document.querySelectorAll('[id^="sort-"]').forEach(el => el.textContent = el.id === 'sort-'+sortKey ? (direction === 1 ? '↑' : '↓') : '');
  renderDetail();
}
const detailRow = (label,value) => `<div class="detail-row"><span>${esc(label)}</span><span>${esc(value)}</span></div>`;
function renderDetail() {
  const t = torrents.find(t => t.id === focused);
  if (!t) { $('#inspector').hidden = true; return; }
  if (tab === 'overview') replaceContent($('#detail-body'), renderOverview(t));
  else if (tab === 'files') {
    if (t.metadata_pending) { replaceContent($('#detail-body'), telemetryNote('Awaiting metadata.')); return; }
    if (!filePage || filePage.id !== focused || filePage.offset !== fileOffset) {
      replaceContent($('#detail-body'), telemetryNote(fileError || (online ? 'Loading files…' : 'Disconnected. File details are unavailable.')));
      if (online && !fileError) void refreshFiles();
    } else {
      replaceContent($('#detail-body'), (fileError ? telemetryNote('File refresh failed: '+fileError) : '') +
        (!online ? telemetryNote('Disconnected. Showing last file snapshot.') : '') + renderFileManifest(filePage, fileHistory.length > 0));
    }
  }
  else {
    if (!online) { replaceContent($('#detail-body'), telemetryNote('Disconnected. Live telemetry is unavailable.')); return; }
    replaceContent($('#detail-body'), tab === 'peers' ? renderPeerTelemetry(t) : renderSourceTelemetry(t));
  }
}
async function mutate(work) {
  if (busy || !online) return;
  busy = true; render();
  try { if (refreshPromise) await refreshPromise; await work(); }
  catch (error) { notify(error.message + ' Refresh to confirm state before retrying.'); }
  finally { busy = false; await refresh(); await refreshDetail(); render(); }
}
async function action(op) {
  const ids = [...selected];
  await mutate(async () => {
    let count = 0;
    const errors = [];
    for (const id of ids) {
      try { await api(op,{id}); count++; }
      catch (e) { errors.push(`${id.slice(0,8)}: ${e.message}`); }
    }
    notify(`${count}/${ids.length} ${op} requests accepted.` + (errors.length ? ' '+errors.join('; ') : ''));
  });
}
$('#navigation').onclick = e => { const b = e.target.closest('[data-filter]'); if (b) { filter = b.dataset.filter; render(); } };
$('#search').oninput = e => { query = e.target.value; render(); };
$('thead').onclick = e => { const b = e.target.closest('[data-sort]'); if (b) { direction = sortKey === b.dataset.sort ? -direction : 1; sortKey = b.dataset.sort; render(); } };
$('#select-all').onchange = e => { visible().forEach(t => e.target.checked ? selected.add(t.id) : selected.delete(t.id)); render(); };
$('#torrents').onclick = e => {
  const row = e.target.closest('tr'); if (!row) return;
  const id = row.dataset.id;
  if (e.target.matches('[data-check]')) e.target.checked ? selected.add(id) : selected.delete(id);
  else selected = new Set([id]);
  if (focused !== id) resetFiles();
  focused = id; $('#inspector').hidden = false; render();
};
$('#torrents').onkeydown = e => { if (e.target.matches('tr') && ['Enter',' '].includes(e.key)) { e.preventDefault(); e.target.click(); } };
$('.tabs').onclick = e => {
  const b = e.target.closest('[data-tab]'); if (!b) return;
  tab = b.dataset.tab;
  fileError = '';
  if (tab === 'files') void refreshFiles(true);
  else if (['peers','sources'].includes(tab)) void refreshDetail();
  document.querySelectorAll('.tab').forEach(el => { el.classList.toggle('active',el === b); el.setAttribute('aria-selected',String(el === b)); });
  renderDetail();
};
$('#detail-body').onclick = e => {
  const button = e.target.closest('[data-file-page]');
  if (!button || !online || fileLoading) return;
  if (button.dataset.filePage === 'next' && filePage?.next_offset != null) {
    fileHistory.push(fileOffset); fileOffset = filePage.next_offset;
  } else if (button.dataset.filePage === 'previous' && fileHistory.length) {
    fileOffset = fileHistory.pop();
  }
  filePage = null; fileError = ''; fileUpdated = 0;
  void refreshFiles(true); renderDetail();
};
$('#detail-body').onchange = e => {
  const select = e.target.closest('[data-file-priority]');
  if (!select || !online || busy || !focused) return;
  const id = focused, index = Number(select.dataset.filePriority), priority = select.value;
  void mutate(async () => {
    const files = await api('files', {id});
    const file_priorities = files.files.map(file => file.priority);
    file_priorities[index] = priority;
    await api('set', {id, values:{file_priorities}});
    fileUpdated = 0;
    await refreshFiles(true);
    notify('File priority saved. Active transfers restart after joined shutdown; paused transfers stay paused.');
  });
};
$('#close-detail').onclick = () => { focused = null; resetFiles(); $('#inspector').hidden = true; };
['pause','resume','recheck','reannounce'].forEach(op => $('#'+op+'-button').onclick = () => action(op));
$('#add-button').onclick = () => $('#add-dialog').showModal();
$('#cancel-add').onclick = () => $('#add-dialog').close();
$('#add-form').onsubmit = e => {
  e.preventDefault();
  const file = $('#torrent-file').files[0], magnet = $('#magnet').value.trim();
  if (!!file === !!magnet) { notify('Choose either one .torrent file or one magnet link.'); return; }
  const output_dir = $('#output').value.trim() || null;
  if (output_dir && !output_dir.startsWith('/')) { notify('Download location must be an absolute daemon-side path.'); return; }
  mutate(async () => {
    let result;
    if (file) {
      if (!file.size || file.size > 16*1024*1024) throw new Error('Torrent file must be nonempty and at most 16 MiB.');
      const options = new URLSearchParams();
      if (output_dir) options.set('output_dir',output_dir);
      result = await request('/api/upload?'+options,file,true);
    } else {
      result = await api('add',{magnet,output_dir});
    }
    const added = await api('get',{id:result.id});
    await api('set',{id:result.id,values:{trackers:$('#allow-trackers').checked,
      dht:added.private ? false : $('#allow-dht').checked, web_seeds:$('#allow-web').checked}});
    if (!$('#start-now').checked) await api('pause',{id:result.id});
    selected = new Set([result.id]); focused = result.id; resetFiles(); filter = 'all'; query = ''; $('#search').value = '';
    $('#inspector').hidden = false; $('#add-dialog').close(); $('#add-form').reset();
    notify('Torrent accepted by daemon.');
  });
};
let seedingId = null;
$('#seeding-button').onclick = () => {
  const t = torrents.find(t => t.id === focused);
  if (!t) return;
  seedingId = t.id;
  $('#seed-ratio').value = t.settings.seed_ratio; $('#seed-time').value = t.settings.seed_time;
  $('#seeding-dialog').showModal();
};
$('#cancel-seeding').onclick = () => $('#seeding-dialog').close();
$('#seeding-form').onsubmit = e => {
  e.preventDefault();
  const id = seedingId, seed_ratio = Number($('#seed-ratio').value), seed_time = Number($('#seed-time').value);
  if (!Number.isFinite(seed_ratio) || seed_ratio < 0 || !Number.isSafeInteger(seed_time) || seed_time < 0) { notify('Use a finite nonnegative ratio and whole nonnegative seconds.'); return; }
  mutate(async () => {
    await api('set', {id, values:{seed_ratio, seed_time}});
    $('#seeding-dialog').close(); notify('Seeding rules accepted. Paused torrents stay paused until resumed.');
  });
};
let limitsId = null;
$('#limits-button').onclick = () => {
  const t = torrents.find(t => t.id === focused);
  if (!t) return;
  limitsId = t.id;
  $('#limit-down').value = t.settings.download_limit; $('#limit-up').value = t.settings.upload_limit;
  $('#limits-dialog').showModal();
};
$('#cancel-limits').onclick = () => $('#limits-dialog').close();
$('#limits-form').onsubmit = e => {
  e.preventDefault();
  const id = limitsId, down = Number($('#limit-down').value), up = Number($('#limit-up').value);
  if (![down,up].every(n => Number.isSafeInteger(n) && n >= 0)) { notify('Use nonnegative whole bytes/s within the displayed range.'); return; }
  mutate(async () => {
    await api('set', {id, values:{download_limit:down, upload_limit:up}});
    $('#limits-dialog').close(); notify('Rate limits accepted.');
  });
};
let removeIds = [];
$('#remove-button').onclick = () => {
  removeIds = [...selected];
  const shown = new Set(visible().map(t => t.id));
  const hidden = removeIds.filter(id => !shown.has(id)).length;
  $('#remove-description').textContent = `Remove ${removeIds.length} selected transfers?${hidden ? ` ${hidden} are hidden by the current filter or search.` : ''} Downloaded files are kept by default.`;
  $('#remove-transfers').innerHTML = removeIds.map(id => {
    const t = torrents.find(t => t.id === id);
    return `<li><strong>${esc(t?.name || id)}</strong><small>${esc(id)}${shown.has(id) ? '' : ' · Hidden from current view'}</small></li>`;
  }).join('');
  $('#delete-data').checked = false; $('#remove-dialog').showModal();
};
$('#cancel-remove').onclick = () => $('#remove-dialog').close();
$('#confirm-remove').onclick = () => {
  const ids = [...removeIds], delete_payload = $('#delete-data').checked;
  mutate(async () => {
    let count = 0; const errors = [];
    for (const id of ids) {
      try { await api('remove',{id,delete_payload}); count++; }
      catch (e) { errors.push(e.message); }
    }
    $('#remove-dialog').close(); notify(`${count}/${ids.length} removed; files ${delete_payload ? 'deleted' : 'kept'}. ${errors.join('; ')}`);
  });
};
const resizer = $('#detail-resizer');
function resize(height) {
  const max = Math.max(60, innerHeight-290), h = Math.max(60,Math.min(max,height));
  $('#inspector').style.setProperty('--detail-height',h+'px');
  resizer.setAttribute('aria-valuenow',h); resizer.setAttribute('aria-valuemax',max);
}
let drag;
resizer.onpointerdown = e => { drag = {y:e.clientY,h:$('#detail-body').getBoundingClientRect().height}; resizer.setPointerCapture(e.pointerId); };
resizer.onpointermove = e => { if (drag) resize(drag.h+drag.y-e.clientY); };
resizer.onpointerup = resizer.onpointercancel = () => drag = null;
resizer.ondblclick = () => resize(245);
resizer.onkeydown = e => { if (['ArrowUp','ArrowDown'].includes(e.key)) { e.preventDefault(); resize($('#detail-body').getBoundingClientRect().height+(e.key === 'ArrowUp' ? 10 : -10)); } };
window.addEventListener('resize',() => resize($('#detail-body').getBoundingClientRect().height));
document.addEventListener('keydown',e => { if (e.key === '/' && !['INPUT','TEXTAREA'].includes(e.target.tagName) && !$('dialog[open]')) { e.preventDefault(); $('#search').focus(); } });
$('#server-address').textContent = window.location.host;
$('#server-address').title = window.location.origin;
resize(245); render(); poll();
