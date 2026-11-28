'use strict';
const $ = s => document.querySelector(s);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const icon = name => `<svg aria-hidden="true"><use href="#${name}"/></svg>`;
const booleanValue = (value, yes = 'Yes', no = 'No') => `<span class="boolean-value ${value ? 'yes' : 'no'}">${icon(value ? 'check' : 'close')}${esc(value ? yes : no)}</span>`;
const trimNumber = (n, decimals = 4) => Number.isFinite(n) ? String(Number(n.toFixed(decimals))) : '0';
function hasSelectionInside(element) {
  const sel = window.getSelection();
  if (!sel || sel.isCollapsed || !sel.toString()) return false;
  return (sel.anchorNode && element.contains(sel.anchorNode)) || (sel.focusNode && element.contains(sel.focusNode));
}
// Refresh dynamic panes without dropping scroll offsets, text selection, or keyboard focus.
function replaceContent(element, html) {
  if (element.spillHTML === html || hasSelectionInside(element)) return;
  const active = document.activeElement;
  const key = element.contains(active) ? active.dataset.focusKey : null;
  const previous = key ? [...element.querySelectorAll('[data-focus-key]')].indexOf(active) : -1;
  const top = element.scrollTop, left = element.scrollLeft;
  const innerScrolls = [...element.querySelectorAll('.telemetry-scroll')].map(el => [el.scrollTop, el.scrollLeft]);
  element.innerHTML = html;
  element.spillHTML = html;
  [...element.querySelectorAll('.telemetry-scroll')].forEach((el, i) => {
    if (innerScrolls[i]) { el.scrollTop = innerScrolls[i][0]; el.scrollLeft = innerScrolls[i][1]; }
  });
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
const filters = [
  ['all','grid','All torrents'],
  ['downloading','down','Downloading'],
  ['checking','refresh','Checking'],
  ['seeding','up','Seeding'],
  ['paused','pause','Paused'],
  ['queued','clock','Queued'],
  ['error','alert','Needs attention'],
];
let torrents = [], selected = new Set(), lastClickedId = null, focused = null;
let filter = filters.some(f => f[0] === localStorage.getItem('spill-filter')) ? localStorage.getItem('spill-filter') : 'all';
let query = '';
let sortKey = localStorage.getItem('spill-sort') || 'name';
let direction = Number(localStorage.getItem('spill-dir')) === -1 ? -1 : 1;
let tab = 'overview';
let detailHeight = Math.max(60, Number(localStorage.getItem('spill-detail-height')) || 245);
let initialized = false, online = false, restarted = false, busy = false, refreshPromise, pollTimer, health = null, lastUpdated = null;
let fileCache = null, fileError = '', fileLoading = null, fileRequest = 0, fileOffset = 0, fileUpdated = 0;
let pendingPriorities = new Map();
function resetFiles() {
  fileCache = null; fileError = ''; fileLoading = null;
  fileOffset = 0; fileUpdated = 0; fileRequest++;
  pendingPriorities.clear();
}
const matches = (t, key) => {
  if (key === 'all') return true;
  if (key === 'downloading') return ['starting', 'metadata', 'downloading'].includes(t.state);
  if (key === 'checking') return ['checking', 'recovering'].includes(t.state);
  if (key === 'error') return !!(t.last_error || t.tracker_error || t.transfer?.metadata_error || t.state === 'retrying');
  return t.state === key;
};
const matchesQuery = (t, q) => {
  if (!q) return true;
  const s = q.toLowerCase();
  return t.name.toLowerCase().includes(s) || t.id.toLowerCase().includes(s) ||
    (t.v1 && t.v1.toLowerCase().includes(s)) || (t.v2 && t.v2.toLowerCase().includes(s)) ||
    (t.output_dir && t.output_dir.toLowerCase().includes(s));
};
const visible = () => torrents.filter(t => matches(t, filter) && matchesQuery(t, query)).sort((a,b) => {
  if (sortKey === 'eta') {
    if (a.etaSeconds === b.etaSeconds) return 0;
    if (a.etaSeconds === Infinity) return 1;
    if (b.etaSeconds === Infinity) return -1;
    return (a.etaSeconds - b.etaSeconds) * direction;
  }
  const x = sortKey === 'size' ? a.total_bytes : sortKey === 'progress' ? progress(a) : a[sortKey];
  const y = sortKey === 'size' ? b.total_bytes : sortKey === 'progress' ? progress(b) : b[sortKey];
  return (typeof x === 'string' ? x.localeCompare(y) : (x ?? -1) - (y ?? -1)) * direction;
});
function pruneSelection() {
  const shown = new Set(visible().map(t => t.id));
  selected = new Set([...selected].filter(id => shown.has(id)));
}
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
function showDialogError(selector, message) {
  const el = $(selector);
  if (!el) { notify(message); return; }
  el.textContent = message;
  el.hidden = !message;
}
function connection(ok, message, authFailed = false) {
  online = ok;
  restarted = authFailed;
  $('#overview-bar').classList.toggle('stale', initialized && !ok);
  $('#daemon-button').dataset.connection = ok ? 'connected' : 'disconnected';
  $('#daemon-button').setAttribute('aria-label',`Daemon status: ${ok ? 'connected' : 'disconnected'}`);
  $('#daemon-button').title = ok ? 'Connected · View daemon status' : 'Disconnected · View last daemon status';
  const status = $('#connection-status');
  if (ok) {
    status.textContent = '';
  } else if (authFailed) {
    status.innerHTML = `Daemon restarted <button type="button" class="secondary" id="reload-page">Reload</button>`;
    $('#reload-page').onclick = () => window.location.reload();
  } else {
    status.textContent = `Disconnected · ${message}`;
  }
}
async function request(path, body, binary = false) {
  const response = await fetch(path, {
    method: 'POST',
    headers: {'X-Spill-Token': token, 'Content-Type': binary ? 'application/x-bittorrent' : 'application/json'},
    body: binary ? body : JSON.stringify(body),
    signal: AbortSignal.timeout(35000)
  });
  const value = await response.json().catch(() => ({}));
  if (response.status === 401) {
    const err = new Error('Daemon restarted; reload this page.');
    err.auth = true;
    throw err;
  }
  if (!response.ok || value.code !== 0) {
    throw new Error(value.error || `HTTP ${response.status}`);
  }
  return value.data;
}
const api = (op, fields = {}) => request('/api', {op, ...fields});
function transferRow(row) {
  const down = row.transfer?.download_bytes_per_second || 0;
  const remaining = Math.max(0, (row.wanted_bytes || 0) - (row.wanted_verified_bytes || 0));
  const etaSeconds = remaining && down > 0 ? remaining / down : Infinity;
  return {...row, name:row.name || row.id, metadata_pending:row.total_bytes === null,
    down, up:row.transfer?.upload_bytes_per_second || 0, peers:row.transfer?.active_peers || 0,
    etaSeconds, eta:Number.isFinite(etaSeconds) ? duration(1000 * etaSeconds) : '—'};
}
function refresh() {
  if (refreshPromise) return refreshPromise;
  refreshPromise = (async () => {
    try {
      const value = await api('status');
      initialized = true;
      health = value.health;
      lastUpdated = new Date();
      $('#daemon-version').textContent = `spill v${health.version}`;
      torrents = value.torrents.map(transferRow);
      const ids = new Set(torrents.map(t => t.id));
      selected = new Set([...selected].filter(id => ids.has(id)));
      if (!ids.has(focused)) { focused = null; resetFiles(); }
      connection(true);
    } catch (error) {
      initialized = true;
      if (error.auth) {
        connection(false, error.message, true);
        if (!$('dialog[open]')) { window.location.reload(); return; }
      } else {
        connection(false, error.message, false);
      }
    }
    finally { render(); }
  })().finally(() => refreshPromise = null);
  return refreshPromise;
}
async function poll() {
  clearTimeout(pollTimer);
  if (restarted) return;
  if (!busy && !document.hidden) { await refresh(); await refreshFiles(); }
  if (!restarted) pollTimer = setTimeout(poll, 1000);
}
document.addEventListener('visibilitychange', () => {
  if (!document.hidden && !restarted) void poll();
});
async function refreshFiles(force = false) {
  if (!focused || !online || tab !== 'files') return;
  const id = focused;
  if (fileLoading === id || (!force && fileCache && fileCache.id === id && Date.now()-fileUpdated < 15000)) return;
  const req = ++fileRequest;
  fileLoading = id;
  try {
    const value = await api('files', {id});
    if (focused === id && req === fileRequest) {
      fileCache = {id, files: value.files};
      fileError = ''; fileUpdated = Date.now();
      if (fileOffset >= value.files.length) fileOffset = 0;
    }
  } catch (error) {
    if (focused === id && req === fileRequest) {
      fileError = error.message; fileUpdated = Date.now();
    }
  } finally {
    if (req === fileRequest) { fileLoading = null; renderDetail(); }
  }
}
function currentFilePage() {
  if (!fileCache || fileCache.id !== focused) return null;
  const end = Math.min(fileOffset + 100, fileCache.files.length);
  return {
    id: fileCache.id, offset: fileOffset,
    files: fileCache.files.slice(fileOffset, end),
    total_files: fileCache.files.length,
    next_offset: end < fileCache.files.length ? end : null
  };
}
function policy(t) {
  return [['trackers','TRK'],['dht','DHT'],['pex','PEX'],['web_seeds','WEB']].map(([key,label]) => `<span class="source-bit ${t.settings[key] ? '' : 'off'}">${label}</span>`).join('');
}
function torrentName(id) {
  return torrents.find(t => t.id === id)?.name || id.slice(0, 12);
}
function render() {
  replaceContent($('#navigation'), filters.map(([key,ic,label]) => {
    const count = torrents.filter(t => matches(t, key)).length;
    return `<button data-filter="${key}" data-focus-key="filter-${key}" aria-label="${label} (${count})" title="${label} (${count})" class="${key === filter ? 'active' : ''}" ${key === filter ? 'aria-current="page"' : ''}>${icon(ic)}<span class="label">${label}</span><span class="count">${count}</span></button>`;
  }).join(''));
  const rows = visible();
  $('#list-title').textContent = `${filters.find(f => f[0] === filter)[2]} · ${rows.length}`;
  replaceContent($('#torrents'), rows.map(t => {
    const p = progress(t);
    const tip = [t.last_error, t.transfer?.metadata_error && 'Metadata warning: '+t.transfer.metadata_error, t.tracker_error && 'Tracker warning: '+t.tracker_error, t.tracker_message].filter(Boolean).join('\n');
    const warn = (t.tracker_error || t.transfer?.metadata_error) && !t.last_error;
    return `<tr data-id="${esc(t.id)}" data-focus-key="row-${esc(t.id)}" class="${selected.has(t.id) ? 'selected ' : ''}${esc(t.state)}" tabindex="0" aria-label="${esc(t.name)}, ${esc(t.state)}">
      <td><input type="checkbox" data-check data-focus-key="select-${esc(t.id)}" aria-label="Select ${esc(t.name)}" ${selected.has(t.id) ? 'checked' : ''}></td>
      <td><div class="name-text"><strong title="${esc(t.name)}">${esc(t.name)}</strong></div></td>
      <td class="mono">${t.metadata_pending ? '—' : bytes(t.total_bytes)}</td>
      <td><div class="progress-wrap"><div class="progress-meta"><span>${p == null ? '—' : p.toFixed(1)+'%'}</span></div><div class="progress-track"><i style="width:${p || 0}%"></i></div></div></td>
      <td class="status-cell" title="${esc(tip)}"><span class="status ${t.last_error ? 'has-error' : esc(t.state)}">${t.last_error ? icon('alert')+'Error' : esc(t.state)}</span>${warn ? ` <span class="tracker-warning" role="img" aria-label="Warning">${icon('alert')}</span>` : ''}</td>
      <td class="mono total" title="${esc(t.downloaded_bytes)} bytes, lifetime">${bytes(t.downloaded_bytes)}</td><td class="mono total" title="${esc(t.uploaded_bytes)} bytes, lifetime">${bytes(t.uploaded_bytes)}</td>
      <td class="mono speed-down">${rate(t.down)}</td><td class="mono speed-up">${rate(t.up)}</td><td class="mono">${t.peers}</td><td class="mono">${esc(t.eta)}</td>
      <td><span class="discovery-bits" title="Allowed by policy; not activity telemetry">${policy(t)}</span></td></tr>`;
  }).join(''));
  const empty = $('#empty');
  empty.hidden = rows.length > 0;
  if (!initialized) {
    empty.textContent = 'Loading torrents…';
  } else if (restarted) {
    empty.innerHTML = 'Daemon restarted. <button type="button" class="secondary" id="empty-reload">Reload page</button>';
    $('#empty-reload').onclick = () => window.location.reload();
  } else if (!online) {
    empty.textContent = 'Disconnected. Retrying the daemon connection…';
  } else if (!torrents.length) {
    empty.textContent = 'No torrents yet. Add a torrent to get started.';
  } else {
    empty.textContent = 'No torrents match. Try another filter or search.';
  }
  $('#visible-count').textContent = `${rows.length} ${rows.length === 1 ? 'torrent' : 'torrents'} · ${selected.size} selected`;
  $('#select-all').checked = rows.length > 0 && rows.every(t => selected.has(t.id));
  $('#select-all').indeterminate = rows.some(t => selected.has(t.id)) && !$('#select-all').checked;
  ['pause','resume','recheck','reannounce','remove'].forEach(a => $('#'+a+'-button').disabled = !online || busy || !selected.size);
  $('#add-button').disabled = !online || busy;
  $('#limits-button').disabled = !online || busy || !focused;
  $('#seeding-button').disabled = !online || busy || !focused;
  $('#seeding-form button[type="submit"]').disabled = !online || busy;
  $('#limits-form button[type="submit"]').disabled = !online || busy;
  $('#open-session-limits').disabled = !online || busy || !health?.bandwidth;
  $('#confirm-remove').disabled = busy || !online;
  $('#add-form button[type="submit"]').disabled = busy || !online;
  $('#download-stat').textContent = initialized ? rate(torrents.reduce((s,t) => s+t.down,0)) : '—';
  $('#upload-stat').textContent = initialized ? rate(torrents.reduce((s,t) => s+t.up,0)) : '—';
  $('#peers-stat').textContent = initialized ? torrents.reduce((s,t) => s+t.peers,0) : '—';
  document.querySelectorAll('[data-sort]').forEach(button => {
    const active = button.dataset.sort === sortKey;
    button.closest('th').setAttribute('aria-sort', active ? (direction === 1 ? 'ascending' : 'descending') : 'none');
    button.querySelector('span').textContent = active ? (direction === 1 ? '↑' : '↓') : '';
  });
  renderDetail();
  if ($('#daemon-dialog').open) renderHealth();
}
const detailRow = (label,value) => `<div class="detail-row"><span>${esc(label)}</span><span>${typeof value === 'boolean' ? booleanValue(value) : esc(value)}</span></div>`;
function renderHealth() {
  let html = detailRow('Connected',online);
  if (health) {
    const network = health.network, mapping = health.port_mapping, logging = health.logging;
    const fmtAgo = secs => secs == null ? 'None since startup' : duration(secs * 1000) + ' ago';
    html += (!online ? telemetryNote('Disconnected. Showing the last daemon snapshot.') : '') + [
      ['Version',health.version], ['PID',health.pid],
      ['Running torrents',health.running], ['Accepting work',!health.admission_closed],
      ['TCP listener',network.tcp_listen || 'Not running'], ['UDP listener',network.udp_listen || 'Not running'],
      ['uTP enabled',network.utp_enabled], ['DHT enabled',network.dht_enabled],
      ['Inbound TCP peers',`${network.inbound_tcp.active} (${network.inbound_tcp.active_public} public)`],
      ['Last public TCP',fmtAgo(network.inbound_tcp.last_public_ago_secs)],
      ['Inbound uTP peers',`${network.inbound_utp.active} (${network.inbound_utp.active_public} public)`],
      ['Last public uTP',fmtAgo(network.inbound_utp.last_public_ago_secs)],
      ['Advertised port',health.advertised_port],
      ['Local discovery',!network.lsd.enabled ? 'Disabled' : network.lsd.active ? 'Listening' : 'Unavailable'],
      ['Port mapping',mapping.protocol ? (mapping.protocol === 'nat-pmp' ? 'Active (NAT-PMP)' : 'Active (UPnP)') : mapping.enabled ? 'Not established' : 'Disabled'],
    ].map(([label,value]) => detailRow(label,value)).join('');
    if (network.lsd.last_error) html += detailRow('LSD error', network.lsd.last_error);
    if (mapping.last_error) html += detailRow('Port mapping error', mapping.last_error);
    if (health.bandwidth) {
      html += detailRow('Session download cap',health.bandwidth.download_limit === 0 ? 'Unlimited' : rate(health.bandwidth.download_limit));
      html += detailRow('Session upload cap',health.bandwidth.upload_limit === 0 ? 'Unlimited' : rate(health.bandwidth.upload_limit));
    }
    if (logging) {
      const logText = !logging.file_enabled ? 'Disabled' : logging.write_failed ? `Write failed (${logging.events_dropped} dropped)` : logging.events_dropped ? `Active (${logging.events_dropped} dropped)` : 'Active';
      html += detailRow('Debug log', logText);
    }
    html += `<div class="detail-row"><span>Last updated</span><span id="health-updated">${esc(lastUpdated.toLocaleTimeString())}</span></div>`;
  } else html += telemetryNote('No daemon status received yet.');
  const updatedSpan = $('#health-updated');
  if (updatedSpan && $('#daemon-details').spillHTML && health) {
    updatedSpan.textContent = lastUpdated.toLocaleTimeString();
  }
  replaceContent($('#daemon-details'), html);
}
$('#daemon-button').onclick = () => { renderHealth(); $('#daemon-dialog').showModal(); };
$('#close-daemon').onclick = () => $('#daemon-dialog').close();
function renderDetail() {
  const t = torrents.find(t => t.id === focused);
  if (!t) { $('#inspector').hidden = true; return; }
  if (tab === 'overview') replaceContent($('#detail-body'), renderOverview(t));
  else if (tab === 'files') {
    if (t.metadata_pending) { replaceContent($('#detail-body'), telemetryNote('Awaiting metadata.')); return; }
    const page = currentFilePage();
    if (!page) {
      replaceContent($('#detail-body'), telemetryNote(fileError || (online ? 'Loading files…' : 'Disconnected. File details are unavailable.')));
      if (online && !fileError) void refreshFiles();
    } else {
      replaceContent($('#detail-body'), (fileError ? telemetryNote('File refresh failed: '+fileError) : '') +
        (!online ? telemetryNote('Disconnected. Showing last file snapshot.') : '') + renderFileManifest(page, pendingPriorities));
    }
  }
  else {
    if (!online) { replaceContent($('#detail-body'), telemetryNote('Disconnected. Live telemetry is unavailable.')); return; }
    replaceContent($('#detail-body'), tab === 'peers' ? renderPeerTelemetry(t) : renderSourceTelemetry(t));
  }
}
async function mutate(work, errorSelector = null) {
  if (busy || !online) return;
  busy = true; render();
  try { if (refreshPromise) await refreshPromise; await work(); }
  catch (error) {
    if (error.auth) { connection(false, error.message, true); }
    else if (errorSelector) showDialogError(errorSelector, error.message);
    else notify(error.message);
  }
  finally { busy = false; if (!restarted) { await refresh(); await refreshFiles(); } render(); }
}
async function action(op) {
  const ids = [...selected];
  await mutate(async () => {
    let count = 0;
    const errors = [];
    for (const id of ids) {
      try { await api(op,{id}); count++; }
      catch (e) { errors.push(`${torrentName(id)}: ${e.message}`); }
    }
    notify(`${count}/${ids.length} ${op} requests accepted.` + (errors.length ? ' '+errors.join('; ') : ''));
  });
}
$('#navigation').onclick = e => {
  const b = e.target.closest('[data-filter]');
  if (b) {
    filter = b.dataset.filter;
    localStorage.setItem('spill-filter', filter);
    pruneSelection();
    render();
  }
};
$('#search').oninput = e => { query = e.target.value; pruneSelection(); render(); };
$('thead').onclick = e => {
  const b = e.target.closest('[data-sort]');
  if (b) {
    direction = sortKey === b.dataset.sort ? -direction : 1;
    sortKey = b.dataset.sort;
    localStorage.setItem('spill-sort', sortKey);
    localStorage.setItem('spill-dir', String(direction));
    render();
  }
};
$('#select-all').onchange = e => { visible().forEach(t => e.target.checked ? selected.add(t.id) : selected.delete(t.id)); render(); };
$('#torrents').onclick = e => {
  const row = e.target.closest('tr'); if (!row) return;
  const id = row.dataset.id;
  if (e.target.matches('[data-check]')) {
    if (e.target.checked) {
      selected.add(id);
      if (focused !== id) resetFiles();
      focused = id; $('#inspector').hidden = false;
    } else {
      selected.delete(id);
    }
    lastClickedId = id;
    render();
    return;
  }
  const rows = visible().map(t => t.id);
  if (e.shiftKey && lastClickedId && rows.includes(lastClickedId)) {
    const a = rows.indexOf(lastClickedId), b = rows.indexOf(id);
    const [lo, hi] = a < b ? [a, b] : [b, a];
    for (let i = lo; i <= hi; i++) selected.add(rows[i]);
  } else if (e.ctrlKey || e.metaKey) {
    selected.has(id) ? selected.delete(id) : selected.add(id);
  } else {
    selected = new Set([id]);
  }
  lastClickedId = id;
  if (focused !== id) resetFiles();
  focused = id; $('#inspector').hidden = false; render();
};
$('#torrents').onkeydown = e => {
  if (!e.target.matches('tr')) return;
  if (['Enter',' '].includes(e.key)) { e.preventDefault(); e.target.click(); return; }
  if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
    e.preventDefault();
    const sibling = e.key === 'ArrowDown' ? e.target.nextElementSibling : e.target.previousElementSibling;
    if (sibling) { sibling.focus(); sibling.click(); }
  }
};
$('.tabs').onclick = e => {
  const b = e.target.closest('[data-tab]'); if (!b) return;
  tab = b.dataset.tab;
  fileError = '';
  if (tab === 'files') void refreshFiles(true);
  document.querySelectorAll('.tab').forEach(el => {
    el.classList.toggle('active',el === b);
    el.setAttribute('aria-selected',String(el === b));
    el.tabIndex = el === b ? 0 : -1;
  });
  $('#detail-body').setAttribute('aria-labelledby',b.id);
  renderDetail();
};
$('.tabs').onkeydown = e => {
  if (!['ArrowLeft','ArrowRight','Home','End'].includes(e.key)) return;
  e.preventDefault();
  const tabs = [...document.querySelectorAll('.tab')], index = tabs.indexOf(e.target);
  const next = e.key === 'Home' ? 0 : e.key === 'End' ? tabs.length-1 : (index+(e.key === 'ArrowRight' ? 1 : -1)+tabs.length)%tabs.length;
  tabs[next].focus(); tabs[next].click();
};
$('#detail-body').onclick = e => {
  const copy = e.target.closest('[data-copy]');
  if (copy) {
    navigator.clipboard.writeText(copy.dataset.copy).then(() => {
      copy.textContent = 'Copied';
      setTimeout(() => { copy.textContent = 'Copy'; }, 1500);
    }).catch(() => notify('Could not copy to clipboard.'));
    return;
  }
  const bulk = e.target.closest('[data-file-bulk]');
  if (bulk && fileCache) {
    const target = bulk.dataset.fileBulk;
    for (const f of fileCache.files) {
      if (f.padding) continue;
      if (f.priority === target) pendingPriorities.delete(f.index);
      else pendingPriorities.set(f.index, target);
    }
    renderDetail();
    return;
  }
  if (e.target.closest('[data-file-discard]')) {
    pendingPriorities.clear();
    renderDetail();
    return;
  }
  if (e.target.closest('[data-file-save]') && focused && fileCache && pendingPriorities.size) {
    const id = focused;
    const patch = Object.fromEntries([...pendingPriorities.entries()].map(([k, v]) => [String(k), v]));
    void mutate(async () => {
      await api('set', {id, values: {file_priorities: patch}});
      pendingPriorities.clear();
      fileUpdated = 0;
      await refreshFiles(true);
      notify('File priorities saved. Active torrents restart after joined shutdown.');
    });
    return;
  }
  const button = e.target.closest('[data-file-page]');
  if (!button || !online || fileLoading) return;
  const page = currentFilePage();
  if (button.dataset.filePage === 'next' && page?.next_offset != null) {
    fileOffset = page.next_offset;
    renderDetail();
  } else if (button.dataset.filePage === 'previous') {
    fileOffset = Math.max(0, fileOffset - 100);
    renderDetail();
  } else if (button.dataset.filePage === 'refresh') {
    fileError = ''; fileUpdated = 0;
    void refreshFiles(true);
    renderDetail();
  }
};
$('#detail-body').onchange = e => {
  const select = e.target.closest('[data-file-priority]');
  if (select && fileCache) {
    const index = Number(select.dataset.filePriority);
    const saved = fileCache.files.find(f => f.index === index)?.priority;
    if (select.value === saved) pendingPriorities.delete(index);
    else pendingPriorities.set(index, select.value);
    renderDetail();
    return;
  }
  const source = e.target.closest('[data-source-setting]');
  if (source && focused) {
    const id = focused, key = source.dataset.sourceSetting, value = source.checked;
    source.checked = !value;
    void mutate(async () => {
      await api('set', {id, values: {[key]: value}});
      notify(`Updated ${key.replaceAll('_', ' ')} setting.`);
    });
  }
};
$('#close-detail').onclick = () => { focused = null; resetFiles(); $('#inspector').hidden = true; };
['pause','resume','recheck','reannounce'].forEach(op => $('#'+op+'-button').onclick = () => action(op));
function openAddDialog(droppedFile = null) {
  $('#add-form').reset();
  showDialogError('#add-error', '');
  for (const [id,key] of [['allow-trackers','trackers'],['allow-dht','dht'],['allow-pex','pex'],['allow-web','web_seeds']]) {
    $('#'+id).checked = addDefaults.settings[key];
  }
  $('#output').placeholder = addDefaults.download_dir ? `${addDefaults.download_dir} (daemon default)` : 'Daemon default (or absolute path)';
  $('#upload-label').textContent = 'Drop a .torrent file here or choose a file';
  if (droppedFile) {
    const dt = new DataTransfer();
    dt.items.add(droppedFile);
    $('#torrent-file').files = dt.files;
    $('#upload-label').textContent = droppedFile.name;
  }
  if (!$('#add-dialog').open) $('#add-dialog').showModal();
}
$('#add-button').onclick = () => openAddDialog();
$('#cancel-add').onclick = () => $('#add-dialog').close();
$('#add-dialog').onclose = () => { $('#add-form').reset(); showDialogError('#add-error', ''); $('#upload-label').textContent = 'Drop a .torrent file here or choose a file'; };
$('#magnet').oninput = () => {
  if ($('#magnet').value.trim()) { $('#torrent-file').value = ''; $('#upload-label').textContent = 'Drop a .torrent file here or choose a file'; }
  showDialogError('#add-error', '');
};
$('#torrent-file').onchange = () => {
  const file = $('#torrent-file').files[0];
  if (file) { $('#magnet').value = ''; $('#upload-label').textContent = file.name; }
  else $('#upload-label').textContent = 'Drop a .torrent file here or choose a file';
  showDialogError('#add-error', '');
};
const uploadArea = $('#upload-area');
uploadArea.ondragover = e => { e.preventDefault(); uploadArea.classList.add('dragover'); };
uploadArea.ondragleave = () => uploadArea.classList.remove('dragover');
uploadArea.ondrop = e => {
  e.preventDefault(); uploadArea.classList.remove('dragover');
  const file = e.dataTransfer?.files?.[0];
  if (file) {
    const dt = new DataTransfer(); dt.items.add(file);
    $('#torrent-file').files = dt.files;
    $('#magnet').value = '';
    $('#upload-label').textContent = file.name;
    showDialogError('#add-error', '');
  }
};
document.addEventListener('dragover', e => { if (e.dataTransfer?.types?.includes('Files')) e.preventDefault(); });
document.addEventListener('drop', e => {
  if (uploadArea.contains(e.target)) return;
  const file = e.dataTransfer?.files?.[0];
  if (!file || !file.name.toLowerCase().endsWith('.torrent')) return;
  e.preventDefault();
  if (online && !busy) openAddDialog(file);
});
$('#add-form').onsubmit = e => {
  e.preventDefault();
  showDialogError('#add-error', '');
  const file = $('#torrent-file').files[0], source = $('#magnet').value.trim();
  if (!!file === !!source) { showDialogError('#add-error', 'Choose one .torrent file, magnet link, or HTTP(S) torrent URL.'); return; }
  const magnet = /^magnet:/i.test(source);
  if (!file && !magnet && !/^https?:\/\//i.test(source)) { showDialogError('#add-error', 'Enter a magnet link or HTTP(S) torrent URL.'); return; }
  const output_dir = $('#output').value.trim() || null;
  if (output_dir && !output_dir.startsWith('/')) { showDialogError('#add-error', 'Download location must be an absolute daemon-side path.'); return; }
  const paused = !$('#start-now').checked;
  const settings = {trackers:$('#allow-trackers').checked, dht:$('#allow-dht').checked,
    pex:$('#allow-pex').checked, web_seeds:$('#allow-web').checked};
  mutate(async () => {
    let result;
    const submit = $('#add-form button[type="submit"]');
    submit.disabled = true;
    submit.textContent = !file && !magnet ? 'Downloading torrent…' : 'Adding…';
    try {
      if (file) {
        if (!file.size || file.size > 16*1024*1024) throw new Error('Torrent file must be nonempty and at most 16 MiB.');
        const options = new URLSearchParams();
        if (output_dir) options.set('output_dir',output_dir);
        options.set('paused',String(paused));
        options.set('settings',JSON.stringify(settings));
        result = await request('/api/upload?'+options,file,true);
      } else {
        result = await api('add',{[magnet ? 'magnet' : 'url']:source,output_dir,paused,settings});
      }
    } finally {
      submit.disabled = false;
      submit.textContent = 'Add torrent';
    }
    selected = new Set([result.id]); focused = result.id; resetFiles(); filter = 'all'; localStorage.setItem('spill-filter', 'all'); query = ''; $('#search').value = '';
    $('#inspector').hidden = false; $('#add-dialog').close();
    notify('Torrent added.');
  }, '#add-error');
};
const rangeControls = [['seed-ratio',10],['seed-time',168]];
function syncRange(id, max) {
  const raw = $('#'+id).value;
  if (raw === '') return;
  const num = Number(raw);
  if (!Number.isFinite(num) || num < 0) return;
  const range = $('#'+id+'-range');
  range.max = Math.max(max, num);
  range.value = raw;
}
rangeControls.forEach(([id,max]) => {
  $('#'+id).oninput = () => syncRange(id,max);
  $('#'+id+'-range').oninput = e => $('#'+id).value = e.target.value;
});
let seedingId = null;
$('#seeding-button').onclick = () => {
  const t = torrents.find(t => t.id === focused);
  if (!t) return;
  seedingId = t.id;
  showDialogError('#seeding-error', '');
  $('#seed-ratio').value = trimNumber(t.settings.seed_ratio, 4);
  $('#seed-time').value = trimNumber(t.settings.seed_time/3600, 4);
  rangeControls.forEach(([id,max]) => syncRange(id,max));
  $('#seeding-dialog').showModal();
};
$('#cancel-seeding').onclick = () => $('#seeding-dialog').close();
$('#seeding-form').onsubmit = e => {
  e.preventDefault();
  showDialogError('#seeding-error', '');
  const id = seedingId, seed_ratio = Number($('#seed-ratio').value), hours = Number($('#seed-time').value);
  const seed_time = Math.round(hours*3600);
  if (!Number.isFinite(seed_ratio) || seed_ratio < 0 || !Number.isFinite(hours) || hours < 0 || !Number.isSafeInteger(seed_time)) {
    showDialogError('#seeding-error', 'Use a finite nonnegative ratio and hours that convert to a safe whole-second value.'); return;
  }
  mutate(async () => {
    await api('set', {id, values:{seed_ratio, seed_time}});
    $('#seeding-dialog').close(); notify('Seeding rules saved. Paused torrents stay paused until resumed.');
  }, '#seeding-error');
};
const limitFields = [['download_limit','#limit-down'],['upload_limit','#limit-up']];
const bytesPerMiB = 1024**2;
let limitsTarget = null;
function openLimits(torrent) {
  limitsTarget = torrent?.id ?? null;
  const current = torrent ? torrent.settings : health.bandwidth;
  $('#limits-title').textContent = torrent ? 'Torrent limits' : 'Session limits';
  $('#limits-scope').textContent = torrent ? torrent.name : 'Shared by all torrents · Resets on daemon restart';
  showDialogError('#limits-error', '');
  for (const [key,selector] of limitFields) {
    const input = $(selector);
    input.defaultValue = input.value = trimNumber(current[key]/bytesPerMiB, 4);
  }
  $('#limits-dialog').showModal();
}
$('#open-session-limits').onclick = () => openLimits();
$('#limits-button').onclick = () => {
  const torrent = torrents.find(t => t.id === focused);
  if (torrent) openLimits(torrent);
};
$('#cancel-limits').onclick = () => $('#limits-dialog').close();
$('#limits-form').onsubmit = e => {
  e.preventDefault();
  const id = limitsTarget;
  showDialogError('#limits-error', '');
  const values = {};
  for (const [key,selector] of limitFields) {
    const input = $(selector);
    if (input.value === input.defaultValue) continue;
    const value = Math.round(input.valueAsNumber*bytesPerMiB);
    if (!Number.isSafeInteger(value) || value < 0 || (value === 0 && input.valueAsNumber !== 0)) {
      showDialogError('#limits-error', 'Enter 0 for unlimited, or a rate that rounds to at least 1 byte/s.'); return;
    }
    values[key] = value;
  }
  if (!Object.keys(values).length) { $('#limits-dialog').close(); return; }
  mutate(async () => {
    await (id ? api('set', {id,values}) : api('set_bandwidth', values));
    $('#limits-dialog').close();
    notify(id ? 'Torrent limits updated.' : 'Session limits updated until restart.');
  }, '#limits-error');
};
let removeIds = [];
function syncDeleteState() {
  const del = $('#delete-data').checked;
  $('#confirm-remove').textContent = del ? 'Remove and delete files' : 'Remove';
  $('#delete-data-label').classList.toggle('danger-label', del);
}
$('#delete-data').onchange = syncDeleteState;
$('#remove-button').onclick = () => {
  removeIds = [...selected];
  showDialogError('#remove-error', '');
  const shown = new Set(visible().map(t => t.id));
  const hidden = removeIds.filter(id => !shown.has(id)).length;
  $('#remove-description').textContent = `Remove ${removeIds.length} selected ${removeIds.length === 1 ? 'torrent' : 'torrents'}?${hidden ? ` ${hidden} are hidden by the current filter or search.` : ''} Downloaded files are kept by default.`;
  $('#remove-transfers').innerHTML = removeIds.map(id => {
    const t = torrents.find(t => t.id === id);
    return `<li><strong>${esc(t?.name || id)}</strong><small>${esc(id)}${shown.has(id) ? '' : ' · Hidden from current view'}</small></li>`;
  }).join('');
  $('#delete-data').checked = false;
  syncDeleteState();
  $('#remove-dialog').showModal();
};
$('#cancel-remove').onclick = () => $('#remove-dialog').close();
$('#confirm-remove').onclick = () => {
  const ids = [...removeIds], delete_payload = $('#delete-data').checked;
  mutate(async () => {
    let count = 0; const errors = [];
    for (const id of ids) {
      try { await api('remove',{id,delete_payload}); count++; }
      catch (e) { errors.push(`${torrentName(id)}: ${e.message}`); }
    }
    if (errors.length && count === 0) { showDialogError('#remove-error', errors.join('; ')); return; }
    $('#remove-dialog').close();
    notify(`${count}/${ids.length} removed; files ${delete_payload ? 'deleted (adopted files kept)' : 'kept'}.${errors.length ? ' '+errors.join('; ') : ''}`);
  }, '#remove-error');
};
const resizer = $('#detail-resizer');
function resize(height) {
  const max = Math.max(60, innerHeight-290), h = Math.max(60,Math.min(max,height));
  detailHeight = h;
  localStorage.setItem('spill-detail-height', String(Math.round(h)));
  $('#inspector').style.setProperty('--detail-height',h+'px');
  resizer.setAttribute('aria-valuenow',h); resizer.setAttribute('aria-valuemax',max);
}
let drag;
resizer.onpointerdown = e => { drag = {y:e.clientY,h:$('#detail-body').getBoundingClientRect().height}; resizer.setPointerCapture(e.pointerId); };
resizer.onpointermove = e => { if (drag) resize(drag.h+drag.y-e.clientY); };
resizer.onpointerup = resizer.onpointercancel = () => drag = null;
resizer.ondblclick = () => resize(245);
resizer.onkeydown = e => { if (['ArrowUp','ArrowDown'].includes(e.key)) { e.preventDefault(); resize(detailHeight+(e.key === 'ArrowUp' ? 10 : -10)); } };
window.addEventListener('resize', () => { if (!$('#inspector').hidden) resize(detailHeight); });
document.addEventListener('keydown', e => {
  if ($('dialog[open]')) return;
  if (e.key === '/' && !['INPUT','TEXTAREA'].includes(e.target.tagName)) { e.preventDefault(); $('#search').focus(); }
  else if (e.key === 'Escape') {
    if (document.activeElement === $('#search')) { query = ''; $('#search').value = ''; $('#search').blur(); pruneSelection(); render(); }
    else if (!$('#inspector').hidden) { focused = null; resetFiles(); $('#inspector').hidden = true; }
  }
});
$('#server-address').textContent = window.location.host;
$('#server-address').title = window.location.origin;
resize(detailHeight); render(); poll();
