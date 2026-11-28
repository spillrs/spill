'use strict';
const duration = ms => {
  if (ms == null) return '—';
  if (ms === 0) return '0s';
  if (ms < 1000) return '<1s';
  if (ms < 60000) return Math.round(ms/1000)+'s';
  if (ms < 3600000) return Math.round(ms/60000)+'m';
  if (ms < 86400000) return (ms/3600000).toFixed(1)+'h';
  const days = Math.floor(ms/86400000), hours = Math.round((ms%86400000)/3600000);
  return hours ? `${days}d ${hours}h` : `${days}d`;
};
const telemetryNote = text => `<p class="telemetry-note">${esc(text)}</p>`;
const date = seconds => seconds ? new Date(seconds*1000).toISOString().replace('T',' ').replace('.000Z',' UTC') : '—';
const neutralBool = (value, yes = 'Yes', no = 'No') => `<span class="neutral-bool">${esc(value ? yes : no)}</span>`;
const copyable = value => value ? `<span class="copy-wrap"><span>${esc(value)}</span><button type="button" class="copy-btn" data-copy="${esc(value)}" title="Copy" aria-label="Copy">Copy</button></span>` : '—';
function telemetryTable(headers, rows) {
  return `<div class="telemetry-scroll"><table class="telemetry-table"><thead><tr>${headers.map(h => `<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(row => `<tr>${row.map(cell => `<td>${cell}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
}
const byteValue = value => `<span title="${esc(value)} bytes">${bytes(value)}</span>`;
const swarmLabel = (t, key) => {
  if (t.v1 && key === t.v1) return `${key} (v1)`;
  if (t.v2 && t.v2.startsWith(key)) return `${key} (v2)`;
  return key;
};
function renderOverview(t) {
  const rawRow = (label, html) => `<div class="detail-row"><span>${esc(label)}</span><span>${html}</span></div>`;
  const group = (title, rows) => `<div><h3 class="detail-name">${esc(title)}</h3>${rows.map(([label, value, raw]) => raw ? rawRow(label, value) : detailRow(label, value)).join('')}</div>`;
  const announcements = t.completed_announces.map(s => [swarmLabel(t, s.swarm), s.delivered]);
  const cap = n => n === 0 ? 'Unlimited' : rate(n);
  const ratioGoal = t.settings.seed_ratio > 0 ? `${t.settings.seed_ratio}×` : 'Off';
  const timeGoal = t.settings.seed_time > 0 ? duration(t.settings.seed_time * 1000) : 'Off';
  return (t.last_error ? `<div class="torrent-error">${esc(t.last_error)}</div>` : '') +
    (t.transfer?.metadata_error ? `<div class="torrent-warning">Metadata warning: ${esc(t.transfer.metadata_error)}</div>` : '') +
    (t.tracker_error ? `<div class="torrent-warning">Tracker warning: ${esc(t.tracker_error)}</div>` : '') +
    `<div class="detail-grid">` +
    group(t.name,[
      ['ID', copyable(t.id), true],
      ['Format', t.format],
      ['SHA-1', copyable(t.v1), true],
      ['SHA-256', copyable(t.v2), true],
      ['Metadata', t.metadata_ready ? 'Ready' : t.info_validated ? `Hash layers: ${bytes(t.hash_bytes)} / ${bytes(t.hash_total_bytes)}` : 'Pending'],
      ['V2 layers', !t.v2 ? '—' : t.layers_pending ? 'Pending (v1 mode)' : t.metadata_ready ? 'Ready' : 'Acquiring'],
      ['Save to', copyable(t.output_dir), true],
      ['Status', t.state],
      ['Intent', t.intent],
      ['Private', t.private === null ? 'Unknown' : neutralBool(t.private), t.private !== null],
      ['Tracker message', t.tracker_message || '—'],
      ['Retry at', date(t.retry_at)],
    ]) +
    group('Lifetime accounting',[
      ['Downloaded', bytes(t.downloaded_bytes)],
      ['Uploaded', bytes(t.uploaded_bytes)],
      ['Upload / size ratio', t.total_bytes ? (t.uploaded_bytes/t.total_bytes).toFixed(2) : '—'],
      ['Hash failed bytes', bytes(t.hash_failed_bytes)],
      ['Hash failed pieces', t.hash_failed_pieces],
      ['Seed time', duration(t.seed_seconds*1000)],
      ['Added', date(t.added_at)],
      ['Completed at', date(t.completed_at)],
      ['Last download', date(t.last_download_at)],
      ['Last upload', date(t.last_upload_at)],
    ]) +
    group('Progress & policy',[
      ['Verified / full size', `${bytes(t.verified_bytes)} / ${bytes(t.total_bytes)}`],
      ['Selected verified / size', `${bytes(t.wanted_verified_bytes)} / ${bytes(t.wanted_bytes)}`],
      ['Selected complete', t.completed],
      ['ETA', t.eta],
      ['Peers', t.peers],
      ['Download / upload cap', `${cap(t.settings.download_limit)} / ${cap(t.settings.upload_limit)}`],
      ['Seeding goals', `Ratio ${ratioGoal} · Time ${timeGoal}`],
    ]) +
    group('Completion acknowledged', announcements.length ? announcements : [['Status','No event']]) + '</div>';
}
function renderPeerTelemetry(t) {
  const peers = t.transfer?.peers || [];
  if (!peers.length) return telemetryNote('No active peers.');
  return telemetryTable(['Peer','Client','Dir','Transport','Enc','Source','↓ /s','↑ /s','Downloaded','Uploaded','Requests','Choked','Choking','Interested','Remote int.','State'], peers.map(p => {
    const d = p.detail;
    const state = d.retiring ? 'Closing' : d.established ? 'Connected' : 'Handshaking';
    const diagnostics = Object.entries({direction:p.direction,encryption:p.encryption,...d,utp_retransmits:p.utp_retransmits}).filter(([key]) => key !== 'connection').map(([key,value]) => `${key}: ${value ?? '—'}`).join('\n');
    const addr = p.endpoint || p.web_seed_url || '—';
    return [`<span title="${esc(addr)}">${esc(addr)}</span>`, esc(p.client || '—'), esc(p.direction || '—'), esc(p.transport), esc(p.encryption || '—'), esc(p.source),
      rate(p.download_bytes_per_second), rate(p.upload_bytes_per_second), byteValue(d.downloaded_bytes), d.uploaded_bytes === null ? '—' : byteValue(d.uploaded_bytes),
      d.assigned_requests, neutralBool(d.remote_choking), neutralBool(d.local_choking), neutralBool(d.local_interested), neutralBool(d.remote_interested),
      `<span title="${esc(diagnostics)}">${state}</span>`];
  }));
}
function renderFileManifest(page, pending) {
  const dirty = pending.size;
  const controls = `<div class="file-actions"><button class="secondary" data-file-bulk="normal" ${busy || !online ? 'disabled' : ''}>All normal</button><button class="secondary" data-file-bulk="skip" ${busy || !online ? 'disabled' : ''}>Skip all</button>` +
    (dirty ? `<button class="primary" data-file-save ${busy || !online ? 'disabled' : ''}>Save changes (${dirty})</button><button class="secondary" data-file-discard ${busy ? 'disabled' : ''}>Discard</button>` : '') +
    `</div>`;
  return controls + telemetryTable(['File','Size','Priority'], page.files.map(f => {
    const current = pending.has(f.index) ? pending.get(f.index) : f.priority;
    const label = f.padding ? `${f.path} [padding]` : f.path;
    return [
      `<span title="${esc(f.path)}">${esc(label)}</span>`,
      byteValue(f.length),
      f.padding
        ? `<span class="neutral-bool">skip (padding)</span>`
        : `<select ${busy || !online ? 'disabled' : ''} data-file-priority="${f.index}" data-focus-key="priority-${f.index}" aria-label="Priority for ${esc(f.path)}">${['skip','normal','high'].map(p => `<option value="${p}" ${p === current ? 'selected' : ''}>${p}</option>`).join('')}</select>`
    ];
  })) +
    `<div class="file-pagination"><button class="secondary" data-file-page="previous" data-focus-key="files-previous" ${page.offset > 0 ? '' : 'disabled'}>Previous</button><span>${page.total_files ? page.offset+1 : 0}–${page.offset+page.files.length} of ${page.total_files}</span><button class="secondary" data-file-page="next" data-focus-key="files-next" ${page.next_offset === null ? 'disabled' : ''}>Next</button><button class="secondary" data-file-page="refresh" data-focus-key="files-refresh">Refresh</button></div>`;
}
function renderSourceTelemetry(t) {
  const trackers = t.transfer?.trackers || [];
  let html = `<div class="source-summary">` + [['trackers','Trackers'],['dht','DHT'],['pex','PEX'],['web_seeds','Web seeds']].map(([key,label]) => {
    const locked = t.private === true && (key === 'dht' || key === 'pex');
    const title = locked ? 'Disabled for private torrents' : `Toggle ${label} for this torrent`;
    return `<label class="source-toggle" title="${esc(title)}"><input type="checkbox" data-source-setting="${key}" data-focus-key="source-${key}" ${t.settings[key] ? 'checked' : ''} ${locked || busy || !online ? 'disabled' : ''}> ${esc(label)}</label>`;
  }).join('') + `</div>`;
  html += trackers.length ? telemetryTable(['Tracker','Swarm key','Tier / slot','Status','Seeds','Leechers','Peers','Last reply','Next','Exchanges','Failures','Message'], trackers.map(s => [
    `<span title="${esc(s.url)}">${esc(s.url)}</span>`, esc(swarmLabel(t, s.swarm)), `${s.tier+1} / ${s.slot+1}`, esc(s.state), s.seeders ?? '—', s.leechers ?? '—', s.peers_returned ?? '—', duration(s.last_reply_ago_ms), duration(s.next_announce_ms), s.exchanges, s.failures, esc([s.error,s.warning].filter(Boolean).join(' · '))])) : telemetryNote('No active tracker observations.');
  const dht = t.transfer?.dht || [];
  html += '<div class="source-section"><h4>DHT</h4>' + (dht.length ? telemetryTable(['Swarm key','Pending','Next lookup','Last lookup','Queried','Announced','Peers','Error'], dht.map(s => [esc(swarmLabel(t, s.swarm)), neutralBool(s.lookup_pending), duration(s.next_lookup_ms), duration(s.last_lookup_ago_ms), s.queried, s.announced, s.peers_returned, esc(s.error || '—')])) : telemetryNote('No active DHT observation.')) + '</div>';
  const seeds = t.transfer?.web_seeds || [];
  if (seeds.length) html += '<div class="source-section"><h4>Web seeds</h4>' + telemetryTable(['URL','State','Last event','Error'], seeds.map(s => [`<span title="${esc(s.url)}">${esc(s.url)}</span>`, esc(s.state), duration(s.last_event_ago_ms), esc(s.error || '—')])) + '</div>';
  return html;
}
