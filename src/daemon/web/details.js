'use strict';
const duration = ms => ms == null ? '—' : ms < 1000 ? '<1s' : ms < 60000 ? Math.round(ms/1000)+'s' : ms < 3600000 ? Math.round(ms/60000)+'m' : (ms/3600000).toFixed(1)+'h';
const telemetryNote = text => `<p class="telemetry-note">${esc(text)}</p>`;
const date = seconds => seconds ? new Date(seconds*1000).toISOString().replace('T',' ').replace('.000Z',' UTC') : '—';
function telemetryTable(headers, rows) {
  return `<div class="telemetry-scroll"><table class="telemetry-table"><thead><tr>${headers.map(h => `<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(row => `<tr>${row.map(cell => `<td>${cell}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
}
const byteValue = value => `<span title="${esc(value)} bytes">${bytes(value)}</span>`;
function renderOverview(t) {
  const run = t.transfer;
  const group = (title,rows) => `<div><h3 class="detail-name">${esc(title)}</h3>${rows.map(([label,value]) => detailRow(label,value)).join('')}</div>`;
  const announcement = t.completed_announce === null ? 'No event' : t.completed_announce ? 'Acknowledged' : 'Pending';
  return (t.last_error ? `<div class="torrent-error">${esc(t.last_error)}</div>` : '') + `<div class="detail-grid">` +
    group(t.name,[['Info hash',t.id],['Save to',t.output_dir],['Status',t.state],['Intent',t.intent],['Private',t.private === null ? 'Unknown' : t.private ? 'Yes' : 'No'],['Tracker message',t.tracker_message || '—'],['Retry at',date(t.retry_at)]]) +
    group('Lifetime accounting',[
      ['Downloaded',bytes(t.downloaded_bytes)],['Uploaded',bytes(t.uploaded_bytes)],
      ['Upload / size ratio',t.total_bytes ? (t.uploaded_bytes/t.total_bytes).toFixed(2) : '—'],
      ['Hash failed bytes',bytes(t.hash_failed_bytes)],['Hash failed pieces',t.hash_failed_pieces],['Seed time',duration(t.seed_seconds*1000)],
      ['Added',date(t.added_at)],['Completed at',date(t.completed_at)],['Last download',date(t.last_download_at)],['Last upload',date(t.last_upload_at)]]) +
    group('Progress',[
      ['Verified / full size',`${bytes(t.verified_bytes)} / ${bytes(t.total_bytes)}`],
      ['Selected verified / size',`${bytes(t.wanted_verified_bytes)} / ${bytes(t.wanted_bytes)}`],
      ['Selected complete',t.completed ? 'Yes' : 'No'],['Completion announce',announcement],['ETA',t.eta],['Peers',t.peers]]) +
    group('Settings',Object.entries(t.settings).map(([key,value]) => [key.replaceAll('_',' '),Array.isArray(value) ? value.length ? value.join(', ') : 'All files' : String(value)])) +
    group('Current run',run ? Object.entries(run).filter(([,value]) => value === null || typeof value !== 'object').map(([key,value]) => [key.replaceAll('_',' '),
      value === null ? '—' : key.endsWith('_bytes') ? bytes(value) : key.endsWith('_bytes_per_second') ? rate(value) : String(value)]) : [['Status','Inactive']]) + '</div>';
}
function renderPeerTelemetry(t) {
  const peers = t.transfer?.peers || [];
  if (!peers.length) return telemetryNote('No active peers.');
  return telemetryTable(['Peer','Client','Transport','Source','↓ /s','↑ /s','Downloaded','Uploaded','Requests','Choked','Choking','Interested','Remote interest','State'], peers.map(p => {
    const d = p.detail;
    const state = d.retiring ? 'Closing' : d.established ? 'Connected' : 'Handshaking';
    const diagnostics = Object.entries({...d,utp_retransmits:p.utp_retransmits}).filter(([key]) => key !== 'connection').map(([key,value]) => `${key}: ${value ?? '—'}`).join('\n');
    return [esc(p.endpoint || p.web_seed_url || '—'),esc(p.client || '—'),esc(p.transport),esc(p.source),
      rate(p.download_bytes_per_second),rate(p.upload_bytes_per_second),byteValue(d.downloaded_bytes),d.uploaded_bytes === null ? '—' : byteValue(d.uploaded_bytes),
      d.assigned_requests,d.remote_choking ? 'Y' : 'N',d.local_choking ? 'Y' : 'N',d.local_interested ? 'Y' : 'N',d.remote_interested ? 'Y' : 'N',
      `<span title="${esc(diagnostics)}">${state}</span>`];
  }));
}
function renderFileManifest(page, previous) {
  return telemetryTable(['File','Size','Priority'],page.files.map(f => [esc(f.path),byteValue(f.length),
    `<select data-file-priority="${f.index}" data-focus-key="priority-${f.index}" aria-label="Priority for ${esc(f.path)}">${['skip','normal','high'].map(p => `<option value="${p}" ${p === f.priority ? 'selected' : ''}>${p}</option>`).join('')}</select>`])) +
    `<div class="file-pagination"><button class="secondary" data-file-page="previous" data-focus-key="files-previous" ${previous ? '' : 'disabled'}>Previous</button><span>${page.offset+1}–${page.offset+page.files.length} of ${page.total_files}</span><button class="secondary" data-file-page="next" data-focus-key="files-next" ${page.next_offset === null ? 'disabled' : ''}>Next</button><button class="secondary" data-file-page="refresh" data-focus-key="files-refresh">Refresh</button></div>`;
}
function renderSourceTelemetry(t) {
  const trackers = t.transfer?.trackers || [];
  let html = `<div class="source-summary">${['trackers','dht','pex','web_seeds'].map(key => `<span>${esc(key.replaceAll('_',' '))}: <strong>${t.settings[key] ? 'On' : 'Off'}</strong></span>`).join('')}</div>`;
  html += trackers.length ? telemetryTable(['Tracker','Tier / slot','Status','Seeds','Leechers','Peers','Last reply','Next','Exchanges','Failures','Message'],trackers.map(s => [
    esc(s.url),`${s.tier+1} / ${s.slot+1}`,esc(s.state),s.seeders ?? '—',s.leechers ?? '—',s.peers_returned ?? '—',duration(s.last_reply_ago_ms),duration(s.next_announce_ms),s.exchanges,s.failures,esc([s.error,s.warning].filter(Boolean).join(' · '))])) : telemetryNote('No active tracker observations.');
  const dht = t.transfer?.dht;
  html += '<div class="source-section"><h4>DHT</h4>' + (dht ? `<div class="source-facts">${Object.entries(dht).map(([key,value]) => `<span>${esc(key.replaceAll('_',' '))}: <b>${esc(value ?? '—')}</b></span>`).join('')}</div>` : telemetryNote('No active DHT observation.')) + '</div>';
  const seeds = t.transfer?.web_seeds || [];
  if (seeds.length) html += '<div class="source-section"><h4>Web seeds</h4>' + telemetryTable(['URL','State','Last event','Error'],seeds.map(s => [esc(s.url),esc(s.state),duration(s.last_event_ago_ms),esc(s.error || '—')])) + '</div>';
  return html;
}
