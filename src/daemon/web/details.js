'use strict';
const duration = ms => ms == null ? '—' : ms < 1000 ? '<1s' : ms < 60000 ? Math.round(ms/1000)+'s' : ms < 3600000 ? Math.round(ms/60000)+'m' : (ms/3600000).toFixed(1)+'h';
const telemetryNote = text => `<p class="telemetry-note">${esc(text)}</p>`;
const date = seconds => seconds ? new Date(seconds*1000).toISOString().replace('T',' ').replace('.000Z',' UTC') : '—';
function telemetryTable(headers, rows) {
  return `<div class="telemetry-scroll"><table class="telemetry-table"><thead><tr>${headers.map(h => `<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(row => `<tr>${row.map(cell => `<td>${cell}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
}
const byteValue = value => `<span title="${esc(value)} bytes">${bytes(value)}</span>`;
function renderOverview(t) {
  const group = (title,rows) => `<div><h3 class="detail-name">${esc(title)}</h3>${rows.map(([label,value]) => detailRow(label,value)).join('')}</div>`;
  const announcements = t.completed_announces.map(s => [s.swarm, s.delivered]);
  return (t.last_error ? `<div class="torrent-error">${esc(t.last_error)}</div>` : '') + `<div class="detail-grid">` +
    group(t.name,[['ID',t.id],['Format',t.format],['SHA-1',t.v1 || '—'],['SHA-256',t.v2 || '—'],['Metadata',t.metadata_ready ? 'Ready' : t.info_validated ? `Hash layers: ${bytes(t.hash_bytes)} / ${bytes(t.hash_total_bytes)}` : 'Pending'],['V2 layers',!t.v2 ? '—' : t.layers_pending ? 'Pending (v1 mode)' : t.metadata_ready ? 'Ready' : 'Acquiring'],['Save to',t.output_dir],['Status',t.state],['Intent',t.intent],['Private',t.private === null ? 'Unknown' : t.private],['Tracker message',t.tracker_message || '—'],['Retry at',date(t.retry_at)]]) +
    group('Lifetime accounting',[
      ['Downloaded',bytes(t.downloaded_bytes)],['Uploaded',bytes(t.uploaded_bytes)],
      ['Upload / size ratio',t.total_bytes ? (t.uploaded_bytes/t.total_bytes).toFixed(2) : '—'],
      ['Hash failed bytes',bytes(t.hash_failed_bytes)],['Hash failed pieces',t.hash_failed_pieces],['Seed time',duration(t.seed_seconds*1000)],
      ['Added',date(t.added_at)],['Completed at',date(t.completed_at)],['Last download',date(t.last_download_at)],['Last upload',date(t.last_upload_at)]]) +
    group('Progress',[
      ['Verified / full size',`${bytes(t.verified_bytes)} / ${bytes(t.total_bytes)}`],
      ['Selected verified / size',`${bytes(t.wanted_verified_bytes)} / ${bytes(t.wanted_bytes)}`],
      ['Selected complete',t.completed],['ETA',t.eta],['Peers',t.peers]]) +
    group('Completion acknowledged',announcements.length ? announcements : [['Status','No event']]) + '</div>';
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
      d.assigned_requests,booleanValue(d.remote_choking),booleanValue(d.local_choking),booleanValue(d.local_interested),booleanValue(d.remote_interested),
      `<span title="${esc(diagnostics)}">${state}</span>`];
  }));
}
function renderFileManifest(page) {
  return telemetryTable(['File','Size','Priority'],page.files.map(f => [esc(f.path),byteValue(f.length),
    `<select ${busy || !online ? 'disabled' : ''} data-file-priority="${f.index}" data-focus-key="priority-${f.index}" aria-label="Priority for ${esc(f.path)}">${['skip','normal','high'].map(p => `<option value="${p}" ${p === f.priority ? 'selected' : ''}>${p}</option>`).join('')}</select>`])) +
    `<div class="file-pagination"><button class="secondary" data-file-page="previous" data-focus-key="files-previous" ${page.offset > 0 ? '' : 'disabled'}>Previous</button><span>${page.offset+1}–${page.offset+page.files.length} of ${page.total_files}</span><button class="secondary" data-file-page="next" data-focus-key="files-next" ${page.next_offset === null ? 'disabled' : ''}>Next</button><button class="secondary" data-file-page="refresh" data-focus-key="files-refresh">Refresh</button></div>`;
}
function renderSourceTelemetry(t) {
  const trackers = t.transfer?.trackers || [];
  let html = `<div class="source-summary">${['trackers','dht','pex','web_seeds'].map(key => `<span>${esc(key.replaceAll('_',' '))}: ${booleanValue(t.settings[key],'On','Off')}</span>`).join('')}</div>`;
  html += trackers.length ? telemetryTable(['Tracker','Swarm key','Tier / slot','Status','Seeds','Leechers','Peers','Last reply','Next','Exchanges','Failures','Message'],trackers.map(s => [
    esc(s.url),esc(s.swarm),`${s.tier+1} / ${s.slot+1}`,esc(s.state),s.seeders ?? '—',s.leechers ?? '—',s.peers_returned ?? '—',duration(s.last_reply_ago_ms),duration(s.next_announce_ms),s.exchanges,s.failures,esc([s.error,s.warning].filter(Boolean).join(' · '))])) : telemetryNote('No active tracker observations.');
  const dht = t.transfer?.dht || [];
  html += '<div class="source-section"><h4>DHT</h4>' + (dht.length ? telemetryTable(['Swarm key','Pending','Next lookup','Last lookup','Queried','Announced','Peers','Error'],dht.map(s => [esc(s.swarm),booleanValue(s.lookup_pending),duration(s.next_lookup_ms),duration(s.last_lookup_ago_ms),s.queried,s.announced,s.peers_returned,esc(s.error || '—')])) : telemetryNote('No active DHT observation.')) + '</div>';
  const seeds = t.transfer?.web_seeds || [];
  if (seeds.length) html += '<div class="source-section"><h4>Web seeds</h4>' + telemetryTable(['URL','State','Last event','Error'],seeds.map(s => [esc(s.url),esc(s.state),duration(s.last_event_ago_ms),esc(s.error || '—')])) + '</div>';
  return html;
}
