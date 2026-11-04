'use strict';
let logTimer, logLoading = false;
async function refreshLogs() {
  if (logLoading || !$('#logs-dialog').open) return;
  clearTimeout(logTimer);
  logLoading = true;
  $('#refresh-logs').disabled = true;
  try {
    const value = await request('/api/logs', {});
    if (!$('#logs-dialog').open) return;
    const output = $('#logs-output'), top = output.scrollTop, left = output.scrollLeft;
    if (output.textContent !== value.text) output.textContent = value.text;
    output.scrollTop = $('#logs-follow').checked ? output.scrollHeight : top;
    output.scrollLeft = left;
    $('#copy-logs').disabled = !value.text;
    $('#logs-status').textContent = !value.enabled ? 'File logging is disabled. Enable logging.file_enabled and restart spill.' :
      `${value.truncated ? 'Latest 64 KiB' : 'debug.log'} · ${value.text ? 'Updated '+new Date().toLocaleTimeString() : 'No log entries yet.'}`;
  } catch (error) {
    if ($('#logs-dialog').open) $('#logs-status').textContent = 'Log refresh failed: '+error.message;
  } finally {
    logLoading = false;
    $('#refresh-logs').disabled = false;
    if ($('#logs-dialog').open && $('#logs-follow').checked) logTimer = setTimeout(refreshLogs, 1000);
  }
}
$('#open-logs').onclick = () => {
  $('#logs-status').textContent = 'Loading…';
  $('#logs-copy-status').textContent = '';
  $('#copy-logs').disabled = !$('#logs-output').textContent;
  $('#logs-dialog').showModal();
  void refreshLogs();
};
$('#refresh-logs').onclick = () => void refreshLogs();
$('#logs-follow').onchange = e => {
  clearTimeout(logTimer);
  if (e.target.checked) void refreshLogs();
};
$('#copy-logs').onclick = async () => {
  try { await navigator.clipboard.writeText($('#logs-output').textContent); $('#logs-copy-status').textContent = 'Copied.'; }
  catch { $('#logs-copy-status').textContent = 'Could not copy. Select the text and copy it manually.'; }
};
$('#close-logs').onclick = () => $('#logs-dialog').close();
$('#logs-dialog').onclose = () => clearTimeout(logTimer);
