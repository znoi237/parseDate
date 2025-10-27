async function fetchJson(url, opts) {
  const res = await fetch(url, opts);
  const txt = await res.text();
  if (!res.ok) throw new Error(txt);
  return JSON.parse(txt);
}

async function syncHistory() {
  const symbol = document.getElementById("train_symbol").value.trim();
  const years = Number(document.getElementById("train_years").value);
  const timeframes = Array.from(document.getElementById("train_tfs").selectedOptions).map(o=>o.value);
  const body = {symbol, years, timeframes};
  const js = await fetchJson("/api/sync_history", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(body)});
  alert("Синхронизация запущена");
}

let _jobId = null;
async function startTraining() {
  const symbol = document.getElementById("train_symbol").value.trim();
  const years = Number(document.getElementById("train_years").value);
  const timeframes = Array.from(document.getElementById("train_tfs").selectedOptions).map(o=>o.value);
  const js = await fetchJson("/api/train", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({symbol,years,timeframes})});
  _jobId = js.job_id;
  document.getElementById("train_job_box").innerHTML = `job ${_jobId}: <span class="badge bg-info">queued</span>`;
  pollJob();
}
async function pollJob() {
  if (!_jobId) return;
  const js = await fetchJson(`/api/training/${_jobId}`);
  const d = js.data;
  document.getElementById("train_job_box").innerHTML = `
    <div>Статус: <span class="badge ${d.status=='finished'?'bg-success':(d.status=='error'?'bg-danger':'bg-warning')}">${d.status}</span></div>
    <div>Прогресс: ${(d.progress*100).toFixed(0)}%</div>
    <div class="text-muted">${d.message||''}</div>
  `;
  if (d.status=='finished' || d.status=='error') return;
  setTimeout(pollJob, 1500);
}