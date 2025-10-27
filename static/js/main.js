async function fetchJson(url, opts) {
  const res = await fetch(url, opts);
  const ct = res.headers.get("content-type") || "";
  const txt = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${txt.slice(0,200)}`);
  if (!ct.includes("application/json")) throw new Error(`Non-JSON: ${txt.slice(0,200)}`);
  return JSON.parse(txt);
}

async function refreshAccounts() {
  const main = await fetchJson("/api/account?network=mainnet");
  const test = await fetchJson("/api/account?network=testnet");
  document.getElementById("acc_mainnet").innerHTML = `Баланс: ${main.data.balance_usdt ?? "—"} | Закрытых: ${main.data.closed_trades} | PnL: ${main.data.total_pnl_percent.toFixed(2)}%`;
  document.getElementById("acc_testnet").innerHTML = `Баланс: ${test.data.balance_usdt ?? "—"} | Открытых: ${test.data.open_positions} | PnL: ${test.data.total_pnl_percent.toFixed(2)}%`;
  document.getElementById("ws_status").innerHTML = `<span class="badge bg-success">активен</span>`;
}

async function refreshPairs() {
  const js = await fetchJson("/api/pairs_status");
  const tb = document.querySelector("#pairs_table tbody");
  tb.innerHTML = "";
  (js.data||[]).forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${r.symbol}</td>
      <td>${r.is_trained ? '✅' : '—'}</td>
      <td>${r.last_full_train_end? new Date(r.last_full_train_end).toLocaleString(): '—'}</td>
      <td>${r.last_incremental_train_end? new Date(r.last_incremental_train_end).toLocaleString(): '—'}</td>
      <td>${r.accuracy? (r.accuracy*100).toFixed(1)+'%':'—'}</td>
      <td><a class="btn btn-sm btn-outline-light" href="/symbol?symbol=${encodeURIComponent(r.symbol)}">Открыть</a></td>`;
    tb.appendChild(tr);
  });
}

async function refreshTrades() {
  const js = await fetchJson("/api/trades?limit=200");
  const tb = document.querySelector("#trades_table tbody");
  tb.innerHTML = "";
  (js.data||[]).forEach(r=>{
    const t = r.exit_time || r.entry_time;
    const pnl = r.pnl_percent==null? '—' : r.pnl_percent.toFixed(2)+'%';
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${t? new Date(t).toLocaleString(): '—'}</td><td>${r.symbol}</td><td>${r.side}</td><td>${pnl}</td>`;
    tb.appendChild(tr);
  });
}

async function refreshNews() {
  const js = await fetchJson("/api/news?hours=24");
  const tb = document.querySelector("#news_table tbody");
  tb.innerHTML = "";
  (js.data||[]).forEach(n=>{
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${n.published_at? new Date(n.published_at).toLocaleString(): '—'}</td>
      <td>${n.provider}</td>
      <td><a href="${n.url}" target="_blank">${n.title}</a></td>
      <td>${n.sentiment!=null? n.sentiment.toFixed(2): '—'}</td>`;
    tb.appendChild(tr);
  });
}

document.getElementById("btn_refresh_pairs")?.addEventListener("click", refreshPairs);
document.getElementById("btn_refresh_trades")?.addEventListener("click", refreshTrades);
document.getElementById("btn_refresh_news")?.addEventListener("click", refreshNews);