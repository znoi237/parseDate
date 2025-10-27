async function fetchJson(url, opts) {
  const res = await fetch(url, opts);
  const txt = await res.text();
  if (!res.ok) throw new Error(txt);
  return JSON.parse(txt);
}
async function startBot() {
  const symbol = document.getElementById("bot_symbol").value.trim();
  const tfs = Array.from(document.getElementById("bot_tfs").selectedOptions).map(o=>o.value);
  const interval_sec = Number(document.getElementById("bot_interval").value);
  const js = await fetchJson("/api/bots/start",{method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({symbol,timeframes:tfs,interval_sec})});
  alert(js.message || "OK");
  loadBots();
}
async function stopBot(symbol) {
  const js = await fetchJson("/api/bots/stop",{method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({symbol})});
  alert(js.message || "OK");
  loadBots();
}
async function loadBots() {
  const js = await fetchJson("/api/bots");
  const tb = document.querySelector("#bots_table tbody");
  tb.innerHTML = "";
  (js.data||[]).forEach(b=>{
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${b.symbol}</td><td>${b.status}</td><td>${JSON.stringify(b.stats||{})}</td><td>${b.started_at}</td>
      <td><button class="btn btn-sm btn-outline-light" onclick="stopBot('${b.symbol}')">Стоп</button></td>`;
    tb.appendChild(tr);
  });
}
window.addEventListener("load", loadBots);