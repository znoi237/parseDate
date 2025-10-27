async function fetchJson(url, opts) {
  const res = await fetch(url, opts);
  const txt = await res.text();
  if (!res.ok) throw new Error(txt);
  return JSON.parse(txt);
}

async function loadSymbol() {
  const params = new URLSearchParams(location.search);
  const symbol = params.get("symbol") || "BTC/USDT";
  document.getElementById("sym_title").textContent = symbol;
  const js = await fetchJson(`/api/live_candles?symbol=${encodeURIComponent(symbol)}&timeframe=1h&limit=200`);
  const data = (js.data||[]).map(r => ({x: new Date(r.open_time), o:r.open, h:r.high, l:r.low, c:r.close}));
  const ctx = document.getElementById("ohlc_chart").getContext("2d");
  const chart = new Chart(ctx, {
    type: 'candlestick',
    data: { datasets: [{ label: symbol, data }] },
    options: { parsing:false, plugins:{legend:{display:false}} }
  });
}
window.addEventListener("load", loadSymbol);