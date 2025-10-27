async function fetchJson(url, opts) {
  const res = await fetch(url, opts);
  const txt = await res.text();
  if (!res.ok) throw new Error(txt);
  return JSON.parse(txt);
}

async function loadKeys() {
  const mn = await fetchJson("/api/keys?network=mainnet");
  const tn = await fetchJson("/api/keys?network=testnet");
  document.getElementById("mainnet_keys").textContent = mn.data? `Сохранено: ${mn.data.api_key} / ${mn.data.api_secret}` : 'Нет';
  document.getElementById("testnet_keys").textContent = tn.data? `Сохранено: ${tn.data.api_key} / ${tn.data.api_secret}` : 'Нет';
}
async function saveKeys(net) {
  const key = document.getElementById(net=='mainnet'?'mn_key':'tn_key').value.trim();
  const sec = document.getElementById(net=='mainnet'?'mn_secret':'tn_secret').value.trim();
  await fetchJson("/api/keys", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({network:net, api_key:key, api_secret:sec})});
  alert("Сохранено");
  loadKeys();
}
window.addEventListener("load", loadKeys);