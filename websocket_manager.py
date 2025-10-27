import asyncio
import aiohttp
import threading
import json
import pandas as pd
from collections import defaultdict, deque
from datetime import datetime
from config import Config
import logging

logger = logging.getLogger("ws")

def norm_stream_symbol(symbol: str):
    return symbol.replace("/","").lower()

class WebsocketManager:
    def __init__(self, cache_max=None):
        self.cache_max = cache_max or Config.WS_CACHE_MAX
        self._loop = None
        self._thread = None
        self._stop = threading.Event()
        # (symbol, timeframe) -> deque of rows
        self._cache = defaultdict(lambda: deque(maxlen=self.cache_max))
        self._session = None
        self._task = None
        self._streams = set()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("WS manager started")

    def stop(self):
        self._stop.set()
        if self._loop:
            fut = asyncio.run_coroutine_threadsafe(self._shutdown(), self._loop)
            try:
                fut.result(timeout=10)
            except Exception:
                pass
        if self._thread: self._thread.join(timeout=5)
        logger.info("WS manager stopped")

    def subscribe(self, symbols, timeframes):
        # update streams set; reconnect combined
        self._streams = set([f"{norm_stream_symbol(s)}@kline_{tf}" for s in symbols for tf in timeframes])
        if self._loop:
            asyncio.run_coroutine_threadsafe(self._restart_combined(), self._loop)

    def get_live_candles(self, symbol, timeframe, limit=200):
        dq = self._cache.get((symbol, timeframe))
        if not dq: return []
        rows = list(dq)[-limit:]
        # convert to dicts
        out = []
        for r in rows:
            out.append({
                "open_time": r["open_time"].isoformat() if isinstance(r["open_time"], datetime) else str(r["open_time"]),
                "open": r["open"], "high": r["high"], "low": r["low"], "close": r["close"], "volume": r["volume"]
            })
        return out

    def _run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._main())

    async def _main(self):
        self._session = aiohttp.ClientSession()
        try:
            await self._restart_combined()
            while not self._stop.is_set():
                await asyncio.sleep(0.25)
        finally:
            await self._shutdown()

    async def _shutdown(self):
        if self._task:
            self._task.cancel()
            self._task = None
        if self._session:
            try: await self._session.close()
            except: pass
            self._session = None

    async def _restart_combined(self):
        if self._task:
            self._task.cancel()
            self._task = None
        if not self._streams:
            return
        base = "wss://stream.binance.com:9443/stream?streams="
        uri = base + "/".join(sorted(self._streams))
        self._task = asyncio.create_task(self._combined(uri))

    async def _combined(self, uri):
        logger.info("Connecting WS combined: %s", uri[:120])
        while not self._stop.is_set():
            try:
                async with self._session.ws_connect(uri, heartbeat=20) as ws:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._on_message(msg.data)
                        else:
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("WS error: %s; reconnecting", e)
                await asyncio.sleep(2)

    async def _on_message(self, data):
        try:
            payload = json.loads(data)
            if "data" in payload: payload = payload["data"]
            k = payload.get("k")
            if not k or not k.get("x"):  # closed only
                return
            sym = payload.get("s") or k.get("s")
            if not sym: return
            # symbol formatting
            if sym.endswith("USDT"):
                symbol = f"{sym[:-4]}/USDT"
            else:
                symbol = sym
            tf = k.get("i")
            row = {
                "open_time": datetime.utcfromtimestamp(k.get("t")/1000),
                "open": float(k.get("o")), "high": float(k.get("h")), "low": float(k.get("l")),
                "close": float(k.get("c")), "volume": float(k.get("v"))
            }
            self._cache[(symbol, tf)].append(row)
        except Exception as e:
            logger.debug("WS parse error: %s", e)