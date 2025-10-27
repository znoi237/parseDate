import threading
import time
from datetime import datetime
import logging
from config import Config
from database import DatabaseManager
from data_manager import CCXTDataManager
from model_manager import ModelManager
from websocket_manager import WebsocketManager

logger = logging.getLogger("bots")

class BotManager:
    def __init__(self, db: DatabaseManager, data: CCXTDataManager, models: ModelManager, ws: WebsocketManager):
        self.db = db
        self.data = data
        self.models = models
        self.ws = ws
        self._bots = {}  # symbol -> thread control

    def start_bot(self, symbol: str, timeframes: list, interval_sec=60):
        if symbol in self._bots and self._bots[symbol]["running"]:
            return False, "already running"
        ctrl = {"running": True}
        t = threading.Thread(target=self._run_loop, args=(symbol,timeframes,interval_sec,ctrl), daemon=True)
        self._bots[symbol] = {"thread": t, "running": True, "started_at": datetime.utcnow()}
        self.db.add_bot(symbol, "active", stats={"pnl":0,"trades":0})
        t.start()
        logger.info("bot started for %s", symbol)
        return True, "started"

    def stop_bot(self, symbol: str):
        b = self._bots.get(symbol)
        if not b: return False, "not running"
        b["running"] = False
        self.db.update_bot(symbol, status="stopped")
        logger.info("bot stop requested for %s", symbol)
        return True, "stopped"

    def _run_loop(self, symbol, timeframes, interval_sec, ctrl):
        # подписка WS для TF
        if self.ws: self.ws.subscribe([symbol], timeframes)
        try:
            while self._bots.get(symbol,{}).get("running"):
                # 1) обновить инкрементальные данные из mainnet в БД (для safety)
                for tf in timeframes:
                    self.data.fetch_ohlcv_incremental(symbol, tf, years=Config.HISTORY_YEARS)
                # 2) собрать “live окна” за последние полгода
                latest = {}
                half_year_ago = datetime.utcnow() - timedelta(days=180)
                for tf in timeframes:
                    # пробуем live cache
                    live = self.ws.get_live_candles(symbol, tf, limit=500) if self.ws else []
                    live_df = None
                    if live:
                        live_df = pd.DataFrame(live)
                        live_df["open_time"] = pd.to_datetime(live_df["open_time"])
                        live_df.set_index("open_time", inplace=True)
                        live_df = live_df[["open","high","low","close","volume"]].astype(float)
                    hist = self.db.load_ohlcv(symbol, tf, since=half_year_ago)
                    if hist is not None and not hist.empty:
                        if live_df is not None and not live_df.empty:
                            merged = pd.concat([hist, live_df]).sort_index().groupby(level=0).last()
                        else:
                            merged = hist
                        latest[tf] = merged.tail(1000)
                # 3) иерархический предикт
                from datetime import timedelta
                result = self.models.predict_hierarchical(symbol, timeframes, latest)
                if result["consensus"] != 0 and result["confidence"] >= Config.SIGNAL_THRESHOLD:
                    side = "BUY" if result["consensus"]==1 else "SELL"
                    # Для MVP: логируем, симулируем сделку (в реальности — отправить ордер на Testnet)
                    entry_price = float(latest[timeframes[-1]]["close"].iloc[-1]) if latest.get(timeframes[-1]) is not None else 0.0
                    qty = 10.0 / max(entry_price, 1e-8)
                    tid = self.db.add_trade(symbol, side, entry_price, qty, datetime.utcnow())
                    # симуляция выхода через интервал
                    time.sleep(max(1, interval_sec//2))
                    exit_price = float(latest[timeframes[-1]]["close"].iloc[-1])
                    pnl = (exit_price - entry_price)/entry_price*100.0 if side=="BUY" else (entry_price - exit_price)/entry_price*100.0
                    self.db.close_trade(tid, exit_price, pnl, datetime.utcnow())
                    logger.info("bot trade %s %s pnl=%.2f%%", symbol, side, pnl)
                time.sleep(interval_sec)
        except Exception as e:
            logger.exception("bot loop error %s: %s", symbol, e)
        finally:
            self._bots.get(symbol,{}).update({"running": False})
            self.db.update_bot(symbol, status="stopped")