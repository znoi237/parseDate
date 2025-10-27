import json
from flask import Blueprint, request, jsonify, current_app, render_template
from config import Config
from database import DatabaseManager
from data_manager import CCXTDataManager
from websocket_manager import WebsocketManager
from model_manager import ModelManager
from news_ingestor import NewsIngestor
from bots_manager import BotManager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import threading
import asyncio
import logging

api_bp = Blueprint("api", __name__)
logger = logging.getLogger("api")

class Services:
    def __init__(self, db, data, ws, models, news, bots, executor, loop):
        self.db = db
        self.data = data
        self.ws = ws
        self.models = models
        self.news = news
        self.bots = bots
        self.executor = executor
        self.loop = loop

def make_services(app):
    db = DatabaseManager()
    data = CCXTDataManager(db)
    ws = WebsocketManager() if Config.ENABLE_WS else None
    if ws: ws.start(); ws.subscribe(Config.SYMBOLS, Config.TIMEFRAMES)
    models = ModelManager(db)
    news = NewsIngestor(db)
    # run news loop in background asyncio thread
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True); t.start()
    asyncio.run_coroutine_threadsafe(news.start(), loop)
    bots = BotManager(db, data, models, ws)
    executor = ThreadPoolExecutor(max_workers=Config.MAX_WORKERS)
    return Services(db, data, ws, models, news, bots, executor, loop)

@api_bp.route("/keys", methods=["GET","POST"])
def keys():
    sv: Services = current_app.extensions["services"]
    if request.method=="GET":
        network = request.args.get("network","mainnet")
        keys = sv.db.load_api_keys(network)
        if not keys: return jsonify({"data": None})
        # mask
        def mask(s, keep=4): 
            return s if not s or len(s)<=keep*2 else (s[:keep]+"..." + s[-keep:])
        return jsonify({"data": {"network":network, "api_key": mask(keys["api_key"]), "api_secret": mask(keys["api_secret"])}})
    else:
        data = request.get_json(force=True)
        network = data.get("network")
        api_key = data.get("api_key")
        api_secret = data.get("api_secret")
        if network not in ("mainnet","testnet"):
            return jsonify({"error":"network must be mainnet|testnet"}), 400
        if not api_key or not api_secret:
            return jsonify({"error":"api_key/api_secret required"}), 400
        sv.db.save_api_keys(network, api_key, api_secret)
        return jsonify({"status":"ok"})

@api_bp.route("/account", methods=["GET"])
def account():
    # Простая “заглушка”: баланс и позиции из нашей БД торговли
    sv: Services = current_app.extensions["services"]
    network = request.args.get("network","mainnet")
    trades = sv.db.get_trades(limit=1000)
    total_closed = [t for t in trades if t["status"]=="closed"]
    pnl = sum([t["pnl_percent"] for t in total_closed if t["pnl_percent"] is not None]) if total_closed else 0.0
    resp = {
        "network": network,
        "balance_usdt": None,  # можно добавить интеграцию с API биржи для реального баланса
        "open_positions": len([t for t in trades if t["status"]=="open"]),
        "closed_trades": len(total_closed),
        "total_pnl_percent": pnl
    }
    return jsonify({"data": resp})

@api_bp.route("/pairs_status", methods=["GET"])
def pairs_status():
    sv: Services = current_app.extensions["services"]
    symbols = request.args.getlist("symbol") or Config.SYMBOLS
    data = sv.db.get_pairs_status(symbols, Config.TIMEFRAMES)
    return jsonify({"data": data})

@api_bp.route("/trades", methods=["GET"])
def trades():
    sv: Services = current_app.extensions["services"]
    limit = int(request.args.get("limit","200"))
    data = sv.db.get_trades(limit=limit)
    return jsonify({"data": data})

@api_bp.route("/sync_history", methods=["POST"])
def sync_history():
    sv: Services = current_app.extensions["services"]
    body = request.get_json(force=True)
    symbol = body.get("symbol")
    timeframes = body.get("timeframes") or Config.TIMEFRAMES
    years = int(body.get("years", Config.HISTORY_YEARS))
    if symbol:
        for tf in timeframes:
            sv.data.fetch_ohlcv_incremental(symbol, tf, years)
    else:
        for sym in Config.SYMBOLS:
            for tf in timeframes:
                sv.data.fetch_ohlcv_incremental(sym, tf, years)
    return jsonify({"status":"ok"})

@api_bp.route("/train", methods=["POST"])
def train():
    sv: Services = current_app.extensions["services"]
    body = request.get_json(force=True)
    symbol = body["symbol"]
    timeframes = body.get("timeframes") or Config.TIMEFRAMES
    years = int(body.get("years", Config.HISTORY_YEARS))
    job_id = sv.db.create_training_job(symbol, timeframes)
    def task():
        try:
            sv.db.update_training_job(job_id, status="running", progress=0.0, message="started")
            # убедиться, что история подгружена
            for tf in timeframes:
                sv.data.fetch_ohlcv_incremental(symbol, tf, years)
            sv.models.train_symbol(symbol, timeframes, years, job_id=job_id)
        except Exception as e:
            sv.db.update_training_job(job_id, status="error", message=str(e))
    sv.executor.submit(task)
    return jsonify({"job_id": job_id, "status": "queued"})

@api_bp.route("/training/<int:job_id>", methods=["GET"])
def training_status(job_id):
    sv: Services = current_app.extensions["services"]
    job = sv.db.get_training_job(job_id)
    if not job: return jsonify({"error":"not found"}),404
    return jsonify({"data": job})

@api_bp.route("/bots/start", methods=["POST"])
def bots_start():
    sv: Services = current_app.extensions["services"]
    body = request.get_json(force=True)
    symbol = body["symbol"]
    timeframes = body.get("timeframes") or Config.TIMEFRAMES
    interval_sec = int(body.get("interval_sec", 60))
    ok,msg = sv.bots.start_bot(symbol, timeframes, interval_sec)
    code = 200 if ok else 400
    return jsonify({"ok":ok, "message":msg}), code

@api_bp.route("/bots/stop", methods=["POST"])
def bots_stop():
    sv: Services = current_app.extensions["services"]
    body = request.get_json(force=True)
    symbol = body["symbol"]
    ok,msg = sv.bots.stop_bot(symbol)
    code = 200 if ok else 400
    return jsonify({"ok":ok, "message":msg}), code

@api_bp.route("/bots", methods=["GET"])
def bots_list():
    sv: Services = current_app.extensions["services"]
    data = sv.db.bots_summary()
    return jsonify({"data": data})

@api_bp.route("/live_candles", methods=["GET"])
def live_candles():
    sv: Services = current_app.extensions["services"]
    symbol = request.args.get("symbol")
    tf = request.args.get("timeframe","1h")
    limit = int(request.args.get("limit","200"))
    data = sv.ws.get_live_candles(symbol, tf, limit=limit) if sv.ws else []
    if not data:
        # fallback recent from db
        df = sv.db.load_ohlcv(symbol, tf, since=datetime.utcnow()-timedelta(days=30))
        if df is not None and not df.empty:
            data = [{"open_time": idx.isoformat(), **row._asdict()} if hasattr(row,"_asdict") else {"open_time": idx.isoformat(), "open": float(row["open"]), "high": float(row["high"]), "low": float(row["low"]), "close": float(row["close"]), "volume": float(row["volume"])} for idx, row in df.tail(limit).iterrows()]
    return jsonify({"data": data})

@api_bp.route("/news", methods=["GET"])
def news():
    sv: Services = current_app.extensions["services"]
    hours = int(request.args.get("hours","24"))
    since = datetime.utcnow() - timedelta(hours=hours)
    df = sv.db.news_since(since)
    return jsonify({"data": df.to_dict(orient="records")})