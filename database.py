import sqlite3
import os
import json
from datetime import datetime, timedelta
import pandas as pd
from config import Config
import logging
import joblib
import io

logger = logging.getLogger("db")

class DatabaseManager:
    def __init__(self, db_path=None):
        self.db_path = db_path or Config.DB_PATH
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True) if os.path.dirname(self.db_path) else None
        self._init_db()

    def _conn(self):
        return sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

    def _init_db(self):
        conn = self._conn()
        c = conn.cursor()
        # Основные таблицы
        c.executescript("""
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            network TEXT NOT NULL CHECK(network IN ('mainnet','testnet')),
            api_key TEXT NOT NULL,
            api_secret TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS historical_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            open_time DATETIME NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            source TEXT DEFAULT 'binance',
            UNIQUE(symbol, timeframe, open_time)
        );

        CREATE INDEX IF NOT EXISTS idx_hist_sym_tf_time ON historical_data(symbol,timeframe,open_time);

        CREATE TABLE IF NOT EXISTS models (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            algo TEXT NOT NULL,
            metrics TEXT,
            last_full_train_end DATETIME,
            last_incremental_train_end DATETIME,
            model_blob BLOB,
            classes_blob BLOB,
            features JSON,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timeframe)
        );

        CREATE TABLE IF NOT EXISTS training_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframes TEXT NOT NULL,
            status TEXT NOT NULL,
            progress REAL DEFAULT 0,
            message TEXT,
            started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL,
            exit_price REAL,
            quantity REAL,
            pnl_percent REAL,
            entry_time DATETIME,
            exit_time DATETIME,
            status TEXT NOT NULL DEFAULT 'closed'
        );

        CREATE TABLE IF NOT EXISTS bots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            stats JSON
        );

        CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT,
            title TEXT,
            url TEXT,
            published_at DATETIME,
            summary TEXT,
            sentiment REAL,
            symbols TEXT, -- CSV
            UNIQUE(url)
        );
        """)
        conn.commit()
        conn.close()
        logger.info("Database initialized at %s", self.db_path)

    # API keys
    def save_api_keys(self, network, api_key, api_secret):
        conn = self._conn()
        c = conn.cursor()
        c.execute("DELETE FROM api_keys WHERE network=?", (network,))
        c.execute("INSERT INTO api_keys (network, api_key, api_secret) VALUES (?,?,?)", (network, api_key, api_secret))
        conn.commit()
        conn.close()
        return True

    def load_api_keys(self, network):
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT api_key, api_secret FROM api_keys WHERE network=? ORDER BY id DESC LIMIT 1", (network,))
        row = c.fetchone()
        conn.close()
        if row:
            return {"api_key": row[0], "api_secret": row[1], "network": network}
        return None

    # Historical data
    def upsert_ohlcv(self, symbol, timeframe, df: pd.DataFrame, source="binance"):
        conn = self._conn()
        c = conn.cursor()
        saved = 0
        for ts, r in df.iterrows():
            c.execute("""
                INSERT INTO historical_data(symbol,timeframe,open_time,open,high,low,close,volume,source)
                VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol,timeframe,open_time) DO UPDATE SET open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,volume=excluded.volume,source=excluded.source
            """, (symbol, timeframe, ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts, float(r.open), float(r.high), float(r.low), float(r.close), float(r.volume), source))
            saved += 1
        conn.commit()
        conn.close()
        return saved

    def get_last_ohlcv_time(self, symbol, timeframe):
        conn = self._conn()
        c = conn.cursor()
        c.execute("""
            SELECT MAX(open_time) FROM historical_data WHERE symbol=? AND timeframe=?
        """, (symbol, timeframe))
        row = c.fetchone()
        conn.close()
        return row[0] if row and row[0] else None

    def load_ohlcv(self, symbol, timeframe, since=None, limit=None):
        conn = self._conn()
        q = "SELECT open_time, open, high, low, close, volume FROM historical_data WHERE symbol=? AND timeframe=?"
        params = [symbol, timeframe]
        if since:
            q += " AND open_time >= ?"
            params.append(since)
        q += " ORDER BY open_time ASC"
        if limit:
            q += " LIMIT ?"
            params.append(limit)
        df = pd.read_sql_query(q, conn, params=params, parse_dates=["open_time"], index_col="open_time")
        conn.close()
        return df

    # Models
    def save_model(self, symbol, timeframe, algo, model, classes, features, last_full_end=None, last_incr_end=None, metrics=None):
        conn = self._conn()
        c = conn.cursor()
        # serialize
        mbuf = io.BytesIO(); joblib.dump(model, mbuf)
        cbuf = io.BytesIO(); joblib.dump(classes, cbuf)
        c.execute("""
            INSERT INTO models(symbol,timeframe,algo,metrics,last_full_train_end,last_incremental_train_end,model_blob,classes_blob,features)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol,timeframe) DO UPDATE SET 
                algo=excluded.algo, metrics=excluded.metrics, last_full_train_end=excluded.last_full_train_end,
                last_incremental_train_end=excluded.last_incremental_train_end, model_blob=excluded.model_blob,
                classes_blob=excluded.classes_blob, features=excluded.features
        """, (symbol, timeframe, algo, json.dumps(metrics or {}), last_full_end, last_incr_end, mbuf.getvalue(), cbuf.getvalue(), json.dumps(features)))
        conn.commit()
        conn.close()

    def load_model(self, symbol, timeframe):
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT algo, metrics, last_full_train_end, last_incremental_train_end, model_blob, classes_blob, features FROM models WHERE symbol=? AND timeframe=?", (symbol, timeframe))
        row = c.fetchone()
        conn.close()
        if not row:
            return None
        algo, metrics, full_end, incr_end, mb, cb, feats = row
        model = joblib.load(io.BytesIO(mb)) if mb else None
        classes = joblib.load(io.BytesIO(cb)) if cb else None
        features = json.loads(feats) if feats else []
        return {
            "algo": algo, "metrics": json.loads(metrics or "{}"), "last_full_train_end": full_end,
            "last_incremental_train_end": incr_end, "model": model, "classes": classes, "features": features
        }

    def get_pairs_status(self, symbols, timeframes):
        # Return training status for dashboard
        conn = self._conn()
        c = conn.cursor()
        res = []
        for sym in symbols:
            # is_trained if any tf has model
            c.execute("SELECT timeframe,last_full_train_end,last_incremental_train_end,metrics FROM models WHERE symbol=?",(sym,))
            rows = c.fetchall()
            if not rows:
                res.append({"symbol": sym, "is_trained": False, "last_full_train_end": None, "last_incremental_train_end": None, "accuracy": None})
            else:
                # aggregate by latest
                full_dates = [r[1] for r in rows if r[1]]
                incr_dates = [r[2] for r in rows if r[2]]
                accs = []
                for r in rows:
                    try:
                        m = json.loads(r[3]) if r[3] else {}
                        if "accuracy" in m:
                            accs.append(float(m["accuracy"]))
                    except:
                        pass
                res.append({
                    "symbol": sym,
                    "is_trained": True,
                    "last_full_train_end": max(full_dates).isoformat() if full_dates else None,
                    "last_incremental_train_end": max(incr_dates).isoformat() if incr_dates else None,
                    "accuracy": sum(accs)/len(accs) if accs else None
                })
        conn.close()
        return res

    # Training jobs
    def create_training_job(self, symbol, timeframes):
        conn = self._conn()
        c = conn.cursor()
        c.execute("INSERT INTO training_jobs(symbol,timeframes,status,progress) VALUES(?,?,?,?)", (symbol, ",".join(timeframes), "queued", 0))
        jid = c.lastrowid
        conn.commit()
        conn.close()
        return jid

    def update_training_job(self, job_id, status=None, progress=None, message=None):
        conn = self._conn()
        c = conn.cursor()
        sets = []
        params = []
        if status is not None:
            sets.append("status=?"); params.append(status)
        if progress is not None:
            sets.append("progress=?"); params.append(progress)
        if message is not None:
            sets.append("message=?"); params.append(message)
        sets.append("updated_at=CURRENT_TIMESTAMP")
        q = f"UPDATE training_jobs SET {', '.join(sets)} WHERE id=?"
        params.append(job_id)
        c.execute(q, params)
        conn.commit()
        conn.close()

    def get_training_job(self, job_id):
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT id,symbol,timeframes,status,progress,message,started_at,updated_at FROM training_jobs WHERE id=?", (job_id,))
        row = c.fetchone()
        conn.close()
        if not row: return None
        return {
            "id": row[0], "symbol": row[1], "timeframes": row[2].split(","), "status": row[3],
            "progress": row[4], "message": row[5], "started_at": row[6], "updated_at": row[7]
        }

    # Trades
    def add_trade(self, symbol, side, entry_price, quantity, entry_time):
        conn = self._conn()
        c = conn.cursor()
        c.execute("""INSERT INTO trades(symbol,side,entry_price,quantity,entry_time,status) VALUES(?,?,?,?,?,?)""",
                  (symbol, side, entry_price, quantity, entry_time, "open"))
        tid = c.lastrowid
        conn.commit(); conn.close()
        return tid

    def close_trade(self, trade_id, exit_price, pnl_percent, exit_time):
        conn = self._conn()
        c = conn.cursor()
        c.execute("""UPDATE trades SET exit_price=?, pnl_percent=?, exit_time=?, status='closed' WHERE id=?""",
                  (exit_price, pnl_percent, exit_time, trade_id))
        conn.commit(); conn.close()

    def get_trades(self, limit=200):
        conn = self._conn()
        df = pd.read_sql_query("""
            SELECT symbol, side, entry_price, exit_price, quantity, pnl_percent, entry_time, exit_time, status
            FROM trades ORDER BY COALESCE(exit_time, entry_time) DESC LIMIT ?
        """, conn, params=[limit])
        conn.close()
        return df.to_dict(orient="records")

    # Bots
    def add_bot(self, symbol, status, stats=None):
        conn = self._conn()
        c = conn.cursor()
        c.execute("INSERT INTO bots(symbol,status,stats) VALUES(?,?,?)", (symbol, status, json.dumps(stats or {})))
        conn.commit(); conn.close()

    def update_bot(self, symbol, status=None, stats=None):
        conn = self._conn()
        c = conn.cursor()
        sets, params = [], []
        if status is not None: sets.append("status=?"); params.append(status)
        if stats is not None: sets.append("stats=?"); params.append(json.dumps(stats))
        if not sets: 
            conn.close(); return
        params.append(symbol)
        c.execute(f"UPDATE bots SET {', '.join(sets)} WHERE symbol=?", params)
        conn.commit(); conn.close()

    def bots_summary(self):
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT symbol,status,stats,started_at FROM bots ORDER BY started_at DESC")
        rows = c.fetchall(); conn.close()
        out = []
        for r in rows:
            try:
                stats = json.loads(r[2]) if r[2] else {}
            except:
                stats = {}
            out.append({"symbol": r[0], "status": r[1], "stats": stats, "started_at": r[3]})
        return out

    # News
    def add_news(self, provider, title, url, published_at, summary, sentiment, symbols_csv=""):
        conn = self._conn()
        c = conn.cursor()
        try:
            c.execute("""
            INSERT OR IGNORE INTO news(provider,title,url,published_at,summary,sentiment,symbols)
            VALUES(?,?,?,?,?,?,?)
            """, (provider, title, url, published_at, summary, sentiment, symbols_csv))
            conn.commit()
        except Exception as e:
            logger.warning("news insert error: %s", e)
        finally:
            conn.close()

    def news_since(self, since_dt, limit=200):
        conn = self._conn()
        df = pd.read_sql_query("""
            SELECT provider,title,url,published_at,summary,sentiment,symbols
            FROM news WHERE published_at >= ? ORDER BY published_at DESC LIMIT ?
        """, conn, params=[since_dt, limit], parse_dates=["published_at"])
        conn.close()
        return df