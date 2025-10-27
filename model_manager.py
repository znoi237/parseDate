import numpy as np
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import Config
from database import DatabaseManager
from features import build_features, make_labels
import logging

logger = logging.getLogger("model")

CLASSES = np.array([-1, 0, 1], dtype=int)

# Минимально необходимое число баров по ТФ (разумные значения для 3 лет истории на 1w ~ 156)
MIN_BARS_BY_TF = {
    "1w": 120,
    "1d": 400,
    "4h": 800,
    "1h": 1200,
    "15m": 2000,
}

class ModelManager:
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.pool = ThreadPoolExecutor(max_workers=Config.MAX_WORKERS)

    def _make_pipeline(self):
        # Инкрементально обучаемый пайплайн: scaler + SGDClassifier (log loss, probas)
        # with_mean=True для плотных данных
        return Pipeline([
            ("scaler", StandardScaler(with_mean=True)),
            ("clf", SGDClassifier(loss="log_loss", max_iter=1, tol=None, random_state=42))
        ])

    def train_symbol(self, symbol: str, timeframes: list, years: int, job_id: int=None):
        futures = [self.pool.submit(self._train_one_tf, symbol, tf, years, job_id) for tf in timeframes]
        total = len(futures)
        done = 0
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                logger.exception("train tf error: %s", e)
            finally:
                done += 1
                if job_id:
                    self.db.update_training_job(job_id, status="running", progress=done/total, message=f"{done}/{total} finished")
        if job_id:
            self.db.update_training_job(job_id, status="finished", progress=1.0, message="Completed")
        return True

    def _enough_bars(self, df_len: int, timeframe: str) -> bool:
        need = MIN_BARS_BY_TF.get(timeframe, 500)
        return df_len >= need

    def _train_one_tf(self, symbol: str, timeframe: str, years: int, job_id: int=None):
        df = self.db.load_ohlcv(symbol, timeframe)
        if df is None or df.empty:
            logger.warning("No data for %s %s", symbol, timeframe)
            return False

        if not self._enough_bars(len(df), timeframe):
            logger.warning("Not enough data for %s %s (have=%d, need=%d)", symbol, timeframe, len(df), MIN_BARS_BY_TF.get(timeframe, 500))
            return False

        feats = build_features(df)
        labels = make_labels(df)

        # Сдвигаем, чтобы не использовать футуристическую информацию
        # Требуем минимум 2 строки для безопасного доступа [-2]
        if len(feats) < 2 or len(labels) < 2:
            logger.warning("Too few feature rows after build for %s %s", symbol, timeframe)
            return False

        X = feats.iloc[:-1].values
        y = labels.iloc[:-1].values

        # Проверяем существующую модель для инкремента
        meta = self.db.load_model(symbol, timeframe)
        if meta and meta["model"] is not None and meta["last_full_train_end"]:
            last_seen = meta["last_incremental_train_end"] or meta["last_full_train_end"]
            mask = feats.index[:-1] > pd.Timestamp(last_seen)
            X_new = feats[mask].values
            y_new = labels[mask].values
            if len(X_new) >= max(50, int(0.05 * len(X))):
                model = meta["model"]
                model.named_steps["clf"].partial_fit(X_new, y_new, classes=CLASSES)
                # оценка на последних N новых выборок
                N = min(500, len(X_new))
                yhat = model.predict(X_new[-N:])
                acc = float(accuracy_score(y_new[-N:], yhat))
                last_end = feats.index[-2] if len(feats) >= 2 else feats.index[-1]
                self.db.save_model(
                    symbol, timeframe, "SGDClassifier", model, CLASSES, list(feats.columns),
                    last_full_end=meta["last_full_train_end"], last_incr_end=last_end.to_pydatetime(),
                    metrics={"accuracy": acc}
                )
                logger.info("Incremental trained %s %s, acc=%.3f", symbol, timeframe, acc)
                return True
            else:
                logger.info("No enough new data for incremental %s %s (new=%d)", symbol, timeframe, len(X_new))
                return False

        # Полное обучение
        model = self._make_pipeline()
        # Если мало данных, обучаем целиком за один проход
        if len(X) <= 1024:
            model.named_steps["clf"].partial_fit(X, y, classes=CLASSES)
        else:
            # Warm start на первом чанке
            first_chunk = min(256, len(X))
            model.named_steps["clf"].partial_fit(X[:first_chunk], y[:first_chunk], classes=CLASSES)
            # Остальные чанки
            for start in range(first_chunk, len(X), 1024):
                end = min(len(X), start + 1024)
                model.named_steps["clf"].partial_fit(X[start:end], y[start:end])

        # Оценка
        N = min(1000, len(X))
        yhat = model.predict(X[-N:])
        acc = float(accuracy_score(y[-N:], yhat))
        last_end = feats.index[-2] if len(feats) >= 2 else feats.index[-1]
        self.db.save_model(
            symbol, timeframe, "SGDClassifier", model, CLASSES, list(feats.columns),
            last_full_end=last_end.to_pydatetime(), last_incr_end=last_end.to_pydatetime(),
            metrics={"accuracy": acc}
        )
        logger.info("Full trained %s %s, acc=%.3f", symbol, timeframe, acc)
        return True

    def predict_hierarchical(self, symbol: str, timeframes: list, latest_windows: dict):
        preds = {}
        probs = {}
        for tf in timeframes:
            df = latest_windows.get(tf)
            meta = self.db.load_model(symbol, tf)
            if (df is not None) and (not df.empty) and meta and (meta["model"] is not None):
                feats = build_features(df)
                if feats.empty:
                    preds[tf] = None
                    probs[tf] = None
                    continue
                X = feats.values[-1:].copy()
                if hasattr(meta["model"], "predict_proba"):
                    pr = meta["model"].predict_proba(X)[0]
                    cls = meta["classes"].tolist()
                    def get_prob(val):
                        return float(pr[cls.index(val)]) if val in cls else 0.0
                    pb = {"buy": get_prob(1), "hold": get_prob(0), "sell": get_prob(-1)}
                    probs[tf] = pb
                    preds[tf] = 1 if pb["buy"] >= pb["sell"] and pb["buy"] >= pb["hold"] else (-1 if pb["sell"] > pb["hold"] else 0)
                else:
                    yh = int(meta["model"].predict(X)[0])
                    preds[tf] = yh
                    probs[tf] = {"buy": 0.5 if yh == 1 else 0.0, "hold": 0.5 if yh == 0 else 0.0, "sell": 0.5 if yh == -1 else 0.0}
            else:
                preds[tf] = None
                probs[tf] = None

        # Иерархия (старшие подтверждают): 1w -> 1d -> 4h -> 1h -> 15m
        order = ["1w", "1d", "4h", "1h", "15m"]
        order = [tf for tf in order if tf in timeframes]
        final = 0
        confidence = 0.0
        ok = True
        dir_ref = None
        weights = []
        confs = []
        for tf in order:
            if preds.get(tf) is None:
                ok = False
                break
            cur_dir = preds[tf]
            if dir_ref is None:
                dir_ref = cur_dir
            else:
                if cur_dir != dir_ref:
                    ok = False
                    break
            pr = probs[tf]
            if pr:
                conf = max(pr["buy"], pr["sell"])
                weights.append(1.0)  # TODO: можно повысить вес старших ТФ
                confs.append(conf)
        if ok and dir_ref is not None:
            final = dir_ref
            if confs:
                confidence = float(np.average(confs, weights=weights)) if len(confs) > 0 else 0.0
        else:
            final = 0
            confidence = 0.0

        return {"preds": preds, "probs": probs, "consensus": final, "confidence": confidence}