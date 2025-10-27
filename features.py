import pandas as pd
import numpy as np

def sma(series: pd.Series, n=20):
    return series.rolling(n).mean()

def ema(series: pd.Series, n=20):
    return series.ewm(span=n, adjust=False).mean()

def rsi(series: pd.Series, n=14):
    delta = series.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(up, index=series.index).ewm(span=n, adjust=False).mean()
    roll_down = pd.Series(down, index=series.index).ewm(span=n, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-9)
    return 100 - (100 / (1 + rs))

def macd(series: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def bollinger(series: pd.Series, n=20, k=2):
    m = sma(series, n)
    sd = series.rolling(n).std()
    upper = m + k * sd
    lower = m - k * sd
    return m, upper, lower

def atr(df: pd.DataFrame, n=14):
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()

def candlestick_patterns(df: pd.DataFrame):
    # простые паттерны: doji, engulfing (bull/bear)
    body = (df["close"] - df["open"]).abs()
    range_ = (df["high"] - df["low"]).replace(0, np.nan)
    doji = (body / range_) < 0.1
    prev_open = df["open"].shift()
    prev_close = df["close"].shift()
    bull_engulf = (df["close"] > df["open"]) & (prev_close < prev_open) & (df["close"] >= prev_open) & (df["open"] <= prev_close)
    bear_engulf = (df["close"] < df["open"]) & (prev_close > prev_open) & (df["close"] <= prev_open) & (df["open"] >= prev_close)
    return pd.DataFrame(
        {
            "doji": doji.fillna(False).astype(int),
            "bull_engulf": bull_engulf.fillna(False).astype(int),
            "bear_engulf": bear_engulf.fillna(False).astype(int),
        },
        index=df.index,
    )

def build_features(df: pd.DataFrame):
    out = pd.DataFrame(index=df.index)
    out["ret_1"] = df["close"].pct_change()
    out["sma_20"] = sma(df["close"], 20)
    out["ema_20"] = ema(df["close"], 20)
    out["rsi_14"] = rsi(df["close"], 14)
    macd_line, signal_line, hist = macd(df["close"])
    out["macd"] = macd_line
    out["macd_sig"] = signal_line
    out["macd_hist"] = hist
    m, up, lo = bollinger(df["close"])
    out["bb_mid"] = m
    out["bb_up"] = up
    out["bb_lo"] = lo
    out["atr_14"] = atr(df, 14)
    patt = candlestick_patterns(df)
    out = pd.concat([out, patt], axis=1)

    # Fix FutureWarning: use .ffill() instead of fillna(method="ffill")
    out = out.ffill().fillna(0)

    # гарантируем числовые типы
    for c in out.columns:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.fillna(0)
    return out

def make_labels(df: pd.DataFrame, horizon=1, up_thr=0.002, down_thr=-0.002):
    # Через 1 bar изменение; BUY=1, SELL=-1, HOLD=0
    fut = df["close"].shift(-horizon)
    ret = (fut - df["close"]) / df["close"]
    y = ret.copy()
    y[:] = 0
    y[ret >= up_thr] = 1
    y[ret <= down_thr] = -1
    return y.astype(int)