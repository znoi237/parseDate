import os
import logging

class Config:
    DB_PATH = os.environ.get("DB_PATH", "ai_trader.db")
    # Список пар по умолчанию
    SYMBOLS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT", "ADA/USDT"]
    # Таймфреймы (по ТЗ: 15m, 1h, 4h, 1d, 1w)
    TIMEFRAMES = ["15m", "1h", "4h", "1d", "1w"]
    # Глубина истории (лет) по умолчанию
    HISTORY_YEARS = int(os.environ.get("HISTORY_YEARS", "3"))
    # Порог уверенности (0..1)
    SIGNAL_THRESHOLD = float(os.environ.get("SIGNAL_THRESHOLD", "0.8"))
    # WebSocket — сколько хранить свечей в памяти на TF
    WS_CACHE_MAX = int(os.environ.get("WS_CACHE_MAX", "1000"))
    # Новости (RSS) — список фидов
    NEWS_FEEDS = [
        "https://www.binance.com/en/support/announcement/rss",
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://cointelegraph.com/rss"
    ]
    # Новости: окно агрегации для фич (минуты)
    NEWS_AGG_MINUTES = int(os.environ.get("NEWS_AGG_MINUTES", "60"))
    # Многопоточность обучения
    MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "4"))
    # CCXT exchange id
    EXCHANGE_ID = os.environ.get("EXCHANGE_ID", "binance")
    # Торговля только тестнет
    TRADE_TESTNET = True
    # Включать обработку WebSocket автоматически
    ENABLE_WS = True
    # Путь для сохранения моделей
    MODELS_DIR = os.environ.get("MODELS_DIR", "models")

def configure_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    )