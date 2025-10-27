import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone
from config import Config
from database import DatabaseManager
import logging

logger = logging.getLogger("news")

# Простая шкала сентимента: +1/-1/0 по ключевым словам (для MVP)
POS = ["surge","rally","bull","soar","win","partnership","approval","growth","record","all-time high","positive","gain","up"]
NEG = ["drop","fall","bear","crash","hack","exploit","ban","lawsuit","negative","loss","down","decline"]

def simple_sentiment(text: str):
    t = text.lower()
    score = 0
    for w in POS:
        if w in t: score += 1
    for w in NEG:
        if w in t: score -= 1
    return float(max(-3, min(3, score))) / 3.0

class NewsIngestor:
    def __init__(self, db: DatabaseManager):
        self.db = db
        self._stop = False
        self._task = None

    async def start(self):
        self._stop = False
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self._stop = True
        if self._task:
            await asyncio.sleep(0.1)

    async def _loop(self):
        # Простой цикл обновления RSS каждые 10 минут
        async with aiohttp.ClientSession() as session:
            while not self._stop:
                try:
                    await self._fetch_feeds(session)
                except Exception as e:
                    logger.warning("news loop error: %s", e)
                await asyncio.sleep(600)

    async def _fetch_feeds(self, session: aiohttp.ClientSession):
        for url in Config.NEWS_FEEDS:
            try:
                async with session.get(url, timeout=20) as r:
                    txt = await r.text()
                # примитивный RSS parse “по ключам”; для прод — используйте feedparser
                items = []
                for chunk in txt.split("<item>")[1:]:
                    try:
                        title = chunk.split("<title>")[1].split("</title>")[0]
                        link = chunk.split("<link>")[1].split("</link>")[0]
                        pubstr = chunk.split("<pubDate>")[1].split("</pubDate>")[0]
                        try:
                            published = datetime.strptime(pubstr[:25], "%a, %d %b %Y %H:%M:%S")
                        except:
                            published = datetime.utcnow()
                        desc = ""
                        if "<description>" in chunk:
                            desc = chunk.split("<description>")[1].split("</description>")[0]
                        items.append((title, link, published, desc))
                    except Exception:
                        continue
                for (title, link, published, desc) in items:
                    sent = simple_sentiment(title + " " + desc)
                    self.db.add_news(provider=url, title=title, url=link, published_at=published, summary=desc, sentiment=sent, symbols_csv="")
                logger.info("news fetched %s items from %s", len(items), url)
            except Exception as e:
                logger.debug("news fetch error %s: %s", url, e)