from __future__ import annotations

import asyncio
import logging
import re
from time import time

import httpx

from .config import Settings
from .models import NewsEvent, to_feed_id
from .store import Store

logger = logging.getLogger(__name__)

_BYBIT_ANNOUNCEMENT_URL = "https://api.bybit.com/v5/announcements/index"

# Maps Bybit announcement type.key to our news_type
_TYPE_MAP = {
    "delistings": "delisting",
    "new_crypto": "new_listing",
}

# Regex to extract symbol-like tokens from announcement titles
_SYMBOL_RE = re.compile(r"\b([A-Z0-9]{2,}USDT)\b")


def _extract_symbols(title: str, tracked: set[str]) -> list[str]:
    """Extract exchange symbol names from announcement title."""
    candidates = _SYMBOL_RE.findall(title)
    # Prefer symbols we're actually tracking, but keep all matches
    result = []
    seen = set()
    for sym in candidates:
        if sym not in seen:
            seen.add(sym)
            result.append(sym)
    return result


async def run_news_poller(
    alert_queue: asyncio.Queue,
    settings: Settings,
    store: Store,
) -> None:
    """Poll Bybit announcements API for delistings and new listings."""
    cfg = settings.news
    interval = cfg.poll_interval_s
    lookback_s = cfg.lookback_hours * 3600
    seen_urls: set[str] = set()

    # Get the Bybit REST URL from exchange config (fallback to public API)
    bybit_cfg = settings.exchanges.get("bybit")
    base_url = bybit_cfg.rest_url if bybit_cfg else "https://api.bybit.com"

    logger.info("News poller started (interval=%ds, lookback=%dh)", interval, cfg.lookback_hours)

    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            try:
                tracked_symbols = {t.symbol for t in store.get_all_sorted()}

                for ann_type, news_type in _TYPE_MAP.items():
                    try:
                        resp = await client.get(
                            f"{base_url}/v5/announcements/index",
                            params={
                                "locale": "en-US",
                                "type": ann_type,
                                "limit": 50,
                            },
                        )
                        if resp.status_code != 200:
                            logger.warning("News API returned %d for %s", resp.status_code, ann_type)
                            continue

                        data = resp.json()
                        items = data.get("result", {}).get("list", [])

                        now = time()
                        cutoff = now - lookback_s

                        for item in items:
                            url = item.get("url", "")
                            if url in seen_urls:
                                continue

                            publish_ts = item.get("publishTime", 0) / 1000
                            if publish_ts < cutoff:
                                continue

                            title = item.get("title", "")
                            description = item.get("description", "")
                            event_ts = item.get("dateTimestamp", 0) / 1000

                            symbols = _extract_symbols(title, tracked_symbols)
                            feed_ids = [to_feed_id(s) for s in symbols]

                            seen_urls.add(url)

                            # Update delist_ts on tracked tickers
                            if news_type == "delisting" and event_ts > 0:
                                for sym in symbols:
                                    ticker = store.get_ticker("bybit", sym)
                                    if ticker is not None:
                                        ticker.delist_ts = event_ts

                            event = NewsEvent(
                                exchange="bybit",
                                news_type=news_type,
                                title=title,
                                description=description,
                                url=url,
                                event_ts=event_ts,
                                symbols=symbols,
                                feed_ids=feed_ids,
                            )
                            try:
                                alert_queue.put_nowait(event)
                            except asyncio.QueueFull:
                                pass
                            logger.info(
                                "News: %s — %s (symbols: %s)",
                                news_type, title, ", ".join(symbols) or "none",
                            )

                    except asyncio.CancelledError:
                        return
                    except Exception:
                        logger.exception("News poll error for %s", ann_type)

            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("News poller error")

            await asyncio.sleep(interval)
