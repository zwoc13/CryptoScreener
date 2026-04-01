from __future__ import annotations

import asyncio
import logging
import re
from time import time
from typing import TYPE_CHECKING

import httpx

from .config import Settings
from .models import NewsEvent, to_feed_id
from .store import Store

if TYPE_CHECKING:
    from .exchanges import BaseExchange

logger = logging.getLogger(__name__)

_BYBIT_ANNOUNCEMENT_URL = "https://api.bybit.com/v5/announcements/index"

# Maps Bybit announcement type.key to our news_type
_TYPE_MAP = {
    "delistings": "delisting",
    "new_crypto": "new_listing",
}

# Regex to extract symbol-like tokens from announcement titles
_SYMBOL_RE = re.compile(r"\b([A-Z0-9]{2,}USDT)\b")

# Buffer after event_ts before attempting subscribe/unsubscribe (seconds)
_LISTING_BUFFER_S = 60
_DELIST_BUFFER_S = 30


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


async def _schedule_new_listing(
    symbol: str,
    event_ts: float,
    exchange: BaseExchange,
    store: Store,
    msg_queue: asyncio.Queue,
) -> None:
    """Wait until listing time, then fetch klines and subscribe to WS streams."""
    now = time()
    delay = event_ts + _LISTING_BUFFER_S - now
    if delay > 0:
        logger.info(
            "New listing %s: waiting %.0fs until %d to subscribe",
            symbol, delay, int(event_ts),
        )
        await asyncio.sleep(delay)

    # Verify symbol is actually live now
    try:
        live_symbols = await exchange.fetch_symbols()
    except Exception:
        logger.exception("Failed to fetch symbols for new listing %s", symbol)
        return

    if symbol not in live_symbols:
        logger.warning("New listing %s not found in live symbols after event_ts, skipping", symbol)
        return

    # Warmup klines
    try:
        candles = await exchange.fetch_klines(symbol, "5", 50)
        if candles:
            store.load_candles(exchange.name, symbol, candles)
            logger.info("Warmed up %d klines for new listing %s", len(candles), symbol)
    except Exception:
        logger.warning("Kline warmup failed for new listing %s", symbol)

    # Subscribe to live WS streams
    try:
        await exchange.subscribe_symbol(symbol, msg_queue)
        logger.info("Subscribed to new listing %s", symbol)
    except Exception:
        logger.exception("Failed to subscribe to new listing %s", symbol)


async def _schedule_delist(
    symbol: str,
    event_ts: float,
    exchange: BaseExchange,
    store: Store,
) -> None:
    """Wait until delist time, then unsubscribe and remove ticker data."""
    now = time()
    delay = event_ts + _DELIST_BUFFER_S - now
    if delay > 0:
        logger.info(
            "Delisting %s: waiting %.0fs until %d to unsubscribe",
            symbol, delay, int(event_ts),
        )
        await asyncio.sleep(delay)

    try:
        await exchange.unsubscribe_symbol(symbol)
        logger.info("Unsubscribed delisted ticker %s", symbol)
    except Exception:
        logger.warning("Failed to unsubscribe delisted ticker %s", symbol)

    store.remove_ticker(exchange.name, symbol)
    logger.info("Removed delisted ticker %s from store", symbol)


async def run_news_poller(
    alert_queue: asyncio.Queue,
    settings: Settings,
    store: Store,
    exchanges: dict[str, BaseExchange] | None = None,
    msg_queue: asyncio.Queue | None = None,
) -> None:
    """Poll Bybit announcements API for delistings and new listings."""
    cfg = settings.news
    interval = cfg.poll_interval_s
    lookback_s = cfg.lookback_hours * 3600
    seen_urls: set[str] = set()
    bg_tasks: set[asyncio.Task] = set()

    # Get the Bybit REST URL from exchange config (fallback to public API)
    bybit_cfg = settings.exchanges.get("bybit")
    base_url = bybit_cfg.rest_url if bybit_cfg else "https://api.bybit.com"

    bybit_exchange = exchanges.get("bybit") if exchanges else None

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

                            # Schedule dynamic subscribe/unsubscribe based on event type
                            if bybit_exchange is not None and event_ts > 0:
                                if news_type == "delisting":
                                    for sym in symbols:
                                        # Set delist_ts so engine suppresses signals
                                        ticker = store.get_ticker("bybit", sym)
                                        if ticker is not None:
                                            ticker.delist_ts = event_ts
                                        # Schedule actual removal at delist time
                                        task = asyncio.create_task(
                                            _schedule_delist(sym, event_ts, bybit_exchange, store),
                                            name=f"delist-{sym}",
                                        )
                                        bg_tasks.add(task)
                                        task.add_done_callback(bg_tasks.discard)

                                elif news_type == "new_listing" and msg_queue is not None:
                                    for sym in symbols:
                                        if sym not in tracked_symbols:
                                            task = asyncio.create_task(
                                                _schedule_new_listing(
                                                    sym, event_ts, bybit_exchange, store, msg_queue,
                                                ),
                                                name=f"listing-{sym}",
                                            )
                                            bg_tasks.add(task)
                                            task.add_done_callback(bg_tasks.discard)

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
