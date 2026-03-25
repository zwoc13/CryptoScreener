"""Client mode: TUI that connects to a running screener server via REST API."""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from time import time
from zoneinfo import ZoneInfo

import httpx

from .config import Settings
from .models import (
    CandleBar,
    FundingAlert,
    ImpulseEvent,
    LargeOrderEvent,
    OrderEatenEvent,
    TickerState,
)

logger = logging.getLogger("screener.client")


class RemoteStore:
    """Drop-in replacement for Store that fetches data from the server API."""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url
        self._tickers: dict[str, TickerState] = {}
        self._client = httpx.AsyncClient(base_url=base_url, timeout=10)
        self.last_message_ts: float = 0

    @property
    def symbol_count(self) -> int:
        return len(self._tickers)

    def get_cvd_rolling(self, exchange: str, symbol: str, seconds: int) -> float:
        key = f"{exchange}:{symbol}"
        t = self._tickers.get(key)
        if not t:
            return 0
        # CVD values are stored on the ticker from the API response
        if seconds <= 300:
            return getattr(t, "_cvd_5m", 0)
        return getattr(t, "_cvd_1h", 0)

    async def refresh(self) -> None:
        """Poll the server API and update local ticker state."""
        try:
            resp = await self._client.get("/tickers", params={"limit": 500})
            resp.raise_for_status()
            rows = resp.json()
        except Exception:
            logger.debug("Failed to fetch tickers from server")
            return

        self.last_message_ts = time()
        seen: set[str] = set()
        for row in rows:
            key = f"{row['exchange']}:{row['symbol']}"
            seen.add(key)
            t = self._tickers.get(key)
            if t is None:
                t = TickerState(exchange=row["exchange"], symbol=row["symbol"])
                self._tickers[key] = t
            t.last_price = row.get("last_price", 0)
            t.daily_change_pct = row.get("daily_change_pct", 0)
            t.range_1m = row.get("range_1m", 0)
            t.range_5m = row.get("range_5m", 0)
            t.natr_5m_14 = row.get("natr_5m_14", 0)
            t.volume_24h = row.get("volume_24h", 0)
            t.funding_rate = row.get("funding_rate", 0)
            t.funding_interval_h = row.get("funding_interval_h", 8)
            t.next_funding_ts = row.get("next_funding_ts", 0)
            t.trend = row.get("trend", "-")
            t.last_update_ts = row.get("last_update_ts", 0)
            # Stash CVD values for display
            t._cvd_5m = row.get("cvd_5m", 0)
            t._cvd_1h = row.get("cvd_1h", 0)

        # Remove tickers that disappeared from server
        stale = set(self._tickers) - seen
        for k in stale:
            del self._tickers[k]

    async def close(self) -> None:
        await self._client.aclose()


class RemoteExchange:
    """Minimal exchange adapter that fetches klines via the server API."""

    name = "remote"

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url
        self._client = httpx.AsyncClient(base_url=base_url, timeout=15)

    async def fetch_klines(
        self, symbol: str, interval: str, limit: int
    ) -> list[CandleBar]:
        try:
            resp = await self._client.get(
                f"/klines/{symbol}",
                params={"interval": interval, "limit": limit},
            )
            resp.raise_for_status()
            rows = resp.json()
        except Exception:
            logger.debug("Failed to fetch klines for %s", symbol)
            return []
        return [
            CandleBar(
                timestamp=r["timestamp"],
                open=r["open"],
                high=r["high"],
                low=r["low"],
                close=r["close"],
                volume=r["volume"],
                confirmed=True,
            )
            for r in rows
        ]

    async def close(self) -> None:
        await self._client.aclose()


def _parse_alert(data: dict):
    """Reconstruct alert event from API JSON."""
    # The /alerts/recent endpoint returns dicts with all fields
    if "direction" in data:
        return ImpulseEvent(**{k: data[k] for k in ImpulseEvent.__dataclass_fields__})
    if "rate" in data:
        return FundingAlert(**{k: data[k] for k in FundingAlert.__dataclass_fields__})
    if "likely_filled" in data:
        return OrderEatenEvent(**{k: data[k] for k in OrderEatenEvent.__dataclass_fields__})
    if "distance_pct" in data:
        return LargeOrderEvent(**{k: data[k] for k in LargeOrderEvent.__dataclass_fields__})
    return None


async def _poll_alerts(base_url: str, alert_history: deque) -> None:
    """Background task that polls /alerts/recent and fills the deque."""
    async with httpx.AsyncClient(base_url=base_url, timeout=10) as client:
        seen_ts: set[tuple] = set()  # (symbol, timestamp) to dedupe
        while True:
            try:
                resp = await client.get("/alerts/recent", params={"limit": 100})
                resp.raise_for_status()
                alerts = resp.json()
                for data in reversed(alerts):  # oldest first
                    key = (data.get("symbol", ""), data.get("timestamp", 0))
                    if key not in seen_ts:
                        seen_ts.add(key)
                        event = _parse_alert(data)
                        if event:
                            alert_history.appendleft(event)
                # Keep seen_ts bounded
                if len(seen_ts) > 2000:
                    seen_ts.clear()
            except Exception:
                pass
            await asyncio.sleep(2)


async def _poll_store(store: RemoteStore, interval: float) -> None:
    """Background task that refreshes the remote store."""
    while True:
        await store.refresh()
        await asyncio.sleep(interval)


async def run_client(server_url: str, settings: Settings) -> None:
    """Launch the TUI in client mode, connecting to a remote server."""
    from .tui import ScreenerApp

    logger.info("Connecting to server at %s", server_url)

    store = RemoteStore(server_url)
    remote_ex = RemoteExchange(server_url)
    alert_history: deque = deque(maxlen=1000)

    # Initial fetch
    await store.refresh()
    logger.info("Connected — %d symbols from server", store.symbol_count)

    # Background pollers
    poll_store_task = asyncio.create_task(
        _poll_store(store, settings.tui.refresh_ms / 1000)
    )
    poll_alerts_task = asyncio.create_task(
        _poll_alerts(server_url, alert_history)
    )

    # All exchanges point to the remote proxy for charts
    exchange_map = {"bybit": remote_ex}

    try:
        tui_app = ScreenerApp(
            store=store,
            settings=settings,
            alert_history=alert_history,
            exchanges=exchange_map,
        )
        await tui_app.run_async()
    finally:
        poll_store_task.cancel()
        poll_alerts_task.cancel()
        await store.close()
        await remote_ex.close()
