from __future__ import annotations

import asyncio
import logging
from collections import deque
from time import time
from typing import TYPE_CHECKING

from .models import LargeOrderEvent, OrderEatenEvent

if TYPE_CHECKING:
    from .config import Settings
    from .store import Store

logger = logging.getLogger("screener.orderbook")


class OrderbookManager:
    """Maintains local orderbook state and detects large resting orders."""

    def __init__(
        self,
        store: Store,
        alert_queue: asyncio.Queue,
        settings: Settings,
    ) -> None:
        self._store = store
        self._alert_queue = alert_queue
        self._settings = settings
        self._cfg = settings.orderbook

        # Local orderbook: {exchange:symbol -> {"bids": {price: size}, "asks": {price: size}}}
        self._books: dict[str, dict[str, dict[float, float]]] = {}

        # Currently tracked large orders: {exchange:symbol -> {(side, price): LargeOrderEvent}}
        self._tracked: dict[str, dict[tuple[str, float], LargeOrderEvent]] = {}

        # Active large orders for TUI display (most recent across all symbols)
        self.active_large_orders: deque[LargeOrderEvent] = deque(maxlen=200)

        # Recent eaten events for TUI display
        self.recent_eaten: deque[OrderEatenEvent] = deque(maxlen=200)

    def process_update(
        self,
        exchange: str,
        symbol: str,
        msg_type: str,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
    ) -> None:
        """Process an orderbook snapshot or delta update.

        Called directly from the WS handler — must be fast.
        """
        key = f"{exchange}:{symbol}"

        if msg_type == "snapshot":
            self._books[key] = {
                "bids": {price: size for price, size in bids},
                "asks": {price: size for price, size in asks},
            }
        else:
            book = self._books.get(key)
            if book is None:
                # Delta before snapshot — skip
                return
            for price, size in bids:
                if size == 0:
                    book["bids"].pop(price, None)
                else:
                    book["bids"][price] = size
            for price, size in asks:
                if size == 0:
                    book["asks"].pop(price, None)
                else:
                    book["asks"][price] = size

        self._scan_large_orders(exchange, symbol, key)

    def _scan_large_orders(self, exchange: str, symbol: str, key: str) -> None:
        ticker = self._store._tickers.get(key)
        if ticker is None:
            return

        last_price = ticker.last_price
        volume_24h = ticker.volume_24h
        if last_price <= 0 or volume_24h <= 0:
            return

        threshold_usd = max(
            volume_24h * last_price * self._cfg.threshold_pct / 100,
            self._cfg.min_size_usd,
        )
        max_dist = self._cfg.max_distance_pct
        now = time()

        book = self._books.get(key)
        if book is None:
            return

        # Build current set of large orders
        current: dict[tuple[str, float], LargeOrderEvent] = {}

        for side_name, levels in (("bid", book["bids"]), ("ask", book["asks"])):
            for price, size in levels.items():
                size_usd = size * price
                if size_usd < threshold_usd:
                    continue
                dist_pct = abs(price - last_price) / last_price * 100
                if dist_pct > max_dist:
                    continue
                current[(side_name, price)] = LargeOrderEvent(
                    exchange=exchange,
                    symbol=symbol,
                    side=side_name,
                    price=price,
                    size_usd=size_usd,
                    distance_pct=dist_pct,
                    last_price=last_price,
                    volume_24h=volume_24h,
                    timestamp=now,
                )

        prev = self._tracked.get(key, {})

        # Detect new large orders
        for loc, event in current.items():
            if loc not in prev:
                self.active_large_orders.append(event)
                if self._cfg.alert_on_new:
                    try:
                        self._alert_queue.put_nowait(event)
                    except asyncio.QueueFull:
                        pass

        # Detect removed (eaten/cancelled) large orders
        for loc, old_event in prev.items():
            if loc not in current:
                self._handle_removal(exchange, symbol, old_event, last_price)

        # Update tracked set — also update size_usd for still-present orders
        self._tracked[key] = current

    def _handle_removal(
        self,
        exchange: str,
        symbol: str,
        old_event: LargeOrderEvent,
        last_price: float,
    ) -> None:
        # Heuristic: filled if price moved through the order level
        if old_event.side == "bid":
            likely_filled = last_price <= old_event.price
        else:
            likely_filled = last_price >= old_event.price

        eaten = OrderEatenEvent(
            exchange=exchange,
            symbol=symbol,
            side=old_event.side,
            price=old_event.price,
            size_usd=old_event.size_usd,
            likely_filled=likely_filled,
            last_price=last_price,
            volume_24h=old_event.volume_24h,
        )
        self.recent_eaten.append(eaten)

        # Remove from active display
        try:
            self.active_large_orders.remove(old_event)
        except ValueError:
            pass

        if self._cfg.alert_on_eaten:
            try:
                self._alert_queue.put_nowait(eaten)
            except asyncio.QueueFull:
                pass
