from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from ..models import CandleBar

if TYPE_CHECKING:
    from ..orderbook import OrderbookManager


class BaseExchange(ABC):
    """Common interface for all exchange adapters."""

    name: str  # e.g. "bybit", "binance"

    @abstractmethod
    async def fetch_symbols(self) -> list[str]:
        """Return list of active USDT perpetual futures symbols."""
        ...

    @abstractmethod
    async def fetch_klines(
        self, symbol: str, interval: str, limit: int
    ) -> list[CandleBar]:
        """Fetch historical klines for a symbol."""
        ...

    @abstractmethod
    async def start_streams(
        self,
        symbols: list[str],
        queue: asyncio.Queue,
        cvd_enabled: bool = True,
        orderbook_manager: OrderbookManager | None = None,
    ) -> None:
        """Connect WS and push TickerMessage/KlineMessage to queue. Blocks until stopped."""
        ...

    async def subscribe_symbol(self, symbol: str, queue: asyncio.Queue) -> None:
        """Dynamically subscribe to a new symbol on live WS connections."""

    async def unsubscribe_symbol(self, symbol: str) -> None:
        """Dynamically unsubscribe a symbol from live WS connections."""

    @abstractmethod
    async def stop(self) -> None:
        """Gracefully close all connections."""
        ...


# Registry of available exchanges
_registry: dict[str, type[BaseExchange]] = {}


def register_exchange(cls: type[BaseExchange]) -> type[BaseExchange]:
    _registry[cls.name] = cls
    return cls


def get_exchange_class(name: str) -> type[BaseExchange]:
    return _registry[name]


def available_exchanges() -> list[str]:
    return list(_registry.keys())
