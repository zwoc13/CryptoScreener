from __future__ import annotations

from dataclasses import dataclass, field
from time import time


@dataclass
class CandleBar:
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    confirmed: bool = False


@dataclass
class TickerState:
    exchange: str
    symbol: str
    last_price: float = 0.0
    daily_change_pct: float = 0.0
    range_1m: float = 0.0
    range_5m: float = 0.0
    natr_5m_14: float = 0.0
    volume_24h: float = 0.0
    funding_rate: float = 0.0
    funding_interval_h: int = 8  # funding interval in hours
    next_funding_ts: float = 0.0  # epoch seconds of next funding
    trend: str = "-"  # "UP" | "DOWN" | "RANGE" | "-"
    reset_price: float = 0.0  # price at daily reset (2 AM)
    last_update_ts: float = field(default_factory=time)

    # 1m range tracking internals
    minute_high: float = 0.0
    minute_low: float = float("inf")
    minute_start_ts: float = 0.0


@dataclass
class ImpulseEvent:
    exchange: str
    symbol: str
    direction: str  # "up" | "down"
    change_pct: float
    natr_value: float
    price: float
    volume_24h: float
    timestamp: float = field(default_factory=time)


@dataclass
class FundingAlert:
    exchange: str
    symbol: str
    rate: float
    price: float
    timestamp: float = field(default_factory=time)


# Normalized messages from exchange adapters to engine
@dataclass
class TickerMessage:
    exchange: str
    symbol: str
    last_price: float | None = None
    volume_24h: float | None = None
    funding_rate: float | None = None
    funding_interval_h: int | None = None
    next_funding_ts: float | None = None  # epoch seconds
    high_24h: float | None = None
    low_24h: float | None = None


@dataclass
class KlineMessage:
    exchange: str
    symbol: str
    interval: str  # "1" or "5"
    candle: CandleBar


@dataclass
class TradeMessage:
    exchange: str
    symbol: str
    side: str  # "Buy" | "Sell"
    size: float
    price: float
    timestamp: float


@dataclass
class LargeOrderEvent:
    exchange: str
    symbol: str
    side: str  # "bid" | "ask"
    price: float
    size_usd: float
    distance_pct: float  # distance from current price as %
    last_price: float
    volume_24h: float
    timestamp: float = field(default_factory=time)


@dataclass
class OrderEatenEvent:
    exchange: str
    symbol: str
    side: str  # "bid" | "ask"
    price: float
    size_usd: float
    likely_filled: bool  # True if price moved through level
    last_price: float
    volume_24h: float
    timestamp: float = field(default_factory=time)
