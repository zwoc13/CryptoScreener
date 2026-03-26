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

    # Open Interest
    open_interest: float = 0.0  # current OI in USD
    oi_change_5m_pct: float = 0.0  # % change in OI over last 5m

    # Long/Short ratio
    long_short_ratio: float = 0.0  # >1 means more longs

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
    # Enrichment: existing computed data
    cvd_5m: float = 0.0
    cvd_1h: float = 0.0
    cvd_daily: float = 0.0
    funding_rate: float = 0.0
    trend: str = "-"
    daily_change_pct: float = 0.0
    range_1m: float = 0.0
    range_5m: float = 0.0
    # Enrichment: new data streams
    open_interest: float = 0.0
    oi_change_5m_pct: float = 0.0
    liq_buys_5m: float = 0.0  # USD vol of long liquidations in last 5m
    liq_sells_5m: float = 0.0  # USD vol of short liquidations in last 5m
    long_short_ratio: float = 0.0
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
    open_interest: float | None = None  # OI in contracts/coins
    open_interest_value: float | None = None  # OI in USD


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
class OpenInterestMessage:
    exchange: str
    symbol: str
    open_interest: float  # OI in contracts/coins
    open_interest_value: float  # OI in USD (0 if unavailable)
    timestamp: float


@dataclass
class LiquidationMessage:
    exchange: str
    symbol: str
    side: str  # "Buy" | "Sell" (the side being liquidated)
    size: float  # quantity liquidated
    price: float
    timestamp: float


@dataclass
class LongShortRatioMessage:
    exchange: str
    symbol: str
    long_short_ratio: float  # >1 means more longs
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
