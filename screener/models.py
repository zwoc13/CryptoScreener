from __future__ import annotations

import re
from dataclasses import dataclass, field
from time import time


def to_feed_id(symbol: str) -> str:
    """Convert exchange symbol to CCXT unified format for USDT perps.

    BTCUSDT   -> BTC/USDT:USDT  (Bybit, Binance)
    BTC_USDT  -> BTC/USDT:USDT  (GateIO)
    """
    # GateIO format: BTC_USDT
    if "_USDT" in symbol:
        base = symbol.replace("_USDT", "")
        return f"{base}/USDT:USDT"
    # Bybit/Binance format: BTCUSDT
    m = re.match(r"^(.+?)USDT$", symbol)
    if m:
        return f"{m.group(1)}/USDT:USDT"
    return symbol


@dataclass(slots=True)
class CandleBar:
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    confirmed: bool = False


@dataclass(slots=True)
class TickerState:
    exchange: str
    symbol: str
    feed_id: str = ""  # CCXT unified format, e.g. "BTC/USDT:USDT"
    last_price: float = 0.0
    daily_change_pct: float = 0.0
    range_1m: float = 0.0
    range_5m: float = 0.0
    range_1h: float = 0.0
    range_4h: float = 0.0
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

    # Delisting info (0 = no delisting scheduled)
    delist_ts: float = 0.0  # epoch seconds when contract gets delisted

    # 1m range tracking internals
    minute_high: float = 0.0
    minute_low: float = float("inf")
    minute_start_ts: float = 0.0

    # Display caches populated by the client-mode HTTP poller (see client.py).
    # Server-side TUI uses its own cvd_cache dict and ignores these.
    _cvd_5m: float = 0.0
    _cvd_1h: float = 0.0


@dataclass(slots=True)
class ImpulseEvent:
    exchange: str
    symbol: str
    feed_id: str
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
    range_1h: float = 0.0
    range_4h: float = 0.0
    # Enrichment: new data streams
    open_interest: float = 0.0
    oi_change_5m_pct: float = 0.0
    liq_buys_5m: float = 0.0  # USD vol of long liquidations in last 5m
    liq_sells_5m: float = 0.0  # USD vol of short liquidations in last 5m
    long_short_ratio: float = 0.0
    # Directional bias layer (computed by screener.bias)
    cvd_4h: float = 0.0
    oi_change_1h_pct: float = 0.0
    oi_change_4h_pct: float = 0.0
    buy_pct_5m: float = 0.5
    buy_pct_15m: float = 0.5
    buy_persistence_5m: float = 0.0  # signed [-1, 1]
    long_bias_score: float = 0.0     # [0, 100]
    short_bias_score: float = 0.0    # [0, 100]
    bias_label: str = "NEUTRAL"      # LONG | SHORT | CONFLICTED | AVOID | NEUTRAL
    timestamp: float = field(default_factory=time)


@dataclass(slots=True)
class FundingAlert:
    exchange: str
    symbol: str
    feed_id: str
    rate: float
    price: float
    timestamp: float = field(default_factory=time)


# Normalized messages from exchange adapters to engine
@dataclass(slots=True)
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


@dataclass(slots=True)
class KlineMessage:
    exchange: str
    symbol: str
    interval: str  # "1" or "5"
    candle: CandleBar


@dataclass(slots=True)
class TradeMessage:
    exchange: str
    symbol: str
    side: str  # "Buy" | "Sell"
    size: float
    price: float
    timestamp: float


@dataclass(slots=True)
class OpenInterestMessage:
    exchange: str
    symbol: str
    open_interest: float  # OI in contracts/coins
    open_interest_value: float  # OI in USD (0 if unavailable)
    timestamp: float


@dataclass(slots=True)
class LiquidationMessage:
    exchange: str
    symbol: str
    side: str  # "Buy" | "Sell" (the side being liquidated)
    size: float  # quantity liquidated
    price: float
    timestamp: float


@dataclass(slots=True)
class LongShortRatioMessage:
    exchange: str
    symbol: str
    long_short_ratio: float  # >1 means more longs
    timestamp: float


@dataclass(slots=True)
class LargeOrderEvent:
    exchange: str
    symbol: str
    feed_id: str
    side: str  # "bid" | "ask"
    price: float
    size_usd: float
    distance_pct: float  # distance from current price as %
    last_price: float
    volume_24h: float
    timestamp: float = field(default_factory=time)


@dataclass(slots=True)
class OrderEatenEvent:
    exchange: str
    symbol: str
    feed_id: str
    side: str  # "bid" | "ask"
    price: float
    size_usd: float
    likely_filled: bool  # True if price moved through level
    last_price: float
    volume_24h: float
    timestamp: float = field(default_factory=time)


@dataclass(slots=True)
class NewsEvent:
    exchange: str
    news_type: str  # "delisting" | "new_listing"
    title: str
    description: str
    url: str
    event_ts: float  # when the delisting/listing happens (epoch seconds)
    symbols: list[str] = field(default_factory=list)  # affected symbols (raw)
    feed_ids: list[str] = field(default_factory=list)  # CCXT format
    timestamp: float = field(default_factory=time)


@dataclass(slots=True)
class BiasChangedEvent:
    """Bias label changed between snapshots (exit signal)."""
    exchange: str
    symbol: str
    feed_id: str
    timestamp: float = field(default_factory=time)
    correlation_id: str | None = None
    direction: str = "neutral"  # "up" | "down" | "neutral"
    confidence: float = 0.0
    prev_label: str = "NEUTRAL"
    new_label: str = "NEUTRAL"
    prev_long: float = 0.0
    prev_short: float = 0.0
    new_long: float = 0.0
    new_short: float = 0.0


@dataclass(slots=True)
class CvdBurstEvent:
    """Aggressive CVD burst relative to recent baseline (entry early-warning)."""
    exchange: str
    symbol: str
    feed_id: str
    timestamp: float = field(default_factory=time)
    correlation_id: str | None = None
    direction: str = "neutral"  # "up" | "down" | "neutral"
    confidence: float = 0.0
    cvd_burst_1m: float = 0.0
    cvd_baseline_5m: float = 0.0
    burst_ratio: float = 0.0


@dataclass(slots=True)
class LiquidationCascadeEvent:
    """Liquidation volume spike — forced flow (react differently than organic)."""
    exchange: str
    symbol: str
    feed_id: str
    timestamp: float = field(default_factory=time)
    correlation_id: str | None = None
    direction: str = "neutral"
    confidence: float = 0.0
    liq_usd_1m: float = 0.0
    liq_baseline_5m: float = 0.0
    burst_ratio: float = 0.0
    dominant_side: str = "-"  # "buy" | "sell" | "mixed"


@dataclass(slots=True)
class OiFlipEvent:
    """OI change sign flipped and persisted (position-closing phase, exit signal)."""
    exchange: str
    symbol: str
    feed_id: str
    timestamp: float = field(default_factory=time)
    correlation_id: str | None = None
    direction: str = "neutral"
    confidence: float = 0.0
    oi_change_prev: float = 0.0
    oi_change_now: float = 0.0
    persist_ticks: int = 0


@dataclass(slots=True)
class DivergenceEvent:
    """Price made new high/low in window but CVD did not (exhaustion, fade signal)."""
    exchange: str
    symbol: str
    feed_id: str
    timestamp: float = field(default_factory=time)
    correlation_id: str | None = None
    direction: str = "neutral"
    confidence: float = 0.0
    price_high: float = 0.0
    price_low: float = 0.0
    cvd_high: float = 0.0
    cvd_low: float = 0.0


@dataclass(slots=True)
class VolumeSpikeEvent:
    """5m volume > K × 24h baseline (attention signal, not directional)."""
    exchange: str
    symbol: str
    feed_id: str
    timestamp: float = field(default_factory=time)
    correlation_id: str | None = None
    direction: str = "neutral"
    confidence: float = 0.0
    volume_5m: float = 0.0
    volume_baseline: float = 0.0
    spike_ratio: float = 0.0


@dataclass(slots=True)
class TrendChangedEvent:
    """Trend string changed between snapshots (regime change, adjust risk)."""
    exchange: str
    symbol: str
    feed_id: str
    timestamp: float = field(default_factory=time)
    correlation_id: str | None = None
    direction: str = "neutral"
    confidence: float = 0.0
    prev_trend: str = "-"
    new_trend: str = "-"


# ── Compatibility aliases for types that botctl defined separately ──────────
# These allow trading bots to keep using the old names.
FundingEvent = FundingAlert
ScreenerEvent = ImpulseEvent | FundingAlert | LargeOrderEvent | OrderEatenEvent | NewsEvent
TickerData = TickerState  # bots used TickerData; in unified app they get the full TickerState
