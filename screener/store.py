from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from time import time

from .models import CandleBar, TickerState, to_feed_id


def _key(exchange: str, symbol: str) -> str:
    return f"{exchange}:{symbol}"


@dataclass
class CvdBucket:
    """One-second CVD bucket."""
    timestamp: int  # truncated epoch second
    delta: float = 0.0  # buy_vol - sell_vol in this second
    buy_vol: float = 0.0
    sell_vol: float = 0.0


@dataclass
class LiqBucket:
    """One-second liquidation bucket."""
    timestamp: int  # truncated epoch second
    buy_liq_usd: float = 0.0  # USD vol of long liquidations
    sell_liq_usd: float = 0.0  # USD vol of short liquidations


class Store:
    def __init__(self, natr_period: int = 14) -> None:
        self._tickers: dict[str, TickerState] = {}
        self._candles: dict[str, deque[CandleBar]] = {}
        self._natr_period = natr_period
        self._lock = asyncio.Lock()
        self._symbol_count = 0
        self._last_message_ts: float = 0.0
        # CVD: per-symbol ring buffer of 1-second buckets (1 hour)
        self._cvd_buckets: dict[str, deque[CvdBucket]] = {}
        # CVD daily accumulator (reset at 2 AM)
        self._cvd_daily: dict[str, float] = {}
        # Liquidation: per-symbol ring buffer of 1-second buckets (1 hour)
        self._liq_buckets: dict[str, deque[LiqBucket]] = {}
        # OI snapshots: per-symbol ring buffer of (timestamp, oi_value) for 5m change
        self._oi_snapshots: dict[str, deque[tuple[float, float]]] = {}

    def get_or_create_ticker(self, exchange: str, symbol: str) -> TickerState:
        k = _key(exchange, symbol)
        if k not in self._tickers:
            self._tickers[k] = TickerState(exchange=exchange, symbol=symbol, feed_id=to_feed_id(symbol))
            self._symbol_count = len(self._tickers)
        return self._tickers[k]

    def get_ticker(self, exchange: str, symbol: str) -> TickerState | None:
        return self._tickers.get(_key(exchange, symbol))

    def get_all_sorted(self) -> list[TickerState]:
        tickers = list(self._tickers.values())
        tickers.sort(key=lambda t: abs(t.daily_change_pct), reverse=True)
        return tickers

    def touch(self) -> None:
        self._last_message_ts = time()

    # -- Candle buffer management --

    def get_candles(self, exchange: str, symbol: str) -> deque[CandleBar]:
        k = _key(exchange, symbol)
        if k not in self._candles:
            self._candles[k] = deque(maxlen=max(self._natr_period + 1, 50))
        return self._candles[k]

    def add_candle(self, exchange: str, symbol: str, candle: CandleBar) -> None:
        buf = self.get_candles(exchange, symbol)
        if buf and buf[-1].timestamp == candle.timestamp:
            buf[-1] = candle
        else:
            buf.append(candle)

    def load_candles(self, exchange: str, symbol: str, candles: list[CandleBar]) -> None:
        k = _key(exchange, symbol)
        self._candles[k] = deque(candles, maxlen=max(self._natr_period + 1, 50))

    def get_range(self, exchange: str, symbol: str, n_candles: int) -> float:
        """High-low range over the last n confirmed 5m candles."""
        k = _key(exchange, symbol)
        candles = self._candles.get(k)
        if not candles:
            return 0.0
        confirmed = [c for c in candles if c.confirmed][-n_candles:]
        if not confirmed:
            return 0.0
        return max(c.high for c in confirmed) - min(c.low for c in confirmed)

    # -- CVD (Cumulative Volume Delta) --

    def add_trade_delta(self, exchange: str, symbol: str, delta: float, usd_size: float, ts: float) -> None:
        k = _key(exchange, symbol)
        buckets = self._cvd_buckets.get(k)
        if buckets is None:
            buckets = deque(maxlen=3600)
            self._cvd_buckets[k] = buckets

        sec = int(ts)
        if buckets and buckets[-1].timestamp == sec:
            b = buckets[-1]
            b.delta += delta
            if delta > 0:
                b.buy_vol += usd_size
            else:
                b.sell_vol += usd_size
        else:
            buy_vol = usd_size if delta > 0 else 0.0
            sell_vol = usd_size if delta <= 0 else 0.0
            buckets.append(CvdBucket(timestamp=sec, delta=delta, buy_vol=buy_vol, sell_vol=sell_vol))

        # Daily accumulator
        self._cvd_daily[k] = self._cvd_daily.get(k, 0.0) + delta

    def get_cvd_rolling(self, exchange: str, symbol: str, window_seconds: int) -> float:
        k = _key(exchange, symbol)
        buckets = self._cvd_buckets.get(k)
        if not buckets:
            return 0.0
        cutoff = int(time()) - window_seconds
        return sum(b.delta for b in buckets if b.timestamp >= cutoff)

    def get_cvd_daily(self, exchange: str, symbol: str) -> float:
        return self._cvd_daily.get(_key(exchange, symbol), 0.0)

    def get_trade_imbalance(self, exchange: str, symbol: str, window_seconds: int) -> float:
        """Returns buy_vol / total_vol in [0.0, 1.0]. 0.5 = balanced."""
        k = _key(exchange, symbol)
        buckets = self._cvd_buckets.get(k)
        if not buckets:
            return 0.5
        cutoff = int(time()) - window_seconds
        buy = sell = 0.0
        for b in buckets:
            if b.timestamp >= cutoff:
                buy += b.buy_vol
                sell += b.sell_vol
        total = buy + sell
        return buy / total if total > 0 else 0.5

    # -- Liquidations --

    def add_liquidation(
        self, exchange: str, symbol: str, side: str, usd_vol: float, ts: float
    ) -> None:
        k = _key(exchange, symbol)
        buckets = self._liq_buckets.get(k)
        if buckets is None:
            buckets = deque(maxlen=3600)
            self._liq_buckets[k] = buckets

        sec = int(ts)
        if buckets and buckets[-1].timestamp == sec:
            bucket = buckets[-1]
        else:
            bucket = LiqBucket(timestamp=sec)
            buckets.append(bucket)

        if side == "Buy":
            bucket.buy_liq_usd += usd_vol
        else:
            bucket.sell_liq_usd += usd_vol

    def get_liq_rolling(
        self, exchange: str, symbol: str, window_seconds: int
    ) -> tuple[float, float]:
        k = _key(exchange, symbol)
        buckets = self._liq_buckets.get(k)
        if not buckets:
            return 0.0, 0.0
        cutoff = int(time()) - window_seconds
        buy_total = 0.0
        sell_total = 0.0
        for b in buckets:
            if b.timestamp >= cutoff:
                buy_total += b.buy_liq_usd
                sell_total += b.sell_liq_usd
        return buy_total, sell_total

    # -- Open Interest snapshots --

    def update_open_interest(
        self, exchange: str, symbol: str, oi_value: float, ts: float
    ) -> None:
        k = _key(exchange, symbol)
        snaps = self._oi_snapshots.get(k)
        if snaps is None:
            snaps = deque(maxlen=360)
            self._oi_snapshots[k] = snaps
        snaps.append((ts, oi_value))

    def get_oi_change_5m_pct(self, exchange: str, symbol: str) -> float:
        k = _key(exchange, symbol)
        snaps = self._oi_snapshots.get(k)
        if not snaps or len(snaps) < 2:
            return 0.0
        current = snaps[-1][1]
        cutoff = time() - 300
        oldest = current
        for ts, val in snaps:
            if ts <= cutoff:
                oldest = val
            else:
                break
        if oldest == 0:
            return 0.0
        return ((current - oldest) / oldest) * 100

    # -- Daily reset --

    def reset_daily(self) -> None:
        for ticker in self._tickers.values():
            if ticker.last_price > 0:
                ticker.reset_price = ticker.last_price
            ticker.daily_change_pct = 0.0
        self._cvd_daily.clear()

    # -- Stats --

    @property
    def symbol_count(self) -> int:
        return self._symbol_count

    @property
    def last_message_ts(self) -> float:
        return self._last_message_ts
