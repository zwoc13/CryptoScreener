from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from time import time

from .models import CandleBar, TickerState


def _key(exchange: str, symbol: str) -> str:
    return f"{exchange}:{symbol}"


@dataclass
class CvdBucket:
    """One-second CVD bucket."""
    timestamp: int  # truncated epoch second
    delta: float = 0.0  # buy_vol - sell_vol in this second


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

    def get_or_create_ticker(self, exchange: str, symbol: str) -> TickerState:
        k = _key(exchange, symbol)
        if k not in self._tickers:
            self._tickers[k] = TickerState(exchange=exchange, symbol=symbol)
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
            self._candles[k] = deque(maxlen=self._natr_period + 1)
        return self._candles[k]

    def add_candle(self, exchange: str, symbol: str, candle: CandleBar) -> None:
        buf = self.get_candles(exchange, symbol)
        if buf and buf[-1].timestamp == candle.timestamp:
            buf[-1] = candle
        else:
            buf.append(candle)

    def load_candles(self, exchange: str, symbol: str, candles: list[CandleBar]) -> None:
        k = _key(exchange, symbol)
        self._candles[k] = deque(candles, maxlen=self._natr_period + 1)

    # -- CVD (Cumulative Volume Delta) --

    def add_trade_delta(self, exchange: str, symbol: str, delta: float, ts: float) -> None:
        k = _key(exchange, symbol)
        buckets = self._cvd_buckets.get(k)
        if buckets is None:
            buckets = deque(maxlen=3600)
            self._cvd_buckets[k] = buckets

        sec = int(ts)
        if buckets and buckets[-1].timestamp == sec:
            buckets[-1].delta += delta
        else:
            buckets.append(CvdBucket(timestamp=sec, delta=delta))

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
