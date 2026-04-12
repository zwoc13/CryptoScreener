from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from time import time

from .models import CandleBar, TickerState, to_feed_id

logger = logging.getLogger(__name__)


def _key(exchange: str, symbol: str) -> str:
    return f"{exchange}:{symbol}"


@dataclass(slots=True)
class CvdBucket:
    """One-second CVD bucket."""
    timestamp: int  # truncated epoch second
    delta: float = 0.0  # buy_vol - sell_vol in this second
    buy_vol: float = 0.0
    sell_vol: float = 0.0


@dataclass(slots=True)
class LiqBucket:
    """One-second liquidation bucket."""
    timestamp: int  # truncated epoch second
    buy_liq_usd: float = 0.0  # USD vol of long liquidations
    sell_liq_usd: float = 0.0  # USD vol of short liquidations


@dataclass(slots=True)
class OiPoint:
    """OI level snapshot. Within a slot we keep the chronologically latest value."""
    timestamp: int  # slot start (already aligned to tier resolution)
    value: float = 0.0


# Tier resolution (seconds) and max age (seconds) for the three-tier pyramid.
# Bucket count per ticker per signal: 300 + 360 + 240 = 900 (vs 14400 single-tier).
_TIER_1S_MAX_AGE = 300       # 5m at 1s resolution → 300 entries
_TIER_10S_MAX_AGE = 3600     # 1h at 10s resolution → 360 entries
_TIER_60S_MAX_AGE = 14400    # 4h at 60s resolution → 240 entries


class Store:
    def __init__(self, natr_period: int = 14,
                 questdb_reader: object | None = None) -> None:
        self._tickers: dict[str, TickerState] = {}
        self._candles: dict[str, deque[CandleBar]] = {}
        self._natr_period = natr_period
        self._lock = asyncio.Lock()
        self._symbol_count = 0
        self._last_message_ts: float = 0.0
        # Optional QuestDB reader for windows beyond the in-memory tier capacity.
        self._questdb_reader = questdb_reader

        # ── Three-tier pyramid for CVD ──
        # _cvd_1s holds the most recent ~5m at 1s resolution.
        # When a 1s bucket ages past _TIER_1S_MAX_AGE it is merged into the
        # corresponding 10s slot in _cvd_10s, and so on into _cvd_60s.
        # Read path picks the right tier(s) based on requested window.
        self._cvd_1s: dict[str, deque[CvdBucket]] = {}
        self._cvd_10s: dict[str, deque[CvdBucket]] = {}
        self._cvd_60s: dict[str, deque[CvdBucket]] = {}
        # CVD daily accumulator (reset at 2 AM)
        self._cvd_daily: dict[str, float] = {}

        # ── Three-tier pyramid for liquidations (same shape as CVD) ──
        self._liq_1s: dict[str, deque[LiqBucket]] = {}
        self._liq_10s: dict[str, deque[LiqBucket]] = {}
        self._liq_60s: dict[str, deque[LiqBucket]] = {}

        # ── Three-tier pyramid for OI snapshots ──
        # Each slot stores the chronologically latest value seen in that slot,
        # so reads can compare "current OI" against "OI ~window seconds ago".
        self._oi_1s: dict[str, deque[OiPoint]] = {}
        self._oi_10s: dict[str, deque[OiPoint]] = {}
        self._oi_60s: dict[str, deque[OiPoint]] = {}

    def get_or_create_ticker(self, exchange: str, symbol: str) -> TickerState:
        k = _key(exchange, symbol)
        if k not in self._tickers:
            self._tickers[k] = TickerState(exchange=exchange, symbol=symbol, feed_id=to_feed_id(symbol))
            self._symbol_count = len(self._tickers)
        return self._tickers[k]

    def get_ticker(self, exchange: str, symbol: str) -> TickerState | None:
        return self._tickers.get(_key(exchange, symbol))

    def remove_ticker(self, exchange: str, symbol: str) -> None:
        k = _key(exchange, symbol)
        self._tickers.pop(k, None)
        self._candles.pop(k, None)
        # CVD tiers
        self._cvd_1s.pop(k, None)
        self._cvd_10s.pop(k, None)
        self._cvd_60s.pop(k, None)
        self._cvd_daily.pop(k, None)
        # Liq tiers
        self._liq_1s.pop(k, None)
        self._liq_10s.pop(k, None)
        self._liq_60s.pop(k, None)
        # OI tiers
        self._oi_1s.pop(k, None)
        self._oi_10s.pop(k, None)
        self._oi_60s.pop(k, None)
        self._symbol_count = len(self._tickers)

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

    # -- CVD (Cumulative Volume Delta) — three-tier pyramid --

    def _cvd_deques(self, k: str) -> tuple[deque[CvdBucket], deque[CvdBucket], deque[CvdBucket]]:
        """Return or create the three CVD tier deques for symbol key *k*."""
        t1 = self._cvd_1s.get(k)
        if t1 is None:
            t1 = deque()
            self._cvd_1s[k] = t1
            self._cvd_10s[k] = deque()
            self._cvd_60s[k] = deque()
        return t1, self._cvd_10s[k], self._cvd_60s[k]

    @staticmethod
    def _merge_cvd(target: deque[CvdBucket], slot_ts: int, src: CvdBucket) -> None:
        """Merge *src* into the bucket at *slot_ts* in *target*, creating one if needed."""
        if target and target[-1].timestamp == slot_ts:
            b = target[-1]
            b.delta += src.delta
            b.buy_vol += src.buy_vol
            b.sell_vol += src.sell_vol
        else:
            target.append(CvdBucket(
                timestamp=slot_ts, delta=src.delta,
                buy_vol=src.buy_vol, sell_vol=src.sell_vol,
            ))

    def _age_cvd(self, t1: deque[CvdBucket], t10: deque[CvdBucket],
                 t60: deque[CvdBucket], now_sec: int) -> None:
        """Age stale buckets up the tier pyramid.

        Each cutoff keeps one extra slot beyond the tier boundary so that
        read queries at the exact edge still find data.
        """
        cutoff_1s = now_sec - _TIER_1S_MAX_AGE - 1
        while t1 and t1[0].timestamp < cutoff_1s:
            old = t1.popleft()
            self._merge_cvd(t10, old.timestamp // 10 * 10, old)

        cutoff_10s = now_sec - _TIER_10S_MAX_AGE - 10
        while t10 and t10[0].timestamp < cutoff_10s:
            old = t10.popleft()
            self._merge_cvd(t60, old.timestamp // 60 * 60, old)

        cutoff_60s = now_sec - _TIER_60S_MAX_AGE - 60
        while t60 and t60[0].timestamp < cutoff_60s:
            t60.popleft()

    def add_trade_delta(self, exchange: str, symbol: str, delta: float,
                        usd_size: float, ts: float) -> None:
        k = _key(exchange, symbol)
        t1, t10, t60 = self._cvd_deques(k)
        sec = int(ts)

        # Append / update in the 1s tier
        if t1 and t1[-1].timestamp == sec:
            b = t1[-1]
            b.delta += delta
            if delta > 0:
                b.buy_vol += usd_size
            else:
                b.sell_vol += usd_size
        else:
            buy_vol = usd_size if delta > 0 else 0.0
            sell_vol = usd_size if delta <= 0 else 0.0
            t1.append(CvdBucket(timestamp=sec, delta=delta,
                                buy_vol=buy_vol, sell_vol=sell_vol))
            # Age only when a NEW second starts (not every trade)
            self._age_cvd(t1, t10, t60, sec)

        # Daily accumulator
        self._cvd_daily[k] = self._cvd_daily.get(k, 0.0) + delta

    @staticmethod
    def _sum_cvd_deque(dq: deque[CvdBucket], cutoff: int) -> float:
        total = 0.0
        for b in reversed(dq):
            if b.timestamp < cutoff:
                break
            total += b.delta
        return total

    def get_cvd_rolling(self, exchange: str, symbol: str, window_seconds: int) -> float:
        k = _key(exchange, symbol)
        t1 = self._cvd_1s.get(k)
        if not t1:
            return 0.0
        cutoff = int(time()) - window_seconds
        total = self._sum_cvd_deque(t1, cutoff)
        if window_seconds > _TIER_1S_MAX_AGE:
            t10 = self._cvd_10s.get(k)
            if t10:
                total += self._sum_cvd_deque(t10, cutoff)
        if window_seconds > _TIER_10S_MAX_AGE:
            t60 = self._cvd_60s.get(k)
            if t60:
                total += self._sum_cvd_deque(t60, cutoff)
        return total

    def get_cvd_daily(self, exchange: str, symbol: str) -> float:
        return self._cvd_daily.get(_key(exchange, symbol), 0.0)

    async def get_cvd_long(self, exchange: str, symbol: str,
                           window_seconds: int) -> tuple[float, bool]:
        """CVD over *window_seconds*, falling back to QuestDB when the
        in-memory tiers don't cover it (>4h).

        Returns ``(cvd_value, is_partial)`` where *is_partial* is True if
        the in-memory result doesn't span the full requested window and
        QuestDB was unavailable to fill the gap.
        """
        in_mem = self.get_cvd_rolling(exchange, symbol, window_seconds)
        if window_seconds <= _TIER_60S_MAX_AGE:
            return in_mem, False

        # In-memory tiers cover at most ~4h.  Try QuestDB for the full window.
        if self._questdb_reader is not None:
            try:
                val = await self._questdb_reader.query_cvd(
                    exchange, symbol, window_seconds,
                )
                if val is not None:
                    return val, False
            except Exception:
                logger.warning("QuestDB CVD query failed for %s:%s", exchange, symbol)

        # Fall back to the in-memory value (partial).
        return in_mem, True

    @staticmethod
    def _imbalance_from_deque(dq: deque[CvdBucket], cutoff: int) -> tuple[float, float]:
        buy = sell = 0.0
        for b in reversed(dq):
            if b.timestamp < cutoff:
                break
            buy += b.buy_vol
            sell += b.sell_vol
        return buy, sell

    def get_trade_imbalance(self, exchange: str, symbol: str, window_seconds: int) -> float:
        """Returns buy_vol / total_vol in [0.0, 1.0]. 0.5 = balanced."""
        k = _key(exchange, symbol)
        t1 = self._cvd_1s.get(k)
        if not t1:
            return 0.5
        cutoff = int(time()) - window_seconds
        buy, sell = self._imbalance_from_deque(t1, cutoff)
        if window_seconds > _TIER_1S_MAX_AGE:
            t10 = self._cvd_10s.get(k)
            if t10:
                b2, s2 = self._imbalance_from_deque(t10, cutoff)
                buy += b2
                sell += s2
        if window_seconds > _TIER_10S_MAX_AGE:
            t60 = self._cvd_60s.get(k)
            if t60:
                b3, s3 = self._imbalance_from_deque(t60, cutoff)
                buy += b3
                sell += s3
        total = buy + sell
        return buy / total if total > 0 else 0.5

    def get_trade_imbalance_persistence(
        self, exchange: str, symbol: str, window_seconds: int,
        threshold: float = 0.6,
    ) -> float:
        """Fraction of per-second buckets in *window* where buy% is one-sided.

        Returns a signed value in [-1.0, 1.0]:
        * positive → buy-dominant (buy% ≥ threshold for most seconds)
        * negative → sell-dominant (buy% ≤ 1 - threshold)
        * near 0 → no persistent imbalance

        Only uses the 1s tier (most recent 5m).  For longer windows the
        bucket resolution is too coarse for per-second persistence to be
        meaningful; use ``get_trade_imbalance`` instead.
        """
        k = _key(exchange, symbol)
        t1 = self._cvd_1s.get(k)
        if not t1:
            return 0.0
        cutoff = int(time()) - min(window_seconds, _TIER_1S_MAX_AGE)
        buy_dominant = 0
        sell_dominant = 0
        counted = 0
        for b in reversed(t1):
            if b.timestamp < cutoff:
                break
            total = b.buy_vol + b.sell_vol
            if total == 0:
                continue
            counted += 1
            pct = b.buy_vol / total
            if pct >= threshold:
                buy_dominant += 1
            elif pct <= 1.0 - threshold:
                sell_dominant += 1
        if counted == 0:
            return 0.0
        if buy_dominant >= sell_dominant:
            return buy_dominant / counted
        return -(sell_dominant / counted)

    # -- Liquidations — three-tier pyramid --

    def _liq_deques(self, k: str) -> tuple[deque[LiqBucket], deque[LiqBucket], deque[LiqBucket]]:
        t1 = self._liq_1s.get(k)
        if t1 is None:
            t1 = deque()
            self._liq_1s[k] = t1
            self._liq_10s[k] = deque()
            self._liq_60s[k] = deque()
        return t1, self._liq_10s[k], self._liq_60s[k]

    @staticmethod
    def _merge_liq(target: deque[LiqBucket], slot_ts: int, src: LiqBucket) -> None:
        if target and target[-1].timestamp == slot_ts:
            b = target[-1]
            b.buy_liq_usd += src.buy_liq_usd
            b.sell_liq_usd += src.sell_liq_usd
        else:
            target.append(LiqBucket(
                timestamp=slot_ts,
                buy_liq_usd=src.buy_liq_usd, sell_liq_usd=src.sell_liq_usd,
            ))

    def _age_liq(self, t1: deque[LiqBucket], t10: deque[LiqBucket],
                 t60: deque[LiqBucket], now_sec: int) -> None:
        cutoff_1s = now_sec - _TIER_1S_MAX_AGE - 1
        while t1 and t1[0].timestamp < cutoff_1s:
            old = t1.popleft()
            self._merge_liq(t10, old.timestamp // 10 * 10, old)
        cutoff_10s = now_sec - _TIER_10S_MAX_AGE - 10
        while t10 and t10[0].timestamp < cutoff_10s:
            old = t10.popleft()
            self._merge_liq(t60, old.timestamp // 60 * 60, old)
        cutoff_60s = now_sec - _TIER_60S_MAX_AGE - 60
        while t60 and t60[0].timestamp < cutoff_60s:
            t60.popleft()

    def add_liquidation(
        self, exchange: str, symbol: str, side: str, usd_vol: float, ts: float
    ) -> None:
        k = _key(exchange, symbol)
        t1, t10, t60 = self._liq_deques(k)
        sec = int(ts)

        if t1 and t1[-1].timestamp == sec:
            bucket = t1[-1]
        else:
            bucket = LiqBucket(timestamp=sec)
            t1.append(bucket)
            self._age_liq(t1, t10, t60, sec)

        if side == "Buy":
            bucket.buy_liq_usd += usd_vol
        else:
            bucket.sell_liq_usd += usd_vol

    def get_liq_rolling(
        self, exchange: str, symbol: str, window_seconds: int
    ) -> tuple[float, float]:
        k = _key(exchange, symbol)
        t1 = self._liq_1s.get(k)
        if not t1:
            return 0.0, 0.0
        cutoff = int(time()) - window_seconds
        buy_total = sell_total = 0.0
        for dq in (t1,
                   self._liq_10s.get(k) if window_seconds > _TIER_1S_MAX_AGE else None,
                   self._liq_60s.get(k) if window_seconds > _TIER_10S_MAX_AGE else None):
            if not dq:
                continue
            for b in reversed(dq):
                if b.timestamp < cutoff:
                    break
                buy_total += b.buy_liq_usd
                sell_total += b.sell_liq_usd
        return buy_total, sell_total

    # -- Open Interest snapshots — three-tier pyramid --

    def _oi_deques(self, k: str) -> tuple[deque[OiPoint], deque[OiPoint], deque[OiPoint]]:
        t1 = self._oi_1s.get(k)
        if t1 is None:
            t1 = deque()
            self._oi_1s[k] = t1
            self._oi_10s[k] = deque()
            self._oi_60s[k] = deque()
        return t1, self._oi_10s[k], self._oi_60s[k]

    @staticmethod
    def _merge_oi(target: deque[OiPoint], slot_ts: int, src: OiPoint) -> None:
        """For OI we keep the chronologically latest value per slot."""
        if target and target[-1].timestamp == slot_ts:
            target[-1].value = src.value  # overwrite — later value wins
        else:
            target.append(OiPoint(timestamp=slot_ts, value=src.value))

    def _age_oi(self, t1: deque[OiPoint], t10: deque[OiPoint],
                t60: deque[OiPoint], now_sec: int) -> None:
        cutoff_1s = now_sec - _TIER_1S_MAX_AGE - 1
        while t1 and t1[0].timestamp < cutoff_1s:
            old = t1.popleft()
            self._merge_oi(t10, old.timestamp // 10 * 10, old)
        cutoff_10s = now_sec - _TIER_10S_MAX_AGE - 10
        while t10 and t10[0].timestamp < cutoff_10s:
            old = t10.popleft()
            self._merge_oi(t60, old.timestamp // 60 * 60, old)
        cutoff_60s = now_sec - _TIER_60S_MAX_AGE - 60
        while t60 and t60[0].timestamp < cutoff_60s:
            t60.popleft()

    def update_open_interest(
        self, exchange: str, symbol: str, oi_value: float, ts: float
    ) -> None:
        k = _key(exchange, symbol)
        t1, t10, t60 = self._oi_deques(k)
        sec = int(ts)

        if t1 and t1[-1].timestamp == sec:
            t1[-1].value = oi_value
        else:
            t1.append(OiPoint(timestamp=sec, value=oi_value))
            self._age_oi(t1, t10, t60, sec)

    def _oi_oldest_in_window(self, k: str, cutoff: float) -> float | None:
        """Find the OI value closest to *cutoff* (from below) across tiers.

        Selects the primary tier that should hold data near the cutoff,
        with a fallback to the next coarser tier for aging-boundary overlap
        (entries can move between tiers between writes and reads).
        For a 5m query this checks ~5 entries instead of ~600.
        """
        now = time()
        age = now - cutoff

        if age <= _TIER_1S_MAX_AGE + 1:
            # Target in 1s tier, no fallback needed
            primary, fallback = self._oi_1s.get(k), None
        elif age <= _TIER_10S_MAX_AGE + 10:
            # Target in 10s tier; fallback to 60s for aging-boundary overlap
            primary = self._oi_10s.get(k)
            fallback = self._oi_60s.get(k)
        else:
            # Target in 60s tier
            primary, fallback = self._oi_60s.get(k), None

        # Forward search in primary tier (target is near the front / oldest)
        if primary:
            best: float | None = None
            for pt in primary:
                if pt.timestamp <= cutoff:
                    best = pt.value
                else:
                    break
            if best is not None:
                return best

        # Reverse search in fallback tier (target is near the back / newest)
        if fallback:
            for pt in reversed(fallback):
                if pt.timestamp <= cutoff:
                    return pt.value

        return None

    def get_oi_change_pct(self, exchange: str, symbol: str, window_seconds: int) -> float:
        """OI % change over the given window. Generalises get_oi_change_5m_pct."""
        k = _key(exchange, symbol)
        t1 = self._oi_1s.get(k)
        if not t1:
            return 0.0
        current = t1[-1].value
        cutoff = time() - window_seconds
        oldest = self._oi_oldest_in_window(k, cutoff)
        if oldest is None or oldest == 0:
            return 0.0
        return ((current - oldest) / oldest) * 100

    def get_oi_change_5m_pct(self, exchange: str, symbol: str) -> float:
        """Backward-compat wrapper."""
        return self.get_oi_change_pct(exchange, symbol, 300)

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

    # -- Snapshot save / restore --

    def snapshot_save(self, path: str | Path = "snapshot.json") -> str:
        """Serialize full store state to a JSON file for restore after restart."""
        data: dict = {"saved_at": time(), "tickers": {}, "candles": {}, "cvd_daily": {}}

        for k, t in self._tickers.items():
            data["tickers"][k] = {
                "exchange": t.exchange,
                "symbol": t.symbol,
                "feed_id": t.feed_id,
                "last_price": t.last_price,
                "daily_change_pct": t.daily_change_pct,
                "reset_price": t.reset_price,
                "natr_5m_14": t.natr_5m_14,
                "range_1m": t.range_1m,
                "range_5m": t.range_5m,
                "range_1h": t.range_1h,
                "range_4h": t.range_4h,
                "volume_24h": t.volume_24h,
                "funding_rate": t.funding_rate,
                "funding_interval_h": t.funding_interval_h,
                "next_funding_ts": t.next_funding_ts,
                "trend": t.trend,
                "open_interest": t.open_interest,
                "oi_change_5m_pct": t.oi_change_5m_pct,
                "long_short_ratio": t.long_short_ratio,
                "delist_ts": t.delist_ts,
                "last_update_ts": t.last_update_ts,
            }

        for k, buf in self._candles.items():
            data["candles"][k] = [
                {
                    "timestamp": c.timestamp,
                    "open": c.open,
                    "high": c.high,
                    "low": c.low,
                    "close": c.close,
                    "volume": c.volume,
                    "confirmed": c.confirmed,
                }
                for c in buf
            ]

        for k, val in self._cvd_daily.items():
            data["cvd_daily"][k] = val

        p = Path(path)
        p.write_text(json.dumps(data))
        logger.info("Snapshot saved: %d tickers, %d candle buffers -> %s", len(data["tickers"]), len(data["candles"]), p)
        return str(p.resolve())

    def snapshot_restore(self, path: str | Path = "snapshot.json") -> bool:
        """Restore store state from a snapshot file. Returns True if restored."""
        p = Path(path)
        if not p.exists():
            return False

        try:
            data = json.loads(p.read_text())
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to read snapshot %s: %s", p, e)
            return False

        saved_at = data.get("saved_at", 0)
        age_s = time() - saved_at
        logger.info("Restoring snapshot from %.0fs ago (%s)", age_s, p)

        # Restore tickers
        for k, td in data.get("tickers", {}).items():
            t = self.get_or_create_ticker(td["exchange"], td["symbol"])
            t.reset_price = td.get("reset_price", 0.0)
            t.daily_change_pct = td.get("daily_change_pct", 0.0)
            t.natr_5m_14 = td.get("natr_5m_14", 0.0)
            t.range_1m = td.get("range_1m", 0.0)
            t.range_5m = td.get("range_5m", 0.0)
            t.range_1h = td.get("range_1h", 0.0)
            t.range_4h = td.get("range_4h", 0.0)
            t.volume_24h = td.get("volume_24h", 0.0)
            t.funding_rate = td.get("funding_rate", 0.0)
            t.funding_interval_h = td.get("funding_interval_h", 8)
            t.next_funding_ts = td.get("next_funding_ts", 0.0)
            t.trend = td.get("trend", "-")
            t.open_interest = td.get("open_interest", 0.0)
            t.oi_change_5m_pct = td.get("oi_change_5m_pct", 0.0)
            t.long_short_ratio = td.get("long_short_ratio", 0.0)
            t.delist_ts = td.get("delist_ts", 0.0)
            t.last_price = td.get("last_price", 0.0)
            t.last_update_ts = td.get("last_update_ts", 0.0)

        # Restore candle buffers
        for k, candles_raw in data.get("candles", {}).items():
            parts = k.split(":", 1)
            if len(parts) != 2:
                continue
            exchange, symbol = parts
            candles = [
                CandleBar(
                    timestamp=c["timestamp"],
                    open=c["open"],
                    high=c["high"],
                    low=c["low"],
                    close=c["close"],
                    volume=c["volume"],
                    confirmed=c.get("confirmed", True),
                )
                for c in candles_raw
            ]
            self.load_candles(exchange, symbol, candles)

        # Restore CVD daily accumulators
        for k, val in data.get("cvd_daily", {}).items():
            self._cvd_daily[k] = val

        logger.info("Snapshot restored: %d tickers, %d candle buffers", len(data.get("tickers", {})), len(data.get("candles", {})))
        return True
