"""Tests for the three-tier CVD/Liq/OI buffer pyramid in Store."""

from __future__ import annotations

from time import time
from unittest.mock import patch

import pytest

from screener.store import (
    Store,
    _TIER_1S_MAX_AGE,
    _TIER_10S_MAX_AGE,
    _TIER_60S_MAX_AGE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _populate_cvd(store: Store, ex: str, sym: str, n_seconds: int,
                  delta: float = 100.0, usd_size: float = 100.0,
                  base_ts: float | None = None) -> float:
    """Insert one trade per second for *n_seconds*. Returns base_ts used."""
    now = int(base_ts or time())
    for i in range(n_seconds):
        ts = now - n_seconds + 1 + i
        store.add_trade_delta(ex, sym, delta, usd_size, ts)
    return float(now)


def _populate_liq(store: Store, ex: str, sym: str, n_seconds: int,
                  every_n: int = 10, base_ts: float | None = None) -> float:
    now = int(base_ts or time())
    for i in range(n_seconds):
        ts = now - n_seconds + 1 + i
        if i % every_n == 0:
            store.add_liquidation(ex, sym, "Buy", 1000.0, ts)
        if i % (every_n * 2) == 0:
            store.add_liquidation(ex, sym, "Sell", 500.0, ts)
    return float(now)


def _populate_oi(store: Store, ex: str, sym: str, n_seconds: int,
                 start_val: float = 1_000_000.0, step: float = 100.0,
                 base_ts: float | None = None) -> float:
    now = int(base_ts or time())
    for i in range(n_seconds):
        ts = now - n_seconds + 1 + i
        store.update_open_interest(ex, sym, start_val + i * step, ts)
    return float(now)


# ---------------------------------------------------------------------------
# CVD tier tests
# ---------------------------------------------------------------------------

class TestCvdTiers:
    def test_short_window_uses_1s_only(self):
        s = Store()
        _populate_cvd(s, "x", "BTC", 60)
        assert s.get_cvd_rolling("x", "BTC", 60) == pytest.approx(6000.0)

    def test_5m_window_within_1s_tier(self):
        s = Store()
        _populate_cvd(s, "x", "BTC", 300)
        assert s.get_cvd_rolling("x", "BTC", 300) == pytest.approx(30000.0)

    def test_cross_tier_10m_window(self):
        """10m window spans both 1s (recent 5m) and 10s (older 5m) tiers."""
        s = Store()
        _populate_cvd(s, "x", "BTC", 600)
        cvd = s.get_cvd_rolling("x", "BTC", 600)
        # 600 seconds × 100 delta = 60000
        assert cvd == pytest.approx(60000.0, rel=0.02)

    def test_1h_window(self):
        s = Store()
        _populate_cvd(s, "x", "BTC", 3600)
        cvd = s.get_cvd_rolling("x", "BTC", 3600)
        assert cvd == pytest.approx(360000.0, rel=0.02)

    def test_4h_window_crosses_all_tiers(self):
        """Needs data in 60s tier — simulate 4h of trades."""
        s = Store()
        _populate_cvd(s, "x", "BTC", 14400)
        cvd = s.get_cvd_rolling("x", "BTC", 14400)
        assert cvd == pytest.approx(1_440_000.0, rel=0.02)

    def test_tier_sizes_bounded(self):
        """After 4h of data, tier sizes should stay bounded."""
        s = Store()
        _populate_cvd(s, "x", "BTC", 14400)
        k = "x:BTC"
        # 1s tier: ~300 entries
        assert len(s._cvd_1s[k]) <= _TIER_1S_MAX_AGE + 5
        # 10s tier: ~360 entries
        assert len(s._cvd_10s[k]) <= _TIER_10S_MAX_AGE // 10 + 5
        # 60s tier: ~240 entries
        assert len(s._cvd_60s[k]) <= _TIER_60S_MAX_AGE // 60 + 5

    def test_trade_imbalance_across_tiers(self):
        s = Store()
        now = int(time())
        for i in range(600):
            ts = now - 600 + i
            # 2/3 buys, 1/3 sells
            if i % 3 != 0:
                s.add_trade_delta("x", "BTC", 100.0, 100.0, ts)
            else:
                s.add_trade_delta("x", "BTC", -100.0, 100.0, ts)
        imb = s.get_trade_imbalance("x", "BTC", 600)
        assert imb == pytest.approx(0.667, abs=0.01)

    def test_daily_cvd_unaffected(self):
        s = Store()
        _populate_cvd(s, "x", "BTC", 100)
        daily = s.get_cvd_daily("x", "BTC")
        assert daily == pytest.approx(10000.0)


# ---------------------------------------------------------------------------
# Liquidation tier tests
# ---------------------------------------------------------------------------

class TestLiqTiers:
    def test_short_window(self):
        s = Store()
        _populate_liq(s, "x", "BTC", 60, every_n=10)
        buy, sell = s.get_liq_rolling("x", "BTC", 60)
        assert buy > 0
        assert sell > 0

    def test_cross_tier_window(self):
        s = Store()
        _populate_liq(s, "x", "BTC", 600, every_n=10)
        buy, sell = s.get_liq_rolling("x", "BTC", 600)
        # 600 seconds, one buy liq every 10s = 60 buys × 1000 = 60000
        assert buy == pytest.approx(60000.0, rel=0.05)
        # sells every 20s = 30 × 500 = 15000
        assert sell == pytest.approx(15000.0, rel=0.05)


# ---------------------------------------------------------------------------
# OI tier tests
# ---------------------------------------------------------------------------

class TestOiTiers:
    def test_5m_change(self):
        s = Store()
        _populate_oi(s, "x", "BTC", 600, start_val=1_000_000, step=100)
        pct = s.get_oi_change_5m_pct("x", "BTC")
        # OI 5m ago ≈ 1,030,000; now ≈ 1,060,000 → ~2.9%
        assert 2.0 < pct < 4.0

    def test_general_oi_change(self):
        s = Store()
        _populate_oi(s, "x", "BTC", 600, start_val=1_000_000, step=100)
        pct10 = s.get_oi_change_pct("x", "BTC", 600)
        assert pct10 > 3.0  # should be ~6%

    def test_cross_tier_oi(self):
        """OI over 4h should work once data spans all three tiers."""
        s = Store()
        _populate_oi(s, "x", "BTC", 14400, start_val=1_000_000, step=1)
        pct = s.get_oi_change_pct("x", "BTC", 14400)
        # OI from 1,000,000 to 1,014,400 → ~1.44%
        assert 1.0 < pct < 2.0


# ---------------------------------------------------------------------------
# remove_ticker cleans all tiers
# ---------------------------------------------------------------------------

class TestRemoveTicker:
    def test_removes_all_tier_data(self):
        s = Store()
        _populate_cvd(s, "x", "BTC", 100)
        _populate_liq(s, "x", "BTC", 100)
        _populate_oi(s, "x", "BTC", 100)
        s.remove_ticker("x", "BTC")
        k = "x:BTC"
        assert k not in s._cvd_1s
        assert k not in s._cvd_10s
        assert k not in s._cvd_60s
        assert k not in s._liq_1s
        assert k not in s._oi_1s
