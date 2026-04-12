"""Tests for screener.detectors — synthetic history fixtures per detector."""

from __future__ import annotations

import time
from collections import deque

from screener.detectors import (
    detect_bias_changed,
    detect_cvd_burst,
    detect_liquidation_cascade,
    detect_oi_flip,
    detect_divergence,
    detect_volume_spike,
    detect_trend_change,
    K_CVD_BASELINE_SAMPLES,
    K_OI_FLIP_PERSIST,
    K_DIVERGENCE_WINDOW,
)
from screener.signal_history import SignalSnapshot
from screener.store import Store
from screener.models import TickerState


def _now():
    return time.time()


def _make_ticker(
    exchange: str = "binance", symbol: str = "BTCUSDT", last_price: float = 50000.0,
    trend: str = "-", natr: float = 0.05, oi_change_5m: float = 0.0,
    funding_rate: float = 0.0,
) -> TickerState:
    return TickerState(
        exchange=exchange, symbol=symbol, last_price=last_price,
        trend=trend, natr_5m_14=natr, oi_change_5m_pct=oi_change_5m,
        funding_rate=funding_rate,
    )


def _make_snap(ts: float, **overrides) -> SignalSnapshot:
    defaults = dict(
        timestamp=ts, price=50000.0, cvd_5m=0.0, cvd_1h=0.0, cvd_4h=0.0,
        oi_change_5m=0.0, oi_change_1h=0.0, buy_pct_5m=0.5, buy_persist_5m=0.0,
        long_bias=0.0, short_bias=0.0, bias_label="NEUTRAL",
        liq_buy_5m=0.0, liq_sell_5m=0.0, natr=0.05, trend="-",
        range_1m=0.0, volume_5m=0.0,
    )
    defaults.update(overrides)
    return SignalSnapshot(**defaults)


def _make_store_with_empty_cvd() -> Store:
    return Store()


class TestBiasChanged:
    def test_no_change_when_label_is_neutral(self):
        hist = deque()
        now = _now()
        hist.append(_make_snap(now - 60))
        hist.append(_make_snap(now))
        assert detect_bias_changed(_make_ticker(), hist, _make_store_with_empty_cvd()) is None

    def test_fire_on_label_flip(self):
        hist = deque()
        now = _now()
        # Baseline: LONG snapshots
        for i in range(25):
            hist.append(_make_snap(now - (25 - i) * 15, bias_label="LONG"))
        # Latest flips to SHORT
        hist.append(_make_snap(now, bias_label="SHORT"))
        result = detect_bias_changed(_make_ticker(), hist, _make_store_with_empty_cvd())
        # The lookback index points at a LONG snapshot, now is SHORT → flip
        assert result is not None
        assert result.prev_label == "LONG"
        assert result.new_label == "SHORT"

    def test_neutral_to_neutral_does_not_fire(self):
        hist = deque([
            _make_snap(_now() - 300, bias_label="NEUTRAL"),
            _make_snap(_now(), bias_label="NEUTRAL"),
        ])
        assert detect_bias_changed(_make_ticker(), hist, _make_store_with_empty_cvd()) is None


class TestCvdBurst:
    def test_burst_fires_when_cvd_exceeds_baseline(self):
        hist = deque()
        base = _now()
        # Baseline: small CVD values
        for i in range(20):
            hist.append(_make_snap(base + i * 15, cvd_5m=100.0))
        # Burst: 10x baseline
        hist.append(_make_snap(base + 20 * 15, cvd_5m=3000.0))

        # Mock store to return specific CVD 1m value
        store = _make_store_with_empty_cvd()
        # Patch get_cvd_rolling to return the burst value
        original = store.get_cvd_rolling
        store.get_cvd_rolling = lambda *args: 3000.0 if args[2] == 60 else original(*args)

        result = detect_cvd_burst(_make_ticker(), hist, store)
        assert result is not None
        assert result.burst_ratio > 1.0
        assert result.direction in ("up", "down")

    def test_no_burst_with_steady_cvd(self):
        hist = deque()
        base = _now()
        for i in range(K_CVD_BASELINE_SAMPLES + 1):
            hist.append(_make_snap(base + i * 15, cvd_5m=100.0))
        store = _make_store_with_empty_cvd()
        store.get_cvd_rolling = lambda *args: 100.0 if args[2] == 60 else 0.0
        assert detect_cvd_burst(_make_ticker(), hist, store) is None

    def test_no_burst_with_insufficient_history(self):
        hist = deque([_make_snap(_now())])
        assert detect_cvd_burst(_make_ticker(), hist, _make_store_with_empty_cvd()) is None


class TestLiquidationCascade:
    def test_cascade_when_liq_exceeds_baseline(self):
        hist = deque()
        base = _now()
        # Baseline: low liq
        for i in range(20):
            hist.append(_make_snap(base + i * 15, liq_buy_5m=2000.0, liq_sell_5m=1000.0))
        # Cascade: high liq
        hist.append(_make_snap(
            base + 20 * 15, liq_buy_5m=100000.0, liq_sell_5m=50000.0,
        ))
        store = _make_store_with_empty_cvd()
        store.get_liq_rolling = lambda *args: (100000.0, 50000.0) if args[2] == 60 else (0.0, 0.0)
        result = detect_liquidation_cascade(_make_ticker(), hist, store)
        assert result is not None
        assert result.burst_ratio > 1.0
        assert result.dominant_side == "buy"

    def test_no_cascade_with_low_liq(self):
        hist = deque()
        base = _now()
        for i in range(5):
            hist.append(_make_snap(base + i * 15, liq_buy_5m=100.0, liq_sell_5m=100.0))
        store = _make_store_with_empty_cvd()
        store.get_liq_rolling = lambda *args: (100.0, 100.0)
        assert detect_liquidation_cascade(_make_ticker(), hist, store) is None


class TestOiFlip:
    def test_oi_flip_fires_on_sign_change_with_persistence(self):
        hist = deque()
        base = _now()
        # Old: negative OI (at least 2 snapshots)
        hist.append(_make_snap(base, oi_change_5m=-2.0))
        hist.append(_make_snap(base + 15, oi_change_5m=-1.0))
        hist.append(_make_snap(base + 30, oi_change_5m=-0.5))
        # Flip: persistent positive OI
        for i in range(K_OI_FLIP_PERSIST + 1):
            hist.append(_make_snap(base + (3 + i) * 15, oi_change_5m=1.5))
        # Total: 3 neg + 3 pos = 6
        result = detect_oi_flip(_make_ticker(), hist, _make_store_with_empty_cvd())
        # persist=3 >= K_OI_FLIP_PERSIST=2, old_sign=-, new_sign=+
        assert result is not None
        assert result.oi_change_now > 0

    def test_no_flip_on_stable_oi(self):
        hist = deque([
            _make_snap(_now() - 30, oi_change_5m=1.0),
            _make_snap(_now(), oi_change_5m=1.2),
        ])
        assert detect_oi_flip(_make_ticker(), hist, _make_store_with_empty_cvd()) is None


class TestDivergence:
    def test_bullish_divergence_in_downtrend(self):
        hist = deque()
        base = _now()
        # Window of prices making new lows, CVD staying flat
        for i in range(40):
            hist.append(_make_snap(
                base + i * 15,
                price=50000.0 - i,        # making new lows
                cvd_5m=-500.0,             # CVD not confirming extreme
                natr=0.15,
            ))
        # Last: price keeps going lower, CVD slightly recovers
        hist.append(_make_snap(
            base + 40 * 15, price=49950.0, cvd_5m=-400.0,
            natr=0.15, trend="DOWN",
        ))
        result = detect_divergence(
            _make_ticker(trend="DOWN", natr=0.15), hist, _make_store_with_empty_cvd(),
        )
        # Should fire if price new low but CVD not confirming
        assert result is not None
        assert result.direction == "up"  # bullish divergence

    def test_no_divergence_when_natr_too_low(self):
        hist = deque()
        base = _now()
        for i in range(40):
            hist.append(_make_snap(
                base + i * 15, price=50000.0 - i, cvd_5m=-500.0, natr=0.02,
            ))
        hist.append(_make_snap(base + 40 * 15, price=49950.0, cvd_5m=-400.0, natr=0.02))
        assert detect_divergence(_make_ticker(natr=0.02), hist, _make_store_with_empty_cvd()) is None

    def test_no_divergence_with_insufficient_history(self):
        hist = deque([_make_snap(_now())])
        assert detect_divergence(_make_ticker(), hist, _make_store_with_empty_cvd()) is None


class TestVolumeSpike:
    def test_spike_when_volume_exceeds_baseline(self):
        hist = deque()
        base = _now()
        for i in range(25):
            hist.append(_make_snap(base + i * 15, cvd_5m=1000.0))
        # Spike: big CVD magnitude
        hist.append(_make_snap(base + 25 * 15, cvd_5m=5000.0))
        # Should fire with high multiplier
        result = detect_volume_spike(_make_ticker(), hist, _make_store_with_empty_cvd())
        assert result is not None  # 25 baseline of 1k, latest 5k → 5× mean, exceeds 3× threshold

    def test_no_spike_with_steady_volume(self):
        hist = deque()
        base = _now()
        for i in range(26):
            hist.append(_make_snap(base + i * 15, cvd_5m=1000.0))
        result = detect_volume_spike(_make_ticker(), hist, _make_store_with_empty_cvd())
        # With steady volume, the latest should not exceed multiplier × mean
        assert result is None

    def test_no_spike_with_insufficient_history(self):
        hist = deque([_make_snap(_now())])
        assert detect_volume_spike(_make_ticker(), hist, _make_store_with_empty_cvd()) is None


class TestTrendChange:
    def test_fire_on_trend_switch(self):
        hist = deque()
        base = _now()
        hist.append(_make_snap(base, trend="UP"))
        hist.append(_make_snap(base + 15, trend="DOWN"))
        result = detect_trend_change(_make_ticker(), hist, _make_store_with_empty_cvd())
        assert result is not None
        assert result.prev_trend == "UP"
        assert result.new_trend == "DOWN"

    def test_no_fire_on_same_trend(self):
        hist = deque([
            _make_snap(_now() - 15, trend="UP"),
            _make_snap(_now(), trend="UP"),
        ])
        assert detect_trend_change(_make_ticker(), hist, _make_store_with_empty_cvd()) is None

    def test_no_fire_with_insufficient_history(self):
        hist = deque([_make_snap(_now())])
        assert detect_trend_change(_make_ticker(), hist, _make_store_with_empty_cvd()) is None
