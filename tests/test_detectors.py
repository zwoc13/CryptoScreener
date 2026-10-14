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
    detect_pump_exhausted,
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


class TestPumpExhausted:
    """Tests for the pump exhaustion detector."""

    def _store_with_impulse(self, exchange: str, symbol: str, ts: float, direction: str) -> Store:
        store = Store()
        store.record_impulse(exchange, symbol, direction, ts)
        return store

    def test_no_fire_without_impulse(self):
        """No impulse recorded → never fires."""
        hist = deque()
        now = _now()
        for i in range(20):
            hist.append(_make_snap(now - (20 - i) * 15, cvd_5m=5000.0, oi_change_5m=3.0))
        store = Store()
        assert detect_pump_exhausted(_make_ticker(), hist, store) is None

    def test_no_fire_when_impulse_too_fresh(self):
        """Impulse < 3 min ago → too fresh, don't fire."""
        now = _now()
        impulse_ts = now - 120  # 2 min ago
        hist = deque()
        # 10 snapshots × 15s = 150s from impulse → last snap age = 150s < 180s
        # Also >= K_PUMP_MIN_SAMPLES (8) so we actually test the time gate
        for i in range(10):
            hist.append(_make_snap(impulse_ts + i * 15, cvd_5m=5000.0 - i * 200, oi_change_5m=3.0 - i * 0.2))
        store = self._store_with_impulse("binance", "BTCUSDT", impulse_ts, "up")
        assert detect_pump_exhausted(_make_ticker(), hist, store) is None

    def test_no_fire_when_impulse_too_stale(self):
        """Impulse > 4h ago → stale, don't fire."""
        now = _now()
        impulse_ts = now - 15000  # ~4.2h ago
        hist = deque()
        for i in range(20):
            hist.append(_make_snap(now - (20 - i) * 15, cvd_5m=100.0, oi_change_5m=0.1))
        store = self._store_with_impulse("binance", "BTCUSDT", impulse_ts, "up")
        assert detect_pump_exhausted(_make_ticker(), hist, store) is None

    def test_no_fire_when_cvd_rollback_insufficient(self):
        """CVD hasn't rolled back enough → don't fire."""
        now = _now()
        impulse_ts = now - 600  # 10 min ago
        hist = deque()
        # CVD stays high (peak = 10000, current = 9000 → only 10% rollback)
        for i in range(30):
            hist.append(_make_snap(impulse_ts + i * 15, cvd_5m=10000.0, oi_change_5m=3.0))
        hist.append(_make_snap(now, cvd_5m=9000.0, oi_change_5m=0.5))
        store = self._store_with_impulse("binance", "BTCUSDT", impulse_ts, "up")
        assert detect_pump_exhausted(_make_ticker(), hist, store) is None

    def test_no_fire_when_oi_rollback_insufficient(self):
        """OI hasn't rolled back enough → don't fire."""
        now = _now()
        impulse_ts = now - 600
        hist = deque()
        # CVD rolled back 50% but OI only 5%
        for i in range(30):
            hist.append(_make_snap(impulse_ts + i * 15, cvd_5m=10000.0, oi_change_5m=3.0))
        hist.append(_make_snap(now, cvd_5m=5000.0, oi_change_5m=2.85))
        store = self._store_with_impulse("binance", "BTCUSDT", impulse_ts, "up")
        assert detect_pump_exhausted(_make_ticker(), hist, store) is None

    def test_fire_when_both_rollbacks_met(self):
        """CVD rollback >= 20% AND OI rollback >= 10% → fires."""
        now = _now()
        impulse_ts = now - 600  # 10 min ago
        hist = deque()
        # Build pump window with high CVD and OI
        for i in range(30):
            hist.append(_make_snap(
                impulse_ts + i * 15,
                price=51000.0,
                cvd_5m=10000.0,
                oi_change_5m=5.0,
                trend="UP",
            ))
        # Latest: CVD rolled back 40%, OI rolled back 60%
        hist.append(_make_snap(now, price=50800.0, cvd_5m=6000.0, oi_change_5m=2.0, trend="RANGE"))
        store = self._store_with_impulse("binance", "BTCUSDT", impulse_ts, "up")
        ticker = _make_ticker(last_price=50800.0, trend="RANGE")
        result = detect_pump_exhausted(ticker, hist, store)
        assert result is not None
        assert result.direction == "down"  # fade the up-pump
        assert result.cvd_rollback_pct >= 20.0
        assert result.oi_rollback_pct >= 10.0
        assert result.impulse_direction == "up"
        assert result.confidence > 30.0

    def test_direction_fades_impulse(self):
        """Up-pump → fade direction is 'down'. Down-pump → fade direction is 'up'."""
        now = _now()
        impulse_ts = now - 600

        # Up-pump exhausted
        hist = deque()
        for i in range(20):
            hist.append(_make_snap(impulse_ts + i * 15, price=51000.0, cvd_5m=8000.0, oi_change_5m=4.0))
        hist.append(_make_snap(now, price=50500.0, cvd_5m=4000.0, oi_change_5m=1.0))
        store = self._store_with_impulse("binance", "BTCUSDT", impulse_ts, "up")
        result = detect_pump_exhausted(_make_ticker(), hist, store)
        assert result is not None
        assert result.direction == "down"

        # Down-pump exhausted
        hist2 = deque()
        for i in range(20):
            hist2.append(_make_snap(impulse_ts + i * 15, price=49000.0, cvd_5m=-8000.0, oi_change_5m=4.0))
        hist2.append(_make_snap(now, price=49500.0, cvd_5m=-3000.0, oi_change_5m=1.0))
        store2 = self._store_with_impulse("binance", "BTCUSDT", impulse_ts, "down")
        result2 = detect_pump_exhausted(_make_ticker(), hist2, store2)
        assert result2 is not None
        assert result2.direction == "up"

    def test_confidence_boost_on_price_rollback(self):
        """Price pulled back 3%+ from peak → confidence boost."""
        now = _now()
        impulse_ts = now - 600
        hist = deque()
        for i in range(20):
            hist.append(_make_snap(impulse_ts + i * 15, price=52000.0, cvd_5m=10000.0, oi_change_5m=5.0))
        # Price pulled back 5%, CVD 40% rollback, OI 60% rollback
        hist.append(_make_snap(now, price=49400.0, cvd_5m=6000.0, oi_change_5m=2.0))
        store = self._store_with_impulse("binance", "BTCUSDT", impulse_ts, "up")
        result = detect_pump_exhausted(_make_ticker(), hist, store)
        assert result is not None
        assert result.price_rollback_pct >= 3.0
        assert result.confidence >= 70.0  # should get price boost

    def test_confidence_boost_on_trend_shift(self):
        """Trend shifted to RANGE or opposite → confidence boost."""
        now = _now()
        impulse_ts = now - 600
        hist = deque()
        for i in range(20):
            hist.append(_make_snap(impulse_ts + i * 15, price=51000.0, cvd_5m=10000.0, oi_change_5m=5.0, trend="UP"))
        # Trend shifted to RANGE, CVD and OI rolled back
        hist.append(_make_snap(now, price=50900.0, cvd_5m=6000.0, oi_change_5m=2.0, trend="RANGE"))
        store = self._store_with_impulse("binance", "BTCUSDT", impulse_ts, "up")
        result = detect_pump_exhausted(_make_ticker(last_price=50900.0, trend="RANGE"), hist, store)
        assert result is not None
        assert result.trend_now == "RANGE"
        assert result.confidence >= 70.0

    def test_no_fire_with_insufficient_samples(self):
        """Not enough snapshots in pump window → don't fire."""
        now = _now()
        impulse_ts = now - 600
        hist = deque()
        # Only 3 snapshots in pump window (< K_PUMP_MIN_SAMPLES=8)
        for i in range(3):
            hist.append(_make_snap(impulse_ts + i * 15, cvd_5m=5000.0, oi_change_5m=3.0))
        store = self._store_with_impulse("binance", "BTCUSDT", impulse_ts, "up")
        assert detect_pump_exhausted(_make_ticker(), hist, store) is None


class TestRaveScenarioDetector:
    """RAVE-like scenarios at the detector level.

    RAVE was a manipulated low-cap token where shorting the pump was
    the wrong call — OI was rising (squeeze building), CVD was flat
    (no real conviction), and the price kept pumping. These tests
    verify the pump exhaustion detector does NOT fire during an
    active squeeze.
    """

    def _store_with_impulse(self, exchange: str, symbol: str, ts: float, direction: str) -> Store:
        store = Store()
        store.record_impulse(exchange, symbol, direction, ts)
        return store

    def test_rave_squeeze_trap_oi_still_climbing(self):
        """THE RAVE TRAP: CVD rolled back 30% but OI is STILL RISING.

        This is the exact scenario that caused the liquidation.
        CVD peaked at 5000, dropped to 3500 (30% rollback).
        But OI went from 8% to 12% — squeeze is building, not dying.
        Must NOT fire exhaustion.
        """
        now = _now()
        impulse_ts = now - 600
        hist = deque()
        # Pump phase: CVD building, OI climbing
        for i in range(15):
            hist.append(_make_snap(
                impulse_ts + i * 15,
                price=100.0 + i * 0.5,  # price still rising
                cvd_5m=5000.0,
                oi_change_5m=8.0 + i * 0.3,  # OI accelerating
            ))
        # Latest: CVD dropped but OI STILL climbing
        hist.append(_make_snap(now, price=107.0, cvd_5m=3500.0, oi_change_5m=12.0))
        store = self._store_with_impulse("bybit", "RAVEUSDT", impulse_ts, "up")
        ticker = _make_ticker(exchange="bybit", symbol="RAVEUSDT", last_price=107.0)
        result = detect_pump_exhausted(ticker, hist, store)
        assert result is None  # OI never rolled back — squeeze still active

    def test_rave_oi_accelerating_through_pump(self):
        """OI keeps accelerating (5% → 15% → 25%) throughout the pump.

        Even though CVD may show some noise/rollback, OI is monotonically
        increasing — new leveraged longs entering every minute. This is
        a squeeze building, not exhaustion.
        """
        now = _now()
        impulse_ts = now - 900  # 15 min ago
        hist = deque()
        for i in range(30):
            hist.append(_make_snap(
                impulse_ts + i * 15,
                price=50.0 + i * 0.2,
                cvd_5m=3000.0 + (i % 5) * 200,  # noisy CVD
                oi_change_5m=5.0 + i * 0.6,  # OI: 5% → 23%
            ))
        hist.append(_make_snap(now, price=56.0, cvd_5m=2800.0, oi_change_5m=22.0))
        store = self._store_with_impulse("bybit", "RAVEUSDT", impulse_ts, "up")
        result = detect_pump_exhausted(_make_ticker(exchange="bybit", symbol="RAVEUSDT"), hist, store)
        assert result is None  # OI never rolled back

    def test_rave_flat_cvd_tiny_peak(self):
        """Low-cap token with tiny CVD peak (500 USD).

        20% rollback of 500 = 100 USD. This is noise, not meaningful
        exhaustion. But if OI also rolled back, the detector will fire.
        This test documents the behavior — the detector fires because
        both percentage gates are met, even though absolute values are tiny.
        This is by design: percentage rollback is relative to the pump.
        """
        now = _now()
        impulse_ts = now - 600
        hist = deque()
        for i in range(20):
            hist.append(_make_snap(
                impulse_ts + i * 15,
                price=0.05,
                cvd_5m=500.0,  # tiny absolute CVD
                oi_change_5m=8.0,
            ))
        # Both rolled back
        hist.append(_make_snap(now, price=0.048, cvd_5m=350.0, oi_change_5m=5.0))
        store = self._store_with_impulse("bybit", "RAVEUSDT", impulse_ts, "up")
        result = detect_pump_exhausted(_make_ticker(exchange="bybit", symbol="RAVEUSDT"), hist, store)
        # Fires because both percentage gates met — this is correct behavior
        assert result is not None
        assert result.cvd_rollback_pct >= 20.0
        assert result.oi_rollback_pct >= 10.0

    def test_rave_cvd_rollback_but_oi_squeeze_building(self):
        """CVD dropped 40% but OI went from 5% to 15%.

        Classic RAVE: the buying pressure (CVD) faded but leveraged
        longs keep piling in (OI rising). This is a squeeze setup —
        price will likely pump harder before it dies. Must NOT fire.
        """
        now = _now()
        impulse_ts = now - 600
        hist = deque()
        # Phase 1: CVD building
        for i in range(10):
            hist.append(_make_snap(
                impulse_ts + i * 15,
                price=200.0 + i,
                cvd_5m=8000.0,
                oi_change_5m=5.0,
            ))
        # Phase 2: CVD fading, OI accelerating (the RAVE pattern)
        for i in range(10):
            hist.append(_make_snap(
                impulse_ts + (10 + i) * 15,
                price=210.0 + i * 0.5,
                cvd_5m=8000.0 - i * 400,  # 8000 → 4400
                oi_change_5m=5.0 + i * 1.0,  # 5% → 15%
            ))
        hist.append(_make_snap(now, price=215.0, cvd_5m=4000.0, oi_change_5m=15.0))
        store = self._store_with_impulse("bybit", "RAVEUSDT", impulse_ts, "up")
        result = detect_pump_exhausted(_make_ticker(exchange="bybit", symbol="RAVEUSDT"), hist, store)
        assert result is None  # OI never rolled back — squeeze still building

    def test_rave_full_squeeze_lifecycle_then_exhaustion(self):
        """Full RAVE lifecycle: squeeze builds → peaks → finally exhausts.

        Phase 1 (0-10min): OI rising 5%→20%, CVD building, price pumping
        Phase 2 (10-15min): OI peaks at 20%, CVD starts fading
        Phase 3 (15-20min): OI drops 20%→8%, CVD collapsed, price stalls
        → Exhaustion fires ONLY in phase 3.
        """
        now = _now()
        impulse_ts = now - 1200  # 20 min ago
        hist = deque()

        # Phase 1: squeeze building (0-10min, 40 snapshots)
        for i in range(40):
            hist.append(_make_snap(
                impulse_ts + i * 15,
                price=100.0 + i * 0.3,
                cvd_5m=5000.0 + i * 100,
                oi_change_5m=5.0 + i * 0.4,  # 5% → 21%
                trend="UP",
            ))

        # Phase 2: OI plateau, CVD fading (10-15min, 20 snapshots)
        for i in range(20):
            hist.append(_make_snap(
                impulse_ts + (40 + i) * 15,
                price=112.0 - i * 0.1,
                cvd_5m=9000.0 - i * 300,  # 9000 → 3000
                oi_change_5m=21.0 - i * 0.2,  # 21% → 17%
                trend="RANGE",
            ))

        # Phase 3: exhaustion — OI collapsed, CVD dead (latest snapshot)
        hist.append(_make_snap(
            now,
            price=110.0,
            cvd_5m=2000.0,  # 78% rollback from peak
            oi_change_5m=8.0,  # 62% rollback from 21% peak
            trend="RANGE",
        ))

        store = self._store_with_impulse("bybit", "RAVEUSDT", impulse_ts, "up")
        result = detect_pump_exhausted(_make_ticker(exchange="bybit", symbol="RAVEUSDT", last_price=110.0, trend="RANGE"), hist, store)
        assert result is not None  # NOW it's safe — squeeze is over
        assert result.direction == "down"
        assert result.cvd_rollback_pct >= 70.0
        assert result.oi_rollback_pct >= 50.0
        assert result.confidence >= 80.0

    def test_rave_price_pumping_but_cvd_diverging(self):
        """Price makes new highs but CVD is declining — divergence.

        Price went from 100 to 120 (+20%) but CVD peaked at 8000
        and is now 3000. OI still rising though — the pump is driven
        by leverage, not real buying. Classic RAVE trap.
        Must NOT fire because OI hasn't rolled back.
        """
        now = _now()
        impulse_ts = now - 600
        hist = deque()
        for i in range(20):
            hist.append(_make_snap(
                impulse_ts + i * 15,
                price=100.0 + i,  # 100 → 120, new highs
                cvd_5m=8000.0 - i * 250,  # 8000 → 3000, declining
                oi_change_5m=3.0 + i * 0.5,  # 3% → 13%, rising
            ))
        hist.append(_make_snap(now, price=120.0, cvd_5m=3000.0, oi_change_5m=13.0))
        store = self._store_with_impulse("bybit", "RAVEUSDT", impulse_ts, "up")
        result = detect_pump_exhausted(_make_ticker(exchange="bybit", symbol="RAVEUSDT"), hist, store)
        assert result is None  # OI still rising — not safe to short
