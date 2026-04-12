"""Tests for screener.bias scoring logic."""

from __future__ import annotations

import pytest

from screener.bias import compute_bias


class TestCvdAlignment:
    def test_pump_with_strong_positive_cvd_favours_long(self):
        long_s, short_s, label = compute_bias(
            direction="up", cvd_5m=100_000, cvd_1h=200_000, cvd_4h=500_000,
            oi_change_5m=0, oi_change_1h=0,
            buy_pct_5m=0.5, buy_persistence_5m=0,
            funding_rate=0, natr=0.01,
        )
        assert long_s > short_s

    def test_pump_with_negative_cvd_favours_short(self):
        long_s, short_s, label = compute_bias(
            direction="up", cvd_5m=-80_000, cvd_1h=-100_000, cvd_4h=-200_000,
            oi_change_5m=0, oi_change_1h=0,
            buy_pct_5m=0.5, buy_persistence_5m=0,
            funding_rate=0, natr=0.01,
        )
        assert short_s > long_s

    def test_dump_with_negative_cvd_favours_short(self):
        long_s, short_s, label = compute_bias(
            direction="down", cvd_5m=-100_000, cvd_1h=-200_000, cvd_4h=-300_000,
            oi_change_5m=0, oi_change_1h=0,
            buy_pct_5m=0.5, buy_persistence_5m=0,
            funding_rate=0, natr=0.01,
        )
        assert short_s > long_s


class TestOiDirection:
    def test_pump_rising_oi_is_squeeze_risk(self):
        """Rising OI on pump → longs piling in → squeeze risk → long bias."""
        long_s, short_s, _ = compute_bias(
            direction="up", cvd_5m=0, cvd_1h=0, cvd_4h=0,
            oi_change_5m=5.0, oi_change_1h=3.0,
            buy_pct_5m=0.5, buy_persistence_5m=0,
            funding_rate=0, natr=0.01,
        )
        assert long_s > short_s

    def test_pump_flat_oi_mild_fade(self):
        """Flat OI on pump → spot-driven → mild short bias."""
        long_s, short_s, _ = compute_bias(
            direction="up", cvd_5m=0, cvd_1h=0, cvd_4h=0,
            oi_change_5m=0.1, oi_change_1h=0.0,
            buy_pct_5m=0.5, buy_persistence_5m=0,
            funding_rate=0, natr=0.01,
        )
        assert short_s >= long_s


class TestBuyPersistence:
    def test_sustained_buys_on_pump_boosts_long(self):
        long_s, short_s, _ = compute_bias(
            direction="up", cvd_5m=0, cvd_1h=0, cvd_4h=0,
            oi_change_5m=0, oi_change_1h=0,
            buy_pct_5m=0.8, buy_persistence_5m=0.7,
            funding_rate=0, natr=0.01,
        )
        assert long_s > short_s

    def test_sustained_sells_on_dump_boosts_short(self):
        long_s, short_s, _ = compute_bias(
            direction="down", cvd_5m=0, cvd_1h=0, cvd_4h=0,
            oi_change_5m=0, oi_change_1h=0,
            buy_pct_5m=0.2, buy_persistence_5m=-0.7,
            funding_rate=0, natr=0.01,
        )
        assert short_s > long_s


class TestFunding:
    def test_extreme_positive_funding_penalises_long(self):
        """Crowded longs → long bias penalised."""
        base_long, base_short, _ = compute_bias(
            direction="up", cvd_5m=100_000, cvd_1h=100_000, cvd_4h=100_000,
            oi_change_5m=0, oi_change_1h=0,
            buy_pct_5m=0.5, buy_persistence_5m=0,
            funding_rate=0, natr=0.01,
        )
        funded_long, funded_short, _ = compute_bias(
            direction="up", cvd_5m=100_000, cvd_1h=100_000, cvd_4h=100_000,
            oi_change_5m=0, oi_change_1h=0,
            buy_pct_5m=0.5, buy_persistence_5m=0,
            funding_rate=0.002, natr=0.01,
        )
        assert funded_long < base_long

    def test_extreme_negative_funding_penalises_short(self):
        base_long, base_short, _ = compute_bias(
            direction="down", cvd_5m=-100_000, cvd_1h=-100_000, cvd_4h=-100_000,
            oi_change_5m=0, oi_change_1h=0,
            buy_pct_5m=0.5, buy_persistence_5m=0,
            funding_rate=0, natr=0.01,
        )
        funded_long, funded_short, _ = compute_bias(
            direction="down", cvd_5m=-100_000, cvd_1h=-100_000, cvd_4h=-100_000,
            oi_change_5m=0, oi_change_1h=0,
            buy_pct_5m=0.5, buy_persistence_5m=0,
            funding_rate=-0.002, natr=0.01,
        )
        assert funded_short < base_short


class TestNatrExhaustion:
    def test_extreme_move_suggests_exhaustion(self):
        """A move of 5× NATR should add fade bias."""
        long_s, short_s, _ = compute_bias(
            direction="up", cvd_5m=0, cvd_1h=0, cvd_4h=0,
            oi_change_5m=0, oi_change_1h=0,
            buy_pct_5m=0.5, buy_persistence_5m=0,
            funding_rate=0, natr=0.01, change_pct=5.0,
        )
        assert short_s > 0  # exhaustion adds short bias on up-impulse


class TestLabels:
    def test_strong_long_label(self):
        _, _, label = compute_bias(
            direction="up", cvd_5m=200_000, cvd_1h=200_000, cvd_4h=200_000,
            oi_change_5m=5.0, oi_change_1h=3.0,
            buy_pct_5m=0.8, buy_persistence_5m=0.8,
            funding_rate=0, natr=0.01, change_pct=1.0,
        )
        assert label == "LONG"

    def test_strong_short_label(self):
        _, _, label = compute_bias(
            direction="up", cvd_5m=-200_000, cvd_1h=-200_000, cvd_4h=-200_000,
            oi_change_5m=-3.0, oi_change_1h=-2.0,
            buy_pct_5m=0.2, buy_persistence_5m=-0.8,
            funding_rate=0, natr=0.01, change_pct=5.0,
        )
        assert label == "SHORT"

    def test_avoid_label_on_zero_signals(self):
        _, _, label = compute_bias(
            direction="up", cvd_5m=0, cvd_1h=0, cvd_4h=0,
            oi_change_5m=0, oi_change_1h=0,
            buy_pct_5m=0.5, buy_persistence_5m=0,
            funding_rate=0, natr=0,
        )
        assert label in ("AVOID", "NEUTRAL")

    def test_scores_clamped_to_100(self):
        long_s, short_s, _ = compute_bias(
            direction="up", cvd_5m=10_000_000, cvd_1h=10_000_000, cvd_4h=10_000_000,
            oi_change_5m=50, oi_change_1h=50,
            buy_pct_5m=1.0, buy_persistence_5m=1.0,
            funding_rate=0, natr=0.001, change_pct=0.5,
        )
        assert long_s <= 100
        assert short_s <= 100


class TestRaveScenario:
    """Simulate the RAVE-like case: pump on a shitcoin with mixed signals."""

    def test_pump_no_cvd_rising_oi_is_not_pure_short(self):
        """Pump + flat CVD + rising OI → not safe to short → label != SHORT."""
        _, _, label = compute_bias(
            direction="up", cvd_5m=500, cvd_1h=0, cvd_4h=0,
            oi_change_5m=8.0, oi_change_1h=5.0,
            buy_pct_5m=0.55, buy_persistence_5m=0.1,
            funding_rate=0.001, natr=0.02, change_pct=10.0,
        )
        assert label != "SHORT"
