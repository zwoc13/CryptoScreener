"""Pure-function detectors that produce typed events from signal history.

Each detector takes ``(ticker, history_deque, store)`` and returns
``Optional[AlertEvent]``.  They are pure functions — trivially unit-testable
against synthetic history deques.

Thresholds live as module constants.  After live testing they can move to
``config.signals.*``.
"""

from __future__ import annotations

import math
from collections import deque

from .models import TickerState
from .signal_history import SignalSnapshot
from .store import Store
from .models import (
    BiasChangedEvent,
    CvdBurstEvent,
    LiquidationCascadeEvent,
    OiFlipEvent,
    DivergenceEvent,
    VolumeSpikeEvent,
    TrendChangedEvent,
)


# ── Thresholds ──────────────────────────────────────────────────────────────
# CVD burst: 1m CVD must exceed baseline by this multiplier.
# Lower = more sensitive.  2.5 means 2.5× the typical 1m CVD magnitude.
K_CVD_BURST_MULTIPLIER = 2.5

# CVD burst baseline: use the last N history samples (at 15s interval, 20 = 5min)
K_CVD_BASELINE_SAMPLES = 20

# Liq cascade: 1m liq USD must exceed this absolute minimum (filter out dust)
K_LIQ_ABSOLUTE_MIN = 50_000.0  # $50k in 1min

# Liq cascade: multiplier over baseline
K_LIQ_BURST_MULTIPLIER = 3.0

# Liq cascade baseline: use last N history samples
K_LIQ_BASELINE_SAMPLES = 20

# OI flip number of consecutive ticks the sign must persist before firing
K_OI_FLIP_PERSIST = 2

# Divergence: how many snapshots back to check for new high/low
K_DIVERGENCE_WINDOW = 40  # ~10 min at 15s interval

# Divergence: minimum NATR to consider divergence meaningful
K_DIVERGENCE_MIN_NATR = 0.1

# Volume spike: 5m volume vs. 24h/288 baseline (5m bucket of 24h)
K_VOLUME_SPIKE_MULTIPLIER = 3.0


def _key(ticker: TickerState) -> str:
    return f"{ticker.exchange}:{ticker.symbol}"


# ── Detectors (ordered by priority) ─────────────────────────────────────────


def detect_bias_changed(
    ticker: TickerState,
    history: deque[SignalSnapshot],
    store: Store,
) -> BiasChangedEvent | None:
    """Bias label at T-0 differs from bias label at T-(N minutes).

    This is the exit signal — highest priority.  We compare the latest
    snapshot against one from ~5 minutes ago (20 samples at 15s).
    """
    if history is None or len(history) < 2:
        return None
    now_snap = history[-1]
    if now_snap.bias_label == "NEUTRAL":
        return None
    # Find the oldest non-NEUTRAL snapshot for reference
    # Compare last vs ~5min ago for the change
    lookback_idx = max(0, len(history) - K_CVD_BASELINE_SAMPLES - 1)
    old_snap = history[lookback_idx]
    if now_snap.bias_label != old_snap.bias_label:
        direction = "neutral"
        if now_snap.bias_label == "LONG":
            direction = "up"
        elif now_snap.bias_label == "SHORT":
            direction = "down"

        confidence = min(100.0, abs(now_snap.long_bias - old_snap.long_bias)
                         + abs(now_snap.short_bias - old_snap.short_bias))
        return BiasChangedEvent(
            exchange=ticker.exchange,
            symbol=ticker.symbol,
            feed_id=ticker.feed_id,
            timestamp=now_snap.timestamp,
            direction=direction,
            confidence=confidence,
            prev_label=old_snap.bias_label,
            new_label=now_snap.bias_label,
            prev_long=old_snap.long_bias,
            prev_short=old_snap.short_bias,
            new_long=now_snap.long_bias,
            new_short=now_snap.short_bias,
        )
    return None


def detect_cvd_burst(
    ticker: TickerState,
    history: deque[SignalSnapshot],
    store: Store,
) -> CvdBurstEvent | None:
    """1m CVD > K × stddev of recent baseline.

    Entry early-warning: aggressive flow between impulses.
    """
    if history is None or len(history) < 10:
        return None
    latest = history[-1]
    recent_1m = store.get_cvd_rolling(ticker.exchange, ticker.symbol, 60)
    if recent_1m == 0:
        return None
    # Build baseline from history snapshots
    baseline_n = min(K_CVD_BASELINE_SAMPLES, len(history) - 1)
    if baseline_n < 3:
        return None
    baseline = [abs(s.cvd_5m) for s in list(history)[-baseline_n - 1:-1]]
    if not baseline:
        return None
    mean_cv = sum(baseline) / len(baseline)
    if mean_cv == 0:
        return None

    if abs(recent_1m) > mean_cv * K_CVD_BURST_MULTIPLIER:
        direction = "up" if recent_1m > 0 else "down"
        ratio = abs(recent_1m) / mean_cv
        return CvdBurstEvent(
            exchange=ticker.exchange,
            symbol=ticker.symbol,
            feed_id=ticker.feed_id,
            timestamp=latest.timestamp,
            direction=direction,
            confidence=min(100.0, ratio * 20),
            cvd_burst_1m=recent_1m,
            cvd_baseline_5m=round(mean_cv, 2),
            burst_ratio=round(ratio, 2),
        )
    return None


def detect_liquidation_cascade(
    ticker: TickerState,
    history: deque[SignalSnapshot],
    store: Store,
) -> LiquidationCascadeEvent | None:
    """1m liq vol > $X AND > K × 30m baseline.

    Forced flow — bot should react differently than to organic flow.
    """
    if history is None or len(history) < 10:
        return None
    latest = history[-1]
    liq_buy, liq_sell = store.get_liq_rolling(ticker.exchange, ticker.symbol, 60)
    total_liq = liq_buy + liq_sell
    if total_liq < K_LIQ_ABSOLUTE_MIN:
        return None

    baseline_n = min(K_LIQ_BASELINE_SAMPLES, len(history) - 1)
    if baseline_n < 3:
        return None
    baseline_liq = [s.liq_buy_5m + s.liq_sell_5m for s in list(history)[-baseline_n - 1:-1]]
    mean_liq = sum(baseline_liq) / len(baseline_liq) if baseline_liq else 0
    if mean_liq == 0:
        return None

    if total_liq > mean_liq * K_LIQ_BURST_MULTIPLIER:
        ratio = total_liq / mean_liq
        dominant = "buy" if liq_buy > liq_sell else ("sell" if liq_sell > liq_buy else "mixed")
        direction = "up" if liq_buy > liq_sell else "down" if liq_sell > liq_buy else "neutral"
        return LiquidationCascadeEvent(
            exchange=ticker.exchange,
            symbol=ticker.symbol,
            feed_id=ticker.feed_id,
            timestamp=latest.timestamp,
            direction=direction,
            confidence=min(100.0, ratio * 15),
            liq_usd_1m=round(total_liq, 2),
            liq_baseline_5m=round(mean_liq, 2),
            burst_ratio=round(ratio, 2),
            dominant_side=dominant,
        )
    return None


def detect_oi_flip(
    ticker: TickerState,
    history: deque[SignalSnapshot],
    store: Store,
) -> OiFlipEvent | None:
    """OI change sign flipped AND has persisted for M ticks.

    Position-closing phase — exit signal.
    """
    if history is None or len(history) < K_OI_FLIP_PERSIST + 1:
        return None

    latest = history[-1]
    current_oi = latest.oi_change_5m

    # Check if sign flipped from previous snapshots and persisted
    sign_current = 1.0 if current_oi > 0 else (-1.0 if current_oi < 0 else 0.0)
    if sign_current == 0:
        return None

    persist_count = 0
    hist_list = list(history)
    for s in reversed(hist_list[:-1]):
        s_sign = 1.0 if s.oi_change_5m > 0 else (-1.0 if s.oi_change_5m < 0 else 0.0)
        if s_sign == sign_current:
            persist_count += 1
        else:
            break

    if persist_count >= K_OI_FLIP_PERSIST:
        # Check that we actually flipped (not just stable)
        older_idx = max(0, len(hist_list) - persist_count - K_OI_FLIP_PERSIST - 1)
        old_snap = hist_list[older_idx]
        old_sign = 1.0 if old_snap.oi_change_5m > 0 else (-1.0 if old_snap.oi_change_5m < 0 else 0.0)
        if old_sign != sign_current:
            direction = "up" if sign_current > 0 else "down"
            return OiFlipEvent(
                exchange=ticker.exchange,
                symbol=ticker.symbol,
                feed_id=ticker.feed_id,
                timestamp=latest.timestamp,
                direction=direction,
                confidence=min(100.0, 30 + persist_count * 10),
                oi_change_prev=old_snap.oi_change_5m,
                oi_change_now=current_oi,
                persist_ticks=persist_count,
            )
    return None


def detect_divergence(
    ticker: TickerState,
    history: deque[SignalSnapshot],
    store: Store,
) -> DivergenceEvent | None:
    """Price made new high/low in window, CVD did not — classic exhaustion.

    Fade signal.
    """
    if history is None or len(history) < K_DIVERGENCE_WINDOW:
        return None
    latest = history[-1]
    if latest.natr < K_DIVERGENCE_MIN_NATR:
        return None

    window = list(history)[-K_DIVERGENCE_WINDOW:]
    prices = [s.price for s in window]
    cvds = [s.cvd_5m for s in window]

    # Check bullish divergence: price made new low but CVD didn't confirm
    if len(prices) < 2:
        return None

    price_high = prices[-1]
    price_high_prev = max(prices[:-1])

    price_low = prices[-1]
    price_low_prev = min(prices[:-1])

    cvd_curr = cvds[-1]
    cvd_prev_high = max(cvds[:-1])
    cvd_prev_low = min(cvds[:-1])

    # Bearish divergence: price new high, CVD not making high
    if price_high > price_high_prev and cvd_curr < cvd_prev_high and ticker.trend == "UP":
        spread_ratio = abs(cvd_prev_high - cvd_curr) / max(abs(cvd_prev_high), 1)
        return DivergenceEvent(
            exchange=ticker.exchange,
            symbol=ticker.symbol,
            feed_id=ticker.feed_id,
            timestamp=latest.timestamp,
            direction="down",
            confidence=min(100.0, spread_ratio * 50),
            price_high=price_high,
            price_low=price_low,
            cvd_high=cvd_curr,
            cvd_low=cvd_prev_low,
        )

    # Bullish divergence: price new low, CVD not making low
    if price_low < price_low_prev and cvd_curr > cvd_prev_low and ticker.trend == "DOWN":
        spread_ratio = abs(cvd_curr - cvd_prev_low) / max(abs(cvd_prev_low), 1)
        return DivergenceEvent(
            exchange=ticker.exchange,
            symbol=ticker.symbol,
            feed_id=ticker.feed_id,
            timestamp=latest.timestamp,
            direction="up",
            confidence=min(100.0, spread_ratio * 50),
            price_high=price_high,
            price_low=price_low,
            cvd_high=cvd_prev_high,
            cvd_low=cvd_curr,
        )

    return None


def detect_volume_spike(
    ticker: TickerState,
    history: deque[SignalSnapshot],
    store: Store,
) -> VolumeSpikeEvent | None:
    """5m volume > K × (24h volume / 288).

    Attention signal — not directional.  We approximate 24h baseline from
    the latest history snapshot's CVD magnitude and the volume_5m field.
    Also check CVD buy+sell magnitude directly.
    """
    if history is None or len(history) < K_CVD_BASELINE_SAMPLES:
        return None
    latest = history[-1]
    # Get recent volume proxy from CVD buy+sell magnitude
    buy_vol = store.get_cvd_rolling(ticker.exchange, ticker.symbol, 300)  # CVD 5m = net delta
    # Use magnitude from recent snapshots to estimate volume
    baseline_n = K_CVD_BASELINE_SAMPLES
    baseline_vol = [abs(s.cvd_5m) for s in list(history)[-baseline_n - 1:-1]]
    if not baseline_vol:
        return None
    mean_vol = sum(baseline_vol) / len(baseline_vol)
    if mean_vol == 0:
        return None

    if abs(latest.cvd_5m) > mean_vol * K_VOLUME_SPIKE_MULTIPLIER:
        ratio = abs(latest.cvd_5m) / mean_vol
        return VolumeSpikeEvent(
            exchange=ticker.exchange,
            symbol=ticker.symbol,
            feed_id=ticker.feed_id,
            timestamp=latest.timestamp,
            direction="neutral",
            confidence=min(100.0, ratio * 15),
            volume_5m=round(abs(latest.cvd_5m), 2),
            volume_baseline=round(mean_vol, 2),
            spike_ratio=round(ratio, 2),
        )
    return None


def detect_trend_change(
    ticker: TickerState,
    history: deque[SignalSnapshot],
    store: Store,
) -> TrendChangedEvent | None:
    """Trend string changed between snapshots — regime change."""
    if history is None or len(history) < 2:
        return None
    latest = history[-1]
    old_snap = history[-2]
    if latest.trend != old_snap.trend and latest.trend != "-":
        confidence = 50.0  # base confidence for any trend change
        if len(history) >= 3 and history[-3].trend != old_snap.trend:
            # Trend changed again — lower confidence, possibly whipsaw
            confidence = 25.0
        elif len(history) >= 5:
            # Check if the trend has stayed consistent
            same_count = sum(1 for s in list(history)[-5:-1] if s.trend == latest.trend)
            if same_count >= 3:
                confidence = 75.0  # trend confirmed across multiple samples
        return TrendChangedEvent(
            exchange=ticker.exchange,
            symbol=ticker.symbol,
            feed_id=ticker.feed_id,
            timestamp=latest.timestamp,
            direction="neutral",
            confidence=confidence,
            prev_trend=old_snap.trend,
            new_trend=latest.trend,
        )
    return None


# Ordered list of detectors for the evaluator to run deterministically.
DETECTORS = [
    detect_bias_changed,
    detect_cvd_burst,
    detect_liquidation_cascade,
    detect_oi_flip,
    detect_divergence,
    detect_volume_spike,
    detect_trend_change,
]
