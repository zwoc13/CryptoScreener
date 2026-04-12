"""Directional bias scoring for impulse events.

Pure functions — no store/engine dependencies. Given an impulse direction and
order-flow/market-structure features, returns a (long_bias, short_bias, label)
tuple that tells the user whether to fade, follow, or avoid the move.

All score contributions are tunable via the module-level constants.
"""

from __future__ import annotations

# ── Weight caps per signal component ────────────────────────────────────────
W_CVD = 30        # CVD / price alignment
W_OI = 25         # OI direction (squeeze detector)
W_BUYS = 20       # Buy% persistence
W_FUNDING = 15    # Funding rate crowding
W_NATR = 10       # Exhaustion vs. continuation

# ── Label thresholds ────────────────────────────────────────────────────────
LABEL_STRONG = 40   # |long - short| > this → LONG or SHORT
LABEL_BOTH = 30     # both > this → CONFLICTED
LABEL_WEAK = 15     # both < this → AVOID

# ── Funding rate levels (per 8h equivalent) ─────────────────────────────────
FUNDING_HIGH = 0.0005    # 0.05% per 8h — moderately crowded
FUNDING_EXTREME = 0.001  # 0.1% per 8h — very crowded


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _normalise_funding(rate: float, interval_h: int = 8) -> float:
    """Normalise funding rate to an 8h equivalent."""
    if interval_h <= 0:
        return rate
    return rate * (8 / interval_h)


def compute_bias(
    direction: str,           # "up" | "down" — the impulse direction
    cvd_5m: float,
    cvd_1h: float,
    cvd_4h: float,
    oi_change_5m: float,      # OI % change over 5m
    oi_change_1h: float,      # OI % change over 1h
    buy_pct_5m: float,        # trade imbalance [0, 1], 0.5 = balanced
    buy_persistence_5m: float,  # signed [-1, 1]
    funding_rate: float,      # raw per-interval rate
    natr: float,              # NATR 5m/14
    change_pct: float = 0.0,  # impulse magnitude %
    funding_interval_h: int = 8,
) -> tuple[float, float, str]:
    """Compute directional bias scores for an impulse event.

    Returns ``(long_bias_score, short_bias_score, label)`` where scores
    are in [0, 100] and label is one of:

    * ``"LONG"``  — order flow supports continuation / long entry
    * ``"SHORT"`` — order flow supports fading / short entry
    * ``"CONFLICTED"`` — mixed signals, caution
    * ``"AVOID"`` — too little conviction either way
    * ``"NEUTRAL"`` — moderate, no strong bias
    """
    is_up = direction == "up"
    long_score = 0.0
    short_score = 0.0

    # ── 1. CVD / price alignment (±W_CVD) ───────────────────────────────
    # Positive CVD on an up-impulse = real buyers → supports longs.
    # Negative/flat CVD on an up-impulse = no real buying → fadeable.
    # Mirror for down-impulse.
    cvd_combined = cvd_5m + cvd_1h * 0.5 + cvd_4h * 0.25
    if is_up:
        if cvd_combined > 0:
            long_score += _clamp(W_CVD * min(abs(cvd_combined) / 50_000, 1.0), 0, W_CVD)
        else:
            short_score += _clamp(W_CVD * min(abs(cvd_combined) / 50_000, 1.0), 0, W_CVD)
    else:
        if cvd_combined < 0:
            short_score += _clamp(W_CVD * min(abs(cvd_combined) / 50_000, 1.0), 0, W_CVD)
        else:
            long_score += _clamp(W_CVD * min(abs(cvd_combined) / 50_000, 1.0), 0, W_CVD)

    # ── 2. OI direction (±W_OI) ─────────────────────────────────────────
    # Up-impulse + OI rising fast → leveraged longs piling in = squeeze risk
    #   → penalise shorts, boost longs slightly.
    # Up-impulse + OI flat/falling → spot-driven, safer to fade.
    oi_signal = oi_change_5m * 0.6 + oi_change_1h * 0.4
    if is_up:
        if oi_signal > 1.0:
            # OI rising on pump — squeeze risk for shorts
            long_score += _clamp(W_OI * min(oi_signal / 5.0, 1.0), 0, W_OI)
        elif oi_signal < -0.5:
            # OI falling on pump — longs closing, likely exhaustion
            short_score += _clamp(W_OI * min(abs(oi_signal) / 5.0, 1.0), 0, W_OI)
        else:
            # OI flat — mild fade signal
            short_score += W_OI * 0.3
    else:
        if oi_signal > 1.0:
            # OI rising on dump — leveraged shorts piling in
            short_score += _clamp(W_OI * min(oi_signal / 5.0, 1.0), 0, W_OI)
        elif oi_signal < -0.5:
            # OI falling on dump — shorts closing, bounce likely
            long_score += _clamp(W_OI * min(abs(oi_signal) / 5.0, 1.0), 0, W_OI)
        else:
            long_score += W_OI * 0.3

    # ── 3. Buy% persistence (±W_BUYS) ───────────────────────────────────
    # Sustained one-sided imbalance reinforces matching direction.
    if buy_persistence_5m > 0.3:
        # Buy-dominant flow
        if is_up:
            long_score += W_BUYS * _clamp(buy_persistence_5m, 0, 1)
        else:
            # Buying into a dump → potential reversal
            long_score += W_BUYS * _clamp(buy_persistence_5m * 0.5, 0, 1)
    elif buy_persistence_5m < -0.3:
        # Sell-dominant flow
        if not is_up:
            short_score += W_BUYS * _clamp(abs(buy_persistence_5m), 0, 1)
        else:
            # Selling into a pump → potential reversal
            short_score += W_BUYS * _clamp(abs(buy_persistence_5m) * 0.5, 0, 1)

    # ── 4. Funding rate crowding (±W_FUNDING) ───────────────────────────
    # Extreme positive funding on a pump → crowded longs → squeeze risk
    # for longs (funding drag), but shorts are also risky (momentum).
    # Net effect: penalise the crowded side.
    norm_rate = _normalise_funding(funding_rate, funding_interval_h)
    if abs(norm_rate) >= FUNDING_EXTREME:
        penalty = W_FUNDING
    elif abs(norm_rate) >= FUNDING_HIGH:
        penalty = W_FUNDING * 0.5
    else:
        penalty = 0.0

    if norm_rate > 0:
        # Positive funding → longs pay shorts → penalise long bias
        long_score = max(0, long_score - penalty)
        # On a pump with extreme positive funding → shorts are dangerous
        if is_up and abs(norm_rate) >= FUNDING_EXTREME:
            short_score = max(0, short_score - penalty * 0.5)
    elif norm_rate < 0:
        # Negative funding → shorts pay longs → penalise short bias
        short_score = max(0, short_score - penalty)
        if not is_up and abs(norm_rate) >= FUNDING_EXTREME:
            long_score = max(0, long_score - penalty * 0.5)

    # ── 5. NATR exhaustion check (±W_NATR) ──────────────────────────────
    # Impulses far above the NATR baseline are more likely exhaustion
    # (favor fade); impulses near baseline are more likely continuation.
    if natr > 0 and change_pct > 0:
        natr_ratio = change_pct / (natr * 100)  # how many NATRs this impulse is
        if natr_ratio > 3.0:
            # Extreme move — likely exhaustion
            exhaust = _clamp(W_NATR * min((natr_ratio - 2) / 3, 1.0), 0, W_NATR)
            if is_up:
                short_score += exhaust
            else:
                long_score += exhaust
        elif natr_ratio < 1.5:
            # Small move — continuation more likely
            cont = W_NATR * 0.5
            if is_up:
                long_score += cont
            else:
                short_score += cont

    # ── Clamp to [0, 100] ───────────────────────────────────────────────
    long_score = _clamp(long_score, 0, 100)
    short_score = _clamp(short_score, 0, 100)

    # ── Label decision ──────────────────────────────────────────────────
    diff = long_score - short_score
    if diff > LABEL_STRONG:
        label = "LONG"
    elif diff < -LABEL_STRONG:
        label = "SHORT"
    elif long_score > LABEL_BOTH and short_score > LABEL_BOTH:
        label = "CONFLICTED"
    elif long_score < LABEL_WEAK and short_score < LABEL_WEAK:
        label = "AVOID"
    else:
        label = "NEUTRAL"

    return round(long_score, 1), round(short_score, 1), label
