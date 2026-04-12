"""Rolling window of computed signal snapshots per ticker.

Each ticker gets a fixed-size deque of ``SignalSnapshot`` objects, appended
at a configurable interval (default 15s).  The signal evaluator reads from
the tiered store, computes bias, and pushes snapshots here — detectors then
inspect the history deques.

Memory budget: 120 samples × ~200 B/sample × 500 tickers ≈ 12 MB.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(slots=True)
class SignalSnapshot:
    """Computed snapshot of all signals for one ticker at one point in time."""
    timestamp: float
    price: float
    cvd_5m: float
    cvd_1h: float
    cvd_4h: float
    oi_change_5m: float
    oi_change_1h: float
    buy_pct_5m: float
    buy_persist_5m: float      # signed [-1, 1]
    long_bias: float
    short_bias: float
    bias_label: str
    liq_buy_5m: float
    liq_sell_5m: float
    natr: float
    trend: str
    range_1m: float = 0.0
    volume_5m: float = 0.0     # CVD volume magnitude for volume spike detection


class SignalHistory:
    """Rolling window of computed snapshots per ticker."""

    def __init__(self, maxlen: int = 120) -> None:
        self._hist: dict[str, deque[SignalSnapshot]] = {}
        self._maxlen = maxlen

    def append(self, key: str, snap: SignalSnapshot) -> None:
        dq = self._hist.setdefault(key, deque(maxlen=self._maxlen))
        dq.append(snap)

    def get(self, key: str) -> deque[SignalSnapshot] | None:
        return self._hist.get(key)

    def latest(self, key: str) -> SignalSnapshot | None:
        dq = self._hist.get(key)
        if dq:
            return dq[-1]
        return None

    def at_age(self, key: str, seconds_ago: float) -> SignalSnapshot | None:
        """Find the snapshot closest to ``now - seconds_ago``.

        Returns the nearest match within a tolerance, or None if the history
        is too short or the age is out of range.
        """
        dq = self._hist.get(key)
        if not dq:
            return None
        target = dq[-1].timestamp - seconds_ago
        best: SignalSnapshot | None = None
        best_dist = float("inf")
        for s in dq:
            d = abs(s.timestamp - target)
            if d < best_dist:
                best_dist = d
                best = s
        return best

    def remove_ticker(self, key: str) -> None:
        self._hist.pop(key, None)
