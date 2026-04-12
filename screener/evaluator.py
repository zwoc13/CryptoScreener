"""Background signal evaluator loop.

Periodically samples all tickers from the tiered store, computes bias, and
appends a ``SignalSnapshot`` to ``SignalHistory``.  Then runs the detector
chain and dispatches any events that fire.
"""

from __future__ import annotations

import asyncio
import logging
from time import time

from .alerts import AlertDispatcher
from .signal_history import SignalHistory, SignalSnapshot
from .store import Store
from .detectors import DETECTORS
from .config import Settings

logger = logging.getLogger(__name__)


def _sample_signals(store: Store, ticker, now: float) -> SignalSnapshot:
    """Read all relevant signals from the store for one ticker."""
    from . import bias as bias_mod

    cvd_5m = store.get_cvd_rolling(ticker.exchange, ticker.symbol, 300)
    cvd_1h = store.get_cvd_rolling(ticker.exchange, ticker.symbol, 3600)
    cvd_4h = store.get_cvd_rolling(ticker.exchange, ticker.symbol, 14400)
    oi_5m = ticker.oi_change_5m_pct
    oi_1h = store.get_oi_change_pct(ticker.exchange, ticker.symbol, 3600)
    buy_pct_5m = store.get_trade_imbalance(ticker.exchange, ticker.symbol, 300)
    buy_persist_5m = store.get_trade_imbalance_persistence(
        ticker.exchange, ticker.symbol, 300,
    )
    liq_buy, liq_sell = store.get_liq_rolling(ticker.exchange, ticker.symbol, 300)
    long_score, short_score, label = bias_mod.compute_bias(
        direction="up",  # neutral direction for sampling; bias is label-agnostic
        cvd_5m=cvd_5m, cvd_1h=cvd_1h, cvd_4h=cvd_4h,
        oi_change_5m=oi_5m, oi_change_1h=oi_1h,
        buy_pct_5m=buy_pct_5m, buy_persistence_5m=buy_persist_5m,
        funding_rate=ticker.funding_rate, natr=ticker.natr_5m_14,
        change_pct=0.0,  # no active impulse for sampling
        funding_interval_h=ticker.funding_interval_h,
    )

    # CVD volume magnitude (buy+sell absolute sum for volume tracking)
    k = f"{ticker.exchange}:{ticker.symbol}"
    buy_vol = sell_vol = 0.0
    t1 = store._cvd_1s.get(k)
    if t1:
        cutoff = int(now) - 300
        for b in t1:
            if b.timestamp < cutoff:
                continue
            buy_vol += b.buy_vol
            sell_vol += b.sell_vol

    return SignalSnapshot(
        timestamp=now,
        price=ticker.last_price,
        cvd_5m=round(cvd_5m, 2),
        cvd_1h=round(cvd_1h, 2),
        cvd_4h=round(cvd_4h, 2),
        oi_change_5m=round(oi_5m, 2),
        oi_change_1h=round(oi_1h, 2),
        buy_pct_5m=round(buy_pct_5m, 4),
        buy_persist_5m=round(buy_persist_5m, 4),
        long_bias=long_score,
        short_bias=short_score,
        bias_label=label,
        liq_buy_5m=round(liq_buy, 2),
        liq_sell_5m=round(liq_sell, 2),
        natr=round(ticker.natr_5m_14, 4),
        trend=ticker.trend,
        range_1m=ticker.range_1m,
        volume_5m=round(buy_vol + sell_vol, 2),
    )


async def run_signal_evaluator(
    store: Store,
    history: SignalHistory,
    dispatcher: AlertDispatcher,
    settings: Settings,
) -> None:
    interval = getattr(settings, "signals", None)
    if interval is not None:
        interval = getattr(interval, "sample_interval_s", 15)
    else:
        interval = 15

    logger.info("Signal evaluator started (interval=%.1fs)", interval)
    fired_counts: dict[str, int] = {}

    while True:
        try:
            now = time()
            for ticker in list(store._tickers.values()):
                key = f"{ticker.exchange}:{ticker.symbol}"
                snap = _sample_signals(store, ticker, now)
                history.append(key, snap)

                hist = history.get(key)
                if hist is None:
                    continue

                for detect_fn in DETECTORS:
                    event = detect_fn(ticker, hist, store)
                    if event is not None:
                        etype = detect_fn.__name__
                        cooldown_key = f"{ticker.exchange}:{ticker.symbol}:{etype}"
                        last_fired = fired_counts.get(cooldown_key, 0.0)
                        # Per-detector cooldown to avoid spam
                        if now - last_fired < 60:  # 1min per-ticker cooldown
                            continue
                        fired_counts[cooldown_key] = now
                        await dispatcher.dispatch(event)
                        logger.debug(
                            "Detector fired: %s %s %s",
                            etype, ticker.exchange, ticker.symbol,
                        )
        except Exception:
            logger.exception("Signal evaluator error")
        await asyncio.sleep(interval)
