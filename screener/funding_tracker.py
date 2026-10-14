"""Funding event tracker — captures before/after market context around extreme funding.

Hooks into the signal evaluator loop. When a symbol hits >= 2% funding rate,
it captures:
  - BEFORE: last 30 min of snapshots from SignalHistory
  - AT EVENT: full market state from Store
  - AFTER: continues capturing every 15s for 30 min

Stores to SQLite for analysis of whale behavior, price reversion, OI shifts.

Usage: instantiate FundingTracker, call check() from evaluator loop.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from .signal_history import SignalHistory, SignalSnapshot
from .store import Store

logger = logging.getLogger(__name__)

# Threshold to start tracking
EXTREME_THRESHOLD = 0.015  # 1.5%
# How long to track after the event (seconds)
AFTER_WINDOW_S = 1800     # 30 min
# How many "before" snapshots to capture (at 15s interval, 120 = 30 min)
BEFORE_SNAPSHOTS = 120


@dataclass
class TrackedEvent:
    """An active funding event being tracked."""
    symbol: str
    exchange: str
    event_ts: float
    funding_rate: float
    direction: str  # "long_heavy" | "short_heavy"
    price_at_event: float
    oi_at_event: float
    cvd_5m_at_event: float
    cvd_1h_at_event: float
    long_short_ratio: float
    before_captured: bool = False
    snapshots_after: int = 0
    end_ts: float = 0.0  # when to stop tracking

    def is_active(self, now: float) -> bool:
        return now < self.end_ts


class FundingTracker:
    """Tracks market state before/after extreme funding events."""

    def __init__(self, db_path: str, store: Store, history: SignalHistory) -> None:
        self._store = store
        self._history = history
        self._db = sqlite3.connect(db_path)
        self._init_db()
        self._active: dict[str, TrackedEvent] = {}  # symbol -> event
        self._last_check_ts: float = 0.0

    def _init_db(self) -> None:
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                ts REAL NOT NULL,
                phase TEXT NOT NULL,
                symbol TEXT NOT NULL,
                price REAL,
                cvd_5m REAL,
                cvd_1h REAL,
                cvd_4h REAL,
                oi_change_5m REAL,
                oi_change_1h REAL,
                buy_pct_5m REAL,
                buy_persist_5m REAL,
                long_bias REAL,
                short_bias REAL,
                bias_label TEXT,
                liq_buy_5m REAL,
                liq_sell_5m REAL,
                natr REAL,
                trend TEXT,
                volume_5m REAL,
                funding_rate REAL,
                long_short_ratio REAL,
                open_interest REAL,
                raw_json TEXT
            )
        """)
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT,
                funding_rate REAL,
                direction TEXT,
                price_at_event REAL,
                oi_at_event REAL,
                cvd_5m_at_event REAL,
                cvd_1h_at_event REAL,
                long_short_ratio REAL,
                before_snapshots INTEGER DEFAULT 0,
                after_snapshots INTEGER DEFAULT 0,
                price_5m_after REAL,
                price_15m_after REAL,
                price_30m_after REAL,
                oi_5m_after REAL,
                oi_15m_after REAL,
                oi_30m_after REAL,
                max_price_move_pct REAL,
                reversion_pct REAL,
                completed INTEGER DEFAULT 0
            )
        """)
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_snap_event ON snapshots(event_id)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_snap_ts ON snapshots(ts)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_events_sym ON events(symbol, ts)")
        self._db.commit()

    def check(self, now: float) -> None:
        """Called every evaluator tick (~15s). Checks for new events and tracks active ones."""
        # Rate-limit checks to every 15s
        if now - self._last_check_ts < 10:
            return
        self._last_check_ts = now

        # 1. Check for new extreme funding events
        for ticker in list(self._store._tickers.values()):
            rate = ticker.funding_rate
            if abs(rate) < EXTREME_THRESHOLD:
                continue
            key = ticker.symbol
            if key in self._active:
                continue  # already tracking

            # New extreme event
            direction = "long_heavy" if rate > 0 else "short_heavy"
            event = TrackedEvent(
                symbol=ticker.symbol,
                exchange=ticker.exchange,
                event_ts=now,
                funding_rate=rate,
                direction=direction,
                price_at_event=ticker.last_price,
                oi_at_event=ticker.open_interest,
                cvd_5m_at_event=self._store.get_cvd_rolling(
                    ticker.exchange, ticker.symbol, 300),
                cvd_1h_at_event=self._store.get_cvd_rolling(
                    ticker.exchange, ticker.symbol, 3600),
                long_short_ratio=ticker.long_short_ratio,
                end_ts=now + AFTER_WINDOW_S,
            )

            # Save event to DB
            cursor = self._db.execute("""
                INSERT INTO events (ts, symbol, exchange, funding_rate, direction,
                    price_at_event, oi_at_event, cvd_5m_at_event, cvd_1h_at_event,
                    long_short_ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                now, ticker.symbol, ticker.exchange, rate, direction,
                ticker.last_price, ticker.open_interest,
                event.cvd_5m_at_event, event.cvd_1h_at_event,
                ticker.long_short_ratio,
            ))
            event_id = cursor.lastrowid

            # Capture BEFORE snapshots from SignalHistory
            self._capture_before(event, event_id)

            self._active[key] = event
            self._db.commit()

            logger.info(
                "[FundingTracker] EXTREME: %s %s rate=%.2f%% price=%.6g OI=%.0f L/S=%.2f "
                "(%d before snapshots captured)",
                ticker.exchange, ticker.symbol, rate * 100,
                ticker.last_price, ticker.open_interest,
                ticker.long_short_ratio, event.snapshots_after,
            )

        # 2. Track active events
        completed = []
        for key, event in self._active.items():
            if not event.is_active(now):
                completed.append(key)
                continue

            ticker = self._store.get_or_create_ticker(event.exchange, event.symbol)
            snap = self._sample_snapshot(ticker, now)

            # Determine event_id
            row = self._db.execute(
                "SELECT id FROM events WHERE symbol = ? AND ts = ?",
                (event.symbol, event.event_ts)
            ).fetchone()
            if row:
                event_id = row[0]
                self._store_snapshot(event_id, "after", snap)
                event.snapshots_after += 1

                # Update event with periodic price/OI checkpoints
                elapsed = now - event.event_ts
                if abs(elapsed - 300) < 20:  # ~5 min
                    self._db.execute(
                        "UPDATE events SET price_5m_after = ?, oi_5m_after = ? WHERE id = ?",
                        (snap.get("price"), snap.get("open_interest"), event_id))
                elif abs(elapsed - 900) < 20:  # ~15 min
                    self._db.execute(
                        "UPDATE events SET price_15m_after = ?, oi_15m_after = ? WHERE id = ?",
                        (snap.get("price"), snap.get("open_interest"), event_id))
                elif abs(elapsed - 1740) < 20:  # ~29 min
                    self._db.execute(
                        "UPDATE events SET price_30m_after = ?, oi_30m_after = ? WHERE id = ?",
                        (snap.get("price"), snap.get("open_interest"), event_id))

        # 3. Finalize completed events
        for key in completed:
            event = self._active.pop(key)
            row = self._db.execute(
                "SELECT id FROM events WHERE symbol = ? AND ts = ?",
                (event.symbol, event.event_ts)
            ).fetchone()
            if row:
                event_id = row[0]
                # Compute max move and reversion
                self._db.execute("""
                    UPDATE events SET
                        after_snapshots = ?,
                        completed = 1,
                        max_price_move_pct = (
                            SELECT MAX(ABS((price - ?) / ? * 100)) FROM snapshots
                            WHERE event_id = ? AND phase = 'after'
                        )
                    WHERE id = ?
                """, (
                    event.snapshots_after,
                    event.price_at_event, event.price_at_event,
                    event_id, event_id,
                ))
            self._db.commit()
            logger.info(
                "[FundingTracker] COMPLETED: %s rate=%.2f%% after=%d snapshots",
                event.symbol, event.funding_rate * 100, event.snapshots_after,
            )

        if completed:
            self._db.commit()

    def _capture_before(self, event: TrackedEvent, event_id: int) -> None:
        """Capture historical snapshots from SignalHistory as 'before' phase."""
        hist = self._history.get(f"{event.exchange}:{event.symbol}")
        if not hist:
            return

        count = 0
        for snap in hist:
            d = self._snapshot_to_dict(snap)
            self._store_snapshot(event_id, "before", d)
            count += 1

        event.before_captured = True
        event.snapshots_after = 0  # reset counter (before doesn't count)
        logger.debug(
            "[FundingTracker] Captured %d before snapshots for %s",
            count, event.symbol,
        )

    def _sample_snapshot(self, ticker, now: float) -> dict:
        """Sample current market state from store (same as evaluator)."""
        from . import bias as bias_mod

        cvd_5m = self._store.get_cvd_rolling(ticker.exchange, ticker.symbol, 300)
        cvd_1h = self._store.get_cvd_rolling(ticker.exchange, ticker.symbol, 3600)
        cvd_4h = self._store.get_cvd_rolling(ticker.exchange, ticker.symbol, 14400)
        oi_5m = ticker.oi_change_5m_pct
        oi_1h = self._store.get_oi_change_pct(ticker.exchange, ticker.symbol, 3600)
        buy_pct = self._store.get_trade_imbalance(ticker.exchange, ticker.symbol, 300)
        buy_persist = self._store.get_trade_imbalance_persistence(
            ticker.exchange, ticker.symbol, 300)
        liq_buy, liq_sell = self._store.get_liq_rolling(
            ticker.exchange, ticker.symbol, 300)

        long_score, short_score, label = bias_mod.compute_bias(
            direction="up",
            cvd_5m=cvd_5m, cvd_1h=cvd_1h, cvd_4h=cvd_4h,
            oi_change_5m=oi_5m, oi_change_1h=oi_1h,
            buy_pct_5m=buy_pct, buy_persistence_5m=buy_persist,
            funding_rate=ticker.funding_rate, natr=ticker.natr_5m_14,
            change_pct=0.0, funding_interval_h=ticker.funding_interval_h,
        )

        return {
            "price": ticker.last_price,
            "cvd_5m": round(cvd_5m, 2),
            "cvd_1h": round(cvd_1h, 2),
            "cvd_4h": round(cvd_4h, 2),
            "oi_change_5m": round(oi_5m, 2),
            "oi_change_1h": round(oi_1h, 2),
            "buy_pct_5m": round(buy_pct, 4),
            "buy_persist_5m": round(buy_persist, 4),
            "long_bias": long_score,
            "short_bias": short_score,
            "bias_label": label,
            "liq_buy_5m": round(liq_buy, 2),
            "liq_sell_5m": round(liq_sell, 2),
            "natr": round(ticker.natr_5m_14, 4),
            "trend": ticker.trend,
            "volume_5m": 0.0,  # not critical for funding analysis
            "funding_rate": ticker.funding_rate,
            "long_short_ratio": ticker.long_short_ratio,
            "open_interest": ticker.open_interest,
        }

    def _snapshot_to_dict(self, snap: SignalSnapshot) -> dict:
        """Convert a SignalSnapshot to dict for storage."""
        return {
            "price": snap.price,
            "cvd_5m": snap.cvd_5m,
            "cvd_1h": snap.cvd_1h,
            "cvd_4h": snap.cvd_4h,
            "oi_change_5m": snap.oi_change_5m,
            "oi_change_1h": snap.oi_change_1h,
            "buy_pct_5m": snap.buy_pct_5m,
            "buy_persist_5m": snap.buy_persist_5m,
            "long_bias": snap.long_bias,
            "short_bias": snap.short_bias,
            "bias_label": snap.bias_label,
            "liq_buy_5m": snap.liq_buy_5m,
            "liq_sell_5m": snap.liq_sell_5m,
            "natr": snap.natr,
            "trend": snap.trend,
            "volume_5m": snap.volume_5m,
            "funding_rate": 0.0,  # not in snapshot
            "long_short_ratio": 0.0,
            "open_interest": 0.0,
        }

    def _store_snapshot(self, event_id: int, phase: str, d: dict) -> None:
        self._db.execute("""
            INSERT INTO snapshots (
                event_id, ts, phase, symbol, price,
                cvd_5m, cvd_1h, cvd_4h,
                oi_change_5m, oi_change_1h,
                buy_pct_5m, buy_persist_5m,
                long_bias, short_bias, bias_label,
                liq_buy_5m, liq_sell_5m,
                natr, trend, volume_5m,
                funding_rate, long_short_ratio, open_interest
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id, time.time(), phase, "", d.get("price"),
            d.get("cvd_5m"), d.get("cvd_1h"), d.get("cvd_4h"),
            d.get("oi_change_5m"), d.get("oi_change_1h"),
            d.get("buy_pct_5m"), d.get("buy_persist_5m"),
            d.get("long_bias"), d.get("short_bias"), d.get("bias_label"),
            d.get("liq_buy_5m"), d.get("liq_sell_5m"),
            d.get("natr"), d.get("trend"), d.get("volume_5m"),
            d.get("funding_rate"), d.get("long_short_ratio"),
            d.get("open_interest"),
        ))

    @property
    def active_count(self) -> int:
        return len(self._active)

    @property
    def stats(self) -> dict:
        row = self._db.execute(
            "SELECT COUNT(*) FROM events WHERE completed = 1"
        ).fetchone()
        return {"completed_events": row[0] if row else 0}
