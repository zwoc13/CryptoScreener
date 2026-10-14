"""Periodically flush signal snapshots from in-memory store to QuestDB.

This is the data backbone for the ML pipeline — every N seconds all
live SignalSnapshot data gets written to QuestDB so we have a labeled
time-series for offline training and streaming scoring.

Uses QuestDB's Line Protocol via the questdb Python package (TCP direct).
"""

from __future__ import annotations

import asyncio
import logging
from time import time

from questdb.ingress import Sender, TimestampNanos

from .config import QuestDBConfig
from .signal_history import SignalHistory

logger = logging.getLogger("screener.snapshot_writer")


class SnapshotWriter:
    """Flushes in-memory signal snapshots to QuestDB via questdb ingress TCP."""

    BATCH_SIZE = 500  # rows per flush

    def __init__(
        self,
        questdb_config: QuestDBConfig,
        history: SignalHistory,
        flush_interval_s: int = 60,
    ) -> None:
        self._cfg = questdb_config
        self._history = history
        self._flush_interval_s = flush_interval_s
        self._sender = None
        self._healthy = True

    def _connect(self) -> Sender:
        return Sender.from_conf(
            f"tcp::addr={self._cfg.host}:{self._cfg.port};"
        )

    def flush(self, store=None) -> int:
        """Build rows and POST to QuestDB via TCP ingress.

        Returns number of rows flushed.
        """
        now_ms = int(time() * 1000)
        n_keys = len(self._history._hist)
        non_empty = sum(1 for d in self._history._hist.values() if d)
        if non_empty == 0:
            logger.debug("SnapshotWriter: no snapshots yet")
            return 0

        total = 0
        sender = None
        try:
            sender = self._connect()
            sender.establish()
        except Exception:
            logger.exception("SnapshotWriter: failed to connect to QuestDB")
            self._healthy = False
            return 0

        try:
            for key, snap_deque in self._history._hist.items():
                if not snap_deque:
                    continue
                latest = snap_deque[-1]

                parts = key.split(":", 1)
                if len(parts) == 2:
                    exchange, symbol = parts
                else:
                    symbol = key
                    exchange = "bybit"

                # Sanitize symbol values — QuestDB Symbol type disallows '-'
                bias_label = (getattr(latest, "bias_label", "NEUTRAL") or "NEUTRAL").replace("-", "_")
                trend = (getattr(latest, "trend", "flat") or "flat").replace("-", "_")

                try:
                    sender.row(
                        "signal_snapshots",
                        symbols={
                            "exchange": exchange,
                            "symbol": symbol,
                            "bias_label": bias_label,
                            "trend": trend,
                        },
                        columns={
                            "price": latest.price,
                            "cvd_5m": getattr(latest, "cvd_5m", 0.0) or 0.0,
                            "cvd_1h": getattr(latest, "cvd_1h", 0.0) or 0.0,
                            "cvd_4h": getattr(latest, "cvd_4h", 0.0) or 0.0,
                            "cvd_daily": getattr(latest, "cvd_daily", 0.0) or 0.0,
                            "oi_change_5m": getattr(latest, "oi_change_5m", 0.0) or 0.0,
                            "oi_change_1h": getattr(latest, "oi_change_1h", 0.0) or 0.0,
                            "oi_change_4h": getattr(latest, "oi_change_4h", 0.0) or 0.0,
                            "oi_change_daily": getattr(latest, "oi_change_daily", 0.0) or 0.0,
                            "buy_pct_5m": getattr(latest, "buy_pct_5m", 0.5) or 0.5,
                            "buy_persist_5m": getattr(latest, "buy_persist_5m", 0.0) or 0.0,
                            "long_bias": getattr(latest, "long_bias", 0.0) or 0.0,
                            "short_bias": getattr(latest, "short_bias", 0.0) or 0.0,
                            "liq_buy_5m": getattr(latest, "liq_buy_5m", 0.0) or 0.0,
                            "liq_sell_5m": getattr(latest, "liq_sell_5m", 0.0) or 0.0,
                            "natr": getattr(latest, "natr", 0.0) or 0.0,
                            "range_1m": getattr(latest, "range_1m", 0.0) or 0.0,
                            "range_5m": getattr(latest, "range_5m", 0.0) or 0.0,
                            "volume_5m": getattr(latest, "volume_5m", 0.0) or 0.0,
                        },
                        at=TimestampNanos(int(now_ms * 1_000_000)),  # ms → ns (QuestDB expects ns)
                    )
                    total += 1

                    if total % self.BATCH_SIZE == 0:
                        sender.flush()

                except Exception:
                    logger.warning("SnapshotWriter: failed to write row for key=%s: %s", key, latest, exc_info=True)
                    continue

            sender.flush()
            self._healthy = True
            logger.info("SnapshotWriter: flushed %d rows (%d symbols)", total, non_empty)

        except Exception:
            logger.exception("SnapshotWriter flush error")
            self._healthy = False
            total = 0
        finally:
            if sender:
                sender.close()

        return total

    def close(self) -> None:
        logger.info("SnapshotWriter closed")


# ── Async wrapper ────────────────────────────────────────────────────────────


class AsyncSnapshotWriter:
    """Thread-safe async wrapper around SnapshotWriter for use in asyncio."""

    def __init__(self, writer: SnapshotWriter) -> None:
        self._writer = writer
        self._loop = asyncio.get_event_loop()
        self._sem = asyncio.Semaphore(1)

    async def flush(self, store=None) -> int:
        if self._sem.locked():
            logger.warning("SnapshotWriter: previous flush still running, skipping")
            return 0
        async with self._sem:
            return await self._loop.run_in_executor(None, self._writer.flush, store)

    async def start_flush_loop(self, store=None, interval_s: int = 60) -> None:
        logger.info("SnapshotWriter TCP flush loop started (interval=%ds)", interval_s)
        while True:
            try:
                await asyncio.sleep(interval_s)
                rows = await self.flush(store)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("SnapshotWriter flush loop error")

    def close(self) -> None:
        self._writer.close()