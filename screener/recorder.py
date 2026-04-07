from __future__ import annotations

import logging
from time import time

from questdb.ingress import IngressError, Protocol, Sender, TimestampNanos

from .config import QuestDBConfig
from .models import (
    LiquidationMessage,
    OpenInterestMessage,
    TickerMessage,
    TradeMessage,
)

logger = logging.getLogger(__name__)


class QuestDBRecorder:
    def __init__(self, config: QuestDBConfig) -> None:
        self._config = config
        self._sender: Sender | None = None
        self._last_ticker_ts: dict[str, float] = {}
        self._throttle_s = config.ticker_throttle_ms / 1000.0

    def start(self) -> None:
        conf = f"tcp::addr={self._config.host}:{self._config.port};"
        conf += f"auto_flush_rows={self._config.auto_flush_rows};"
        conf += f"auto_flush_interval={self._config.auto_flush_interval_ms};"
        self._sender = Sender.from_conf(conf)
        self._sender.establish()
        logger.info("Recorder connected to QuestDB at %s:%d", self._config.host, self._config.port)

    def on_message(self, msg: object) -> None:
        if self._sender is None:
            return
        try:
            if isinstance(msg, TradeMessage):
                self._record_trade(msg)
            elif isinstance(msg, LiquidationMessage):
                self._record_liquidation(msg)
            elif isinstance(msg, OpenInterestMessage):
                self._record_oi(msg)
            elif isinstance(msg, TickerMessage):
                self._record_ticker(msg)
        except IngressError:
            logger.warning("Recorder flush error, attempting reconnect", exc_info=True)
            self._reconnect()
        except Exception:
            logger.exception("Recorder error")

    def _record_trade(self, msg: TradeMessage) -> None:
        assert self._sender is not None
        usd_size = msg.size * msg.price
        self._sender.row(
            "trades",
            symbols={"exchange": msg.exchange, "symbol": msg.symbol, "side": msg.side},
            columns={"price": msg.price, "qty": msg.size, "usd_size": usd_size},
            at=TimestampNanos(int(msg.timestamp * 1_000_000_000)),
        )

    def _record_liquidation(self, msg: LiquidationMessage) -> None:
        assert self._sender is not None
        usd_size = msg.size * msg.price
        self._sender.row(
            "liquidations",
            symbols={"exchange": msg.exchange, "symbol": msg.symbol, "side": msg.side},
            columns={"price": msg.price, "qty": msg.size, "usd_size": usd_size},
            at=TimestampNanos(int(msg.timestamp * 1_000_000_000)),
        )

    def _record_oi(self, msg: OpenInterestMessage) -> None:
        assert self._sender is not None
        self._sender.row(
            "oi_snapshots",
            symbols={"exchange": msg.exchange, "symbol": msg.symbol},
            columns={
                "oi_usd": msg.open_interest_value,
                "oi_contracts": msg.open_interest,
            },
            at=TimestampNanos(int(msg.timestamp * 1_000_000_000)),
        )

    def _record_ticker(self, msg: TickerMessage) -> None:
        assert self._sender is not None
        now = time()
        key = f"{msg.exchange}:{msg.symbol}"

        # Throttle: at most one ticker record per symbol per ticker_throttle_ms
        last = self._last_ticker_ts.get(key, 0.0)
        if now - last < self._throttle_s:
            return
        self._last_ticker_ts[key] = now

        # Record price/volume ticker
        if msg.last_price is not None:
            columns: dict[str, float] = {"price": msg.last_price}
            if msg.volume_24h is not None:
                columns["volume_24h"] = msg.volume_24h
            if msg.open_interest_value is not None:
                columns["oi_usd"] = msg.open_interest_value
            self._sender.row(
                "tickers",
                symbols={"exchange": msg.exchange, "symbol": msg.symbol},
                columns=columns,
                at=TimestampNanos(int(now * 1_000_000_000)),
            )

        # Record funding rate changes separately
        if msg.funding_rate is not None:
            funding_cols: dict[str, float] = {"rate": msg.funding_rate}
            if msg.next_funding_ts is not None:
                funding_cols["next_funding_ts"] = msg.next_funding_ts
            self._sender.row(
                "funding",
                symbols={"exchange": msg.exchange, "symbol": msg.symbol},
                columns=funding_cols,
                at=TimestampNanos(int(now * 1_000_000_000)),
            )

    def _reconnect(self) -> None:
        try:
            if self._sender is not None:
                self._sender.close()
        except Exception:
            pass
        self._sender = None
        try:
            self.start()
        except Exception:
            logger.error("Recorder reconnect failed", exc_info=True)

    def stop(self) -> None:
        if self._sender is not None:
            try:
                self._sender.flush()
                self._sender.close()
            except Exception:
                logger.warning("Recorder close error", exc_info=True)
            self._sender = None
        logger.info("Recorder stopped")
