from __future__ import annotations

import asyncio
import json
import logging
from time import time
from typing import TYPE_CHECKING

import httpx
import websockets
import websockets.exceptions

from ..config import ExchangeConfig
from ..config import Settings
from ..models import (
    CandleBar,
    KlineMessage,
    LiquidationMessage,
    LongShortRatioMessage,
    TickerMessage,
    TradeMessage,
)
from . import BaseExchange, register_exchange

if TYPE_CHECKING:
    from ..orderbook import OrderbookManager

logger = logging.getLogger(__name__)

# Bybit allows max 10 args per subscribe message.
_SUB_BATCH_SIZE = 10
# Ping interval to keep WS alive (Bybit requires ping every 20s)
_PING_INTERVAL = 18


@register_exchange
class BybitExchange(BaseExchange):
    name = "bybit"

    def __init__(self, config: ExchangeConfig, settings: Settings | None = None) -> None:
        self._config = config
        self._settings = settings
        self._ws_tasks: list[asyncio.Task] = []
        self._running = False
        self._ob_manager: OrderbookManager | None = None

    # -- REST --

    async def fetch_symbols(self) -> list[str]:
        symbols: list[str] = []
        cursor = ""
        async with httpx.AsyncClient(base_url=self._config.rest_url, timeout=15) as client:
            while True:
                params: dict[str, str | int] = {
                    "category": "linear",
                    "limit": 1000,
                    "status": "Trading",
                }
                if cursor:
                    params["cursor"] = cursor
                resp = await client.get("/v5/market/instruments-info", params=params)
                resp.raise_for_status()
                data = resp.json()
                result = data.get("result", {})
                for item in result.get("list", []):
                    # Only USDT perpetuals
                    if item.get("settleCoin") == "USDT":
                        symbols.append(item["symbol"])
                cursor = result.get("nextPageCursor", "")
                if not cursor:
                    break
        logger.info("Bybit: fetched %d USDT perpetual symbols", len(symbols))
        return symbols

    async def fetch_klines(
        self, symbol: str, interval: str, limit: int
    ) -> list[CandleBar]:
        async with httpx.AsyncClient(base_url=self._config.rest_url, timeout=15) as client:
            resp = await client.get(
                "/v5/market/kline",
                params={
                    "category": "linear",
                    "symbol": symbol,
                    "interval": interval,
                    "limit": limit,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            candles: list[CandleBar] = []
            for row in data.get("result", {}).get("list", []):
                # Bybit kline: [startTime, open, high, low, close, volume, turnover]
                candles.append(
                    CandleBar(
                        timestamp=float(row[0]) / 1000,
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        volume=float(row[5]),
                        confirmed=True,
                    )
                )
            # Bybit returns newest first, reverse to chronological
            candles.reverse()
            return candles

    # -- WebSocket --

    async def start_streams(
        self,
        symbols: list[str],
        queue: asyncio.Queue,
        cvd_enabled: bool = True,
        orderbook_manager: OrderbookManager | None = None,
    ) -> None:
        self._running = True
        self._ob_manager = orderbook_manager
        n_conns = self._config.ws_connections

        ds = self._settings.data_streams if self._settings else None

        # Build ticker+kline topics, split across n_conns connections
        ticker_kline_topics: list[list[str]] = [[] for _ in range(n_conns)]
        for i, sym in enumerate(symbols):
            bucket = i % n_conns
            ticker_kline_topics[bucket].append(f"tickers.{sym}")
            ticker_kline_topics[bucket].append(f"kline.5.{sym}")
            if ds and ds.liquidations_enabled:
                ticker_kline_topics[bucket].append(f"liquidation.{sym}")

        conn_id = 0
        for topics in ticker_kline_topics:
            task = asyncio.create_task(
                self._ws_loop(conn_id, topics, queue), name=f"bybit-ws-{conn_id}"
            )
            self._ws_tasks.append(task)
            conn_id += 1

        # Trade stream connections for CVD (3 extra connections)
        if cvd_enabled:
            n_trade_conns = 3
            trade_topics: list[list[str]] = [[] for _ in range(n_trade_conns)]
            for i, sym in enumerate(symbols):
                trade_topics[i % n_trade_conns].append(f"publicTrade.{sym}")

            for topics in trade_topics:
                task = asyncio.create_task(
                    self._ws_loop(conn_id, topics, queue), name=f"bybit-ws-trade-{conn_id}"
                )
                self._ws_tasks.append(task)
                conn_id += 1

        # Orderbook connections for large order tracking
        if orderbook_manager is not None:
            from ..config import OrderbookConfig
            ob_cfg = orderbook_manager._cfg
            n_ob_conns = ob_cfg.ws_connections
            depth = ob_cfg.depth
            ob_topics: list[list[str]] = [[] for _ in range(n_ob_conns)]
            for i, sym in enumerate(symbols):
                ob_topics[i % n_ob_conns].append(f"orderbook.{depth}.{sym}")

            for topics in ob_topics:
                task = asyncio.create_task(
                    self._ws_loop(conn_id, topics, queue), name=f"bybit-ws-ob-{conn_id}"
                )
                self._ws_tasks.append(task)
                conn_id += 1

        # Long/Short ratio REST poller
        if ds and ds.long_short_ratio_enabled:
            task = asyncio.create_task(
                self._long_short_ratio_poller(symbols, queue),
                name="bybit-ls-ratio-poller",
            )
            self._ws_tasks.append(task)

        logger.info("Bybit: launching %d WS connections + pollers", conn_id)
        await asyncio.gather(*self._ws_tasks, return_exceptions=True)

    async def _ws_loop(
        self, conn_id: int, topics: list[str], queue: asyncio.Queue
    ) -> None:
        """Single WS connection loop with reconnect logic."""
        while self._running:
            try:
                await self._ws_session(conn_id, topics, queue)
            except (
                websockets.exceptions.ConnectionClosed,
                ConnectionError,
                OSError,
            ) as e:
                if not self._running:
                    return
                logger.warning(
                    "Bybit WS-%d disconnected: %s. Reconnecting in 3s...",
                    conn_id, e,
                )
                await asyncio.sleep(3)
            except Exception:
                if not self._running:
                    return
                logger.exception("Bybit WS-%d unexpected error", conn_id)
                await asyncio.sleep(5)

    async def _ws_session(
        self, conn_id: int, topics: list[str], queue: asyncio.Queue
    ) -> None:
        url = self._config.ws_url
        async with websockets.connect(url, ping_interval=None) as ws:
            logger.info("Bybit WS-%d connected to %s", conn_id, url)

            # Subscribe in batches
            for i in range(0, len(topics), _SUB_BATCH_SIZE):
                batch = topics[i : i + _SUB_BATCH_SIZE]
                msg = json.dumps({"op": "subscribe", "args": batch})
                await ws.send(msg)

            logger.info(
                "Bybit WS-%d subscribed to %d topics",
                conn_id, len(topics),
            )

            # Start ping task
            ping_task = asyncio.create_task(self._ping_loop(ws, conn_id))

            try:
                async for raw in ws:
                    if not self._running:
                        break
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    topic = data.get("topic", "")
                    msg_data = data.get("data")
                    if not topic or msg_data is None:
                        continue

                    if topic.startswith("tickers."):
                        parsed = self._parse_ticker(topic, msg_data, data.get("type"))
                        if parsed:
                            await queue.put(parsed)
                    elif topic.startswith("kline."):
                        parsed_k = self._parse_kline(topic, msg_data)
                        if parsed_k:
                            await queue.put(parsed_k)
                    elif topic.startswith("publicTrade."):
                        for trade in (msg_data if isinstance(msg_data, list) else [msg_data]):
                            parsed_t = self._parse_trade(topic, trade)
                            if parsed_t:
                                await queue.put(parsed_t)
                    elif topic.startswith("liquidation."):
                        parsed_l = self._parse_liquidation(topic, msg_data)
                        if parsed_l:
                            await queue.put(parsed_l)
                    elif topic.startswith("orderbook."):
                        if self._ob_manager is not None:
                            self._handle_orderbook(topic, msg_data, data.get("type"))
            finally:
                ping_task.cancel()

    async def _ping_loop(self, ws, conn_id: int) -> None:
        try:
            while self._running:
                await asyncio.sleep(_PING_INTERVAL)
                await ws.send(json.dumps({"op": "ping"}))
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.debug("Bybit WS-%d ping error", conn_id)

    def _parse_ticker(
        self, topic: str, data: dict, msg_type: str | None
    ) -> TickerMessage | None:
        symbol = topic.split(".", 1)[1]
        # data is either a full snapshot or partial delta
        # Parse funding interval and next funding time
        funding_interval_h = None
        raw_interval = data.get("fundingIntervalHour")
        if raw_interval is not None and raw_interval != "":
            funding_interval_h = int(raw_interval)

        next_funding_ts = None
        raw_next = data.get("nextFundingTime")
        if raw_next is not None and raw_next != "":
            next_funding_ts = float(raw_next) / 1000  # ms -> seconds

        # Open Interest (included in tickers stream)
        oi = _float_or_none(data.get("openInterest"))
        oi_value = _float_or_none(data.get("openInterestValue"))

        return TickerMessage(
            exchange="bybit",
            symbol=symbol,
            last_price=_float_or_none(data.get("lastPrice")),
            volume_24h=_float_or_none(data.get("volume24h")),
            funding_rate=_float_or_none(data.get("fundingRate")),
            funding_interval_h=funding_interval_h,
            next_funding_ts=next_funding_ts,
            high_24h=_float_or_none(data.get("highPrice24h")),
            low_24h=_float_or_none(data.get("lowPrice24h")),
            open_interest=oi,
            open_interest_value=oi_value,
        )

    def _parse_kline(self, topic: str, data: list | dict) -> KlineMessage | None:
        # topic: "kline.5.BTCUSDT"
        parts = topic.split(".")
        interval = parts[1]
        symbol = parts[2]
        # data is a list of candle objects
        rows = data if isinstance(data, list) else [data]
        if not rows:
            return None
        row = rows[0]
        candle = CandleBar(
            timestamp=float(row["start"]) / 1000,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
            confirmed=row.get("confirm", False),
        )
        return KlineMessage(
            exchange="bybit",
            symbol=symbol,
            interval=interval,
            candle=candle,
        )

    def _handle_orderbook(self, topic: str, data: dict, msg_type: str | None) -> None:
        # topic: "orderbook.50.BTCUSDT"
        symbol = topic.split(".", 2)[2]
        ob_type = "snapshot" if msg_type == "snapshot" else "delta"
        bids = [(float(p), float(s)) for p, s in data.get("b", [])]
        asks = [(float(p), float(s)) for p, s in data.get("a", [])]
        self._ob_manager.process_update("bybit", symbol, ob_type, bids, asks)

    def _parse_trade(self, topic: str, data: dict) -> TradeMessage | None:
        # topic: "publicTrade.BTCUSDT"
        symbol = topic.split(".", 1)[1]
        try:
            return TradeMessage(
                exchange="bybit",
                symbol=symbol,
                side=data["S"],  # "Buy" or "Sell"
                size=float(data["v"]),
                price=float(data["p"]),
                timestamp=float(data["T"]) / 1000,
            )
        except (KeyError, ValueError):
            return None

    def _parse_liquidation(self, topic: str, data: dict) -> LiquidationMessage | None:
        # topic: "liquidation.BTCUSDT"
        symbol = topic.split(".", 1)[1]
        try:
            return LiquidationMessage(
                exchange="bybit",
                symbol=symbol,
                side=data["side"],  # "Buy" or "Sell"
                size=float(data["size"]),
                price=float(data["price"]),
                timestamp=float(data["updatedTime"]) / 1000,
            )
        except (KeyError, ValueError):
            return None

    async def _long_short_ratio_poller(
        self, symbols: list[str], queue: asyncio.Queue
    ) -> None:
        """Poll Bybit REST API for long/short ratio every N seconds."""
        ds = self._settings.data_streams if self._settings else None
        interval = ds.ls_ratio_poll_interval_s if ds else 300
        async with httpx.AsyncClient(base_url=self._config.rest_url, timeout=15) as client:
            while self._running:
                try:
                    for sym in symbols:
                        if not self._running:
                            break
                        try:
                            resp = await client.get(
                                "/v5/market/account-ratio",
                                params={"category": "linear", "symbol": sym, "period": "5min", "limit": 1},
                            )
                            if resp.status_code != 200:
                                continue
                            data = resp.json()
                            items = data.get("result", {}).get("list", [])
                            if items:
                                ratio = float(items[0].get("buyRatio", 0)) / max(
                                    float(items[0].get("sellRatio", 1)), 0.0001
                                )
                                await queue.put(LongShortRatioMessage(
                                    exchange="bybit",
                                    symbol=sym,
                                    long_short_ratio=round(ratio, 4),
                                    timestamp=time(),
                                ))
                        except Exception:
                            pass  # skip individual symbol errors
                        await asyncio.sleep(0.1)  # rate limit protection
                except asyncio.CancelledError:
                    return
                except Exception:
                    logger.exception("Bybit L/S ratio poller error")
                await asyncio.sleep(interval)

    async def stop(self) -> None:
        self._running = False
        for task in self._ws_tasks:
            task.cancel()
        self._ws_tasks.clear()


def _float_or_none(val) -> float | None:
    if val is None or val == "":
        return None
    return float(val)
