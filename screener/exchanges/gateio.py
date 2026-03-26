from __future__ import annotations

import asyncio
import json
import logging
import math
from time import time
from typing import TYPE_CHECKING

import httpx
import websockets
import websockets.exceptions

from ..config import ExchangeConfig, Settings
from ..models import CandleBar, KlineMessage, LiquidationMessage, TickerMessage, TradeMessage
from . import BaseExchange, register_exchange

if TYPE_CHECKING:
    from ..orderbook import OrderbookManager

logger = logging.getLogger(__name__)

_PING_INTERVAL = 15
# Gate.io subscribe in batches
_SUB_BATCH_SIZE = 50

# Interval mapping: our internal format -> Gate.io format
_INTERVAL_MAP = {
    "1": "1m",
    "5": "5m",
    "15": "15m",
    "60": "1h",
    "D": "1d",
}


@register_exchange
class GateioExchange(BaseExchange):
    name = "gateio"

    def __init__(self, config: ExchangeConfig, settings: Settings | None = None) -> None:
        self._config = config
        self._settings = settings
        self._ws_tasks: list[asyncio.Task] = []
        self._running = False

    # -- REST --

    async def fetch_symbols(self) -> list[str]:
        symbols: list[str] = []
        async with httpx.AsyncClient(base_url=self._config.rest_url, timeout=15) as client:
            resp = await client.get("/api/v4/futures/usdt/contracts")
            resp.raise_for_status()
            contracts = resp.json()
            for item in contracts:
                # Gate.io uses BTC_USDT format, only include active contracts
                if item.get("in_delisting", False):
                    continue
                name = item.get("name", "")
                if name and name.endswith("_USDT"):
                    symbols.append(name)
        logger.info("Gate.io: fetched %d USDT perpetual symbols", len(symbols))
        return symbols

    async def fetch_klines(
        self, symbol: str, interval: str, limit: int
    ) -> list[CandleBar]:
        gi_interval = _INTERVAL_MAP.get(interval, interval)
        async with httpx.AsyncClient(base_url=self._config.rest_url, timeout=15) as client:
            resp = await client.get(
                "/api/v4/futures/usdt/candlesticks",
                params={"contract": symbol, "interval": gi_interval, "limit": limit},
            )
            resp.raise_for_status()
            rows = resp.json()
            candles: list[CandleBar] = []
            for row in rows:
                candles.append(
                    CandleBar(
                        timestamp=float(row["t"]),
                        open=float(row["o"]),
                        high=float(row["h"]),
                        low=float(row["l"]),
                        close=float(row["c"]),
                        volume=float(row.get("v", 0)),
                        confirmed=True,
                    )
                )
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
        n_conns = self._config.ws_connections

        # Split symbols across connections
        chunks: list[list[str]] = [[] for _ in range(n_conns)]
        for i, sym in enumerate(symbols):
            chunks[i % n_conns].append(sym)

        conn_id = 0
        for sym_chunk in chunks:
            task = asyncio.create_task(
                self._ws_loop(conn_id, sym_chunk, queue, cvd_enabled),
                name=f"gateio-ws-{conn_id}",
            )
            self._ws_tasks.append(task)
            conn_id += 1

        logger.info("Gate.io: launching %d WS connections", conn_id)
        await asyncio.gather(*self._ws_tasks, return_exceptions=True)

    async def _ws_loop(
        self,
        conn_id: int,
        symbols: list[str],
        queue: asyncio.Queue,
        cvd_enabled: bool,
    ) -> None:
        while self._running:
            try:
                await self._ws_session(conn_id, symbols, queue, cvd_enabled)
            except (
                websockets.exceptions.ConnectionClosed,
                ConnectionError,
                OSError,
            ) as e:
                if not self._running:
                    return
                logger.warning(
                    "Gate.io WS-%d disconnected: %s. Reconnecting in 3s...",
                    conn_id, e,
                )
                await asyncio.sleep(3)
            except Exception:
                if not self._running:
                    return
                logger.exception("Gate.io WS-%d unexpected error", conn_id)
                await asyncio.sleep(5)

    async def _ws_session(
        self,
        conn_id: int,
        symbols: list[str],
        queue: asyncio.Queue,
        cvd_enabled: bool,
    ) -> None:
        url = self._config.ws_url
        async with websockets.connect(url, ping_interval=None) as ws:
            logger.info("Gate.io WS-%d connected to %s", conn_id, url)

            now = int(time())

            # Subscribe to tickers in batches
            for i in range(0, len(symbols), _SUB_BATCH_SIZE):
                batch = symbols[i : i + _SUB_BATCH_SIZE]
                await ws.send(json.dumps({
                    "time": now,
                    "channel": "futures.tickers",
                    "event": "subscribe",
                    "payload": batch,
                }))

            # Subscribe to klines (5m) — each sub is ["5m", "SYMBOL"]
            for sym in symbols:
                await ws.send(json.dumps({
                    "time": now,
                    "channel": "futures.candlesticks",
                    "event": "subscribe",
                    "payload": ["5m", sym],
                }))

            # Subscribe to trades for CVD
            if cvd_enabled:
                for i in range(0, len(symbols), _SUB_BATCH_SIZE):
                    batch = symbols[i : i + _SUB_BATCH_SIZE]
                    await ws.send(json.dumps({
                        "time": now,
                        "channel": "futures.trades",
                        "event": "subscribe",
                        "payload": batch,
                    }))

            # Subscribe to liquidations
            ds = self._settings.data_streams if self._settings else None
            if ds and ds.liquidations_enabled:
                for i in range(0, len(symbols), _SUB_BATCH_SIZE):
                    batch = symbols[i : i + _SUB_BATCH_SIZE]
                    await ws.send(json.dumps({
                        "time": now,
                        "channel": "futures.liquidates",
                        "event": "subscribe",
                        "payload": batch,
                    }))

            logger.info(
                "Gate.io WS-%d subscribed for %d symbols", conn_id, len(symbols)
            )

            ping_task = asyncio.create_task(self._ping_loop(ws, conn_id))

            try:
                async for raw in ws:
                    if not self._running:
                        break
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    channel = msg.get("channel", "")
                    event = msg.get("event", "")
                    if event != "update":
                        continue

                    result = msg.get("result")
                    if result is None:
                        continue

                    if channel == "futures.tickers":
                        for item in (result if isinstance(result, list) else [result]):
                            parsed = self._parse_ticker(item)
                            if parsed:
                                await queue.put(parsed)
                    elif channel == "futures.candlesticks":
                        for item in (result if isinstance(result, list) else [result]):
                            parsed_k = self._parse_kline(item)
                            if parsed_k:
                                await queue.put(parsed_k)
                    elif channel == "futures.trades":
                        for item in (result if isinstance(result, list) else [result]):
                            parsed_t = self._parse_trade(item)
                            if parsed_t:
                                await queue.put(parsed_t)
                    elif channel == "futures.liquidates":
                        for item in (result if isinstance(result, list) else [result]):
                            parsed_l = self._parse_liquidation(item)
                            if parsed_l:
                                await queue.put(parsed_l)
            finally:
                ping_task.cancel()

    async def _ping_loop(self, ws, conn_id: int) -> None:
        try:
            while self._running:
                await asyncio.sleep(_PING_INTERVAL)
                await ws.send(json.dumps({
                    "time": int(time()),
                    "channel": "futures.ping",
                }))
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.debug("Gate.io WS-%d ping error", conn_id)

    def _parse_ticker(self, data: dict) -> TickerMessage | None:
        try:
            contract = data.get("contract", "")
            funding_rate = None
            raw_rate = data.get("funding_rate")
            if raw_rate is not None and raw_rate != "":
                funding_rate = float(raw_rate)

            # total_size is OI in contracts (available in GateIO futures.tickers)
            oi = _float_or_none(data.get("total_size"))

            return TickerMessage(
                exchange="gateio",
                symbol=contract,
                last_price=_float_or_none(data.get("last")),
                volume_24h=_float_or_none(data.get("volume_24h")),
                funding_rate=funding_rate,
                high_24h=_float_or_none(data.get("high_24h")),
                low_24h=_float_or_none(data.get("low_24h")),
                open_interest=oi,
                open_interest_value=None,  # GateIO doesn't provide USD value directly
            )
        except (KeyError, ValueError):
            return None

    def _parse_kline(self, data: dict) -> KlineMessage | None:
        try:
            # "n" field is like "5m_BTC_USDT"
            n = data.get("n", "")
            parts = n.split("_", 1)
            if len(parts) < 2:
                return None
            gi_interval = parts[0]
            contract = parts[1]

            # Map back to our interval format
            interval = "5"
            for our, theirs in _INTERVAL_MAP.items():
                if theirs == gi_interval:
                    interval = our
                    break

            candle = CandleBar(
                timestamp=float(data["t"]),
                open=float(data["o"]),
                high=float(data["h"]),
                low=float(data["l"]),
                close=float(data["c"]),
                volume=float(data.get("v", 0)),
                confirmed=data.get("w", False),
            )
            return KlineMessage(
                exchange="gateio",
                symbol=contract,
                interval=interval,
                candle=candle,
            )
        except (KeyError, ValueError):
            return None

    def _parse_trade(self, data: dict) -> TradeMessage | None:
        try:
            contract = data.get("contract", "")
            size = float(data.get("size", 0))
            # Gate.io: positive size = buy taker, negative = sell taker
            side = "Buy" if size > 0 else "Sell"
            return TradeMessage(
                exchange="gateio",
                symbol=contract,
                side=side,
                size=abs(size),
                price=float(data["price"]),
                timestamp=float(data.get("create_time_ms", data.get("create_time", 0) * 1000)) / 1000,
            )
        except (KeyError, ValueError):
            return None

    def _parse_liquidation(self, data: dict) -> LiquidationMessage | None:
        try:
            contract = data.get("contract", "")
            # GateIO: positive size = long liquidation, negative = short liquidation
            size = float(data.get("size", 0))
            side = "Buy" if size > 0 else "Sell"
            return LiquidationMessage(
                exchange="gateio",
                symbol=contract,
                side=side,
                size=abs(size),
                price=float(data.get("order_price", data.get("fill_price", 0))),
                timestamp=float(data.get("time_ms", data.get("time", 0) * 1000)) / 1000,
            )
        except (KeyError, ValueError):
            return None

    async def stop(self) -> None:
        self._running = False
        for task in self._ws_tasks:
            task.cancel()
        self._ws_tasks.clear()


def _float_or_none(val) -> float | None:
    if val is None or val == "":
        return None
    return float(val)
