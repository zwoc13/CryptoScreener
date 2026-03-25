from __future__ import annotations

import asyncio
import json
import logging
import math
from typing import TYPE_CHECKING

import httpx
import websockets
import websockets.exceptions

from ..config import ExchangeConfig
from ..models import CandleBar, KlineMessage, TickerMessage, TradeMessage
from . import BaseExchange, register_exchange

if TYPE_CHECKING:
    from ..orderbook import OrderbookManager

logger = logging.getLogger(__name__)

# Binance combined stream limit per connection
_MAX_STREAMS_PER_CONN = 1000
# Ping interval (Binance requires pong within 10 min, ping every 3 min)
_PING_INTERVAL = 180

# Interval mapping: our internal format -> Binance format
_INTERVAL_MAP = {
    "1": "1m",
    "5": "5m",
    "15": "15m",
    "60": "1h",
    "D": "1d",
}


@register_exchange
class BinanceExchange(BaseExchange):
    name = "binance"

    def __init__(self, config: ExchangeConfig) -> None:
        self._config = config
        self._ws_tasks: list[asyncio.Task] = []
        self._running = False

    # -- REST --

    async def fetch_symbols(self) -> list[str]:
        symbols: list[str] = []
        async with httpx.AsyncClient(base_url=self._config.rest_url, timeout=15) as client:
            resp = await client.get("/fapi/v1/exchangeInfo")
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("symbols", []):
                if (
                    item.get("contractType") == "PERPETUAL"
                    and item.get("quoteAsset") == "USDT"
                    and item.get("status") == "TRADING"
                ):
                    symbols.append(item["symbol"])
        logger.info("Binance: fetched %d USDT perpetual symbols", len(symbols))
        return symbols

    async def fetch_klines(
        self, symbol: str, interval: str, limit: int
    ) -> list[CandleBar]:
        bi_interval = _INTERVAL_MAP.get(interval, interval)
        async with httpx.AsyncClient(base_url=self._config.rest_url, timeout=15) as client:
            resp = await client.get(
                "/fapi/v1/klines",
                params={"symbol": symbol, "interval": bi_interval, "limit": limit},
            )
            resp.raise_for_status()
            rows = resp.json()
            candles: list[CandleBar] = []
            for row in rows:
                # [openTime, open, high, low, close, volume, closeTime, ...]
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

        # Build stream names for tickers + klines
        # Binance uses combined stream URL: /stream?streams=sym@miniTicker/sym@kline_5m/...
        # Symbols must be lowercase
        all_streams: list[str] = []
        for sym in symbols:
            s = sym.lower()
            all_streams.append(f"{s}@miniTicker")
            all_streams.append(f"{s}@kline_5m")

        # Split across connections (max 1000 per connection)
        streams_per_conn = min(_MAX_STREAMS_PER_CONN, math.ceil(len(all_streams) / n_conns))
        conn_id = 0
        for i in range(0, len(all_streams), streams_per_conn):
            batch = all_streams[i : i + streams_per_conn]
            task = asyncio.create_task(
                self._ws_loop(conn_id, batch, queue),
                name=f"binance-ws-{conn_id}",
            )
            self._ws_tasks.append(task)
            conn_id += 1

        # Trade streams for CVD
        if cvd_enabled:
            trade_streams: list[str] = [f"{sym.lower()}@aggTrade" for sym in symbols]
            n_trade_conns = max(1, math.ceil(len(trade_streams) / _MAX_STREAMS_PER_CONN))
            chunk = math.ceil(len(trade_streams) / n_trade_conns)
            for i in range(0, len(trade_streams), chunk):
                batch = trade_streams[i : i + chunk]
                task = asyncio.create_task(
                    self._ws_loop(conn_id, batch, queue),
                    name=f"binance-ws-trade-{conn_id}",
                )
                self._ws_tasks.append(task)
                conn_id += 1

        logger.info("Binance: launching %d WS connections", conn_id)
        await asyncio.gather(*self._ws_tasks, return_exceptions=True)

    async def _ws_loop(
        self, conn_id: int, streams: list[str], queue: asyncio.Queue
    ) -> None:
        while self._running:
            try:
                await self._ws_session(conn_id, streams, queue)
            except (
                websockets.exceptions.ConnectionClosed,
                ConnectionError,
                OSError,
            ) as e:
                if not self._running:
                    return
                logger.warning(
                    "Binance WS-%d disconnected: %s. Reconnecting in 3s...",
                    conn_id, e,
                )
                await asyncio.sleep(3)
            except Exception:
                if not self._running:
                    return
                logger.exception("Binance WS-%d unexpected error", conn_id)
                await asyncio.sleep(5)

    async def _ws_session(
        self, conn_id: int, streams: list[str], queue: asyncio.Queue
    ) -> None:
        # Binance combined stream URL
        stream_path = "/".join(streams)
        url = f"{self._config.ws_url}/stream?streams={stream_path}"
        async with websockets.connect(url, ping_interval=None) as ws:
            logger.info(
                "Binance WS-%d connected (%d streams)", conn_id, len(streams)
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

                    stream = msg.get("stream", "")
                    data = msg.get("data")
                    if not stream or data is None:
                        continue

                    if stream.endswith("@miniTicker"):
                        parsed = self._parse_ticker(data)
                        if parsed:
                            await queue.put(parsed)
                    elif "@kline_" in stream:
                        parsed_k = self._parse_kline(data)
                        if parsed_k:
                            await queue.put(parsed_k)
                    elif stream.endswith("@aggTrade"):
                        parsed_t = self._parse_trade(data)
                        if parsed_t:
                            await queue.put(parsed_t)
            finally:
                ping_task.cancel()

    async def _ping_loop(self, ws, conn_id: int) -> None:
        try:
            while self._running:
                await asyncio.sleep(_PING_INTERVAL)
                await ws.ping()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.debug("Binance WS-%d ping error", conn_id)

    def _parse_ticker(self, data: dict) -> TickerMessage | None:
        try:
            return TickerMessage(
                exchange="binance",
                symbol=data["s"],
                last_price=float(data["c"]),
                volume_24h=float(data["v"]),
                high_24h=float(data["h"]),
                low_24h=float(data["l"]),
            )
        except (KeyError, ValueError):
            return None

    def _parse_kline(self, data: dict) -> KlineMessage | None:
        try:
            k = data["k"]
            candle = CandleBar(
                timestamp=float(k["t"]) / 1000,
                open=float(k["o"]),
                high=float(k["h"]),
                low=float(k["l"]),
                close=float(k["c"]),
                volume=float(k["v"]),
                confirmed=k.get("x", False),
            )
            # Map Binance interval back to our format
            bi_interval = k.get("i", "5m")
            interval = "5"  # default
            for our, theirs in _INTERVAL_MAP.items():
                if theirs == bi_interval:
                    interval = our
                    break
            return KlineMessage(
                exchange="binance",
                symbol=data["s"],
                interval=interval,
                candle=candle,
            )
        except (KeyError, ValueError):
            return None

    def _parse_trade(self, data: dict) -> TradeMessage | None:
        try:
            # m=True means buyer is market maker -> seller is taker -> Sell
            side = "Sell" if data["m"] else "Buy"
            return TradeMessage(
                exchange="binance",
                symbol=data["s"],
                side=side,
                size=float(data["q"]),
                price=float(data["p"]),
                timestamp=float(data["T"]) / 1000,
            )
        except (KeyError, ValueError):
            return None

    async def stop(self) -> None:
        self._running = False
        for task in self._ws_tasks:
            task.cancel()
        self._ws_tasks.clear()
