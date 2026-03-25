from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from time import time

import httpx
import uvicorn

from .alerts import AlertDispatcher
from .api import create_api
from .config import load_settings
from .engine import Engine
from .exchanges import get_exchange_class
from .exchanges.bybit import BybitExchange  # noqa: F401 — triggers registration
from .scheduler import run_daily_reset
from .store import Store

logger = logging.getLogger("screener")


async def warmup_klines(
    exchange, symbols: list[str], interval: str, limit: int, store: Store
) -> None:
    """Fetch historical klines to seed NATR computation."""
    logger.info("Warming up %d symbols with %d %sm candles...", len(symbols), limit, interval)
    semaphore = asyncio.Semaphore(20)  # limit concurrent REST calls

    async def fetch_one(sym: str) -> None:
        async with semaphore:
            try:
                candles = await exchange.fetch_klines(sym, interval, limit)
                if candles:
                    store.load_candles(exchange.name, sym, candles)
            except Exception:
                logger.debug("Kline warmup failed for %s", sym)

    await asyncio.gather(*[fetch_one(s) for s in symbols])
    logger.info("Kline warmup complete")


async def run(mode: str) -> None:
    settings = load_settings()
    store = Store(natr_period=settings.natr.period)
    msg_queue: asyncio.Queue = asyncio.Queue(maxsize=50_000)
    alert_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    start_time = time()

    # Initialize exchanges
    exchanges = []
    for name, cfg in settings.exchanges.items():
        if not cfg.enabled:
            continue
        cls = get_exchange_class(name)
        ex = cls(cfg)
        exchanges.append(ex)

    if not exchanges:
        logger.error("No exchanges enabled in config")
        return

    # Fetch symbols from all exchanges
    all_symbols: dict[str, list[str]] = {}
    for ex in exchanges:
        symbols = await ex.fetch_symbols()
        all_symbols[ex.name] = symbols
        logger.info("%s: %d symbols", ex.name, len(symbols))

    # Warmup klines for NATR
    for ex in exchanges:
        syms = all_symbols.get(ex.name, [])
        await warmup_klines(ex, syms, settings.natr.interval, settings.natr.period + 1, store)

    # Initialize engine, alert dispatcher, and orderbook manager
    from .api import EventBus
    event_bus = EventBus()
    engine = Engine(store, alert_queue, settings)
    dispatcher = AlertDispatcher(settings, event_bus=event_bus)

    ob_manager = None
    if settings.orderbook.enabled:
        from .orderbook import OrderbookManager
        ob_manager = OrderbookManager(store, alert_queue, settings)

    # Compute initial NATR from warmup candles
    for ex in exchanges:
        for sym in all_symbols.get(ex.name, []):
            natr = engine._compute_natr(ex.name, sym)
            if natr is not None:
                ticker = store.get_or_create_ticker(ex.name, sym)
                ticker.natr_5m_14 = natr
    logger.info("Initial NATR computed for all symbols with sufficient candle history")

    # Build list of async tasks
    tasks: list[asyncio.Task] = []

    # Exchange WS streams
    for ex in exchanges:
        syms = all_symbols.get(ex.name, [])
        tasks.append(asyncio.create_task(
            ex.start_streams(syms, msg_queue, orderbook_manager=ob_manager),
            name=f"{ex.name}-streams",
        ))

    # Engine
    tasks.append(asyncio.create_task(engine.run(msg_queue), name="engine"))

    # Alert dispatcher
    tasks.append(asyncio.create_task(dispatcher.run(alert_queue), name="alerts"))

    # Scheduler
    tasks.append(asyncio.create_task(run_daily_reset(store, settings), name="scheduler"))

    # API server
    exchange_map = {ex.name: ex for ex in exchanges}

    if mode in ("api", "both", "server"):
        app = create_api(store, dispatcher.recent, start_time, event_bus=event_bus, exchanges=exchange_map)
        api_config = uvicorn.Config(
            app,
            host=settings.api.host,
            port=settings.api.port,
            log_level="warning",
        )
        server = uvicorn.Server(api_config)
        tasks.append(asyncio.create_task(server.serve(), name="api"))
        logger.info("API server on http://%s:%d", settings.api.host, settings.api.port)

    # TUI
    if mode in ("tui", "both"):
        from .tui import ScreenerApp
        large_orders = ob_manager.active_large_orders if ob_manager else None
        tui_app = ScreenerApp(
            store=store, settings=settings, alert_history=dispatcher.recent,
            exchanges=exchange_map, large_orders=large_orders,
        )
        tasks.append(asyncio.create_task(tui_app.run_async(), name="tui"))

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        shutdown_event.set()
        for ex in exchanges:
            asyncio.create_task(ex.stop())
        for t in tasks:
            t.cancel()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass  # Windows

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass

    logger.info("Screener stopped")


def cli() -> None:
    parser = argparse.ArgumentParser(description="Bybit Futures Screener")
    parser.add_argument(
        "--mode",
        choices=["tui", "api", "both", "server", "client"],
        default="both",
        help="Run mode: both (default), server (headless daemon), client (TUI connects to server)",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config file",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level",
    )
    parser.add_argument(
        "--server-url",
        default=None,
        help="Server URL for client mode (default: http://localhost:{api.port})",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.mode == "client":
        from .client import run_client
        settings = load_settings(args.config)
        url = args.server_url or f"http://localhost:{settings.api.port}"
        asyncio.run(run_client(url, settings))
    else:
        asyncio.run(run(args.mode))


if __name__ == "__main__":
    cli()
