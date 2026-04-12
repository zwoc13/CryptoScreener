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
from .exchanges.binance import BinanceExchange  # noqa: F401 — triggers registration
from .exchanges.gateio import GateioExchange  # noqa: F401 — triggers registration
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


_SNAPSHOT_PATH = "snapshot.json"


async def _mem_profile_loop(interval_s: int = 300) -> None:
    """Periodically log top memory allocators. Enabled by --mem-profile."""
    import tracemalloc
    log = logging.getLogger("memprof")
    while True:
        await asyncio.sleep(interval_s)
        try:
            snap = tracemalloc.take_snapshot()
            total_mb = sum(s.size for s in snap.statistics("filename")) / 1024 / 1024
            log.info("=== Tracemalloc total: %.1f MB ===", total_mb)
            for stat in snap.statistics("lineno")[:15]:
                log.info("  %s", stat)
        except Exception:
            log.exception("memprof iteration failed")


async def run(mode: str) -> None:
    settings = load_settings()

    # Optionally attach a QuestDB reader for long-window queries (>4h)
    questdb_reader = None
    if settings.questdb.enabled:
        from .questdb_reader import QuestDBReader
        questdb_reader = QuestDBReader(settings.questdb)

    store = Store(natr_period=settings.natr.period, questdb_reader=questdb_reader)
    msg_queue: asyncio.Queue = asyncio.Queue(maxsize=50_000)
    alert_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    start_time = time()

    # Restore previous state if available
    restored = store.snapshot_restore(_SNAPSHOT_PATH)

    # Initialize exchanges
    exchanges = []
    for name, cfg in settings.exchanges.items():
        if not cfg.enabled:
            continue
        cls = get_exchange_class(name)
        ex = cls(cfg, settings=settings)
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

    # Prune restored tickers that are no longer tradeable on their exchange
    # (delisted pairs, expired dated futures, etc. that linger in snapshot.json)
    if restored:
        active_per_exchange = {name: set(syms) for name, syms in all_symbols.items()}
        stale = [
            (t.exchange, t.symbol)
            for t in store.get_all_sorted()
            if t.symbol not in active_per_exchange.get(t.exchange, set())
        ]
        for exch, sym in stale:
            store.remove_ticker(exch, sym)
        if stale:
            logger.info("Pruned %d stale tickers from snapshot: %s",
                        len(stale), ", ".join(f"{e}:{s}" for e, s in stale[:10])
                        + (" ..." if len(stale) > 10 else ""))

    # Warmup klines for NATR + 4h range (need 48 candles for 4h range)
    # If we restored a snapshot, kline warmup still runs to get the freshest candles
    warmup_limit = max(settings.natr.period + 1, 50)
    for ex in exchanges:
        syms = all_symbols.get(ex.name, [])
        await warmup_klines(ex, syms, settings.natr.interval, warmup_limit, store)

    # Initialize recorder (QuestDB time-series persistence)
    recorder = None
    if settings.questdb.enabled:
        from .recorder import QuestDBRecorder
        recorder = QuestDBRecorder(settings.questdb)
        recorder.start()

    # Initialize engine, alert dispatcher, and orderbook manager
    from .api import EventBus
    event_bus = EventBus()
    engine = Engine(store, alert_queue, settings, recorder=recorder)
    dispatcher = AlertDispatcher(settings, event_bus=event_bus)

    # Signal history layer + evaluator (Phase C: continuous event generation)
    from .signal_history import SignalHistory
    from .evaluator import run_signal_evaluator
    signal_history = SignalHistory()

    ob_manager = None
    if settings.orderbook.enabled:
        from .orderbook import OrderbookManager
        ob_manager = OrderbookManager(store, alert_queue, settings)

    # Compute initial NATR and ranges from warmup candles
    for ex in exchanges:
        for sym in all_symbols.get(ex.name, []):
            natr = engine._compute_natr(ex.name, sym)
            ticker = store.get_or_create_ticker(ex.name, sym)
            if natr is not None:
                ticker.natr_5m_14 = natr
            ticker.range_1h = store.get_range(ex.name, sym, 12)
            ticker.range_4h = store.get_range(ex.name, sym, 48)
    logger.info("Initial NATR and ranges computed from warmup candles")

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

    # Memory profiler (only if tracemalloc was started in cli())
    import tracemalloc
    if tracemalloc.is_tracing():
        tasks.append(asyncio.create_task(_mem_profile_loop(), name="memprof"))
        logger.info("Memory profiler enabled (logs every 5m)")

    exchange_map = {ex.name: ex for ex in exchanges}

    # News poller (delistings, new listings, dynamic subscribe/unsubscribe)
    if settings.news.enabled:
        from .news import run_news_poller
        tasks.append(asyncio.create_task(
            run_news_poller(
                alert_queue, settings, store,
                exchanges=exchange_map, msg_queue=msg_queue,
            ),
            name="news-poller",
        ))

    # Signal evaluator (continuous event detection)
    tasks.append(asyncio.create_task(
        run_signal_evaluator(store, signal_history, dispatcher, settings),
        name="signal-evaluator",
    ))

    # API server

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

    def _do_shutdown():
        if shutdown_event.is_set():
            return
        logger.info("Shutting down, saving snapshot...")
        if recorder is not None:
            recorder.stop()
        store.snapshot_save(_SNAPSHOT_PATH)
        shutdown_event.set()
        for ex in exchanges:
            asyncio.create_task(ex.stop())
        for t in tasks:
            t.cancel()

    def _signal_handler():
        _do_shutdown()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass  # Windows

    # When the TUI exits (user pressed q), shut down everything else
    if mode in ("tui", "both"):
        tui_task = next(t for t in tasks if t.get_name() == "tui")
        tui_task.add_done_callback(lambda _: _do_shutdown())

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
    parser.add_argument(
        "--mem-profile",
        action="store_true",
        help="Enable tracemalloc memory profiling (logs top allocators every 5 min)",
    )
    args = parser.parse_args()

    if args.mem_profile:
        import tracemalloc
        tracemalloc.start(10)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    # When TUI is active, redirect logs from stderr into the TUI log panel
    if args.mode in ("tui", "both"):
        from .tui import install_tui_logging
        install_tui_logging()

    if args.mode == "client":
        from .client import run_client
        settings = load_settings(args.config)
        url = args.server_url or f"http://localhost:{settings.api.port}"
        asyncio.run(run_client(url, settings))
    else:
        asyncio.run(run(args.mode))


if __name__ == "__main__":
    cli()
