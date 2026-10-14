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
    from .snapshot_writer import SnapshotWriter, AsyncSnapshotWriter
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
        run_signal_evaluator(store, signal_history, alert_queue, settings),
        name="signal-evaluator",
    ))

    # Snapshot writer — flush signal history to QuestDB for ML pipeline
    if settings.questdb.enabled:
        writer = SnapshotWriter(settings.questdb, signal_history,
                                flush_interval_s=settings.questdb.snapshot_flush_interval_s)
        async_writer = AsyncSnapshotWriter(writer)
        tasks.append(asyncio.create_task(
            async_writer.start_flush_loop(store, settings.questdb.snapshot_flush_interval_s),
            name="snapshot-writer",
        ))
        # Clean up on shutdown
        def _close_writer():
            async_writer.close()
        import atexit
        atexit.register(_close_writer)

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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="screener",
        description="Bybit Futures Screener — real-time crypto screener daemon + TUI.",
    )
    # Global flags (work with or without the "run" subcommand for backward compat)
    parser.add_argument(
        "--mode",
        choices=["tui", "api", "both", "server", "client"],
        default=None,
        help="Run mode (default: both)",
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument("--server-url", default=None)
    parser.add_argument("--mem-profile", action="store_true")

    sub = parser.add_subparsers(dest="command")

    # ── run: Run the screener daemon ─────────────────────────────────────────
    run_parser = sub.add_parser(
        "run",
        help="Run the screener (TUI + API in one process, same as bare screener)",
    )
    run_parser.add_argument(
        "--mode",
        choices=["tui", "api", "both", "server", "client"],
        default="both",
    )
    run_parser.add_argument("--config", default="config.yaml")
    run_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    run_parser.add_argument("--server-url", default=None)
    run_parser.add_argument("--mem-profile", action="store_true")

    # ── status: one-shot ticker dump ────────────────────────────────────────
    status_parser = sub.add_parser(
        "status",
        help="One-shot ticker snapshot — connect to a running daemon via bash.",
    )
    status_parser.add_argument(
        "--server-url",
        default="http://localhost:8000",
        help="Daemon URL (default: http://localhost:8000)",
    )
    status_parser.add_argument(
        "--sort",
        "-s",
        choices=["change", "symbol", "trend", "cvd5m", "cvd1h", "natr", "volume", "funding", "price", "oi"],
        default="change",
        help="Sort column (default: change)",
    )
    status_parser.add_argument("--exchange", "-e", default=None)
    status_parser.add_argument("--min-change", "-m", type=float, default=None)
    status_parser.add_argument(
        "--limit", "-l", type=int, default=200,
    )
    status_parser.add_argument(
        "--no-health",
        action="store_true",
        help="Skip health header",
    )

    # ── watch: live terminal dashboard ─────────────────────────────────────
    watch_parser = sub.add_parser(
        "watch",
        help="Live terminal dashboard — polls the daemon every N seconds.",
    )
    watch_parser.add_argument(
        "--server-url",
        default="http://localhost:8000",
        help="Daemon URL (default: http://localhost:8000)",
    )
    watch_parser.add_argument(
        "--sort",
        "-s",
        choices=["change", "symbol", "trend", "cvd5m", "cvd1h", "natr", "volume", "funding", "price", "oi"],
        default="change",
        help="Sort column (default: change)",
    )
    watch_parser.add_argument("--exchange", "-e", default=None)
    watch_parser.add_argument("--min-change", "-m", type=float, default=None)
    watch_parser.add_argument("--limit", "-l", type=int, default=500)
    watch_parser.add_argument(
        "--interval", "-i", type=float, default=3.0,
        help="Refresh interval in seconds (default: 3.0)",
    )
    watch_parser.add_argument(
        "--top", "-n", type=int, default=50,
        help="Show only top N tickers (default: 50, 0 = all)",
    )
    watch_parser.add_argument(
        "--filter", "-f", type=str, default="",
        help="Filter tickers by symbol or exchange containing string (case-insensitive)",
    )

    return parser


def cli() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    # Normalize: bare "screener" or "screener run" → daemon
    # Default: run the daemon (either via "screener run" or bare "screener")
    if args.command is None or args.command == "run":
        # args.mode comes from the main parser (or the run subparser when
        # "screener run --mode server" is used — argparse merges them so
        # the subparser value overwrites the main one).
        effective_mode = args.mode or "both"
        effective_config = args.config
        effective_log_level = args.log_level
        effective_server_url = args.server_url
        effective_mem_profile = args.mem_profile

        _run_daemon(
            mode=effective_mode,
            config=effective_config,
            log_level=effective_log_level,
            server_url=effective_server_url,
            mem_profile=effective_mem_profile,
        )
        return

    # Bash-friendly commands: status / watch
    if args.command == "status":
        from .bash_client import run_status
        asyncio.run(
            run_status(
                server_url=args.server_url,
                sort_by=args.sort,
                exchange=args.exchange,
                min_change=args.min_change,
                limit=args.limit,
                show_health=not args.no_health,
            )
        )
        return

    if args.command == "watch":
        from .bash_client import run_watch
        try:
            asyncio.run(
                run_watch(
                    server_url=args.server_url,
                    sort_by=args.sort,
                    exchange=args.exchange,
                    min_change=args.min_change,
                    limit=args.limit,
                    interval=args.interval,
                    top_n=args.top,
                    filter_text=args.filter,
                )
            )
        except KeyboardInterrupt:
            pass
        return


def _run_daemon(
    mode: str,
    config: str,
    log_level: str,
    server_url: str | None,
    mem_profile: bool,
) -> None:
    """Run the screener daemon (tui/api/both/server/client mode)."""
    if mem_profile:
        import tracemalloc
        tracemalloc.start(10)

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    # When TUI is active, redirect logs from stderr into the TUI log panel
    if mode in ("tui", "both"):
        from .tui import install_tui_logging
        install_tui_logging()

    settings = load_settings(config)

    if mode == "client":
        from .client import run_client
        url = server_url or f"http://localhost:{settings.api.port}"
        asyncio.run(run_client(url, settings))
    else:
        asyncio.run(run(mode))


if __name__ == "__main__":
    cli()
