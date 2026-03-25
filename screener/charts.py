from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Grid, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Footer, Header, Static
from textual_plotext import PlotextPlot

from .config import Settings
from .exchanges import BaseExchange
from .models import CandleBar


TIMEFRAMES = [
    ("1", "1m", 60),
    ("5", "5m", 60),
    ("60", "1h", 48),
    ("D", "1D", 60),
]


@dataclass
class CandleStats:
    """Average candle metrics for a timeframe — useful for TP/SL sizing."""
    avg_range: float = 0.0        # avg (high - low)
    avg_range_pct: float = 0.0    # avg range as % of close
    avg_body: float = 0.0         # avg abs(close - open)
    avg_body_pct: float = 0.0     # avg body as % of close
    avg_upper_wick: float = 0.0   # avg (high - max(open,close))
    avg_lower_wick: float = 0.0   # avg (min(open,close) - low)
    max_range: float = 0.0        # biggest candle range
    max_range_pct: float = 0.0    # biggest candle range as %
    atr: float = 0.0              # ATR(14) or ATR(all if < 14)
    atr_pct: float = 0.0          # ATR as % of close
    bullish_pct: float = 0.0      # % of candles that closed green
    count: int = 0


def compute_stats(candles: list[CandleBar]) -> CandleStats:
    if not candles:
        return CandleStats()

    ranges = []
    bodies = []
    upper_wicks = []
    lower_wicks = []
    range_pcts = []
    bullish = 0

    for c in candles:
        r = c.high - c.low
        body = abs(c.close - c.open)
        top = max(c.open, c.close)
        bot = min(c.open, c.close)
        uw = c.high - top
        lw = bot - c.low
        mid = c.close if c.close > 0 else 1

        ranges.append(r)
        bodies.append(body)
        upper_wicks.append(uw)
        lower_wicks.append(lw)
        range_pcts.append((r / mid) * 100)
        if c.close >= c.open:
            bullish += 1

    n = len(candles)
    last_close = candles[-1].close if candles[-1].close > 0 else 1

    # ATR: use min(14, n-1) periods
    tr_values = []
    for i in range(1, len(candles)):
        prev_close = candles[i - 1].close
        cur = candles[i]
        tr = max(
            cur.high - cur.low,
            abs(cur.high - prev_close),
            abs(cur.low - prev_close),
        )
        tr_values.append(tr)
    atr = sum(tr_values[-14:]) / min(14, len(tr_values)) if tr_values else 0

    return CandleStats(
        avg_range=sum(ranges) / n,
        avg_range_pct=sum(range_pcts) / n,
        avg_body=sum(bodies) / n,
        avg_body_pct=(sum(bodies) / n / last_close) * 100,
        avg_upper_wick=sum(upper_wicks) / n,
        avg_lower_wick=sum(lower_wicks) / n,
        max_range=max(ranges),
        max_range_pct=max(range_pcts),
        atr=atr,
        atr_pct=(atr / last_close) * 100,
        bullish_pct=(bullish / n) * 100,
        count=n,
    )


class ChartPanel(PlotextPlot):
    DEFAULT_CSS = """
    ChartPanel {
        height: 1fr;
        width: 1fr;
    }
    """

    def __init__(self, title: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._title = title

    def on_mount(self) -> None:
        self.plt.title(self._title)
        self.plt.theme("dark")

    def draw_candles(self, candles: list[CandleBar], label: str, tz: ZoneInfo, is_daily: bool = False) -> None:
        if not candles:
            return

        plt = self.plt
        plt.clear_data()
        plt.clear_figure()
        plt.theme("dark")
        plt.title(label)

        dates = []
        data = {"Open": [], "Close": [], "High": [], "Low": []}

        for c in candles:
            dt = datetime.fromtimestamp(c.timestamp, tz=tz)
            if is_daily:
                dates.append(dt.strftime("%d/%m/%Y"))
            else:
                dates.append(dt.strftime("%d/%m/%Y %H:%M"))
            data["Open"].append(c.open)
            data["Close"].append(c.close)
            data["High"].append(c.high)
            data["Low"].append(c.low)

        plt.date_form("d/m/Y" if is_daily else "d/m/Y H:M")
        plt.candlestick(dates, data)
        self.refresh()


class StatsPanel(Static):
    """Shows candle statistics for a timeframe."""

    DEFAULT_CSS = """
    StatsPanel {
        width: 1fr;
        height: auto;
        min-height: 10;
        padding: 0 1;
        border-top: solid $surface-lighten-2;
    }
    """

    def set_stats(self, label: str, stats: CandleStats, price: float) -> None:
        if stats.count == 0:
            self.update(f"[bold]{label}[/bold]\n[dim]No data[/dim]")
            return

        # Format price values adaptively
        def fp(v: float) -> str:
            if price >= 100:
                return f"{v:.2f}"
            if price >= 1:
                return f"{v:.4f}"
            return f"{v:.6f}"

        bull_color = "green" if stats.bullish_pct >= 50 else "red"

        lines = [
            f"[bold underline]{label}[/bold underline] ({stats.count} candles)",
            "",
            f"  Avg Range:    {fp(stats.avg_range)}  ([cyan]{stats.avg_range_pct:.2f}%[/cyan])",
            f"  Avg Body:     {fp(stats.avg_body)}  ([cyan]{stats.avg_body_pct:.2f}%[/cyan])",
            f"  Avg Wick  ^:  {fp(stats.avg_upper_wick)}   v: {fp(stats.avg_lower_wick)}",
            f"  Max Range:    {fp(stats.max_range)}  ([yellow]{stats.max_range_pct:.2f}%[/yellow])",
            f"  ATR(14):      {fp(stats.atr)}  ([cyan]{stats.atr_pct:.2f}%[/cyan])",
            f"  Bullish:      [{bull_color}]{stats.bullish_pct:.0f}%[/{bull_color}]",
        ]
        self.update("\n".join(lines))


class LoadingIndicator(Static):
    DEFAULT_CSS = """
    LoadingIndicator {
        height: 1fr;
        width: 1fr;
        content-align: center middle;
        text-align: center;
    }
    """


class ChartScreen(Screen):
    CSS = """
    #main-layout {
        height: 1fr;
    }
    #charts-col {
        width: 3fr;
    }
    #stats-col {
        width: 1fr;
        overflow-y: auto;
        border-left: solid $surface-lighten-2;
        padding: 0;
    }
    #chart-grid {
        layout: grid;
        grid-size: 2 2;
        grid-gutter: 0;
        height: 1fr;
    }
    #loading {
        height: 1fr;
        content-align: center middle;
        text-align: center;
    }
    #stats-header {
        text-align: center;
        padding: 1 0;
        text-style: bold;
    }
    """

    BINDINGS = [
        Binding("escape", "dismiss", "Back"),
        Binding("q", "dismiss", "Back"),
    ]

    def __init__(
        self,
        exchange: BaseExchange,
        exchange_name: str,
        symbol: str,
        settings: Settings,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._exchange = exchange
        self._exchange_name = exchange_name
        self._symbol = symbol
        self._settings = settings
        self._tz = ZoneInfo(settings.schedule.timezone)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield LoadingIndicator(
            f"Loading charts for [bold]{self._symbol}[/bold]...",
            id="loading",
        )
        yield Footer()

    async def on_mount(self) -> None:
        tasks = [self._fetch_candles(interval, limit) for interval, _, limit in TIMEFRAMES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        loading = self.query_one("#loading")
        await loading.remove()

        # Build layout: 3/4 charts | 1/4 stats
        main = Horizontal(id="main-layout")
        await self.mount(main, before=self.query_one(Footer))

        charts_col = Vertical(id="charts-col")
        stats_col = Vertical(id="stats-col")
        await main.mount(charts_col)
        await main.mount(stats_col)

        grid = Grid(id="chart-grid")
        await charts_col.mount(grid)

        # Stats header
        header = Static(f"[bold]{self._symbol}[/bold] — Candle Stats (for TP/SL)", id="stats-header")
        await stats_col.mount(header)

        # Get last price for adaptive formatting
        last_price = 0.0

        for i, ((interval, label, _limit), candles) in enumerate(zip(TIMEFRAMES, results)):
            chart_label = f"{self._symbol} {label}"
            panel = ChartPanel(title=chart_label, id=f"chart-{i}")
            await grid.mount(panel)

            if isinstance(candles, list) and candles:
                panel.draw_candles(candles, chart_label, self._tz, is_daily=(interval == "D"))
                if last_price == 0:
                    last_price = candles[-1].close

                # Compute and display stats
                stats = compute_stats(candles)
                stats_panel = StatsPanel(id=f"stats-{i}")
                await stats_col.mount(stats_panel)
                stats_panel.set_stats(f"{label}", stats, last_price)
            else:
                stats_panel = StatsPanel(id=f"stats-{i}")
                await stats_col.mount(stats_panel)
                stats_panel.set_stats(f"{label}", CandleStats(), last_price)

        # Start live polling every 5 seconds
        self._poll_timer = self.set_interval(5, self._poll_candles)

    async def _poll_candles(self) -> None:
        tasks = [self._fetch_candles(interval, limit) for interval, _, limit in TIMEFRAMES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        last_price = 0.0
        for i, ((interval, label, _limit), candles) in enumerate(zip(TIMEFRAMES, results)):
            if not isinstance(candles, list) or not candles:
                continue

            if last_price == 0:
                last_price = candles[-1].close

            # Update chart
            chart_label = f"{self._symbol} {label}"
            try:
                panel = self.query_one(f"#chart-{i}", ChartPanel)
                panel.draw_candles(candles, chart_label, self._tz, is_daily=(interval == "D"))
            except Exception:
                pass

            # Update stats
            try:
                stats = compute_stats(candles)
                stats_panel = self.query_one(f"#stats-{i}", StatsPanel)
                stats_panel.set_stats(f"{label}", stats, last_price)
            except Exception:
                pass

    async def _fetch_candles(self, interval: str, limit: int) -> list[CandleBar]:
        return await self._exchange.fetch_klines(self._symbol, interval, limit)

    def action_dismiss(self) -> None:
        self.app.pop_screen()
