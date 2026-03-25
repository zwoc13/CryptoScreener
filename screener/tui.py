from __future__ import annotations

from collections import deque
from datetime import datetime
from time import time
from typing import Any
from zoneinfo import ZoneInfo

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Input, Static

from .alerts import AlertEvent
from .config import Settings
from .models import FundingAlert, ImpulseEvent, LargeOrderEvent, OrderEatenEvent, TickerState
from .store import Store

# Column definitions: (label, sort_field)
# Fields starting with "_" are computed at display time, not from TickerState
COLUMNS: list[tuple[str, str]] = [
    ("Exchange", "exchange"),
    ("Ticker", "symbol"),
    ("Trend", "trend"),
    ("Change %", "daily_change_pct"),
    ("CVD 5m", "_cvd_5m"),
    ("CVD 1h", "_cvd_1h"),
    ("Range 1m", "range_1m"),
    ("Range 5m", "range_5m"),
    ("NATR 5/14", "natr_5m_14"),
    ("Vol 24h", "volume_24h"),
    ("Fund %", "funding_rate"),
    ("Fund In", "next_funding_ts"),
    ("Fund Int", "funding_interval_h"),
]


def _sort_key(ticker: TickerState, field: str, descending: bool, extra: dict[str, float] | None = None) -> Any:
    if field.startswith("_") and extra:
        return abs(extra.get(field, 0.0)) if descending else extra.get(field, 0.0)
    val = getattr(ticker, field, 0)
    if field in ("exchange", "symbol", "trend"):
        return val.lower() if isinstance(val, str) else val
    if field == "daily_change_pct" and descending:
        return abs(val)
    return val


class StatusBar(Static):
    pass


class AlertPanel(Static):
    """Scrollable panel showing recent alert history."""

    DEFAULT_CSS = """
    AlertPanel {
        height: 1fr;
        overflow-y: auto;
        padding: 0 1;
        border-left: solid $surface-lighten-2;
    }
    """

    def __init__(self, alerts: deque[AlertEvent], timezone: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._alerts = alerts
        self._tz = ZoneInfo(timezone)
        self._last_count = 0

    def refresh_alerts(self) -> None:
        # Only re-render if new alerts arrived
        count = len(self._alerts)
        if count == self._last_count:
            return
        self._last_count = count

        lines: list[str] = []
        lines.append("[bold underline]Alerts[/bold underline]\n")

        for event in self._alerts:
            ts = datetime.fromtimestamp(event.timestamp, tz=self._tz)
            time_str = ts.strftime("%H:%M:%S")

            if isinstance(event, ImpulseEvent):
                arrow = "[green]^[/green]" if event.direction == "up" else "[red]v[/red]"
                color = "green" if event.direction == "up" else "red"
                sign = "+" if event.direction == "up" else "-"
                lines.append(
                    f"[dim]{time_str}[/dim] {arrow} "
                    f"[bold]{event.symbol}[/bold] "
                    f"[{color}]{sign}{event.change_pct}%[/{color}] "
                    f"[dim]{event.exchange}[/dim]"
                )
                lines.append(
                    f"         NATR:{event.natr_value:.2f} "
                    f"Vol:{_fmt_volume(event.volume_24h)}"
                )
                lines.append("")
            elif isinstance(event, FundingAlert):
                color = "red" if event.rate > 0 else "green"
                lines.append(
                    f"[dim]{time_str}[/dim] [yellow]F[/yellow] "
                    f"[bold]{event.symbol}[/bold] "
                    f"[{color}]{event.rate * 100:.4f}%[/{color}] "
                    f"[dim]{event.exchange}[/dim]"
                )
                lines.append("")
            elif isinstance(event, OrderEatenEvent):
                side_label = "BID" if event.side == "bid" else "ASK"
                color = "green" if event.side == "bid" else "red"
                status = "FILLED" if event.likely_filled else "CANCELLED"
                lines.append(
                    f"[dim]{time_str}[/dim] [{color}]{side_label} EATEN[/{color}] "
                    f"[bold]{event.symbol}[/bold] "
                    f"${_fmt_volume(event.size_usd)} "
                    f"[dim]{status}[/dim]"
                )
                lines.append("")

        if len(self._alerts) == 0:
            lines.append("[dim]No alerts yet...[/dim]")

        self.update("\n".join(lines))


class LargeOrderPanel(Static):
    """Shows currently active large resting orders in the orderbook."""

    DEFAULT_CSS = """
    LargeOrderPanel {
        height: 1fr;
        overflow-y: auto;
        padding: 0 1;
        border-top: solid $surface-lighten-2;
    }
    """

    def __init__(self, orders: deque, timezone: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._orders = orders
        self._tz = ZoneInfo(timezone)
        self._last_count = 0

    def refresh_orders(self) -> None:
        count = len(self._orders)
        if count == self._last_count:
            return
        self._last_count = count

        lines: list[str] = []
        lines.append("[bold underline]Large Orders[/bold underline]\n")

        for event in self._orders:
            ts = datetime.fromtimestamp(event.timestamp, tz=self._tz)
            time_str = ts.strftime("%H:%M:%S")
            side_label = "BID" if event.side == "bid" else "ASK"
            color = "green" if event.side == "bid" else "red"
            lines.append(
                f"[dim]{time_str}[/dim] [{color}]{side_label}[/{color}] "
                f"[bold]{event.symbol}[/bold] "
                f"${_fmt_volume(event.size_usd)} "
                f"@ {_fmt_price(event.price)} "
                f"[dim]({event.distance_pct:.1f}%)[/dim]"
            )

        if count == 0:
            lines.append("[dim]No large orders yet...[/dim]")

        self.update("\n".join(lines))


class ScreenerApp(App):
    CSS = """
    Screen {
        layout: vertical;
    }
    #main-area {
        height: 1fr;
    }
    #table-pane {
        width: 2fr;
    }
    #right-pane {
        width: 1fr;
    }
    #alert-pane {
        height: 1fr;
    }
    #ob-pane {
        height: 1fr;
    }
    #status-bar {
        dock: bottom;
        height: 1;
        background: $surface;
        color: $text-muted;
        padding: 0 1;
    }
    DataTable {
        height: 1fr;
    }
    #search-bar {
        dock: bottom;
        height: 1;
        display: none;
    }
    #search-bar.visible {
        display: block;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh_now", "Refresh"),
        ("t", "toggle_direction", "Asc/Desc"),
        ("e", "sort(0)", "sExchange"),
        ("n", "sort(1)", "sName"),
        ("d", "sort(2)", "sTrend"),
        ("c", "sort(3)", "sChange"),
        ("v", "sort(4)", "sCVD5m"),
        ("V", "sort(5)", "sCVD1h"),
        ("1", "sort(6)", "sRng1m"),
        ("5", "sort(7)", "sRng5m"),
        ("a", "sort(8)", "sNATR"),
        ("o", "sort(9)", "sVolume"),
        ("f", "sort(10)", "sFund%"),
        ("i", "sort(11)", "sFundIn"),
        ("slash", "search", "/Search"),
    ]

    def __init__(
        self,
        store: Store,
        settings: Settings,
        alert_history: deque[AlertEvent],
        exchanges: dict[str, Any] | None = None,
        large_orders: deque | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._store = store
        self._settings = settings
        self._alert_history = alert_history
        self._exchanges = exchanges or {}  # name -> BaseExchange instance
        self._large_orders = large_orders or deque(maxlen=0)
        self._sort_col = 2  # default: Change %
        self._sort_desc = True
        self._row_keys: dict[str, Any] = {}
        self._search_filter: str = ""

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="main-area"):
            with Vertical(id="table-pane"):
                yield DataTable(id="table")
            with Vertical(id="right-pane"):
                yield AlertPanel(
                    self._alert_history,
                    self._settings.schedule.timezone,
                    id="alert-pane",
                )
                yield LargeOrderPanel(
                    self._large_orders,
                    self._settings.schedule.timezone,
                    id="ob-pane",
                )
        yield Input(placeholder="Search ticker...", id="search-bar")
        yield StatusBar(id="status-bar")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#table", DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        for i, (label, _field) in enumerate(COLUMNS):
            marker = ""
            if i == self._sort_col:
                marker = " v" if self._sort_desc else " ^"
            table.add_column(f"{label}{marker}", key=f"col_{i}")
        self.set_interval(
            self._settings.tui.refresh_ms / 1000,
            self._refresh_all,
        )

    def _refresh_all(self) -> None:
        self._refresh_table()
        self._refresh_alerts()
        self._refresh_large_orders()

    def _refresh_alerts(self) -> None:
        panel = self.query_one("#alert-pane", AlertPanel)
        panel.refresh_alerts()

    def _refresh_large_orders(self) -> None:
        panel = self.query_one("#ob-pane", LargeOrderPanel)
        panel.refresh_orders()

    def _refresh_table(self) -> None:
        table = self.query_one("#table", DataTable)
        field = COLUMNS[self._sort_col][1]
        tickers = list(self._store._tickers.values())

        # Apply search filter
        if self._search_filter:
            q = self._search_filter.upper()
            tickers = [t for t in tickers if q in t.symbol]

        # Pre-compute CVD values for sorting and display
        cvd_cache: dict[str, dict[str, float]] = {}
        for t in tickers:
            k = f"{t.exchange}:{t.symbol}"
            cvd_cache[k] = {
                "_cvd_5m": self._store.get_cvd_rolling(t.exchange, t.symbol, 300),
                "_cvd_1h": self._store.get_cvd_rolling(t.exchange, t.symbol, 3600),
            }

        tickers.sort(
            key=lambda t: _sort_key(t, field, self._sort_desc, cvd_cache.get(f"{t.exchange}:{t.symbol}")),
            reverse=self._sort_desc,
        )
        max_rows = self._settings.tui.max_rows

        visible_keys: set[str] = set()

        for i, t in enumerate(tickers):
            if i >= max_rows:
                break
            k = f"{t.exchange}:{t.symbol}"
            visible_keys.add(k)
            cvd = cvd_cache.get(k, {"_cvd_5m": 0, "_cvd_1h": 0})
            row_data = (
                t.exchange.upper(),
                t.symbol,
                _color_trend(t.trend),
                _color_pct(t.daily_change_pct),
                _color_cvd(cvd["_cvd_5m"]),
                _color_cvd(cvd["_cvd_1h"]),
                _fmt_price(t.range_1m),
                _fmt_price(t.range_5m),
                f"{t.natr_5m_14:.1f}" if t.natr_5m_14 > 0 else "-",
                _fmt_volume(t.volume_24h),
                _color_funding(t.funding_rate),
                _fmt_funding_countdown(t.next_funding_ts),
                f"{t.funding_interval_h}h" if t.funding_interval_h else "-",
            )

            if k in self._row_keys:
                row_key = self._row_keys[k]
                for col_idx, val in enumerate(row_data):
                    try:
                        table.update_cell(row_key, f"col_{col_idx}", val)
                    except Exception:
                        pass
            else:
                row_key = table.add_row(*row_data, key=k)
                self._row_keys[k] = row_key

        stale = set(self._row_keys) - visible_keys
        for k in stale:
            try:
                table.remove_row(self._row_keys[k])
            except Exception:
                pass
            del self._row_keys[k]

        self._update_column_headers(table)

        status = self.query_one("#status-bar", StatusBar)
        last = self._store.last_message_ts
        ago = time() - last if last > 0 else 0
        sort_label = COLUMNS[self._sort_col][0]
        direction = "DESC" if self._sort_desc else "ASC"
        alert_count = len(self._alert_history)
        filter_info = f"  |  Filter: {self._search_filter}" if self._search_filter else ""
        status.update(
            f" Symbols: {self._store.symbol_count}"
            f"  |  Sort: {sort_label} {direction}"
            f"  |  Alerts: {alert_count}"
            f"  |  Last update: {ago:.0f}s ago"
            f"{filter_info}"
            f"  |  /=search  e n d c v V 1 5 a o f i  t=flip"
        )

    def _update_column_headers(self, table: DataTable) -> None:
        for i, (label, _field) in enumerate(COLUMNS):
            marker = ""
            if i == self._sort_col:
                marker = " v" if self._sort_desc else " ^"
            col_key = f"col_{i}"
            try:
                col = table.columns[col_key]
                col.label = f"{label}{marker}"
            except (KeyError, AttributeError):
                pass

    def action_sort(self, col_index: int) -> None:
        if col_index == self._sort_col:
            self._sort_desc = not self._sort_desc
        else:
            self._sort_col = col_index
            self._sort_desc = True
        self._rebuild_table()

    def action_toggle_direction(self) -> None:
        self._sort_desc = not self._sort_desc
        self._rebuild_table()

    def _rebuild_table(self) -> None:
        table = self.query_one("#table", DataTable)
        cursor_key: str | None = None
        try:
            if table.row_count > 0:
                cursor_row = table.cursor_row
                for k, rk in self._row_keys.items():
                    row_idx = table.get_row_index(rk)
                    if row_idx == cursor_row:
                        cursor_key = k
                        break
        except Exception:
            pass

        table.clear()
        self._row_keys.clear()
        self._refresh_table()

        if cursor_key and cursor_key in self._row_keys:
            try:
                row_key = self._row_keys[cursor_key]
                idx = table.get_row_index(row_key)
                table.move_cursor(row=idx)
            except Exception:
                pass

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Open chart screen when Enter is pressed on a row."""
        row_key = event.row_key
        # Find the exchange:symbol key for this row
        for k, rk in self._row_keys.items():
            if rk == row_key:
                exchange_name, symbol = k.split(":", 1)
                exchange = self._exchanges.get(exchange_name)
                if exchange:
                    from .charts import ChartScreen
                    self.push_screen(
                        ChartScreen(
                            exchange=exchange,
                            exchange_name=exchange_name,
                            symbol=symbol,
                            settings=self._settings,
                        )
                    )
                return

    def action_refresh_now(self) -> None:
        self._refresh_all()

    def action_search(self) -> None:
        search = self.query_one("#search-bar", Input)
        search.add_class("visible")
        search.value = ""
        search.focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "search-bar":
            self._search_filter = event.value.strip()
            self._rebuild_table()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search-bar":
            # Close search bar, keep filter active
            search = self.query_one("#search-bar", Input)
            search.remove_class("visible")
            self.query_one("#table", DataTable).focus()

    def _dismiss_search(self) -> None:
        """Clear search and hide the bar."""
        self._search_filter = ""
        search = self.query_one("#search-bar", Input)
        search.value = ""
        search.remove_class("visible")
        self.query_one("#table", DataTable).focus()
        self._rebuild_table()

    def on_key(self, event) -> None:
        # Escape while search is focused clears it
        if event.key == "escape":
            search = self.query_one("#search-bar", Input)
            if search.has_class("visible"):
                self._dismiss_search()
                event.prevent_default()
                event.stop()


def _color_trend(trend: str) -> str:
    if trend == "UP":
        return "[green]UP[/green]"
    if trend == "DOWN":
        return "[red]DOWN[/red]"
    if trend == "RANGE":
        return "[yellow]RANGE[/yellow]"
    return "-"


def _color_cvd(val: float) -> str:
    if val == 0:
        return "-"
    if val > 0:
        return f"[green]+{_fmt_volume(val)}[/green]"
    return f"[red]-{_fmt_volume(abs(val))}[/red]"


def _color_pct(val: float) -> str:
    if val > 0:
        return f"[green]+{val:.1f}[/green]"
    elif val < 0:
        return f"[red]{val:.1f}[/red]"
    return "0.0"


def _color_funding(val: float) -> str:
    pct = val * 100
    if abs(pct) >= 0.5:
        color = "red" if pct > 0 else "green"
        return f"[{color}]{pct:.4f}[/{color}]"
    return f"{pct:.4f}"


def _fmt_price(val: float) -> str:
    if val <= 0:
        return "-"
    if val >= 1:
        return f"{val:.2f}"
    if val >= 0.01:
        return f"{val:.4f}"
    return f"{val:.6f}"


def _fmt_volume(v: float) -> str:
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.1f}B"
    if v >= 1_000_000:
        return f"[bold]{v / 1_000_000:.0f}M[/bold]" if v >= 100_000_000 else f"{v / 1_000_000:.0f}M"
    if v >= 1_000:
        return f"{v / 1_000:.0f}K"
    return f"{v:.0f}"


def _fmt_funding_countdown(next_ts: float) -> str:
    if next_ts <= 0:
        return "-"
    remaining = next_ts - time()
    if remaining <= 0:
        return "[bold yellow]NOW[/bold yellow]"
    hours = int(remaining // 3600)
    minutes = int((remaining % 3600) // 60)
    if hours > 0:
        return f"{hours}h{minutes:02d}m"
    if minutes > 5:
        return f"{minutes}m"
    # Less than 5 minutes — highlight
    return f"[bold yellow]{minutes}m[/bold yellow]"
