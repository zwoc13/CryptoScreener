"""Bash-accessible client for the screener daemon — status and watch commands."""

from __future__ import annotations

import asyncio
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx


# ── ANSI helpers ────────────────────────────────────────────────────────────

BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
MAGENTA = "\033[35m"
RESET = "\033[0m"
CLEAR_LINE = "\033[2K"
CARRIAGE_RETURN = "\r"

# Trend colors
TREND_COLORS = {"UP": GREEN, "DOWN": RED, "RANGE": YELLOW, "-": DIM}

SIGN_COLORS = {"+": GREEN, "-": RED}


def colorize(text: str, color: str) -> str:
    return f"{color}{text}{RESET}"


def bold(text: str) -> str:
    return f"{BOLD}{text}{RESET}"


def dim(text: str) -> str:
    return f"{DIM}{text}{RESET}"


def format_pct(v: float, show_sign: bool = True) -> str:
    sign = "+" if v >= 0 else ""
    color = GREEN if v >= 0 else RED
    s = f"{sign}{v:.2f}%" if show_sign else f"{v:.2f}%"
    return colorize(s, color)


def format_volume(v: float) -> str:
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.1f}B"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.0f}M"
    if v >= 1_000:
        return f"{v / 1_000:.0f}K"
    return f"{v:.0f}"


def format_price(price: float) -> str:
    if price >= 1000:
        return f"{price:,.1f}"
    if price >= 1:
        return f"{price:.4f}"
    return f"{price:.6f}"


def format_natr(v: float) -> str:
    return f"{v:.2f}"


# ── Data model ──────────────────────────────────────────────────────────────

@dataclass
class TickerRow:
    exchange: str
    symbol: str
    last_price: float
    daily_change_pct: float
    trend: str
    cvd_5m: float
    cvd_1h: float
    natr_5m_14: float
    range_1m: float
    range_5m: float
    volume_24h: float
    funding_rate: float
    trend_icon: str = "─"
    bias_label: str = ""
    oi_change_5m_pct: float = 0.0

    @property
    def sort_key(self) -> tuple:
        return (self.exchange, self.symbol)


@dataclass
class HealthInfo:
    status: str
    symbols: int
    last_message_ago_s: float | None
    uptime_s: float


# ── API Client ───────────────────────────────────────────────────────────────

SORT_COLUMNS = {
    "symbol": lambda r: r.symbol,
    "change": lambda r: r.daily_change_pct,
    "trend": lambda r: r.trend,
    "cvd5m": lambda r: r.cvd_5m,
    "cvd1h": lambda r: r.cvd_1h,
    "natr": lambda r: r.natr_5m_14,
    "volume": lambda r: r.volume_24h,
    "funding": lambda r: r.funding_rate,
    "price": lambda r: r.last_price,
    "oi": lambda r: r.oi_change_5m_pct,
}


class ScreenerAPIClient:
    """HTTP client for the screener server API."""

    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        self._base = base_url.rstrip("/")
        self._client = httpx.AsyncClient(base_url=self._base, timeout=timeout)

    async def close(self) -> None:
        await self._client.aclose()

    async def get_tickers(
        self,
        exchange: str | None = None,
        min_change: float | None = None,
        limit: int = 500,
    ) -> list[TickerRow]:
        params: dict = {"limit": limit}
        if exchange:
            params["exchange"] = exchange
        if min_change is not None:
            params["min_change"] = min_change
        resp = await self._client.get("/tickers", params=params)
        resp.raise_for_status()
        rows = resp.json()
        tickers = []
        for r in rows:
            t = TickerRow(
                exchange=r["exchange"],
                symbol=r["symbol"],
                last_price=r.get("last_price", 0),
                daily_change_pct=r.get("daily_change_pct", 0),
                trend=r.get("trend", "-"),
                cvd_5m=r.get("cvd_5m", 0),
                cvd_1h=r.get("cvd_1h", 0),
                natr_5m_14=r.get("natr_5m_14", 0),
                range_1m=r.get("range_1m", 0),
                range_5m=r.get("range_5m", 0),
                volume_24h=r.get("volume_24h", 0),
                funding_rate=r.get("funding_rate", 0),
                oi_change_5m_pct=r.get("oi_change_5m_pct", 0),
            )
            t.trend_icon = {"UP": "↑", "DOWN": "↓", "RANGE": "↔", "-": "─"}[t.trend]
            tickers.append(t)
        return tickers

    async def get_health(self) -> HealthInfo:
        resp = await self._client.get("/health")
        resp.raise_for_status()
        d = resp.json()
        return HealthInfo(
            status=d.get("status", "?"),
            symbols=d.get("symbols", 0),
            last_message_ago_s=d.get("last_message_ago_s"),
            uptime_s=d.get("uptime_s", 0),
        )

    async def check_connection(self) -> bool:
        try:
            resp = await self._client.get("/health", timeout=3)
            return resp.status_code == 200
        except Exception:
            return False


def _format_uptime(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


# ── Table formatting ─────────────────────────────────────────────────────────

HEADER = (
    f"{DIM}{'EX':<6} {'SYM':<12} {'PRICE':>10} {'CHG%':>8} {'TR':>2} "
    f"{'CVD 5m':>10} {'CVD 1h':>10} {'NATR':>6} {'R1m':>8} {'R5m':>8} "
    f"{'VOL 24h':>9} {'FUND%':>7} {'OI%':>7}{RESET}"
)

SEP = DIM + "─" * 104 + RESET


def print_header(out=sys.stdout) -> None:
    out.write(HEADER + "\n")
    out.write(SEP + "\n")


def print_ticker_row(t: TickerRow, out=sys.stdout) -> None:
    trend_color = TREND_COLORS.get(t.trend, DIM)
    trend_icon = colorize(t.trend_icon, trend_color)
    change_str = format_pct(t.daily_change_pct)
    cvd5m_str = format_pct(t.cvd_5m, show_sign=True)
    cvd1h_str = format_pct(t.cvd_1h, show_sign=True)
    natr_str = format_natr(t.natr_5m_14)
    vol_str = format_volume(t.volume_24h)
    fund_str = f"{t.funding_rate * 100:+.4f}%"
    oi_str = format_pct(t.oi_change_5m_pct)
    price_str = format_price(t.last_price)

    line = (
        f"{DIM}{t.exchange:<6}{RESET} "
        f"{bold(t.symbol):<12} "
        f"{price_str:>10} "
        f"{change_str:>8} "
        f"{trend_icon:>2} "
        f"{cvd5m_str:>10} "
        f"{cvd1h_str:>10} "
        f"{natr_str:>6} "
        f"{t.range_1m:>8.4f} "
        f"{t.range_5m:>8.4f} "
        f"{vol_str:>9} "
        f"{fund_str:>7} "
        f"{oi_str:>7}"
    )
    out.write(line + "\n")


def print_health_bar(health: HealthInfo, out=sys.stdout) -> None:
    uptime_str = _format_uptime(health.uptime_s)
    last_msg = (
        f"{health.last_message_ago_s:.0f}s ago"
        if health.last_message_ago_s is not None
        else "N/A"
    )
    status_color = GREEN if health.status == "ok" else RED
    status_str = colorize(health.status.upper(), status_color)

    bar = (
        f"{bold('SERVER')} {status_str} | "
        f"{bold(str(health.symbols))} symbols | "
        f"{bold('uptime')} {uptime_str} | "
        f"{bold('last msg')} {last_msg}"
    )
    out.write(f"{bar}\n\n")


# ── Status command (one-shot) ───────────────────────────────────────────────

async def run_status(
    server_url: str,
    sort_by: str = "change",
    exchange: str | None = None,
    min_change: float | None = None,
    limit: int = 200,
    show_health: bool = True,
) -> None:
    """One-shot ticker dump formatted for bash consumption."""
    client = ScreenerAPIClient(server_url)

    try:
        connected = await client.check_connection()
        if not connected:
            print(
                f"{RED}✗{RESET} Cannot connect to screener server at {server_url}",
                file=sys.stderr,
            )
            print(
                f"{DIM}  Is the daemon running? (screener --mode server){RESET}",
                file=sys.stderr,
            )
            sys.exit(1)

        tasks = [client.get_tickers(exchange=exchange, min_change=min_change, limit=limit)]
        if show_health:
            tasks.append(client.get_health())

        results = await asyncio.gather(*tasks)
        tickers: list[TickerRow] = results[0]
        health: HealthInfo | None = results[1] if show_health else None

        if health:
            print_health_bar(health)

        if not tickers:
            print(f"{DIM}No tickers match the filter.{RESET}", file=sys.stderr)
            return

        sort_key = SORT_COLUMNS.get(sort_by, SORT_COLUMNS["change"])
        tickers.sort(key=sort_key, reverse=True)

        print_header()
        for t in tickers:
            print_ticker_row(t)
        print(dim(f"\n({len(tickers)} tickers)"))
    finally:
        await client.close()


# ── Watch command (live terminal dashboard) ──────────────────────────────────

async def _watch_loop(
    client: ScreenerAPIClient,
    interval: float,
    sort_by: str,
    exchange: str | None,
    min_change: float | None,
    limit: int,
    top_n: int,
    filter_text: str,
) -> None:
    """Poll loop for the watch command. Runs until Ctrl+C."""
    sort_key = SORT_COLUMNS.get(sort_by, SORT_COLUMNS["change"])
    poll_interval = max(interval, 1.0)

    while True:
        try:
            tickers = await client.get_tickers(
                exchange=exchange, min_change=min_change, limit=limit
            )
        except httpx.HTTPError as e:
            sys.stdout.write(
                f"\r{CLEAR_LINE}{RED}Connection error: {e}{RESET}    "
            )
            sys.stdout.flush()
            await asyncio.sleep(poll_interval)
            continue

        # Filter
        if filter_text:
            fl = filter_text.lower()
            tickers = [t for t in tickers if fl in t.symbol.lower() or fl in t.exchange.lower()]

        # Sort
        tickers.sort(key=sort_key, reverse=True)
        if top_n > 0:
            tickers = tickers[:top_n]

        # Clear screen from cursor up
        sys.stdout.write("\033[J")  # Clear from cursor to end of screen
        sys.stdout.write(CARRIAGE_RETURN)

        # Health line
        try:
            health = await client.get_health()
            print_health_bar(health, out=sys.stdout)
        except Exception:
            pass

        print_header(out=sys.stdout)
        for t in tickers:
            print_ticker_row(t, out=sys.stdout)

        # Footer
        footer = (
            f"{DIM}watching {len(tickers)} tickers | "
            f"sort={sort_by} | refresh={interval:.0f}s | "
            f"Ctrl+C to quit{RESET}"
        )
        sys.stdout.write(f"\n{footer}\n")

        sys.stdout.flush()
        await asyncio.sleep(poll_interval)


async def run_watch(
    server_url: str,
    sort_by: str = "change",
    exchange: str | None = None,
    min_change: float | None = None,
    limit: int = 500,
    interval: float = 3.0,
    top_n: int = 50,
    filter_text: str = "",
) -> None:
    """Live-updating terminal dashboard reading from the screener API."""
    client = ScreenerAPIClient(server_url)

    try:
        connected = await client.check_connection()
        if not connected:
            print(
                f"{RED}✗{RESET} Cannot connect to screener server at {server_url}",
                file=sys.stderr,
            )
            print(
                f"{DIM}  Is the daemon running? (screener --mode server){RESET}",
                file=sys.stderr,
            )
            sys.exit(1)

        # Enable alternate screen buffer
        sys.stdout.write("\033[?1049h")  # Alternate screen buffer
        sys.stdout.flush()

        try:
            await _watch_loop(
                client, interval, sort_by, exchange,
                min_change, limit, top_n, filter_text,
            )
        finally:
            # Disable alternate screen buffer and clear
            sys.stdout.write("\033[?1049l")
            sys.stdout.flush()
    finally:
        await client.close()
