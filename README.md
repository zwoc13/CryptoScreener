# Crypto Futures Screener

A real-time terminal screener for cryptocurrency perpetual futures. Tracks every USDT-margined perpetual on Bybit (578+ symbols) via WebSocket, displays live metrics in a sortable TUI, exposes a REST API for bot integration, and fires alerts on price impulses and extreme funding rates.

## Why This Exists

Most crypto screeners are web-based, closed-source, and offer no way to programmatically act on what they show you. You can watch a price spike happen on screen, but there's no API to feed that signal into a trading bot. This project was built to solve that — a screener that both a human and a machine can consume.

The screener is designed as a **market event bus**: it observes the entire futures market in real-time, detects interesting events (impulses, funding anomalies, large orderbook walls), and publishes them to a stream. Downstream bots can subscribe, filter, and act autonomously.

## What It Tracks

| Metric | Source | Description |
|--------|--------|-------------|
| Daily Change % | Ticker stream | Price change since daily reset (2 AM configurable) |
| Trend | 5m candles | UP / DOWN / RANGE from higher-highs/lower-lows pattern |
| CVD 5m / 1h | Trade stream | Cumulative Volume Delta — net buy vs sell pressure |
| Range 1m | Ticker stream | High - Low within the current minute |
| Range 5m | 5m kline stream | High - Low of the latest 5-minute candle |
| NATR 5m/14 | 5m candles | Normalized ATR(14) — volatility as % of price |
| Volume 24h | Ticker stream | Rolling 24-hour trading volume |
| Funding Rate | Ticker stream | Current funding rate with countdown to next settlement |
| Large Orders | Orderbook depth | Resting limit orders that are large relative to volume |

## Features

### Terminal UI (TUI)

- Live-updating sortable table with 13 columns
- Color-coded metrics (green/red for direction, yellow for warnings)
- 2/3 + 1/3 split layout: ticker table on the left, alerts + large orders on the right
- Mnemonic keyboard shortcuts for sorting (e, n, d, c, v, 1, 5, a, o, f, i)
- Search/filter tickers with `/`
- Press Enter on any ticker to open multi-timeframe candlestick charts (1m, 5m, 1h, 1D) with candle statistics for TP/SL sizing

### REST API

- `GET /tickers` — all tickers with full metrics, filterable
- `GET /tickers/{symbol}` — single ticker detail
- `GET /klines/{symbol}?interval=5&limit=60` — historical candles
- `GET /alerts/recent` — recent alert history
- `GET /events/stream` — **SSE endpoint** for real-time event streaming to bots
- `GET /health` — server status

### Alerts

- **Price impulses**: detects sudden moves using static threshold (e.g. 3%) and/or NATR-based dynamic threshold
- **Extreme funding rates**: alerts when funding exceeds configurable threshold
- **Orderbook walls eaten**: detects when large resting orders disappear (optional, disabled by default)
- Dispatched via **HTTP webhooks** and **Telegram** with per-symbol cooldowns
- All events also published to the **SSE stream** for bot consumption

### Client-Server Architecture

Run the screener as a headless daemon on a VPS and connect from anywhere:

```
VPS: screener --mode server     (streams + engine + alerts + API)
     |  HTTP
You: screener --mode client     (TUI connects to server API)
Bot: curl /events/stream        (subscribes to real-time events)
```

### Multi-Exchange Ready

The exchange layer uses a pluggable adapter pattern (`BaseExchange` ABC). Currently Bybit is implemented. Adding Binance, OKX, or others means adding a single file in `screener/exchanges/`.

## Quick Start

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager

### Install and Run

```bash
git clone <repo-url> && cd screener

# Install dependencies
uv sync

# Edit config
vi config.yaml  # adjust as needed

# Run everything (TUI + API in one process)
uv run python -m screener

# Or just the API
uv run python -m screener --mode api

# Or headless server
uv run python -m screener --mode server

# Connect TUI to a running server
uv run python -m screener --mode client --server-url http://your-server:8000
```

### Docker

```bash
# Start headless server
docker compose up -d

# Connect from your machine
uv run python -m screener --mode client --server-url http://your-vps:8000
```

The VPS only needs Docker installed. Python, uv, and all dependencies are bundled inside the container image.

## Configuration

All settings live in `config.yaml`. Key sections:

```yaml
exchanges:
  bybit:
    enabled: true
    ws_connections: 2             # WS connections for tickers + klines

impulse:
  threshold_pct: 3.0             # static trigger: price moves >= 3% in 60s
  natr_multiplier: 2.5           # dynamic trigger: move >= 2.5x NATR
  combine_mode: "or"             # "or" = either triggers, "and" = both required

funding:
  alert_threshold: 0.01          # alert if abs(funding_rate) >= 1%

alerts:
  cooldown_seconds: 300           # 5 min cooldown per symbol per alert type
  history_ttl_seconds: 3600       # drop alerts older than 1 hour
  webhooks:
    - url: "https://your-bot.example.com/webhook"
  telegram:
    bot_token: ""                 # or set SCREENER_TELEGRAM_TOKEN env var
    chat_ids: ["your-chat-id"]

schedule:
  reset_hour: 2                   # daily metrics reset at 2:00 AM
  timezone: "Europe/Kyiv"

orderbook:
  enabled: false                  # large order tracking (bandwidth-intensive)
  min_size_usd: 50000             # minimum USD to qualify as "large"
  threshold_pct: 0.1              # or % of 24h volume, whichever is greater

api:
  port: 8000
  host: "0.0.0.0"
```

## Consuming Events (Bot Integration)

### SSE Stream

Connect to the SSE endpoint and receive events in real-time. This is the recommended integration path for trading bots:

```python
import httpx
import json

with httpx.stream("GET", "http://localhost:8000/events/stream") as r:
    for line in r.iter_lines():
        if line.startswith("data: "):
            event = json.loads(line[6:])
            print(event["event"], event["symbol"], event.get("change_pct"))
```

Filter by event type:

```
GET /events/stream?event_type=impulse
GET /events/stream?event_type=funding
GET /events/stream?event_type=order_eaten
```

### Webhooks

Configure webhook URLs in `config.yaml`. Each alert is POSTed as JSON:

```json
{
  "event": "impulse",
  "exchange": "bybit",
  "symbol": "BTCUSDT",
  "direction": "up",
  "change_pct": 3.45,
  "natr_value": 1.23,
  "price": 65420.5,
  "volume_24h": 52000.0,
  "timestamp": 1711400000.0
}
```

### Telegram

Set `SCREENER_TELEGRAM_TOKEN` environment variable or put the token directly in `config.yaml`. Add your chat IDs and alerts will be delivered as formatted Telegram messages.

## Architecture

```
Bybit WebSocket (5+ connections: 2 ticker+kline, 3 trade/CVD, 10 orderbook)
    |
    v
asyncio.Queue --> Engine --> Store (in-memory)
                    |              ^
                    v              | read
              Alert Queue    TUI / API
                    |
                    v
            AlertDispatcher --> Webhooks + Telegram + SSE EventBus
```

All components share a single asyncio event loop. No threads, no database — pure async Python. Data is ephemeral and resets daily at the configured hour.

## TUI Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `/` | Search/filter tickers |
| `Esc` | Clear search |
| `Enter` | Open charts for selected ticker |
| `t` | Toggle sort direction (asc/desc) |
| `e` | Sort by Exchange |
| `n` | Sort by Name |
| `d` | Sort by Trend |
| `c` | Sort by Change % |
| `v` / `V` | Sort by CVD 5m / 1h |
| `1` / `5` | Sort by Range 1m / 5m |
| `a` | Sort by NATR |
| `o` | Sort by Volume |
| `f` / `i` | Sort by Funding % / Funding countdown |
| `r` | Force refresh |
| `q` | Quit |

## Project Structure

```
screener/
  __main__.py        # CLI entry, wires everything together
  config.py          # YAML config loading with Pydantic models
  models.py          # TickerState, CandleBar, alert event dataclasses
  store.py           # In-memory state: tickers, candles, CVD buckets
  engine.py          # Message processing: NATR, trend, impulse, funding
  orderbook.py       # Orderbook state management, large order detection
  alerts.py          # Webhook + Telegram dispatch with cooldowns
  scheduler.py       # Daily reset
  tui.py             # Textual terminal UI
  charts.py          # Multi-timeframe candlestick charts + stats
  api.py             # FastAPI REST endpoints + SSE stream
  client.py          # Client mode: TUI backed by remote API
  exchanges/
    __init__.py      # BaseExchange ABC + registry
    bybit.py         # Bybit REST + multi-connection WebSocket
```

## Tech Stack

- **Python 3.13** with **uv** for dependency management
- **websockets** — native asyncio WebSocket client
- **httpx** — async HTTP client for REST calls and alert dispatch
- **Textual** — terminal UI framework
- **textual-plotext** — candlestick charts in the terminal
- **FastAPI + uvicorn** — REST API and SSE streaming
- **Pydantic** — configuration and data validation
- **Docker** — containerized deployment

## License

Personal project. Not affiliated with Bybit or any exchange.
