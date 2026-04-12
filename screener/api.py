from __future__ import annotations

import asyncio
import json
from collections import deque
from dataclasses import asdict
from time import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse

from .alerts import AlertEvent
from .bus import EventBus
from .store import Store


def _ticker_dict(store: Store, t) -> dict:
    return {
        "exchange": t.exchange,
        "symbol": t.symbol,
        "feed_id": t.feed_id,
        "trend": t.trend,
        "daily_change_pct": round(t.daily_change_pct, 2),
        "cvd_5m": round(store.get_cvd_rolling(t.exchange, t.symbol, 300), 2),
        "cvd_1h": round(store.get_cvd_rolling(t.exchange, t.symbol, 3600), 2),
        "cvd_daily": round(store.get_cvd_daily(t.exchange, t.symbol), 2),
        "range_1m": t.range_1m,
        "range_5m": t.range_5m,
        "range_1h": t.range_1h,
        "range_4h": t.range_4h,
        "natr_5m_14": round(t.natr_5m_14, 4),
        "volume_24h": t.volume_24h,
        "funding_rate": t.funding_rate,
        "funding_interval_h": t.funding_interval_h,
        "next_funding_ts": t.next_funding_ts,
        "last_price": t.last_price,
        "last_update_ts": t.last_update_ts,
        "open_interest": t.open_interest,
        "oi_change_5m_pct": round(t.oi_change_5m_pct, 2),
        "long_short_ratio": t.long_short_ratio,
        "delist_ts": t.delist_ts,
    }


def create_api(
    store: Store,
    alert_history: deque[AlertEvent],
    start_time: float,
    event_bus: EventBus | None = None,
    exchanges: dict | None = None,
) -> FastAPI:
    app = FastAPI(title="Bybit Screener API", version="0.1.0")
    _exchanges = exchanges or {}

    @app.get("/tickers")
    def get_tickers(
        exchange: str | None = None,
        min_change: float | None = None,
        limit: int = 200,
    ):
        tickers = store.get_all_sorted()
        if exchange:
            tickers = [t for t in tickers if t.exchange == exchange.lower()]
        if min_change is not None:
            tickers = [t for t in tickers if abs(t.daily_change_pct) >= min_change]
        tickers = tickers[:limit]
        return [_ticker_dict(store, t) for t in tickers]

    @app.get("/tickers/{symbol}")
    def get_ticker(symbol: str, exchange: str = "bybit"):
        ticker = store.get_ticker(exchange.lower(), symbol.upper())
        if not ticker:
            return JSONResponse(status_code=404, content={"error": "Symbol not found"})
        return _ticker_dict(store, ticker)

    @app.get("/alerts/recent")
    def get_recent_alerts(limit: int = 50):
        alerts = list(alert_history)[:limit]
        return [asdict(a) for a in alerts]

    @app.get("/klines/{symbol}")
    async def get_klines(
        symbol: str,
        exchange: str = "bybit",
        interval: str = "5",
        limit: int = 60,
    ):
        ex = _exchanges.get(exchange.lower())
        if not ex:
            return JSONResponse(status_code=404, content={"error": "Exchange not found"})
        candles = await ex.fetch_klines(symbol.upper(), interval, limit)
        return [
            {
                "timestamp": c.timestamp,
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
            }
            for c in candles
        ]

    @app.get("/events/stream")
    async def event_stream(event_type: str | None = None):
        """SSE endpoint. Bots connect here to receive real-time events.

        Optional filter: ?event_type=impulse|funding|large_order|order_eaten
        """
        if event_bus is None:
            return JSONResponse(status_code=503, content={"error": "Event bus not available"})

        q = event_bus.subscribe()

        async def generate():
            try:
                while True:
                    event = await q.get()
                    # Determine event type name
                    from .models import (
                        BiasChangedEvent, CvdBurstEvent, DivergenceEvent,
                        FundingAlert, ImpulseEvent, LargeOrderEvent,
                        LiquidationCascadeEvent, NewsEvent, OiFlipEvent,
                        OrderEatenEvent, TrendChangedEvent, VolumeSpikeEvent,
                    )
                    if isinstance(event, ImpulseEvent):
                        etype = "impulse"
                    elif isinstance(event, FundingAlert):
                        etype = "funding"
                    elif isinstance(event, LargeOrderEvent):
                        etype = "large_order"
                    elif isinstance(event, OrderEatenEvent):
                        etype = "order_eaten"
                    elif isinstance(event, NewsEvent):
                        etype = "news"
                    elif isinstance(event, BiasChangedEvent):
                        etype = "bias_changed"
                    elif isinstance(event, CvdBurstEvent):
                        etype = "cvd_burst"
                    elif isinstance(event, LiquidationCascadeEvent):
                        etype = "liq_cascade"
                    elif isinstance(event, OiFlipEvent):
                        etype = "oi_flip"
                    elif isinstance(event, DivergenceEvent):
                        etype = "divergence"
                    elif isinstance(event, VolumeSpikeEvent):
                        etype = "volume_spike"
                    elif isinstance(event, TrendChangedEvent):
                        etype = "trend_changed"
                    else:
                        etype = "unknown"

                    # Apply filter if specified
                    if event_type and etype != event_type:
                        continue

                    payload = {"event": etype, **asdict(event)}
                    yield f"event: {etype}\ndata: {json.dumps(payload)}\n\n"
            except asyncio.CancelledError:
                pass
            finally:
                event_bus.unsubscribe(q)

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.post("/snapshot")
    def save_snapshot():
        """Save full store state to disk for restore after restart."""
        path = store.snapshot_save()
        return {"status": "ok", "path": path}

    @app.get("/health")
    def health():
        last = store.last_message_ts
        return {
            "status": "ok",
            "symbols": store.symbol_count,
            "last_message_ago_s": round(time() - last, 1) if last > 0 else None,
            "uptime_s": round(time() - start_time, 1),
        }

    return app
