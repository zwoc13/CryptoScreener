from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import asdict
from time import time

import httpx

from .config import Settings
from .models import FundingAlert, ImpulseEvent, LargeOrderEvent, NewsEvent, OrderEatenEvent

logger = logging.getLogger(__name__)

AlertEvent = ImpulseEvent | FundingAlert | LargeOrderEvent | OrderEatenEvent | NewsEvent


class AlertDispatcher:
    def __init__(self, settings: Settings, event_bus=None) -> None:
        self._settings = settings
        self._cooldowns: dict[str, float] = {}  # "exchange:symbol:type" -> last_ts
        self.recent: deque[AlertEvent] = deque(maxlen=1000)
        self._event_bus = event_bus

    async def run(self, alert_queue: asyncio.Queue) -> None:
        logger.info("Alert dispatcher started")
        self._bg_tasks: set[asyncio.Task] = set()
        async with httpx.AsyncClient(timeout=10) as client:
            while True:
                event = await alert_queue.get()
                try:
                    dispatched = self._prepare_dispatch(event)
                    if dispatched is not None:
                        task = asyncio.create_task(self._send_all(client, *dispatched))
                        self._bg_tasks.add(task)
                        task.add_done_callback(self._bg_tasks.discard)
                    self._expire_old()
                except Exception:
                    logger.exception("Alert dispatch error")
                finally:
                    alert_queue.task_done()

    def _expire_old(self) -> None:
        """Remove alerts older than history_ttl_seconds."""
        ttl = self._settings.alerts.history_ttl_seconds
        cutoff = time() - ttl
        while self.recent and self.recent[-1].timestamp < cutoff:
            self.recent.pop()

    def _prepare_dispatch(self, event: AlertEvent) -> tuple[AlertEvent, str] | None:
        """Check cooldowns and record the event. Returns (event, alert_type) or None if suppressed."""
        if isinstance(event, ImpulseEvent):
            alert_type = "impulse"
        elif isinstance(event, FundingAlert):
            alert_type = "funding"
        elif isinstance(event, LargeOrderEvent):
            alert_type = "large_order"
        elif isinstance(event, OrderEatenEvent):
            alert_type = "order_eaten"
        elif isinstance(event, NewsEvent):
            alert_type = "news"
        else:
            return None

        # Check cooldown (news uses URL as unique key)
        if isinstance(event, NewsEvent):
            key = f"news:{event.url}"
        else:
            key = f"{event.exchange}:{event.symbol}:{alert_type}"
        now = time()
        if isinstance(event, ImpulseEvent):
            cooldown = self._settings.impulse.cooldown_seconds
        else:
            cooldown = self._settings.alerts.cooldown_seconds
        last = self._cooldowns.get(key, 0)
        if now - last < cooldown:
            return None

        self._cooldowns[key] = now
        self.recent.appendleft(event)

        # Publish to SSE event bus
        if self._event_bus is not None:
            self._event_bus.publish(event)

        return event, alert_type

    async def _send_all(
        self, client: httpx.AsyncClient, event: AlertEvent, alert_type: str
    ) -> None:
        """Send webhooks and Telegram notifications in parallel (runs as background task)."""
        tasks: list[asyncio.Task] = []
        for webhook in self._settings.alerts.webhooks:
            tasks.append(
                asyncio.create_task(
                    self._send_webhook(client, webhook.url, webhook.headers, event, alert_type)
                )
            )

        tg = self._settings.alerts.telegram
        if tg.bot_token and tg.chat_ids:
            tasks.append(
                asyncio.create_task(self._send_telegram(client, event, alert_type))
            )

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_webhook(
        self,
        client: httpx.AsyncClient,
        url: str,
        headers: dict[str, str],
        event: AlertEvent,
        alert_type: str,
    ) -> None:
        payload = {"event": alert_type, **asdict(event)}
        for attempt in range(3):
            try:
                resp = await client.post(url, json=payload, headers=headers)
                if resp.status_code < 400:
                    sym = getattr(event, "symbol", getattr(event, "news_type", ""))
                    logger.info("Webhook sent to %s: %s %s", url, alert_type, sym)
                    return
                logger.warning("Webhook %s returned %d", url, resp.status_code)
            except Exception as e:
                logger.warning("Webhook %s attempt %d failed: %s", url, attempt + 1, e)
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)

    async def _send_telegram(
        self,
        client: httpx.AsyncClient,
        event: AlertEvent,
        alert_type: str,
    ) -> None:
        tg = self._settings.alerts.telegram
        text = self._format_telegram(event, alert_type)

        for chat_id in tg.chat_ids:
            url = f"https://api.telegram.org/bot{tg.bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
            }
            for attempt in range(3):
                try:
                    resp = await client.post(url, json=payload)
                    if resp.status_code < 400:
                        sym = getattr(event, "symbol", getattr(event, "news_type", ""))
                        logger.info("Telegram sent to %s: %s", chat_id, sym)
                        return
                    logger.warning("Telegram %s returned %d", chat_id, resp.status_code)
                except Exception as e:
                    logger.warning("Telegram attempt %d failed: %s", attempt + 1, e)
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)

    def _format_telegram(self, event: AlertEvent, alert_type: str) -> str:
        if isinstance(event, ImpulseEvent):
            arrow = "↑" if event.direction == "up" else "↓"
            lines = [
                f"*Impulse {arrow}* `{event.exchange.upper()}`",
                f"*{event.symbol}* {'+' if event.direction == 'up' else '-'}{event.change_pct}%",
                f"Price: `{event.price}`  |  Daily: `{event.daily_change_pct:+.2f}%`",
                f"NATR: `{event.natr_value}`  |  Trend: `{event.trend}`",
                f"CVD 5m/1h/D: `{event.cvd_5m}` / `{event.cvd_1h}` / `{event.cvd_daily}`",
                f"Range 1m/5m/1h/4h: `{event.range_1m}` / `{event.range_5m}` / `{event.range_1h}` / `{event.range_4h}`",
                f"Funding: `{event.funding_rate * 100:.4f}%`",
                f"Vol 24h: `{_fmt_volume(event.volume_24h)}`",
            ]
            if event.open_interest > 0:
                lines.append(
                    f"OI: `${_fmt_volume(event.open_interest)}` ({event.oi_change_5m_pct:+.2f}% 5m)"
                )
            if event.liq_buys_5m > 0 or event.liq_sells_5m > 0:
                lines.append(
                    f"Liq 5m: L `${_fmt_volume(event.liq_buys_5m)}` / S `${_fmt_volume(event.liq_sells_5m)}`"
                )
            if event.long_short_ratio > 0:
                lines.append(f"L/S Ratio: `{event.long_short_ratio:.2f}`")
            return "\n".join(lines)
        elif isinstance(event, FundingAlert):
            return (
                f"*High Funding Rate* `{event.exchange.upper()}`\n"
                f"*{event.symbol}* rate: `{event.rate * 100:.4f}%`\n"
                f"Price: `{event.price}`"
            )
        elif isinstance(event, LargeOrderEvent):
            side_label = "BID" if event.side == "bid" else "ASK"
            return (
                f"*Large {side_label}* `{event.exchange.upper()}`\n"
                f"*{event.symbol}* `${_fmt_volume(event.size_usd)}` @ `{event.price}`\n"
                f"Distance: `{event.distance_pct:.2f}%` from price"
            )
        elif isinstance(event, OrderEatenEvent):
            side_label = "BID" if event.side == "bid" else "ASK"
            status = "FILLED" if event.likely_filled else "CANCELLED"
            return (
                f"*{side_label} Eaten* `{event.exchange.upper()}`\n"
                f"*{event.symbol}* likely {status}\n"
                f"Size: `${_fmt_volume(event.size_usd)}` @ `{event.price}`\n"
                f"Last: `{event.last_price}`"
            )
        elif isinstance(event, NewsEvent):
            label = "Delisting" if event.news_type == "delisting" else "New Listing"
            sym_lines = ""
            if event.symbols:
                pairs = [f"{s} ({f})" for s, f in zip(event.symbols, event.feed_ids)]
                sym_lines = "\n".join(pairs) + "\n"
            time_info = ""
            if event.event_ts > 0:
                from datetime import datetime, timezone
                dt = datetime.fromtimestamp(event.event_ts, tz=timezone.utc)
                time_info = f"Date: `{dt.strftime('%Y-%m-%d %H:%M')} UTC`\n"
            return (
                f"*{label}* `{event.exchange.upper()}`\n"
                f"{sym_lines}"
                f"{time_info}"
                f"{event.title}\n"
                f"[Read more]({event.url})"
            )
        return str(event)


def _fmt_volume(v: float) -> str:
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.1f}B"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.0f}M"
    if v >= 1_000:
        return f"{v / 1_000:.0f}K"
    return f"{v:.0f}"
