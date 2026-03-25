from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import asdict
from time import time

import httpx

from .config import Settings
from .models import FundingAlert, ImpulseEvent, LargeOrderEvent, OrderEatenEvent

logger = logging.getLogger(__name__)

AlertEvent = ImpulseEvent | FundingAlert | LargeOrderEvent | OrderEatenEvent


class AlertDispatcher:
    def __init__(self, settings: Settings, event_bus=None) -> None:
        self._settings = settings
        self._cooldowns: dict[str, float] = {}  # "exchange:symbol:type" -> last_ts
        self.recent: deque[AlertEvent] = deque(maxlen=1000)
        self._event_bus = event_bus

    async def run(self, alert_queue: asyncio.Queue) -> None:
        logger.info("Alert dispatcher started")
        async with httpx.AsyncClient(timeout=10) as client:
            while True:
                event = await alert_queue.get()
                try:
                    await self._dispatch(event, client)
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

    async def _dispatch(self, event: AlertEvent, client: httpx.AsyncClient) -> None:
        if isinstance(event, ImpulseEvent):
            alert_type = "impulse"
        elif isinstance(event, FundingAlert):
            alert_type = "funding"
        elif isinstance(event, LargeOrderEvent):
            alert_type = "large_order"
        elif isinstance(event, OrderEatenEvent):
            alert_type = "order_eaten"
        else:
            return

        # Check cooldown
        key = f"{event.exchange}:{event.symbol}:{alert_type}"
        now = time()
        cooldown = self._settings.alerts.cooldown_seconds
        last = self._cooldowns.get(key, 0)
        if now - last < cooldown:
            return

        self._cooldowns[key] = now
        self.recent.appendleft(event)

        # Publish to SSE event bus
        if self._event_bus is not None:
            self._event_bus.publish(event)

        # Dispatch in parallel
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
                    logger.info("Webhook sent to %s: %s %s", url, alert_type, event.symbol)
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
                        logger.info("Telegram sent to %s: %s", chat_id, event.symbol)
                        return
                    logger.warning("Telegram %s returned %d", chat_id, resp.status_code)
                except Exception as e:
                    logger.warning("Telegram attempt %d failed: %s", attempt + 1, e)
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)

    def _format_telegram(self, event: AlertEvent, alert_type: str) -> str:
        if isinstance(event, ImpulseEvent):
            arrow = "↑" if event.direction == "up" else "↓"
            return (
                f"*Impulse {arrow}* `{event.exchange.upper()}`\n"
                f"*{event.symbol}* {'+' if event.direction == 'up' else '-'}{event.change_pct}%\n"
                f"Price: `{event.price}`\n"
                f"NATR: `{event.natr_value}`\n"
                f"Vol 24h: `{_fmt_volume(event.volume_24h)}`"
            )
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
        return str(event)


def _fmt_volume(v: float) -> str:
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.1f}B"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.0f}M"
    if v >= 1_000:
        return f"{v / 1_000:.0f}K"
    return f"{v:.0f}"
