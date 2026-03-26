"""In-process event bus: broadcast alert events to multiple subscribers."""

from __future__ import annotations

import asyncio

from .alerts import AlertEvent


class EventBus:
    """Simple async broadcast: screener -> bots + SSE API + TUI."""

    def __init__(self) -> None:
        self._subscribers: list[asyncio.Queue] = []

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=256)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    def publish(self, event: AlertEvent) -> None:
        for q in self._subscribers:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass  # slow consumer, drop event
