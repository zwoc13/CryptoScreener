from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .config import Settings
from .store import Store

logger = logging.getLogger(__name__)


async def run_daily_reset(store: Store, settings: Settings) -> None:
    tz = ZoneInfo(settings.schedule.timezone)
    logger.info(
        "Scheduler: daily reset at %02d:%02d %s",
        settings.schedule.reset_hour,
        settings.schedule.reset_minute,
        settings.schedule.timezone,
    )

    while True:
        now = datetime.now(tz)
        target = now.replace(
            hour=settings.schedule.reset_hour,
            minute=settings.schedule.reset_minute,
            second=0,
            microsecond=0,
        )
        if target <= now:
            target += timedelta(days=1)

        wait_seconds = (target - now).total_seconds()
        logger.info(
            "Scheduler: next reset in %.0f seconds (%s)",
            wait_seconds, target.isoformat(),
        )
        await asyncio.sleep(wait_seconds)

        store.reset_daily()
        logger.info("Scheduler: daily reset completed at %s", datetime.now(tz).isoformat())
