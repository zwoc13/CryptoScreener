"""Lightweight QuestDB query client for reading historical aggregates.

Used as a fallback when the in-memory tiered buffers don't cover the
requested window (>4h).  Communicates with QuestDB's REST SQL endpoint
(default http://host:9000/exec).
"""

from __future__ import annotations

import logging
from time import time

import httpx

from .config import QuestDBConfig

logger = logging.getLogger(__name__)

# Cache entries expire after this many seconds.
_CACHE_TTL = 60


class QuestDBReader:
    def __init__(self, config: QuestDBConfig) -> None:
        self._base_url = f"http://{config.host}:9000"
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=5)
        self._healthy = True
        self._cache: dict[str, tuple[float, float]] = {}  # key → (ts, value)

    async def query_cvd(self, exchange: str, symbol: str,
                        window_seconds: int) -> float | None:
        """Sum of signed trade deltas over the given window from QuestDB.

        Returns None if QuestDB is unavailable or data is missing.
        """
        cache_key = f"cvd:{exchange}:{symbol}:{window_seconds}"
        cached = self._cache.get(cache_key)
        if cached and time() - cached[0] < _CACHE_TTL:
            return cached[1]

        sql = (
            f"SELECT sum(CASE WHEN side='Buy' THEN usd_size ELSE -usd_size END) AS cvd "
            f"FROM trades "
            f"WHERE exchange = '{exchange}' AND symbol = '{symbol}' "
            f"AND timestamp > dateadd('s', -{window_seconds}, now())"
        )
        val = await self._exec_scalar(sql)
        if val is not None:
            self._cache[cache_key] = (time(), val)
        return val

    async def query_oi_at(self, exchange: str, symbol: str,
                          seconds_ago: int) -> float | None:
        """OI value closest to *seconds_ago* from QuestDB.

        Returns None if QuestDB is unavailable.
        """
        cache_key = f"oi:{exchange}:{symbol}:{seconds_ago}"
        cached = self._cache.get(cache_key)
        if cached and time() - cached[0] < _CACHE_TTL:
            return cached[1]

        sql = (
            f"SELECT oi_usd FROM oi_snapshots "
            f"WHERE exchange = '{exchange}' AND symbol = '{symbol}' "
            f"AND timestamp <= dateadd('s', -{seconds_ago}, now()) "
            f"ORDER BY timestamp DESC LIMIT 1"
        )
        val = await self._exec_scalar(sql)
        if val is not None:
            self._cache[cache_key] = (time(), val)
        return val

    async def _exec_scalar(self, sql: str) -> float | None:
        if not self._healthy:
            return None
        try:
            resp = await self._client.get("/exec", params={"query": sql})
            resp.raise_for_status()
            data = resp.json()
            rows = data.get("dataset", [])
            if rows and rows[0][0] is not None:
                return float(rows[0][0])
            return None
        except httpx.HTTPError:
            logger.warning("QuestDB query failed, marking unhealthy for %ds", _CACHE_TTL)
            self._healthy = False
            # Schedule recovery
            import asyncio
            asyncio.get_event_loop().call_later(_CACHE_TTL, self._mark_healthy)
            return None
        except Exception:
            logger.exception("QuestDB query unexpected error")
            return None

    def _mark_healthy(self) -> None:
        self._healthy = True
        logger.info("QuestDB reader marked healthy again")

    async def close(self) -> None:
        await self._client.aclose()
