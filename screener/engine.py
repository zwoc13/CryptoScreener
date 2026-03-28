from __future__ import annotations

import asyncio
import logging
from collections import deque
from time import time

from .config import Settings
from .models import (
    CandleBar,
    FundingAlert,
    ImpulseEvent,
    KlineMessage,
    LiquidationMessage,
    LongShortRatioMessage,
    OpenInterestMessage,
    TickerMessage,
    TickerState,
    TradeMessage,
)
from .store import Store

logger = logging.getLogger(__name__)


class Engine:
    def __init__(
        self,
        store: Store,
        alert_queue: asyncio.Queue,
        settings: Settings,
    ) -> None:
        self._store = store
        self._alert_queue = alert_queue
        self._settings = settings
        # Per-symbol sliding window of recent prices for impulse detection
        self._price_history: dict[str, deque[tuple[float, float]]] = {}
        # Per-symbol impulse cooldown: key -> timestamp of last impulse alert
        self._impulse_cooldown: dict[str, float] = {}
        # Pending impulses awaiting confirmation: key -> (detected_at, ref_price, direction)
        self._pending_impulses: dict[str, tuple[float, float, str]] = {}
        # Track last funding alert timestamp per symbol
        self._last_funding_alert_ts: dict[str, float] = {}

    async def run(self, msg_queue: asyncio.Queue) -> None:
        logger.info("Engine started")
        while True:
            msg = await msg_queue.get()
            try:
                if isinstance(msg, TickerMessage):
                    self._handle_ticker(msg)
                elif isinstance(msg, KlineMessage):
                    self._handle_kline(msg)
                elif isinstance(msg, TradeMessage):
                    self._handle_trade(msg)
                elif isinstance(msg, OpenInterestMessage):
                    self._handle_open_interest(msg)
                elif isinstance(msg, LiquidationMessage):
                    self._handle_liquidation(msg)
                elif isinstance(msg, LongShortRatioMessage):
                    self._handle_long_short_ratio(msg)
            except Exception:
                logger.exception("Engine error processing message")
            finally:
                msg_queue.task_done()

    def _handle_ticker(self, msg: TickerMessage) -> None:
        ticker = self._store.get_or_create_ticker(msg.exchange, msg.symbol)
        self._store.touch()
        now = time()

        old_price = ticker.last_price

        if msg.last_price is not None:
            ticker.last_price = msg.last_price
        if msg.volume_24h is not None:
            ticker.volume_24h = msg.volume_24h
        if msg.funding_rate is not None:
            ticker.funding_rate = msg.funding_rate
        if msg.funding_interval_h is not None:
            ticker.funding_interval_h = msg.funding_interval_h
        if msg.next_funding_ts is not None:
            ticker.next_funding_ts = msg.next_funding_ts
        if msg.open_interest_value is not None:
            oi_usd = msg.open_interest_value
            self._store.update_open_interest(msg.exchange, msg.symbol, oi_usd, now)
            ticker.open_interest = oi_usd
            ticker.oi_change_5m_pct = self._store.get_oi_change_5m_pct(msg.exchange, msg.symbol)
        elif msg.open_interest is not None and ticker.last_price > 0:
            # Exchange sent OI in contracts only — convert to USD using current price
            oi_usd = msg.open_interest * ticker.last_price
            self._store.update_open_interest(msg.exchange, msg.symbol, oi_usd, now)
            ticker.open_interest = oi_usd
            ticker.oi_change_5m_pct = self._store.get_oi_change_5m_pct(msg.exchange, msg.symbol)
        ticker.last_update_ts = now

        price = ticker.last_price
        if price <= 0:
            return

        # Daily change from reset price
        if ticker.reset_price > 0:
            ticker.daily_change_pct = ((price - ticker.reset_price) / ticker.reset_price) * 100
        elif ticker.reset_price == 0:
            ticker.reset_price = price

        # 1m range tracking
        self._update_1m_range(ticker, price, now)

        # Skip signal generation for tickers delisting within the configured window
        delist_ts = ticker.delist_ts
        if delist_ts > 0:
            delist_cutoff = self._settings.filters.delist_filter_days * 86_400
            if delist_ts - now <= delist_cutoff:
                return

        # Impulse detection
        if old_price > 0 and price != old_price:
            self._check_impulse(ticker, old_price, price, now)

        # Funding rate alert
        if msg.funding_rate is not None:
            self._check_funding(ticker)

    def _handle_kline(self, msg: KlineMessage) -> None:
        self._store.touch()

        if msg.interval == "5":
            self._store.add_candle(msg.exchange, msg.symbol, msg.candle)
            ticker = self._store.get_or_create_ticker(msg.exchange, msg.symbol)

            # Update range_5m from latest candle
            ticker.range_5m = msg.candle.high - msg.candle.low

            # Recompute NATR, trend, and ranges when candle is confirmed
            if msg.candle.confirmed:
                natr = self._compute_natr(msg.exchange, msg.symbol)
                if natr is not None:
                    ticker.natr_5m_14 = natr
                ticker.trend = self._compute_trend(msg.exchange, msg.symbol)
                ticker.range_1h = self._store.get_range(msg.exchange, msg.symbol, 12)
                ticker.range_4h = self._store.get_range(msg.exchange, msg.symbol, 48)

    def _handle_trade(self, msg: TradeMessage) -> None:
        self._store.touch()
        usd_size = msg.size * msg.price
        delta = usd_size if msg.side == "Buy" else -usd_size
        self._store.add_trade_delta(msg.exchange, msg.symbol, delta, usd_size, msg.timestamp)

    def _handle_open_interest(self, msg: OpenInterestMessage) -> None:
        self._store.touch()
        ticker = self._store.get_or_create_ticker(msg.exchange, msg.symbol)
        if msg.open_interest_value > 0:
            oi_usd = msg.open_interest_value
        elif msg.open_interest > 0 and ticker.last_price > 0:
            # Exchange returned OI in contracts — convert to USD using current price
            oi_usd = msg.open_interest * ticker.last_price
        else:
            return
        self._store.update_open_interest(msg.exchange, msg.symbol, oi_usd, msg.timestamp)
        ticker.open_interest = oi_usd
        ticker.oi_change_5m_pct = self._store.get_oi_change_5m_pct(msg.exchange, msg.symbol)

    def _handle_liquidation(self, msg: LiquidationMessage) -> None:
        self._store.touch()
        usd_vol = msg.size * msg.price
        self._store.add_liquidation(msg.exchange, msg.symbol, msg.side, usd_vol, msg.timestamp)

    def _handle_long_short_ratio(self, msg: LongShortRatioMessage) -> None:
        self._store.touch()
        ticker = self._store.get_or_create_ticker(msg.exchange, msg.symbol)
        ticker.long_short_ratio = msg.long_short_ratio

    def _update_1m_range(self, ticker: TickerState, price: float, now: float) -> None:
        if now - ticker.minute_start_ts >= 60:
            if ticker.minute_high > 0 and ticker.minute_low < float("inf"):
                ticker.range_1m = ticker.minute_high - ticker.minute_low
            ticker.minute_high = price
            ticker.minute_low = price
            ticker.minute_start_ts = now
        else:
            if price > ticker.minute_high:
                ticker.minute_high = price
            if price < ticker.minute_low:
                ticker.minute_low = price
            ticker.range_1m = ticker.minute_high - ticker.minute_low

    def _compute_natr(self, exchange: str, symbol: str) -> float | None:
        candles = self._store.get_candles(exchange, symbol)
        period = self._settings.natr.period
        if len(candles) < period + 1:
            return None

        recent = list(candles)[-period - 1 :]
        tr_values: list[float] = []
        for i in range(1, len(recent)):
            prev_close = recent[i - 1].close
            cur = recent[i]
            tr = max(
                cur.high - cur.low,
                abs(cur.high - prev_close),
                abs(cur.low - prev_close),
            )
            tr_values.append(tr)

        atr = sum(tr_values) / len(tr_values)
        close = recent[-1].close
        if close <= 0:
            return None
        return (atr / close) * 100

    def _compute_trend(self, exchange: str, symbol: str) -> str:
        """Detect trend from 5m candles using higher-highs/lower-lows."""
        candles = self._store.get_candles(exchange, symbol)
        if len(candles) < 6:
            return "-"

        # Use last 6 confirmed candles for trend detection
        recent = [c for c in candles if c.confirmed][-6:]
        if len(recent) < 4:
            return "-"

        # Count higher-highs and lower-lows
        higher_highs = 0
        lower_lows = 0
        higher_lows = 0
        lower_highs = 0

        for i in range(1, len(recent)):
            if recent[i].high > recent[i - 1].high:
                higher_highs += 1
            else:
                lower_highs += 1
            if recent[i].low > recent[i - 1].low:
                higher_lows += 1
            else:
                lower_lows += 1

        n = len(recent) - 1
        # Uptrend: majority higher-highs AND higher-lows
        if higher_highs >= n * 0.6 and higher_lows >= n * 0.6:
            return "UP"
        # Downtrend: majority lower-highs AND lower-lows
        if lower_highs >= n * 0.6 and lower_lows >= n * 0.6:
            return "DOWN"
        return "RANGE"

    def _fire_impulse(
        self, ticker: TickerState, new_price: float, change_pct: float, direction: str, now: float
    ) -> None:
        self._impulse_cooldown[key := f"{ticker.exchange}:{ticker.symbol}"] = now
        liq_buys, liq_sells = self._store.get_liq_rolling(ticker.exchange, ticker.symbol, 300)
        event = ImpulseEvent(
            exchange=ticker.exchange,
            symbol=ticker.symbol,
            feed_id=ticker.feed_id,
            direction=direction,
            change_pct=round(change_pct, 2),
            natr_value=round(ticker.natr_5m_14, 4),
            price=new_price,
            volume_24h=ticker.volume_24h,
            cvd_5m=round(self._store.get_cvd_rolling(ticker.exchange, ticker.symbol, 300), 2),
            cvd_1h=round(self._store.get_cvd_rolling(ticker.exchange, ticker.symbol, 3600), 2),
            cvd_daily=round(self._store.get_cvd_daily(ticker.exchange, ticker.symbol), 2),
            funding_rate=ticker.funding_rate,
            trend=ticker.trend,
            daily_change_pct=round(ticker.daily_change_pct, 2),
            range_1m=ticker.range_1m,
            range_5m=ticker.range_5m,
            range_1h=ticker.range_1h,
            range_4h=ticker.range_4h,
            open_interest=ticker.open_interest,
            oi_change_5m_pct=round(ticker.oi_change_5m_pct, 2),
            liq_buys_5m=round(liq_buys, 2),
            liq_sells_5m=round(liq_sells, 2),
            long_short_ratio=ticker.long_short_ratio,
        )
        self._alert_queue.put_nowait(event)
        logger.info(
            "Impulse: %s %s %s %.2f%% (NATR: %.4f)",
            ticker.exchange, ticker.symbol, direction, change_pct, ticker.natr_5m_14,
        )

    def _check_impulse(
        self, ticker: TickerState, old_price: float, new_price: float, now: float
    ) -> None:
        key = f"{ticker.exchange}:{ticker.symbol}"

        # Engine-level cooldown: don't check again for N seconds after firing
        last_impulse = self._impulse_cooldown.get(key, 0)
        if now - last_impulse < self._settings.impulse.cooldown_seconds:
            self._pending_impulses.pop(key, None)
            return

        confirmation_seconds = self._settings.impulse.confirmation_seconds

        # If there is a pending impulse awaiting confirmation, check it first
        if key in self._pending_impulses:
            detected_at, ref_price, pending_direction = self._pending_impulses[key]
            elapsed = now - detected_at
            # Recalculate move relative to the original reference price
            current_change = (new_price - ref_price) / ref_price * 100
            current_direction = "up" if current_change > 0 else "down"
            if current_direction != pending_direction:
                # Move reversed — discard without firing
                logger.debug("Impulse reversed before confirmation: %s", key)
                del self._pending_impulses[key]
            elif elapsed >= confirmation_seconds:
                # Move held through the confirmation window — fire
                del self._pending_impulses[key]
                history = self._price_history.get(key)
                if history is not None:
                    history.clear()
                    history.append((now, new_price))
                self._fire_impulse(ticker, new_price, abs(current_change), current_direction, now)
            # else: still within confirmation window, keep waiting
            return

        history = self._price_history.setdefault(key, deque(maxlen=120))
        history.append((now, new_price))

        cutoff = now - 60
        while history and history[0][0] < cutoff:
            history.popleft()

        if len(history) < 2:
            return

        oldest_price = history[0][1]
        if oldest_price <= 0:
            return

        change_pct = abs((new_price - oldest_price) / oldest_price) * 100
        direction = "up" if new_price > oldest_price else "down"

        threshold = self._settings.impulse.threshold_pct
        natr_mult = self._settings.impulse.natr_multiplier
        combine = self._settings.impulse.combine_mode

        static_triggered = change_pct >= threshold
        natr_triggered = False
        if ticker.natr_5m_14 > 0:
            natr_triggered = change_pct >= (natr_mult * ticker.natr_5m_14)

        triggered = False
        if combine == "or":
            triggered = static_triggered or natr_triggered
        elif combine == "and":
            triggered = static_triggered and natr_triggered

        if triggered:
            if confirmation_seconds > 0:
                # Park as pending — fire only after move holds for confirmation_seconds
                self._pending_impulses[key] = (now, oldest_price, direction)
                logger.debug(
                    "Impulse pending confirmation: %s %s %.2f%% (wait %ds)",
                    key, direction, change_pct, confirmation_seconds,
                )
            else:
                # Fire immediately (original behaviour)
                history.clear()
                history.append((now, new_price))
                self._fire_impulse(ticker, new_price, change_pct, direction, now)

    def _check_funding(self, ticker: TickerState) -> None:
        threshold = self._settings.funding.alert_threshold
        rate = ticker.funding_rate
        if abs(rate) < threshold:
            return

        key = f"{ticker.exchange}:{ticker.symbol}"
        now = time()
        repeat_interval = self._settings.funding.repeat_interval_s
        last_ts = self._last_funding_alert_ts.get(key, 0.0)
        if now - last_ts < repeat_interval:
            return
        self._last_funding_alert_ts[key] = now

        alert = FundingAlert(
            exchange=ticker.exchange,
            symbol=ticker.symbol,
            feed_id=ticker.feed_id,
            rate=rate,
            price=ticker.last_price,
        )
        self._alert_queue.put_nowait(alert)
        logger.info(
            "Funding alert: %s %s rate=%.4f%%",
            ticker.exchange, ticker.symbol, rate * 100,
        )
