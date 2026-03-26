from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class ExchangeConfig(BaseModel):
    enabled: bool = True
    ws_url: str = ""
    rest_url: str = ""
    ws_connections: int = 2


class ImpulseConfig(BaseModel):
    threshold_pct: float = 3.0
    natr_multiplier: float = 2.5
    combine_mode: str = "or"  # "or" | "and"


class NatrConfig(BaseModel):
    period: int = 14
    interval: str = "5"


class FundingConfig(BaseModel):
    alert_threshold: float = 0.01


class WebhookTarget(BaseModel):
    url: str
    headers: dict[str, str] = Field(default_factory=dict)


class TelegramConfig(BaseModel):
    bot_token: str = ""
    chat_ids: list[str] = Field(default_factory=list)


class AlertsConfig(BaseModel):
    cooldown_seconds: int = 300
    history_ttl_seconds: int = 3600  # drop alerts older than 1 hour
    webhooks: list[WebhookTarget] = Field(default_factory=list)
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)


class ScheduleConfig(BaseModel):
    reset_hour: int = 2
    reset_minute: int = 0
    timezone: str = "Europe/Kyiv"


class TuiConfig(BaseModel):
    refresh_ms: int = 750
    max_rows: int = 100


class OrderbookConfig(BaseModel):
    enabled: bool = True
    depth: int = 50
    threshold_pct: float = 0.1  # order size as % of (24h_volume * price)
    min_size_usd: float = 50_000  # absolute minimum USD to qualify as "large"
    max_distance_pct: float = 2.0  # ignore orders further than 2% from price
    alert_on_new: bool = False
    alert_on_eaten: bool = True
    ws_connections: int = 10


class DataStreamsConfig(BaseModel):
    oi_enabled: bool = True
    liquidations_enabled: bool = True
    long_short_ratio_enabled: bool = True
    ls_ratio_poll_interval_s: int = 300  # 5 minutes
    oi_poll_interval_s: int = 60  # for Binance REST polling


class ApiConfig(BaseModel):
    port: int = 8000
    host: str = "0.0.0.0"


class Settings(BaseModel):
    exchanges: dict[str, ExchangeConfig] = Field(default_factory=dict)
    impulse: ImpulseConfig = Field(default_factory=ImpulseConfig)
    natr: NatrConfig = Field(default_factory=NatrConfig)
    funding: FundingConfig = Field(default_factory=FundingConfig)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)
    tui: TuiConfig = Field(default_factory=TuiConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)
    orderbook: OrderbookConfig = Field(default_factory=OrderbookConfig)
    data_streams: DataStreamsConfig = Field(default_factory=DataStreamsConfig)


def load_settings(path: str | Path = "config.yaml") -> Settings:
    p = Path(path)
    if p.exists():
        with open(p) as f:
            raw = yaml.safe_load(f) or {}
    else:
        raw = {}

    # Env var overrides for secrets
    tg_token = os.environ.get("SCREENER_TELEGRAM_TOKEN")
    if tg_token:
        raw.setdefault("alerts", {}).setdefault("telegram", {})["bot_token"] = tg_token

    return Settings(**raw)
