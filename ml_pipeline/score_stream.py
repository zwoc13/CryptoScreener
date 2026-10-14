#!/usr/bin/env python3
"""
Real-time anomaly scoring service.
Loads the trained IF model from /mnt/models/ and scores incoming
signal snapshots from QuestDB every N seconds, firing Telegram alerts
when a symbol's anomaly score drops below the threshold.

Usage (cron or background):
  python score_stream.py              # score every 60s, alert on anomalies
  python score_stream.py --dry-run   # don't send Telegram alerts
  python score_stream.py --interval 30

Cron setup (every 5 minutes):
  */5 * * * * cd /home/yaros/work/tradesystem/screener && uv run python ml_pipeline/score_stream.py
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from threading import Event

import joblib
import numpy as np

# ── Config ───────────────────────────────────────────────────────────────────
MODEL_DIR = Path(os.getenv("MODEL_DIR", "/home/yaros/work/tradesystem/screener/ml_pipeline/models"))
MODEL_PATH = MODEL_DIR / "if_pump_detector.joblib"
SCALER_PATH = MODEL_DIR / "feature_scaler.json"
METADATA_PATH = MODEL_DIR / "model_metadata.json"

QUESTDB_URL = os.getenv("QUESTDB_URL", "http://localhost:9000/exec?query=")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
ALERT_THRESHOLD_SCORE = os.getenv("ALERT_THRESHOLD_SCORE")  # override from env

# Features (must match training)
FEATURE_COLS = [
    "cvd_5m", "cvd_1h", "cvd_4h", "cvd_daily",
    "oi_change_5m", "oi_change_1h", "oi_change_4h", "oi_change_daily",
    "buy_pct_5m", "buy_persist_5m",
    "long_bias", "short_bias",
    "liq_buy_5m", "liq_sell_5m",
    "natr", "range_1m", "range_5m", "volume_5m",
]
DERIVED_FEATURES = [
    "oi_magnitude", "vol_spike_ratio", "liq_net", "liq_total",
    "cvd_velocity", "oi_cvd_product", "bias_imbalance", "range_per_natr",
]
ALL_FEATURES = FEATURE_COLS + DERIVED_FEATURES

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("score")


# ── QuestDB ─────────────────────────────────────────────────────────────────
def q(query: str) -> list[dict]:
    url = QUESTDB_URL + urllib.parse.quote(query)
    with urllib.request.urlopen(url, timeout=30) as resp:
        data = json.loads(resp.read())
    if data.get("error"):
        raise RuntimeError(f"QuestDB error: {data['error']}")
    cols = [c["name"] for c in data["columns"]]
    return [dict(zip(cols, row)) for row in data["dataset"]]


def fetch_latest_snapshots(lookback_ms: int = 120_000) -> list[dict]:
    """Fetch the most recent snapshot for each symbol within lookback window."""
    now_ms = int(time.time() * 1000)  # QuestDB HTTP API uses milliseconds
    cutoff = now_ms - lookback_ms  # lookback_ms is already in ms
    rows = q(f"""
        SELECT
            symbol, timestamp, price,
            cvd_5m, cvd_1h, cvd_4h, cvd_daily,
            oi_change_5m, oi_change_1h, oi_change_4h, oi_change_daily,
            buy_pct_5m, buy_persist_5m,
            long_bias, short_bias,
            liq_buy_5m, liq_sell_5m,
            natr, trend, range_1m, range_5m,
            volume_5m
        FROM signal_snapshots
        WHERE timestamp >= {cutoff}
        LATEST ON timestamp PARTITION BY symbol
    """)
    return rows


# ── Derived features (must match training) ───────────────────────────────────
def compute_derived_features(df: dict) -> dict:
    df = dict(df)  # don't mutate original
    df["oi_magnitude"] = abs(df.get("oi_change_5m", 0))
    vol_24h = df.get("volume_24h", 0)
    vol_5m = df.get("volume_5m", 0)
    if vol_24h > 0 and vol_5m > 0:
        expected_5m = vol_24h / 288
        df["vol_spike_ratio"] = vol_5m / expected_5m if expected_5m > 0 else 1.0
    else:
        df["vol_spike_ratio"] = 1.0
    liq_buy = df.get("liq_buy_5m", 0)
    liq_sell = df.get("liq_sell_5m", 0)
    df["liq_net"] = liq_buy - liq_sell
    df["liq_total"] = liq_buy + liq_sell
    cvd_5m = df.get("cvd_5m", 0)
    cvd_1h = df.get("cvd_1h", 0)
    df["cvd_velocity"] = cvd_5m - cvd_1h / 12
    df["oi_cvd_product"] = df.get("oi_change_5m", 0) * cvd_5m
    long_b = df.get("long_bias", 50)
    short_b = df.get("short_bias", 50)
    df["bias_imbalance"] = (long_b - short_b) / 100
    natr = df.get("natr", 0)
    range_5m = df.get("range_5m", 0)
    df["range_per_natr"] = range_5m / natr if natr > 0 else 0
    return df


def row_to_vector(row: dict) -> np.ndarray:
    return np.array([row.get(f, 0.0) or 0.0 for f in ALL_FEATURES], dtype=np.float32)


# ── Telegram ────────────────────────────────────────────────────────────────
def send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set — skipping alert")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        log.debug("Telegram response: %s", r.read())


# ── Alert formatters ────────────────────────────────────────────────────────
def score_emoji(score: float, threshold: float) -> str:
    deficit = threshold - score
    if deficit > 0.5:
        return "🚨 ANOMALY"
    elif deficit > 0.2:
        return "⚠️  WARNING"
    return "🔔 ELEVATED"


def format_anomaly_alert(symbol: str, score: float, threshold: float,
                         row: dict, metadata: dict) -> str:
    ts = datetime.fromtimestamp(row["timestamp"] / 1000, tz=timezone.utc)
    emoji = score_emoji(score, threshold)
    bias = row.get("bias_label", "NEUTRAL")
    direction = "📈 PUMP" if row.get("cvd_5m", 0) > 0 else "📉 DUMP"
    return (
        f"{emoji} *Pump/Dump Alert*\n"
        f"Symbol: *{symbol}*\n"
        f"Time: {ts.strftime('%H:%M:%S UTC')}\n"
        f"Anomaly Score: `{score:.4f}` (threshold: `{threshold:.4f}`)\n"
        f"Signal: {direction} | Bias: {bias}\n"
        f"CVD 5m: `{row.get('cvd_5m', 0):+.0f}`\n"
        f"OI 5m: `{row.get('oi_change_5m', 0):+.2f}%`\n"
        f"Range 5m: `{row.get('range_5m', 0):.4f}` | NATR: `{row.get('natr', 0):.4f}`\n"
        f"LiQ Net: `${row.get('liq_net', 0):+.0f}` | Vol Spike: `{row.get('vol_spike_ratio', 1.0):.1f}x`\n"
        f"Model trained: {metadata.get('trained_at', 'unknown')[:10]}"
    )


# ── Cooldown tracking ────────────────────────────────────────────────────────
class CooldownTracker:
    """Prevent alert spam for the same symbol."""
    def __init__(self, cooldown_seconds: int = 300):
        self._cooldown = cooldown_seconds
        self._last_alert: dict[str, float] = {}

    def should_alert(self, symbol: str) -> bool:
        now = time.time()
        last = self._last_alert.get(symbol, 0)
        if now - last < self._cooldown:
            return False
        self._last_alert[symbol] = now
        return True


# ── Main scoring loop ────────────────────────────────────────────────────────
def score_loop(
    interval: int = 60,
    cooldown: int = 300,
    dry_run: bool = False,
    min_score: float | None = None,
) -> None:
    log.info("Loading model from %s", MODEL_PATH)
    clf = joblib.load(MODEL_PATH)
    log.info("Model loaded OK")

    with open(METADATA_PATH) as f:
        metadata = json.load(f)
    threshold = min_score or metadata.get("score_threshold", -0.1)
    log.info("Score threshold: %.4f", threshold)
    log.info("Features: %d", len(ALL_FEATURES))

    tracker = CooldownTracker(cooldown)
    stop_event = Event()

    log.info("Starting scoring loop (interval=%ds, cooldown=%ds)", interval, cooldown)

    while not stop_event.is_set():
        try:
            rows = fetch_latest_snapshots(lookback_ms=interval * 2000)
        except Exception as e:
            log.error("Failed to fetch snapshots: %s", e)
            time.sleep(interval)
            continue

        if not rows:
            log.debug("No snapshots in latest window")
            time.sleep(interval)
            continue

        alerts_sent = 0
        for row in rows:
            try:
                enriched = compute_derived_features(row)
                vec = row_to_vector(enriched)
                score = float(clf.score_samples(vec.reshape(1, -1))[0])
                row["anomaly_score"] = score

                if score < threshold:
                    symbol = row["symbol"]
                    if tracker.should_alert(symbol):
                        log.info(
                            "🚨 ANOMALY: %s score=%.4f < threshold=%.4f",
                            symbol, score, threshold,
                        )
                        if not dry_run:
                            msg = format_anomaly_alert(symbol, score, threshold, row, metadata)
                            send_telegram(msg)
                        alerts_sent += 1
            except Exception as e:
                log.warning("Scoring error for %s: %s", row.get("symbol", "?"), e)

        log.info(
            "Scored %d symbols | alerts fired: %d | next in %ds",
            len(rows), alerts_sent, interval,
        )
        time.sleep(interval)


# ── Entry point ──────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Real-time pump/dump scoring")
    parser.add_argument("--interval", type=int, default=60,
                        help="Scoring interval in seconds (default: 60)")
    parser.add_argument("--cooldown", type=int, default=300,
                        help="Alert cooldown per symbol in seconds (default: 300)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Score but don't send Telegram alerts")
    parser.add_argument("--threshold", type=float, default=None,
                        help="Override score threshold")
    args = parser.parse_args()

    if not MODEL_PATH.exists():
        log.error("Model not found at %s — run train_model.py first", MODEL_PATH)
        sys.exit(1)

    if args.dry_run:
        log.info("DRY RUN — Telegram alerts disabled")

    score_loop(
        interval=args.interval,
        cooldown=args.cooldown,
        dry_run=args.dry_run,
        min_score=args.threshold,
    )


if __name__ == "__main__":
    main()
