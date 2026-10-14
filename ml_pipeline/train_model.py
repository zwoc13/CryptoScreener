#!/usr/bin/env python3
"""
Pump/Dump ML Training Pipeline
=============================
Pulls feature snapshots from QuestDB via psql, trains an Isolation Forest,
exports it for real-time scoring.

Architecture:
  QuestDB (psql:8812, microsecond timestamps)
         ↓
  Feature Engineering (add derived features)
         ↓
  Isolation Forest (unsupervised anomaly detection)
         ↓
  Trained model → ml_pipeline/models/
         ↓
  Real-time scoring service (score_stream.py)

Usage:
  python train_model.py                          # full retrain
  python train_model.py --days 7                # last 7 days
  python train_model.py --symbol BTCUSDT        # single-symbol model
  python train_model.py --retrain-once           # for cron use
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

# ── Paths ────────────────────────────────────────────────────────────────────
MODEL_DIR = Path(os.getenv("MODEL_DIR", "/home/yaros/work/tradesystem/screener/ml_pipeline/models"))
MODEL_DIR.mkdir(parents=True, exist_ok=True)
MODEL_PATH = MODEL_DIR / "if_pump_detector.joblib"
SCALER_PATH = MODEL_DIR / "feature_scaler.json"
METADATA_PATH = MODEL_DIR / "model_metadata.json"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("train")


# ── QuestDB psql connection ─────────────────────────────────────────────────
try:
    import psycopg2
    _PSQL_AVAILABLE = True
except ImportError:
    _PSQL_AVAILABLE = False
    log.warning("psycopg2 not available — training queries will fall back to HTTP")


def get_psql_conn():
    return psycopg2.connect(
        host=os.getenv("QUESTDB_HOST", "localhost"),
        port=int(os.getenv("QUESTDB_PSQL_PORT", "8812")),
        database="qdb",
        user="admin",
        password="quest",
    )


def q_psql(query: str, params=None) -> list[dict]:
    """Execute a QuestDB SQL query via psql and return list of dicts."""
    conn = get_psql_conn()
    cur = conn.cursor()
    cur.execute(query, params)
    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()
    return rows


# ── HTTP fallback for non-psql environments ──────────────────────────────────
def q_http(query: str) -> list[dict]:
    import urllib.request, urllib.parse
    url = os.getenv("QUESTDB_URL", "http://localhost:9000/exec?query=") + urllib.parse.quote(query)
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read())
    if data.get("error"):
        raise RuntimeError(f"QuestDB error: {data['error']}")
    cols = [c["name"] for c in data["columns"]]
    return [dict(zip(cols, row)) for row in data["dataset"]]


# ── Feature engineering ───────────────────────────────────────────────────────
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


def compute_derived_features(df: dict) -> dict:
    df = dict(df)
    oi_5m = df.get("oi_change_5m") or 0
    df["oi_magnitude"] = abs(oi_5m)
    vol_24h = df.get("volume_24h") or 0
    vol_5m = df.get("volume_5m") or 0
    if vol_24h > 0 and vol_5m > 0:
        df["vol_spike_ratio"] = vol_5m / (vol_24h / 288) if vol_24h else 1.0
    else:
        df["vol_spike_ratio"] = 1.0
    liq_buy = df.get("liq_buy_5m") or 0
    liq_sell = df.get("liq_sell_5m") or 0
    df["liq_net"] = liq_buy - liq_sell
    df["liq_total"] = liq_buy + liq_sell
    cvd_5m = df.get("cvd_5m") or 0
    cvd_1h = df.get("cvd_1h") or 0
    df["cvd_velocity"] = cvd_5m - cvd_1h / 12
    df["oi_cvd_product"] = oi_5m * cvd_5m
    long_b = df.get("long_bias") or 50
    short_b = df.get("short_bias") or 50
    df["bias_imbalance"] = (long_b - short_b) / 100
    natr = df.get("natr") or 0
    range_5m = df.get("range_5m") or 0
    df["range_per_natr"] = range_5m / natr if natr > 0 else 0
    return df


def row_to_vector(row: dict) -> np.ndarray:
    return np.array([(row.get(f) or 0.0) for f in ALL_FEATURES], dtype=np.float32)


# ── Data loading ─────────────────────────────────────────────────────────────
DEFAULT_CONTAMINATION = 0.005


def load_training_data(
    symbol: str | None = None,
    min_days: int = 3,
    max_days: int = 30,
) -> tuple[np.ndarray, list[dict]]:
    log.info("Loading data from QuestDB (symbol=%s, min_days=%d, max_days=%d)",
             symbol, min_days, max_days)

    t_end = datetime.now(timezone.utc)
    t_start = t_end - timedelta(days=max_days)

    # QuestDB psql stores microseconds; timestamp comparisons must use ::bigint
    # We intentionally omit an upper bound — seed data may have near-future timestamps
    # and the signal_snapshots table only contains recent data anyway
    ts_start_us = int(t_start.timestamp() * 1_000_000)
    symbol_clause = f"AND symbol = '{symbol}'" if symbol else ""

    query = f"""
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
        WHERE symbol IS NOT NULL
          AND timestamp::bigint >= {ts_start_us}
          {symbol_clause}
        ORDER BY timestamp ASC
    """

    if _PSQL_AVAILABLE:
        rows = q_psql(query)  # f-string has no params
    else:
        # HTTP fallback (milliseconds)
        ts_start_ms = ts_start_us // 1000
        http_query = query.replace(f"timestamp::bigint >= {ts_start_us}", f"timestamp >= {ts_start_ms}")
        rows = q_http(http_query)

    log.info("  Loaded %d snapshot rows", len(rows))

    if len(rows) < 100:
        raise ValueError(f"Insufficient training data: only {len(rows)} rows available")

    for row in rows:
        compute_derived_features(row)

    X = np.vstack([row_to_vector(r) for r in rows])
    nan_count = np.isnan(X).sum()
    if nan_count > 0:
        log.warning("  Replacing %d NaN values with 0", nan_count)
        X = np.nan_to_num(X, nan=0.0)
    inf_count = np.isinf(X).sum()
    if inf_count > 0:
        X = np.clip(X, -1e10, 1e10)

    log.info("  Feature matrix shape: %s", X.shape)
    return X, rows


# ── Training ─────────────────────────────────────────────────────────────────
def train_iforest(
    X: np.ndarray,
    contamination: float = DEFAULT_CONTAMINATION,
    n_estimators: int = 200,
    random_state: int = 42,
):
    from sklearn.ensemble import IsolationForest
    log.info("Training Isolation Forest (n=%d, contamination=%.3f, samples=%d)",
             n_estimators, contamination, X.shape[0])
    t0 = time.time()
    clf = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        max_samples=min(10_000, X.shape[0]),
        random_state=random_state,
        n_jobs=-1,
    )
    clf.fit(X)
    log.info("  Training done in %.1fs", time.time() - t0)
    scores = clf.score_samples(X)
    log.info("  Score range: [%.4f, %.4f]  mean=%.4f",
             scores.min(), scores.max(), scores.mean())
    predictions = clf.predict(X)
    n_outliers = (predictions == -1).sum()
    log.info("  Predicted outliers: %d / %d (%.2f%%)",
             n_outliers, X.shape[0], n_outliers / X.shape[0] * 100)
    return clf


def find_score_threshold(clf, X: np.ndarray, target: float = DEFAULT_CONTAMINATION) -> float:
    scores = clf.score_samples(X)
    return float(np.percentile(scores, target * 100))


def compute_feature_stats(X: np.ndarray) -> dict:
    mean = np.mean(X, axis=0)
    std = np.std(X, axis=0)
    std[std < 1e-8] = 1.0
    return {"mean": mean.tolist(), "std": std.tolist(), "features": ALL_FEATURES}


def save_model(clf, scaler_stats: dict, threshold: float, metadata: dict) -> None:
    import joblib
    joblib.dump(clf, MODEL_PATH)
    with open(SCALER_PATH, "w") as f:
        json.dump(scaler_stats, f, indent=2)
    metadata["score_threshold"] = threshold
    metadata["features"] = ALL_FEATURES
    metadata["n_features"] = len(ALL_FEATURES)
    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)
    log.info("Model: %s  Scaler: %s  Metadata: %s", MODEL_PATH, SCALER_PATH, METADATA_PATH)


# ── Top anomalies ─────────────────────────────────────────────────────────────
def top_anomalies(clf, X: np.ndarray, rows: list[dict], top_n: int = 20) -> list[dict]:
    scores = clf.score_samples(X)
    indices = np.argsort(scores)[:top_n]
    anomalies = []
    for idx in indices:
        r = dict(rows[idx])
        r["anomaly_score"] = float(scores[idx])
        anomalies.append(r)
    return anomalies


def print_top_anomalies(anomalies: list[dict]) -> None:
    log.info("\nTop %d anomalous snapshots:", len(anomalies))
    for a in anomalies:
        from datetime import datetime as dt
        ts_val = a["timestamp"]
        if isinstance(ts_val, dt):
            ts = ts_val.strftime("%m-%d %H:%M")
        else:
            ts = dt.fromtimestamp(float(ts_val) / 1_000_000, tz=timezone.utc).strftime("%m-%d %H:%M")
        log.info(
            "  %s  %-15s  score=%.3f  cvd_5m=%+.0f  oi_5m=%+.1f%%  "
            "liq_net=$%.0f  range_5m=%.4f  natr=%.3f",
            ts, a.get("symbol", "?"), a["anomaly_score"],
            a.get("cvd_5m") or 0, a.get("oi_change_5m") or 0,
            a.get("liq_net") or 0, a.get("range_5m") or 0, a.get("natr") or 0,
        )


# ── Data sufficiency ──────────────────────────────────────────────────────────
def assess_data_sufficiency(n_samples: int, n_features: int, days_covered: int) -> dict:
    min_normal_samples = n_features * 100
    expected_pumps = n_samples * 0.01
    rules = [
        ("n_samples >= 500", n_samples >= 500),
        ("n_samples >= n_features * 100", n_samples >= min_normal_samples),
        ("days_covered >= 3", days_covered >= 3),
        ("expected_pumps >= 10", expected_pumps >= 10),
    ]
    status = "✅ SUFFICIENT" if all(v for _, v in rules) else "⚠️  MARGINAL"
    advice = {
        "n_samples": n_samples,
        "n_features": n_features,
        "days_covered": days_covered,
        "min_normal_samples_required": min_normal_samples,
        "expected_pumps_in_window": round(expected_pumps, 0),
        "status": status,
        "rules": rules,
    }
    if status == "✅ SUFFICIENT":
        advice["message"] = (
            f"Data sufficient. {n_samples} samples across {days_covered} days "
            f"({n_features} features). Expected ~{expected_pumps:.0f} pump events."
        )
    else:
        advice["message"] = (
            f"⚠️  Marginal — need min {min_normal_samples} samples, got {n_samples}. "
            f"Use --retrain-once flag in cron to avoid errors on sparse days."
        )
    return advice


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Pump/Dump IF Training Pipeline")
    parser.add_argument("--symbol", type=str, default=None)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--min-days", type=int, default=3)
    parser.add_argument("--max-days", type=int, default=30)
    parser.add_argument("--contamination", type=float, default=DEFAULT_CONTAMINATION)
    parser.add_argument("--n-estimators", type=int, default=200)
    parser.add_argument("--retrain-once", action="store_true")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("Pump/Dump ML Training Pipeline")
    log.info("  symbol:        %s", args.symbol or "ALL")
    log.info("  days:          %d", args.days)
    log.info("  contamination: %.3f", args.contamination)
    log.info("  n_estimators:  %d", args.n_estimators)
    log.info("=" * 60)

    try:
        X, rows = load_training_data(symbol=args.symbol, min_days=args.min_days, max_days=args.max_days)
    except (ValueError, RuntimeError) as e:
        log.error("Data loading failed: %s", e)
        if args.retrain_once:
            log.info("Exiting gracefully (--retrain-once).")
            sys.exit(0)
        sys.exit(1)

    sufficiency = assess_data_sufficiency(X.shape[0], len(ALL_FEATURES), args.days)
    log.info("\nData sufficiency: %s", sufficiency["message"])

    clf = train_iforest(X, contamination=args.contamination, n_estimators=args.n_estimators)
    threshold = find_score_threshold(clf, X, target=args.contamination)
    scaler_stats = compute_feature_stats(X)

    anomalies = top_anomalies(clf, X, rows)
    print_top_anomalies(anomalies)

    metadata = {
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "n_samples": X.shape[0],
        "n_features": X.shape[1],
        "days_covered": args.days,
        "symbol": args.symbol,
        "contamination": args.contamination,
        "n_estimators": args.n_estimators,
        "sufficiency_status": sufficiency["status"],
        "expected_pumps": round(sufficiency["expected_pumps_in_window"], 0),
    }
    save_model(clf, scaler_stats, threshold, metadata)
    log.info("\n✅ Training complete. Model: %s  threshold=%.4f", MODEL_PATH, threshold)


if __name__ == "__main__":
    main()
