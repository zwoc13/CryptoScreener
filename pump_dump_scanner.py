#!/usr/bin/env python3
"""
Pump/Dump scanner — runs against QuestDB every 5 minutes.
Scores candidates by: range_ratio, volume, buy/sell imbalance, OI change.
Delivers top candidates via Telegram.
"""

import json
import urllib.request
import urllib.parse
import os
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
QUESTDB_HTTP = "http://localhost:9000/exec?query="
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID")
LOOKBACK_MIN      = 15   # minutes to analyze
MIN_TRADES        = 100  # minimum trades to consider
TOP_N             = 8    # how many candidates to report
# ─────────────────────────────────────────────────────────────────────────────

def query(q: str) -> list:
    url = QUESTDB_HTTP + urllib.parse.quote(q)
    with urllib.request.urlopen(url, timeout=15) as r:
        data = json.load(r)
    if data.get("error"):
        raise RuntimeError(data["error"])
    cols = [c["name"] for c in data["columns"]]
    return [dict(zip(cols, row)) for row in data["dataset"]]


def make_lookback() -> str:
    """QuestDB accepts timestamp - offset_ms arithmetic as integer ms."""
    from datetime import datetime, timezone
    cutoff = int(datetime.now(timezone.utc).timestamp() * 1000 - LOOKBACK_MIN * 60 * 1000)
    return f"timestamp >= {cutoff}"


def pct(n: float) -> str:
    return f"{n*100:.2f}%"


def signal_emoji(bias: float) -> str:
    if bias > 0.3:  return "🟢 PUMP"
    if bias < -0.3: return "🔴 DUMP"
    return "🟡 MIXED"


def build_message(candidates: list, ts: str) -> str:
    lines = [
        f"📊 *Pump/Dump Scan* `{ts}`",
        f"Analyzing last {LOOKBACK_MIN} min | min {MIN_TRADES} trades",
        "",
    ]
    for i, c in enumerate(candidates, 1):
        direction = signal_emoji(c["buy_ratio"] - 1)
        lines.append(
            f"{i}. {direction} *{c['symbol']}*\n"
            f"   Range: {c['pct_move']:.1f}%  "
            f"Vol: ${c['total_vol']/1e6:.2f}M  "
            f"Trades: {c['trades']}\n"
            f"   High: `{c['high']:.6f}`  Low: `{c['low']:.6f}`\n"
            f"   Buy: ${c['buy_vol']/1e3:.0f}K  "
            f"Sell: ${c['sell_vol']/1e3:.0f}K  "
            f"B/S: {c['buy_ratio']:.2f}x\n"
        )
    return "\n".join(lines)


def send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set — skipping")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        print(f"Telegram sent: {r.read()}")


def main():
    ts = datetime.now(timezone.utc).strftime("%H:%M UTC %d %b")

    cutoff_filter = make_lookback()

    q = (
        f"SELECT "
        f"  symbol, "
        f"  max(price) as high, "
        f"  min(price) as low, "
        f"  (max(price) / min(price) - 1)*100 as pct_move, "
        f"  count() as trades, "
        f"  sum(usd_size) as total_vol, "
        f"  sum(case when side='Buy' then usd_size else 0 end) as buy_vol, "
        f"  sum(case when side='Sell' then usd_size else 0 end) as sell_vol "
        f"FROM trades "
        f"WHERE {cutoff_filter} "
        f"GROUP BY symbol "
        f"ORDER BY pct_move DESC"
    )

    rows = query(q)

    # ── Filter + score ────────────────────────────────────────────────────────
    candidates = []
    for r in rows:
        if r["trades"] < MIN_TRADES:
            continue
        try:
            buy_ratio = r["buy_vol"] / r["sell_vol"] if r["sell_vol"] > 0 else 1.0
        except ZeroDivisionError:
            buy_ratio = 1.0

        # Skip stable pairs (tiny moves)
        if abs(r["pct_move"]) < 2.0:
            continue

        r["buy_ratio"] = buy_ratio
        candidates.append(r)

    # Sort by abs(pct_move) * volume_weight, descending
    candidates.sort(
        key=lambda x: abs(x["pct_move"]) * (x["total_vol"] / 1e6),
        reverse=True,
    )
    top = candidates[:TOP_N]

    print(f"\n[{ts}] Scanned {len(rows)} symbols, {len(candidates)} passed filter. Top {len(top)}:")
    for c in top:
        print(f"  {c['symbol']}: {c['pct_move']:.1f}% | buy_ratio {c['buy_ratio']:.2f} | ${c['total_vol']/1e6:.2f}M")

    # ── Deliver ───────────────────────────────────────────────────────────────
    if top:
        msg = build_message(top, ts)
        print(msg)
        send_telegram(msg)
    else:
        print("No candidates this round — skipping Telegram.")


if __name__ == "__main__":
    main()