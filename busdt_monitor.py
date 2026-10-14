#!/usr/bin/env python3
"""BUSDT position monitor — polls screener API, prints changes, alerts on threshold breach."""

import time
import json
import urllib.request
import sys

API = "http://localhost:8000/tickers/BUSDT?exchange=bybit"
POLL_INTERVAL = 30  # seconds

# --- THRESHOLDS (edit these) ---
DANGER_CVD_5M_POSITIVE = True      # CVD 5m flips above 0
DANGER_OI_5M_SPIKE = 3.0           # OI 5m change > +3%
DANGER_BUY_PERSIST = -0.3          # buy persistence > -0.3
DANGER_PRICE_HIGH = 0.228          # new local high
DANGER_FUNDING_SPIKE = 0.03        # funding > 0.03% = real long conviction

# --- Targets ---
TP1 = 0.204
TP2 = 0.180
TP3 = 0.130

def fetch():
    try:
        with urllib.request.urlopen(API, timeout=5) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"[!] Fetch error: {e}")
        return None

def fmt(t):
    return (
        f"  Price:  {t['last_price']:.5f}  (daily {t['daily_change_pct']:+.1f}%)\n"
        f"  CVD 5m: {t['cvd_5m']:+,.0f}  |  1h: {t['cvd_1h']:+,.0f}  |  4h: {t['cvd_4h']:+,.0f}  |  daily: {t['cvd_daily']:+,.0f}\n"
        f"  OI 5m:  {t['oi_change_5m_pct']:+.2f}%  |  1h: {t['oi_change_1h_pct']:+.2f}%\n"
        f"  BuyP:   {t['buy_persistence_5m']:.4f}  |  L/S: {t['long_short_ratio']:.3f}  |  NATR: {t['natr_5m_14']:.2f}\n"
        f"  Fund:   {t['funding_rate']*100:.4f}%"
    )

def check_alerts(t):
    alerts = []
    if DANGER_CVD_5M_POSITIVE and t['cvd_5m'] > 0:
        alerts.append(f"🔴 CVD 5m FLIPPED POSITIVE ({t['cvd_5m']:+,.0f}) — buyers took tape")
    if t['oi_change_5m_pct'] > DANGER_OI_5M_SPIKE:
        alerts.append(f"🔴 OI 5m SPIKE ({t['oi_change_5m_pct']:+.2f}%) — squeeze building")
    if t['buy_persistence_5m'] > DANGER_BUY_PERSIST:
        alerts.append(f"🔴 Buy persistence weak ({t['buy_persistence_5m']:.4f}) — sell pressure gone")
    if t['last_price'] > DANGER_PRICE_HIGH:
        alerts.append(f"🔴 Price above {DANGER_PRICE_HIGH} — new high, squeeze risk")
    if t['funding_rate'] > DANGER_FUNDING_SPIKE:
        alerts.append(f"🔴 Funding spiked ({t['funding_rate']*100:.4f}%) — longs paying premium")
    # TP proximity
    dist_tp1 = (t['last_price'] - TP1) / t['last_price'] * 100
    if dist_tp1 < 3:
        alerts.append(f"🎯 Near TP1 ({TP1}) — {dist_tp1:.1f}% away")
    return alerts

def main():
    print(f"=== BUSDT SHORT MONITOR (polling every {POLL_INTERVAL}s) ===")
    print(f"Danger: CVD5m>0 | OI5m>+{DANGER_OI_5M_SPIKE}% | BuyP>{DANGER_BUY_PERSIST} | Price>{DANGER_PRICE_HIGH} | Fund>{DANGER_FUNDING_SPIKE*100:.1f}%")
    print(f"TPs: TP1={TP1} | TP2={TP2} | TP3={TP3}")
    print(f"Ctrl+C to stop\n")

    prev_cvd = None
    while True:
        t = fetch()
        if t:
            ts = time.strftime("%H:%M:%S")
            cvd_delta = ""
            if prev_cvd is not None:
                d = t['cvd_5m'] - prev_cvd
                cvd_delta = f"  (Δ{d:+,.0f})"
            prev_cvd = t['cvd_5m']

            alerts = check_alerts(t)
            status = " ".join(a for a in alerts) if alerts else "✅ HOLD"

            print(f"[{ts}] {status}")
            print(fmt(t) + cvd_delta)
            print()

            # Print separator on alert
            if alerts:
                for a in alerts:
                    print(f"  >>> {a}")
                print()

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
