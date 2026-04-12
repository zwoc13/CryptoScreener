"""Tests for screener.signal_history — deque bounds, lookup-by-age, isolation."""

from __future__ import annotations

import time

from screener.signal_history import SignalHistory, SignalSnapshot


def _make_snap(ts: float, bias_label: str = "NEUTRAL", cvd_5m: float = 0.0,
               price: float = 100.0) -> SignalSnapshot:
    return SignalSnapshot(
        timestamp=ts, price=price, cvd_5m=cvd_5m, cvd_1h=0.0, cvd_4h=0.0,
        oi_change_5m=0.0, oi_change_1h=0.0, buy_pct_5m=0.5, buy_persist_5m=0.0,
        long_bias=0.0, short_bias=0.0, bias_label=bias_label,
        liq_buy_5m=0.0, liq_sell_5m=0.0, natr=0.01, trend="-",
    )


class TestSignalHistoryBasics:
    def test_append_and_get(self):
        h = SignalHistory(maxlen=10)
        snap = _make_snap(time.time())
        h.append("binance:BTCUSDT", snap)
        assert h.get("binance:BTCUSDT") is not None
        assert len(h.get("binance:BTCUSDT")) == 1
        assert h.get("binance:BTCUSDT")[-1] is snap

    def test_get_unknown_returns_none(self):
        h = SignalHistory()
        assert h.get("unknown") is None

    def test_latest_returns_last_snap(self):
        h = SignalHistory()
        base = time.time()
        h.append("x:A", _make_snap(base, "LONG"))
        h.append("x:A", _make_snap(base + 15, "NEUTRAL"))
        latest = h.latest("x:A")
        assert latest is not None
        assert latest.bias_label == "NEUTRAL"

    def test_latest_none_for_unknown(self):
        h = SignalHistory()
        assert h.latest("x:B") is None

    def test_remove_ticker(self):
        h = SignalHistory()
        h.append("x:C", _make_snap(time.time()))
        h.remove_ticker("x:C")
        assert h.get("x:C") is None

    def test_remove_nonexistent_ticker_is_noop(self):
        h = SignalHistory()
        h.remove_ticker("nope")


class TestMaxlen:
    def test_deque_capped_at_maxlen(self):
        h = SignalHistory(maxlen=5)
        base = time.time()
        for i in range(10):
            h.append("x:D", _make_snap(base + i * 15))
        assert len(h.get("x:D")) == 5  # only last 5 remain

    def test_oldest_snap_evicted(self):
        h = SignalHistory(maxlen=3)
        base = time.time()
        for i in range(5):
            h.append("x:E", _make_snap(base + i * 15, price=100 + i))
        assert h.get("x:E")[0].price == 102  # oldest 3 remain: 102, 103, 104


class TestAtAge:
    def test_at_age_returns_closest(self):
        h = SignalHistory(maxlen=20)
        base = time.time()
        # 15-second intervals
        for i in range(10):
            h.append("x:F", _make_snap(base + i * 15, cvd_5m=100 + i))
        # 60 seconds ago = 4 samples back → target at index 5
        result = h.at_age("x:F", 60.0)
        assert result is not None
        assert abs(result.timestamp - (base + 75)) < 1  # closest to target

    def test_at_age_out_of_range_returns_closest_available(self):
        h = SignalHistory(maxlen=10)
        base = time.time()
        for i in range(5):
            h.append("x:G", _make_snap(base + i * 15, cvd_5m=i))
        result = h.at_age("x:G", 300.0)  # age exceeds history
        assert result is not None  # still returns closest

    def test_at_age_empty_history(self):
        h = SignalHistory()
        assert h.at_age("x:H", 15.0) is None


class TestPerTickerIsolation:
    def test_two_tickers_do_not_interfere(self):
        h = SignalHistory(maxlen=10)
        base = time.time()
        h.append("a:X", _make_snap(base, bias_label="LONG"))
        h.append("b:Y", _make_snap(base, bias_label="SHORT"))
        assert h.latest("a:X").bias_label == "LONG"
        assert h.latest("b:Y").bias_label == "SHORT"
