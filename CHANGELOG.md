# Changelog

## 2026-04-12 — Phase C: Continuous Events + Signal History

### Why

The screener was **reactive** — it only fired events when price crossed an impulse
threshold. Gaps between impulses had no visibility:

1. **No exit timing** — entering on an impulse, a bot had no way to know that
   the setup was degrading until the next impulse (possibly in the opposite direction)
2. **No flow events** — aggressive CVD bursts, liquidation cascades, and volume
   spikes were visible in the tiered buffers but never signaled
3. **No state-change detection** — trend flips, OI inflections, and CVD/price
   divergences weren't detected

Phase C adds a continuous evaluator loop that samples all signals at 15s intervals,
maintains a rolling history, and runs a detector chain that fires typed, schema'd
events on the same bus as impulse alerts.

### What changed

| File | Change |
|------|--------|
| `screener/signal_history.py` | **New** — `SignalSnapshot` dataclass + `SignalHistory` rolling window (120 samples, ~30 min) |
| `screener/evaluator.py` | **New** — async background loop: sample signals → append to history → run detectors → dispatch |
| `screener/detectors.py` | **New** — pure-function detectors for 7 event types, ordered in `DETECTORS` list |
| `screener/models.py` | Added 7 new event dataclasses: `BiasChangedEvent`, `CvdBurstEvent`, `LiquidationCascadeEvent`, `OiFlipEvent`, `DivergenceEvent`, `VolumeSpikeEvent`, `TrendChangedEvent` |
| `screener/alerts.py` | Extended `AlertEvent` union, `_prepare_dispatch` mapping, `_format_telegram` for all new types |
| `screener/api.py` | Extended SSE `/events/stream` endpoint with new event type mappings |
| `screener/tui.py` | AlertPanel renders all new events with emoji prefixes (`B` bias, `F` flow, `D` divergence, `V` volume, `T` trend) |
| `screener/__main__.py` | Instantiates `SignalHistory`, launches `signal-evaluator` background task |

### Event catalogue

| Event | Trigger | Priority |
|-------|---------|----------|
| `BiasChangedEvent` | Bias label differs vs ~5 min ago | Exit signal — highest |
| `CvdBurstEvent` | 1m CVD > 2.5× 5m baseline | Entry early-warning |
| `LiquidationCascadeEvent` | 1m liq > $50k AND > 3× baseline | Forced flow detection |
| `OiFlipEvent` | OI sign flipped + persisted ≥ 2 ticks | Exit / regime change |
| `DivergenceEvent` | Price new high/low, CVD did not confirm | Exhaustion fade signal |
| `VolumeSpikeEvent` | 5m volume > 3× 24h/288 baseline | Attention signal |
| `TrendChangedEvent` | Trend string changed between snapshots | Regime change |

### Cooldown strategy

- State-change events (`BiasChanged`, `TrendChanged`, `OiFlip`): **60-120s** —
  rare and load-bearing, so short cooldown
- Flow events (`CvdBurst`, `LiquidationCascade`, `VolumeSpike`): **300s** —
  can fire repeatedly during trends, need longer cooldown

### Detector thresholds (module constants in `detectors.py`)

| Constant | Value | Purpose |
|----------|-------|---------|
| `K_CVD_BURST_MULTIPLIER` | 2.5× | 1m CVD must exceed 5m baseline by this factor |
| `K_CVD_BASELINE_SAMPLES` | 20 | How many history snapshots to use for baseline (~5 min) |
| `K_LIQ_ABSOLUTE_MIN` | $50k | Minimum 1m liquidation USD to qualify |
| `K_LIQ_BURST_MULTIPLIER` | 3.0× | Liq volume must exceed baseline by this factor |
| `K_OI_FLIP_PERSIST` | 2 | Minimum ticks of same-sign OI for flip detection |
| `K_DIVERGENCE_WINDOW` | 40 | How many snapshots back to check for new high/low (~10 min) |
| `K_DIVERGENCE_MIN_NATR` | 0.1 | Minimum NATR to consider divergence meaningful |
| `K_VOLUME_SPIKE_MULTIPLIER` | 3.0× | Volume spike threshold vs 24h/288 baseline |

### Tests

| Test file | Count | Coverage |
|-----------|-------|----------|
| `tests/test_signal_history.py` | 12 | Deque bounds, maxlen enforcement, `at_age()` lookup, per-ticker isolation |
| `tests/test_detectors.py` | 19 | Burst fires/doesn't fire, bias flip, liq cascade, oi flip, divergence, volume spike, trend change |

Total: **60 tests** passing (15 bias + 14 store + 31 new).

## 2026-04-10 — Memory Optimization + Directional Bias Layer

### Why

The screener's impulse alerts were **direction-agnostic** — they flagged fast
moves but couldn't distinguish between continuation (follow) and exhaustion
(fade). This led to a liquidation on RAVE (a manipulated low-cap token) where
shorting a pump was the wrong call because the order flow context (CVD rising,
OI rising, sustained buy-side volume) indicated a squeeze, not a top.

Simultaneously, the in-memory ring buffers were consuming excessive RAM because
Python `@dataclass` instances carry ~220 bytes of overhead per object. Extending
windows to 4 hours for the bias layer would have added ~700 MB without
restructuring the storage.

### What changed

#### Phase A — Memory optimization

| File | Change |
|------|--------|
| `screener/models.py` | Added `slots=True` to **all** dataclasses (~60% per-instance memory reduction) |
| `screener/store.py` | Replaced single flat deques with a **three-tier pyramid** for CVD, Liq, and OI |
| `screener/store.py` | All rolling-window reads use reverse iteration with early break (O(window) not O(maxlen)) |
| `screener/__main__.py` | Added `--mem-profile` CLI flag for tracemalloc profiling (logs top 15 allocators every 5m) |
| `screener/questdb_reader.py` | **New** — lightweight HTTP query client for QuestDB REST API, used as fallback for windows >4h |

**Three-tier pyramid** (CVD/Liq/OI):
- **Tier 1 (1s)**: last 5 minutes at 1-second resolution (300 buckets)
- **Tier 2 (10s)**: last 1 hour at 10-second resolution (360 buckets)
- **Tier 3 (60s)**: last 4 hours at 1-minute resolution (240 buckets)
- Total: **900 buckets per ticker per signal** instead of the 14,400 that a
  single-tier 4h buffer would require (~16x reduction)
- Aging is triggered lazily on writes — buckets roll up the pyramid as they
  age past each tier's boundary
- Read path assembles results from multiple tiers transparently

**New store dataclass**: `OiPoint(timestamp, value)` — replaces raw tuples for
OI snapshots, gains slots optimization.

**New store methods**:
- `get_oi_change_pct(exchange, symbol, window_seconds)` — generalised OI %
  change over any window (replaces the 5m-only version)
- `get_trade_imbalance_persistence(exchange, symbol, window_seconds, threshold)`
  — signed [-1, 1] measure of how consistently buy% or sell% exceeds a
  threshold within the window
- `get_cvd_long(exchange, symbol, window_seconds)` — async, falls back to
  QuestDB for windows beyond in-memory capacity

#### Phase B — Directional bias layer

| File | Change |
|------|--------|
| `screener/bias.py` | **New** — pure-function `compute_bias()` scoring engine |
| `screener/models.py` | Extended `ImpulseEvent` with 9 new fields (cvd_4h, oi_change_1h/4h, buy_pct_5m/15m, buy_persistence_5m, long/short_bias_score, bias_label) |
| `screener/engine.py` | `_fire_impulse` now computes all bias features and calls `compute_bias()` |
| `screener/tui.py` | Added columns: CVD 4h, OI Chg 1h, Buy% 5m. Alert panel shows colored bias chip |
| `screener/alerts.py` | Telegram messages now include bias label/scores, Buy% 5m/15m, OI at multiple windows |

**Bias scoring components** (each contributes weighted points, capped at 100):

| Component | Weight | Signal |
|-----------|--------|--------|
| CVD/price alignment | 30 | Pump + positive CVD = real buyers (long); pump + flat/negative CVD = fadeable (short) |
| OI direction | 25 | Rising OI on pump = leveraged longs entering = squeeze risk; falling OI = exhaustion |
| Buy% persistence | 20 | Sustained >60% buy% for 5 minutes = conviction, not noise |
| Funding rate | 15 | Extreme positive funding = crowded longs = penalise long bias |
| NATR exhaustion | 10 | Move far above NATR baseline → more likely exhaustion (favor fade) |

**Labels** from the score difference:
- `LONG` — long_score - short_score > 40
- `SHORT` — short_score - long_score > 40
- `CONFLICTED` — both > 30
- `AVOID` — both < 15
- `NEUTRAL` — otherwise

#### Tests

| Test file | Count | Coverage |
|-----------|-------|----------|
| `tests/test_store_tiers.py` | 14 | Tier boundary aging, cross-tier reads, tier size bounds, remove_ticker cleanup |
| `tests/test_bias.py` | 15 | CVD alignment, OI squeeze detection, buy% persistence, funding penalty, NATR exhaustion, label decisions, RAVE scenario |

### How to use

1. **Run with memory profiling**: `python -m screener --mem-profile`
2. **New TUI columns**: CVD 4h, OI Chg 1h, Buy% 5m appear automatically
3. **Impulse alerts**: Check the bias chip `[LONG]`/`[SHORT]`/`[CONFL]`/`[AVOID]`
   in the alert panel. Telegram messages include the full bias breakdown.
4. **Interpreting bias scores**: L:75 S:20 means strong long conviction. If you
   were about to short, reconsider. L:25 S:70 on a pump means the fade is
   supported by order flow — proceed with more confidence.

### What's NOT included (deferred)

- DEX integration (cross-venue CVD divergence for manipulation detection)
- Per-ticker manipulability/blacklist scoring
- VPIN / formal toxicity metric (buy% persistence is the lighter proxy)
- Automatic position sizing from bias score
- Numpy-backed buffers (only needed at >500 tickers)
