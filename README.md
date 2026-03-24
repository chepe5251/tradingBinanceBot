# Binance Futures Trading Bot

Trading bot for Binance USDT-M futures with:
- REST scheduler over closed candles
- EMA-pullback strategy shared by live and backtest
- paper and live modes
- position monitoring (TP/SL, breakeven, trailing)

## Runtime (Live/Paper)

Main runtime modules:
- `main.py`: orchestration only
- `config.py`: settings + `.env` parser
- `services/`: bootstrap, signal scan, entry flow, position helpers, telegram
- `execution.py`, `monitor.py`, `risk.py`, `data_stream.py`
- `strategy.py`, `indicators.py`

Run:

```bash
python main.py
```

## Stage 3 Operations (Runtime Hardening)

Stage 3 adds an operational layer for observability and safety without changing
the default trading behaviour.

Important defaults:
- `ENABLE_OPERATIONAL_KILL_SWITCHES=false`
- `ENABLE_OPERATIONAL_ALERTS=false`

With defaults, leverage/sizing/exposure/entry-exit logic remain unchanged.

### Operational Metrics

The bot now tracks operational counters such as:
- `signals_detected`
- `signals_rejected`
- `entries_attempted`
- `entries_executed`
- `entries_failed`
- `protection_failures`
- `order_failures`
- `orphan_recoveries`
- `heartbeat_count`
- `polling_cycles`
- `last_signal_time`
- `last_entry_time`
- `last_exit_time`
- `uptime_sec`

### Operational Dashboard/Reports

Generated periodically (default each 60s):
- `logs/ops_status.json` (machine-readable snapshot)
- `logs/ops_summary.md` (human-readable summary)

`ops_status.json` includes:
- mode (`LIVE/PAPER/TESTNET`)
- health (`healthy/paused/degraded`)
- scheduler and polling status
- metrics block (keys above)
- recent signals/entries/exits
- recent errors
- suspension reasons (if any)

State counters are persisted at shutdown in:
- `logs/ops_state.json`

### Kill Switches and Auto Suspension

A configurable framework can pause **new entries only** while keeping monitors alive.
Rules include:
- consecutive errors
- repeated API degradation (`KILL_SWITCH_MAX_API_ERRORS`)
- consecutive order failures
- consecutive protection failures
- scheduler stale/idle
- orphan unrecoverable (optional)

Suspensions are time-based (`OPERATIONAL_SUSPEND_SEC`) and reversible.

Example (optional; disabled by default):
```env
ENABLE_OPERATIONAL_KILL_SWITCHES=true
KILL_SWITCH_MAX_CONSECUTIVE_ERRORS=6
KILL_SWITCH_MAX_ORDER_FAILURES=4
KILL_SWITCH_MAX_PROTECTION_FAILURES=3
KILL_SWITCH_MAX_SCHEDULER_IDLE_SEC=1800
OPERATIONAL_SUSPEND_SEC=900
```

### Paper Trading Traceability

Paper runs now expose clearer operational traces:
- signal detected
- signal alert sent
- entry attempted/executed
- order placement failed/success
- protection status
- exit result and realized pnl
- paper equity snapshots in ops status artifacts
- shared `trace_id` to follow signal → entry → monitor → exit

### Operational Alerts (Optional)

When enabled, Telegram can receive rate-limited alerts for:
- startup
- suspension activated/cleared
- order failure (relevant stage)
- protection failure
- orphan detected/resumed/unrecoverable
- hourly/daily operational summaries

## Stage 2 Analysis (Offline Only)

Stage 2 tooling is focused on diagnosis/validation/experimentation.

Important:
- It does **not** change leverage.
- It does **not** change effective sizing/exposure.
- It does **not** change live default behaviour.
- It does **not** auto-enable any filter in production.

### 1. Standard Backtest

```bash
python backtest/backtest.py
```

Outputs in `backtest/results/`:
- `backtest_*.csv` (trade-level rows)
- `analysis_*.csv` (segment aggregates)
- `equity_*.csv` (equity/drawdown curve)

### 2. Market Regime Analysis

`analysis/regime.py` adds deterministic labels from trade snapshots:
- trend: `trending_up*`, `trending_down*`, `ranging`, `transitioning`
- volatility: `high_volatility`, `normal_volatility`, `low_volatility`
- structure: `compression`, `balanced`, `expansion`

### 3. Walk-Forward Stability

```bash
python backtest/walk_forward.py --csv backtest/results/backtest_YYYYMMDD_HHMMSS.csv
```

Supports:
- `--mode rolling|expanding`
- per-window metrics (PnL, trades, WR, expectancy, PF, max DD, top-winner concentration)
- global consistency summary
- CSV output (`walk_forward_*.csv`) and optional markdown (`--markdown`)

### 4. OOS Validation

```bash
python backtest/oos_split.py --csv backtest/results/backtest_YYYYMMDD_HHMMSS.csv --split 2026-03-01
```

Also supports:
- `--pct` (percentage split)
- `--splits` (multi-cut by dates)

Outputs:
- console comparison
- optional CSV/markdown summaries

Verdict labels:
- `estabilidad_aceptable`
- `degradacion_leve`
- `degradacion_moderada`
- `degradacion_severa`

### 5. Candidate Filter Experiments (Offline)

`analysis/filter_experiments.py` provides baseline-vs-variant comparison.

Examples of candidate filters:
- minimum score
- excluded hours/weekdays
- excluded symbols
- excluded intervals
- excluded market/regime buckets
- simple combinations (`AND` / `OR`)

Outputs available:
- console report
- CSV summary
- markdown summary

### 6. Comparative Reporting Helpers

`analysis/reporting.py` includes pure functions for:
- markdown table rendering
- compact Stage 2 summary assembly (baseline + walk-forward + OOS + filters)

## Tests

Run full suite:

```bash
python -m pytest tests/ -v
```

Unit-only:

```bash
python -m pytest tests/ -v -m unit
```

## Backtest Docs

See `backtest/README.md` for detailed Stage 2 workflows and interpretation guidance.
