# Binance Futures Trading Bot

Trading bot for Binance USDT-M futures with:
- REST scheduler over closed candles
- EMA-pullback strategy shared by live and backtest
- paper and live modes
- position monitoring (TP/SL + breakeven)

## Versioning and Releases

- Latest release tag: `v1.0.3`.
- Project versioning uses repository tags with SemVer format: `vMAJOR.MINOR.PATCH`.
- Detailed release notes are maintained in [CHANGELOG.md](CHANGELOG.md).

Recommended release gate before tagging:

```bash
ruff check .
python -m py_compile main.py config.py strategy.py execution.py monitor.py monitor_runtime.py monitor_orphan.py backtest/backtest.py
python -m pytest tests/ -v -m unit
python -m pytest tests/ -v -m integration
python -m pytest tests/ --cov=risk --cov=strategy --cov=sizing --cov=execution --cov=monitor_logic --cov=indicators --cov-report=term-missing -q
docker build -t binance_bot_ci .
```

## Runtime (Live/Paper)

Main runtime modules:
- `main.py`: orchestration only
- `config.py`: settings + `.env` parser
- `services/runtime_controller.py`: lifecycle controller (`startup -> scheduler -> heartbeat -> shutdown`)
- `services/bootstrap_service.py`: runtime wiring and startup dependencies
- `services/exchange_metadata_service.py`: centralized exchange filter metadata cache
- `services/`: signal scan, entry flow, position helpers, telegram, operational telemetry
- `execution.py`, `monitor.py`, `monitor_runtime.py`, `monitor_protection.py`, `monitor_orphan.py`, `monitor_scaling.py`, `risk.py`, `data_stream.py`
- `strategy.py`, `indicators.py`

Run:

```bash
python main.py
```

### Monitoring Behavior

- Protections are managed with fixed TP/SL orders plus breakeven updates.
- Trailing logic is intentionally disabled in runtime monitor/orphan flows.
- This keeps live and backtest exit behavior aligned with current validated baselines.

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

Risk/operational state writes use atomic tmp+replace persistence. Corrupt JSON payloads are quarantined as
`*.bad-*` so startup can continue safely.

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
- shared `trace_id` to follow signal -> entry -> monitor -> exit

### Operational Alerts (Optional)

When enabled, Telegram can receive rate-limited alerts for:
- startup
- suspension activated/cleared
- order failure (relevant stage)
- protection failure
- orphan detected/resumed/unrecoverable
- hourly/daily operational summaries

## Backtest (Offline)

Run:

```bash
python backtest/backtest.py
```

Outputs in `backtest/results/`:
- `backtest_*.csv` (trade-level rows)
- `analysis_*.csv` (aggregates by interval/score/RSI/volume/etc.)
- `equity_*.csv` (equity and drawdown curve)

Behavior notes:
- Backtest uses the same `evaluate_signal` strategy logic as runtime.
- 15m short is evaluated as second pass (`interval="15m_short"`) only when 15m long returns `None`.
- If interrupted (`Ctrl+C`), partial progress is preserved and partial CSV outputs are still generated.

## Tests

Run full suite:

```bash
python -m pytest tests/ -v
```

Unit-only:

```bash
python -m pytest tests/ -v -m unit
```

Golden regression tests (recommended before strategy/backtest refactors):

```bash
python -m pytest tests/test_golden_regression.py -q
```

Golden fixtures:
- `tests/fixtures/golden_strategy_signal_1h.json`
- `tests/fixtures/golden_backtest_sim_1h.json`

## Backtest Docs

See `backtest/README.md` for backtest runtime details and reliability notes.
