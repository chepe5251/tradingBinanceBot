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
