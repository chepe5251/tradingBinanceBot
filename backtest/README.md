# Backtest and Stage 2 Analysis

`backtest/backtest.py` simulates trades and writes:
- `backtest/results/backtest_*.csv` (trade rows)
- `backtest/results/analysis_*.csv` (segment aggregates)
- `backtest/results/equity_*.csv` (equity/drawdown)

## Scope Guard (Critical)

All Stage 2 tools are offline analysis only.

They do **not** modify:
- leverage
- live sizing behavior
- margin per trade in production
- max exposure / max positions
- live defaults
- entry/exit runtime logic

Any candidate improvement must be validated offline and only then considered
for a future gated runtime flag.

## 1) Run Standard Backtest

```bash
python backtest/backtest.py
```

## 2) Regime Analysis

`analysis/regime.py` labels each trade using deterministic snapshot features:
- `trend_regime`
- `volatility_regime`
- `structure_regime`
- `combined_regime` (`trend|volatility`)

Use from Python:

```python
import csv
from analysis.regime import add_regime_labels, regime_analysis, print_regime_report

with open("backtest/results/backtest_YYYYMMDD_HHMMSS.csv", encoding="utf-8") as f:
    trades = list(csv.DictReader(f))
for t in trades:
    t["pnl_usdt"] = float(t["pnl_usdt"])
    t["ema_spread"] = float(t.get("ema_spread", 0) or 0)
    t["vol_ratio"] = float(t.get("vol_ratio", 0) or 0)
    t["body_ratio"] = float(t.get("body_ratio", 0) or 0)

add_regime_labels(trades)
report = regime_analysis(trades)
print_regime_report(report)
```

## 3) Walk-Forward Robustness

```bash
python backtest/walk_forward.py \
  --csv backtest/results/backtest_YYYYMMDD_HHMMSS.csv \
  --mode rolling --window 30 --step 15
```

Also supports:
- `--mode expanding`
- `--output` (CSV path)
- `--markdown` (optional markdown summary)

Per-window metrics include:
- trades
- pnl
- winrate
- expectancy
- profit factor
- max drawdown %
- top-winner concentration (Top5 %)

Global summary includes:
- % windows with PF > 1
- % windows with positive expectancy
- % windows with positive PnL
- variance/stdev indicators
- high-drawdown window count
- consistency verdict

## 4) OOS Validation

Single cut by date:

```bash
python backtest/oos_split.py \
  --csv backtest/results/backtest_YYYYMMDD_HHMMSS.csv \
  --split 2026-03-01
```

By percentage:

```bash
python backtest/oos_split.py \
  --csv backtest/results/backtest_YYYYMMDD_HHMMSS.csv \
  --pct 0.7
```

Multi-cut:

```bash
python backtest/oos_split.py \
  --csv backtest/results/backtest_YYYYMMDD_HHMMSS.csv \
  --splits 2026-02-01 2026-03-01 2026-04-01
```

Optional exports:
- `--output` CSV
- `--markdown` markdown summary

Verdicts:
- `estabilidad_aceptable`
- `degradacion_leve`
- `degradacion_moderada`
- `degradacion_severa`

## 5) Candidate Filter Framework (Offline)

`analysis/filter_experiments.py` supports baseline vs variants:
- min/max score
- include/exclude symbols
- include/exclude intervals
- include/exclude hours
- include/exclude weekdays
- market phase / regime filters
- AND/OR combinations

Example:

```python
import csv
from analysis.filter_experiments import (
    compare_variants,
    combine_filters,
    filter_min_score,
    filter_exclude_weekdays,
    save_experiment_csv,
    save_experiment_markdown,
)

with open("backtest/results/backtest_YYYYMMDD_HHMMSS.csv", encoding="utf-8") as f:
    trades = list(csv.DictReader(f))
for t in trades:
    t["pnl_usdt"] = float(t["pnl_usdt"])
    t["score"] = float(t.get("score", 0) or 0)

variants = {
    "baseline": None,
    "score>=2.0": filter_min_score(2.0),
    "score>=2.0_no_weekend": combine_filters([
        filter_min_score(2.0),
        filter_exclude_weekdays({"Sat", "Sun"}),
    ]),
}
results = compare_variants(trades, variants)
save_experiment_csv(results, "backtest/results/filter_experiments.csv")
save_experiment_markdown(results, "backtest/results/filter_experiments.md")
```

## 6) Comparative Summary

`analysis/reporting.py` provides pure markdown summary builders to combine:
- baseline metrics
- walk-forward summary
- OOS comparison rows
- filter-variant rows

Use it to generate fast decision docs without touching runtime code.
