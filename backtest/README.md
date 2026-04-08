# Backtest Guide

`backtest/backtest.py` runs an offline simulation using the same signal engine as runtime.

## What It Produces

Each run writes three files in `backtest/results/`:
- `backtest_YYYYMMDD_HHMMSS.csv`: one row per simulated trade
- `analysis_YYYYMMDD_HHMMSS.csv`: grouped metrics by interval/score/RSI/volume/hour/day
- `equity_YYYYMMDD_HHMMSS.csv`: cumulative PnL and drawdown per trade

## Run

```bash
python backtest/backtest.py
```

## Runtime Model

- Phase 1 (I/O): parallel download of klines with rate limiter and optional cache.
- Phase 2 (CPU): parallel simulation by symbol using process workers.
- Strategy path is shared via `strategy.evaluate_signal`.
- For `15m`, the engine does a second pass with `interval="15m_short"` only when long returns `None`.
- Exits are fixed TP/SL plus timeout in simulation (no trailing behavior in backtest).

## KeyboardInterrupt Safety

If you interrupt the run (`Ctrl+C`) during download or simulation:
- the script keeps completed work,
- marks the run as partial,
- and still saves available CSV outputs.

## Main Environment Controls

Common knobs (from `.env`):
- `BACKTEST_TOP_SYMBOLS`
- `BACKTEST_CANDLES_15M`, `BACKTEST_CANDLES_1H`, `BACKTEST_CANDLES_4H`, `BACKTEST_CANDLES_1D`
- `BACKTEST_DL_WORKERS`
- `BACKTEST_SIM_WORKERS`
- `BACKTEST_RATE_LIMIT_PER_MIN`
- `BACKTEST_USE_CACHE`
- `BACKTEST_CACHE_MAX_AGE_MIN`

## Quick Validation

```bash
python -m py_compile backtest/backtest.py
python -m pytest tests/test_golden_regression.py -q
```

Golden fixtures used by regression tests:
- `tests/fixtures/golden_strategy_signal_1h.json`
- `tests/fixtures/golden_backtest_sim_1h.json`
