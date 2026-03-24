# Backtest

`backtest/backtest.py` downloads Binance Futures klines, simulates trades candle-by-candle, and writes:
- `backtest/results/backtest_*.csv`
- `backtest/results/analysis_*.csv`
- `backtest/results/equity_*.csv`

## Key behavior
- Uses the same `strategy.evaluate_signal(cfg)` logic as live runtime via a shared `StrategyConfig`
- Loads all strategy and interval parameters from `config.py` / `.env` at startup
- Symbol universe: top **300** USDT perpetual symbols by 24h quote volume (fixed in `config.py`)
- Sizing: simulates flat margin per trade (`FIXED_MARGIN_PER_TRADE_USDT`); live bot uses `pct_balance`

## Run
From repository root:
```bash
python backtest/backtest.py
```

## Core parameters
- `TOP_SYMBOLS`: fixed at **300** — `top_volume_symbols_count` in `config.py` (not overridable via `.env`)
- `INTERVALS`: derived from `MAIN_INTERVAL`, `CONTEXT_INTERVAL`, and higher timeframe map
- `CANDLES_PER_INTERVAL`: from `HISTORY_CANDLES_MAIN` / `HISTORY_CANDLES_CONTEXT` with minimum floors
- `MARGIN_PER_TRADE`: `FIXED_MARGIN_PER_TRADE_USDT`
- `LEVERAGE`: `LEVERAGE`
- `MAX_CANDLES_HOLD`: 50 candles (matches `MAX_HOLD_CANDLES` default in live bot)

## Strategy configuration
The backtest builds a `StrategyConfig` from `APP_SETTINGS` at module load time:
```python
_STRATEGY_CFG = StrategyConfig(ema_fast=..., ema_mid=..., ...)
signal = evaluate_signal(sub, context_sub, _STRATEGY_CFG)
```
This is the same object structure used by the live runtime, guaranteeing parameter parity.

## Notes
- SELL trades are blocked on intervals listed in `BLOCK_SELL_ON_INTERVALS` (default `4h`) to mirror live long-only behavior.
- `ENABLE_LOSS_SCALING=false` by default; DCA scaling is not simulated unless explicitly enabled.
- Rate limiter enforces Binance weight budget (2300/min) during parallel Phase 1 downloads.
- Phase 2 simulation runs in parallel worker processes (`cpu_count`).
