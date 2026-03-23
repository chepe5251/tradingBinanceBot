# Backtest

`backtest/backtest.py` downloads Binance Futures klines, simulates trades candle-by-candle, and writes:
- `backtest/results/backtest_*.csv`
- `backtest/results/analysis_*.csv`
- `backtest/results/equity_*.csv`

## Key behavior
- Uses the same `strategy.evaluate_signal(...)` logic as live runtime
- Loads strategy and interval defaults from `config.py` via `.env`
- Uses top-volume symbol selection (`TOP_VOLUME_SYMBOLS_COUNT`) unless explicit symbols are configured and top-volume mode is disabled

## Run
From repository root:
```bash
python backtest/backtest.py
```

## Core parameters
- `TOP_SYMBOLS`: derived from `TOP_VOLUME_SYMBOLS_COUNT` / `TOP_SYMBOLS_LIMIT`
- `INTERVALS`: derived from `MAIN_INTERVAL`, `CONTEXT_INTERVAL`, and higher timeframe map
- `CANDLES_PER_INTERVAL`: derived from `HISTORY_CANDLES_MAIN` and `HISTORY_CANDLES_CONTEXT` with backtest-safe minimums
- `MARGIN_PER_TRADE`: `FIXED_MARGIN_PER_TRADE_USDT`
- `LEVERAGE`: `LEVERAGE`

## Notes
- `4h` SELL trades are still blocked by simulation filter to mirror live behavior.
- Rate limiter enforces Binance weight budget during parallel downloads.
