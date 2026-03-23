# Binance Futures Trading Bot

Algorithmic bot for Binance USDT-M perpetual futures with:
- REST scheduler over closed candles (no websocket dependency)
- EMA pullback long-only strategy shared by live and backtest
- strict TP/SL monitor lifecycle
- paper and live trading support

## Current Defaults
- Symbol universe: top `80` USDT perpetual symbols by 24h quote volume
- Evaluation intervals: `15m`, `1h`, `4h` (derived from `MAIN_INTERVAL` + `CONTEXT_INTERVAL`)
- Max concurrent positions: `2`
- Sizing mode: `fixed_margin` (`5 USDT` per trade by default)
- Scaling logic: implemented but disabled in active monitor runtime (`scale_fn=None`)

## Architecture
Core modules:
- `main.py`: orchestration only (bootstrap, scheduler, shutdown, heartbeat)
- `config.py`: single source of runtime settings and `.env` parsing
- `indicators.py`: shared EMA/ATR/RSI/context helpers
- `strategy.py`: configurable `evaluate_signal(...)` used by live and backtest
- `execution.py`: order routing, rounding, TP/SL and monitor loop primitives
- `monitor.py`: position supervision and orphan recovery flow
- `data_stream.py`: REST candle polling scheduler + in-memory cache
- `risk.py`: thread-safe `RiskManager`

Service layer:
- `services/bootstrap_service.py`
- `services/signal_service.py`
- `services/entry_service.py`
- `services/position_service.py`
- `services/telegram_service.py`

Backtest:
- `backtest/backtest.py`: parallel downloader + simulator using the same strategy parameters

## Sizing Policy
`SIZING_MODE`:
- `fixed_margin` (default): uses `FIXED_MARGIN_PER_TRADE_USDT`
- `risk_based`: uses `RISK_PER_TRADE_PCT` and `MARGIN_UTILIZATION` with stop distance

## Configuration
Copy and edit:

```bash
cp .env.example .env
```

Most relevant settings:
- Universe: `USE_TOP_VOLUME_SYMBOLS`, `TOP_VOLUME_SYMBOLS_COUNT`, `TOP_VOLUME_ALLOWLIST`, `SYMBOLS`, `EXTRA_SYMBOLS`
- Timeframes/data: `MAIN_INTERVAL`, `CONTEXT_INTERVAL`, `HISTORY_CANDLES_MAIN`, `HISTORY_CANDLES_CONTEXT`
- Risk/positions: `MAX_POSITIONS`, `COOLDOWN_SEC`, `MAX_CONSECUTIVE_LOSSES`, `DAILY_DRAWDOWN_LIMIT_USDT`
- Execution: `USE_LIMIT_ONLY`, `LIMIT_OFFSET_PCT`, `LIMIT_TIMEOUT_SEC`
- Strategy: `EMA_*`, `ATR_*`, `RSI_*`, volume/score thresholds

All runtime defaults are defined in `config.py`.

## Run
```bash
python main.py
```

Paper mode:
- `USE_PAPER_TRADING=true`
- optional `PAPER_START_BALANCE`

Live mode:
- `USE_PAPER_TRADING=false`
- set `BINANCE_API_KEY` and `BINANCE_API_SECRET`

## Backtest
```bash
python backtest/backtest.py
```

Outputs are written to `backtest/results/`.

## Docker
```bash
docker compose up -d --build
```

Healthcheck uses `logs/.alive` updated by the heartbeat loop.

## Notes
- Orphan open positions are recovered on startup in live mode.
- Scheduler callbacks are interval-based and trigger global evaluation per timeframe.
- Telegram alerts are best-effort and rate-limited.
