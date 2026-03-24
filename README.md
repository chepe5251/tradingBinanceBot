# Binance Futures Trading Bot

Algorithmic bot for Binance USDT-M perpetual futures with:
- REST scheduler over closed candles (no websocket dependency)
- EMA pullback long-only strategy shared by live and backtest
- Strict TP/SL monitor lifecycle with breakeven and ATR trailing
- Paper and live trading support

## Current Defaults

| Setting | Value |
|---------|-------|
| Symbol universe | Top **300** USDT perpetual symbols by 24h quote volume (hardcoded in `config.py`) |
| Evaluation intervals | `15m` (main) + `1h` (context) |
| Max concurrent positions | `2` |
| Sizing mode | `pct_balance` — **5% of available balance** per position |
| SELL filter | SELL signals blocked on `4h` interval (`BLOCK_SELL_ON_INTERVALS`) |
| Loss scaling | Implemented but disabled by default (`ENABLE_LOSS_SCALING=false`) |
| Max hold | Position closed at market after `MAX_HOLD_CANDLES=50` candles |

## Architecture

Core modules:
- `main.py` — orchestration (bootstrap, scheduler, shutdown, heartbeat)
- `config.py` — single source of runtime settings and `.env` parsing
- `indicators.py` — pure EMA / ATR / RSI helpers (**no Binance dependency**)
- `exchange_utils.py` — exchange helpers that require a Binance client (`safe_mark_price`)
- `strategy.py` — `StrategyConfig` dataclass + `evaluate_signal()` used by live and backtest
- `execution.py` — order routing, rounding, TP/SL placement, OCO monitor loop
- `monitor.py` — position supervision (`PositionMonitor`) and orphan recovery
- `data_stream.py` — REST candle polling scheduler + in-memory cache
- `risk.py` — thread-safe `RiskManager` with JSON state persistence
- `sizing.py` — position sizing policies (`pct_balance`, `fixed_margin`, `risk_based`)

Service layer:
- `services/bootstrap_service.py` — symbol loading and executor setup
- `services/signal_service.py` — `StrategyConfig` factory + signal evaluation per interval
- `services/entry_service.py` — signal gating, entry execution, monitor spawn
- `services/position_service.py` — position cache, orphan recovery, balance helpers
- `services/telegram_service.py` — alert formatting and delivery (non-blocking threads)

Backtest:
- `backtest/backtest.py` — parallel downloader + candle-by-candle simulator

## Strategy API

Signal evaluation uses a structured config object:

```python
from strategy import StrategyConfig, evaluate_signal

cfg = StrategyConfig(ema_fast=20, ema_mid=50, ema_trend=200, ...)
signal = evaluate_signal(main_df, context_df, cfg)
```

Live runtime builds the config via `services/signal_service.strategy_config_from_settings(settings)`.
Backtest builds `_STRATEGY_CFG` from `APP_SETTINGS` at startup — both use identical parameters.

## Sizing Policy

Controlled by `SIZING_MODE` in `.env` (default `pct_balance`):

| Mode | Behaviour |
|------|-----------|
| `pct_balance` *(default)* | `margin = available_balance × RISK_PER_TRADE_PCT` (e.g. 100 USDT × 5% = 5 USDT) |
| `fixed_margin` | Flat `FIXED_MARGIN_PER_TRADE_USDT` per trade |
| `risk_based` | Sizes by risk budget derived from stop distance and `MARGIN_UTILIZATION` |

Leverage is applied on top: `notional = margin × leverage`.

## Configuration

### What is configurable via `.env`

All parameters listed in `.env.example` with a value are user-configurable.
Copy and edit:
```bash
cp .env.example .env
```

Minimum required keys:
```env
BINANCE_API_KEY=your_key
BINANCE_API_SECRET=your_secret
BINANCE_TESTNET=true        # set false only for live capital
USE_PAPER_TRADING=true      # set false only for live capital
```

### What is fixed by design

| Parameter | Fixed value | Reason |
|-----------|-------------|--------|
| `top_volume_symbols_count` | **300** | Predictable API weight and memory budget |

> `TOP_VOLUME_SYMBOLS_COUNT` is **not** read from `.env` — the value in `config.py` is authoritative.

Key configurable parameters:

| Variable | Description |
|----------|-------------|
| `RISK_PER_TRADE_PCT` | Fraction of balance used as margin per trade (default `0.05`) |
| `SIZING_MODE` | `pct_balance` / `fixed_margin` / `risk_based` |
| `MAX_POSITIONS` | Max simultaneous open positions |
| `MAX_HOLD_CANDLES` | Candles before force-close at market (default `50`) |
| `COOLDOWN_SEC` | Seconds between entries |
| `DAILY_DRAWDOWN_LIMIT_USDT` | Max daily loss before bot pauses |
| `BLOCK_SELL_ON_INTERVALS` | Comma-separated intervals where SELL signals are suppressed (default `4h`) |
| `MAX_ATR_AVG_RATIO` | Reject signals when ATR exceeds this multiple of rolling ATR avg (default `2.5`) |
| `ENABLE_LOSS_SCALING` | Enable DCA on losing positions — **only after exhaustive backtest** (default `false`) |
| `MAIN_INTERVAL` / `CONTEXT_INTERVAL` | Candle intervals for signal and HTF confirmation |

## Run

```bash
python main.py
```

Paper mode — set in `.env`:
```env
USE_PAPER_TRADING=true
PAPER_START_BALANCE=100
```

Live mode — set in `.env`:
```env
BINANCE_TESTNET=false
USE_PAPER_TRADING=false
BINANCE_API_KEY=...
BINANCE_API_SECRET=...
```

## Backtest

```bash
python backtest/backtest.py
```

Results are written to `backtest/results/`.

## Tests

```bash
python -m pytest tests/ -v
```

All tests run without a Binance API key. The `pythonpath = ["."]` setting in `pyproject.toml`
ensures imports resolve from the repo root in any environment, including CI.

## Docker

```bash
docker compose up -d --build
```

Healthcheck is based on `logs/.alive` updated by the heartbeat loop.

## Notes

- Orphaned open positions (opened before bot restart) are automatically recovered on startup in live mode.
- TP/SL orders are placed on-exchange and survive bot restarts.
- `RiskState` (equity, consecutive losses, daily drawdown) is persisted to `logs/risk_state.json` and reloaded on restart.
- Scheduler callbacks fire per closed candle and evaluate all 300 symbols in parallel.
- Telegram alerts are sent in background daemon threads (non-blocking).
