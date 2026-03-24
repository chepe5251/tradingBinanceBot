# Binance Futures Trading Bot

EMA Pullback long-only strategy for Binance USDT-M perpetuals.

- REST scheduler over closed candles — no websocket dependency
- Shared `StrategyConfig` between live runtime and backtest
- Strict TP/SL lifecycle with breakeven, ATR trailing, and early exit
- Paper and live trading modes
- Thread-safe `RiskManager` with JSON persistence across restarts

## Quick Start

```bash
# Clone and install
git clone https://github.com/chepe5251/tradingBinance.git
cd tradingBinance
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env — at minimum set USE_PAPER_TRADING=true for first run

# Run
python main.py
```

## Architecture

### Module map

| Module | Binance dep? | Purpose |
|--------|-------------|---------|
| `indicators.py` | **No** | Pure math: EMA, ATR, RSI |
| `strategy.py` | **No** | `StrategyConfig` + `evaluate_signal()` |
| `risk.py` | **No** | Thread-safe `RiskManager`, cooldown/drawdown/consecutive-loss guards |
| `sizing.py` | **No** | Position sizing: `pct_balance`, `fixed_margin`, `risk_based` |
| `monitor_logic.py` | **No** | Early exit evaluation (structure, volume, context) |
| `config.py` | **No** | `Settings` dataclass + `.env` parser |
| `exchange_utils.py` | Yes | `safe_mark_price` — mark price with error handling |
| `execution.py` | Yes | Order routing, rounding, OCO monitor loop, breakeven/trailing |
| `monitor.py` | Yes | `PositionMonitor.run()` + orphan recovery |
| `data_stream.py` | Yes | REST candle poller + in-memory OHLCV cache |
| `services/bootstrap_service.py` | Yes | Symbol loading, risk init, executor factory |
| `services/signal_service.py` | **No** | `strategy_config_from_settings()` + per-interval scan |
| `services/entry_service.py` | Yes | Signal gating, sizing, order placement, monitor spawn |
| `services/position_service.py` | Yes | Position cache, orphan detection, balance fetch |
| `services/telegram_service.py` | No | Alert formatting + non-blocking delivery |
| `backtest/backtest.py` | Yes | Parallel downloader + candle-by-candle simulator |

### Bot lifecycle

```
main.py
 ├── load_env() + validate API keys
 ├── bootstrap_runtime()
 │    ├── load symbol universe (top 300 by 24h volume)
 │    ├── build_interval_plan() → [15m, 1h, 4h]
 │    ├── MarketDataStream.load_initial()   ← REST fetch, no WS
 │    ├── RiskManager.load(logs/risk_state.json)
 │    └── logs: Startup summary + Strategy config
 ├── resume_orphaned_positions()   ← live mode only
 ├── stream.start_scheduler(callbacks)
 │    └── on every candle close:
 │         ├── evaluate_interval_signals()  ← 300 symbols
 │         ├── risk + position gates
 │         └── _execute_candidate()
 │              ├── size + place order
 │              └── PositionMonitor.run() in daemon thread
 │                   ├── place TP/SL with retry
 │                   ├── OCO loop (breakeven → trailing → review)
 │                   └── risk_updater(pnl) on exit
 └── heartbeat loop → logs/.alive + INFO log every 60s
```

## Configuration

### Minimum `.env` for live trading

```env
BINANCE_API_KEY=your_key
BINANCE_API_SECRET=your_secret
BINANCE_TESTNET=false
USE_PAPER_TRADING=false
```

### Key parameters

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_PAPER_TRADING` | `false` | Paper mode — no real orders placed |
| `BINANCE_TESTNET` | `true` | Use Binance Futures testnet endpoint |
| `LEVERAGE` | `20` | Exchange leverage (auto-capped per symbol if needed) |
| `SIZING_MODE` | `pct_balance` | `pct_balance` / `fixed_margin` / `risk_based` |
| `RISK_PER_TRADE_PCT` | `0.05` | Fraction of available balance per position (5%) |
| `MAX_POSITIONS` | `2` | Max simultaneous open positions |
| `MAX_HOLD_CANDLES` | `50` | Force-close after N candles (50 × 900s = 12.5h on 15m) |
| `MAIN_INTERVAL` | `15m` | Primary signal interval |
| `CONTEXT_INTERVAL` | `1h` | HTF confirmation interval |
| `DAILY_DRAWDOWN_LIMIT_USDT` | `6.0` | Max daily loss (USDT) before trading pauses |
| `MAX_CONSECUTIVE_LOSSES` | `3` | Losses before pause |
| `COOLDOWN_SEC` | `180` | Seconds between entries |
| `MAX_ATR_AVG_RATIO` | `2.5` | Reject spike-volatility signals |
| `BLOCK_SELL_ON_INTERVALS` | `4h` | Intervals where SELL signals are suppressed (long-only) |
| `ENABLE_LOSS_SCALING` | `false` | DCA on losing positions — **only after exhaustive backtest** |

### Fixed by design (not `.env` configurable)

| Parameter | Value | Reason |
|-----------|-------|--------|
| `top_volume_symbols_count` | `300` | Predictable API weight and memory budget |

> Setting `TOP_VOLUME_SYMBOLS_COUNT` in `.env` has no effect — the value in `config.py` is authoritative.

## Startup summary log

On every start the bot logs two summary lines (no API keys or secrets exposed):

```
INFO | Startup | mode=LIVE leverage=20x sizing=pct_balance pct=5% max_pos=2 symbols=300 intervals=['15m', '1h', '4h'] hold=50 candles
INFO | Strategy config | ema=20/50/200 atr=14 rsi=48-68 vol=1.05-1.50x body>=0.35 score>=1.5 rr=2.0
```

Use these lines to verify your `.env` is loaded correctly before risking capital.

## Sizing policy

| Mode | Behaviour |
|------|-----------|
| `pct_balance` *(default)* | `margin = available_balance × RISK_PER_TRADE_PCT` (e.g. 100 USDT × 5% = 5 USDT) |
| `fixed_margin` | Flat `FIXED_MARGIN_PER_TRADE_USDT` per trade |
| `risk_based` | Sizes by risk budget derived from stop distance and `MARGIN_UTILIZATION` |

Leverage is applied on top: `notional = margin × leverage`.

## Risk management

| Guard | Parameter | Default |
|-------|-----------|---------|
| Cooldown between entries | `COOLDOWN_SEC` | 180s |
| Daily loss % cap | `DAILY_DRAWDOWN_LIMIT` | 20% |
| Daily loss USDT cap | `DAILY_DRAWDOWN_LIMIT_USDT` | 6 USDT |
| Consecutive loss pause | `MAX_CONSECUTIVE_LOSSES` | 3 |
| Pause duration | `RISK_PAUSE_AFTER_LOSSES_SEC` | 86400s (1 day) |
| Max hold | `MAX_HOLD_CANDLES` | 50 candles |

`RiskState` (equity, consecutive losses, daily drawdown) is persisted to `logs/risk_state.json` and reloaded on restart.

## Tests

```bash
# All tests
python -m pytest tests/ -v

# Unit tests only (no network, no filesystem I/O)
python -m pytest tests/ -v -m unit

# Integration tests only
python -m pytest tests/ -v -m integration

# Coverage report for core modules
python -m pytest tests/ --cov=risk --cov=strategy --cov=sizing \
  --cov=execution --cov=monitor_logic --cov=indicators \
  --cov-report=term-missing
```

All 31 tests run without a live Binance API key. `pyproject.toml` adds the repo root to `sys.path` automatically.

## Lint

```bash
pip install ruff==0.4.4
ruff check .
```

## Backtest

```bash
python backtest/backtest.py
```

Uses the same `StrategyConfig` as the live runtime. Results written to `backtest/results/`.

## Docker

```bash
docker compose up -d --build
docker logs -f binance_bot
docker logs --tail 200 binance_bot
```

Container healthcheck: `logs/.alive` is updated every `LOG_HEARTBEAT_SEC` seconds (default 60).

## Interpreting logs

| Log pattern | Meaning |
|-------------|---------|
| `Startup \| mode=...` | Bot startup summary — verify config here |
| `Strategy config \| ema=...` | Active strategy parameters |
| `Loaded N symbols by top-volume` | Symbol universe loaded |
| `Starting scheduler symbols=N` | Scheduler started, polling begins |
| `signal SYM tf=15m side=BUY` | Valid signal found |
| `scan tf=15m scanned=N signals=N` | DEBUG scan summary per interval |
| `ENTRY SYM tf=15m side=BUY entry=... sl=... tp=... margin=...` | Trade opened |
| `monitor SYM tp_ok=True sl_ok=True` | Position protection health check |
| `breakeven SYM new_sl=...` | Stop loss moved to entry |
| `trail SYM new_sl=...` | Stop loss trailed upward |
| `exit SYM result=TP pnl=...` | Trade closed at take profit |
| `exit SYM result=SL pnl=...` | Trade closed at stop loss |
| `exit SYM result=EARLY:... pnl=...` | Trade closed early (structure/volume break or max hold) |
| `Heartbeat: bot alive polls=...` | Scheduler alive (logged every 60s) |
| `critical SYM reason=tp_sl_emergency` | Protection orders failing — check exchange permissions |

## Orphan recovery

Positions opened before the bot restarted are automatically detected at startup (live mode only).
The bot reads open orders, reconstructs SL/TP from them (or estimates from ATR), and starts a monitor thread for each orphan. Look for `orphan_resumed SYM` in the logs.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `Missing BINANCE_API_KEY` | Key not set in `.env` | Add `BINANCE_API_KEY=...` to `.env` |
| `exchange_info_load_failed` | API unreachable or testnet mismatch | Check `BINANCE_TESTNET` and network |
| `balance_fetch_failed` | Missing Futures permissions | Enable Futures trading on API key |
| No signals after startup | Filters too strict or symbol not trending | Widen `RSI_LONG_MIN/MAX`, `MIN_SCORE` in `.env` |
| `critical ... tp_sl_emergency` | Can't place TP/SL orders | Check Futures permissions, margin balance |
| Startup summary shows wrong mode | `.env` not loaded or wrong path | Confirm `.env` is in repo root, not subdirectory |
| Container exits immediately | Startup error | Run `docker logs binance_bot` for the error |

## Stage 1 hardening — operational guarantee

Stage 1 (this codebase) contains **zero changes** to:

- `leverage` or leverage application logic
- position sizing formulas (`pct_balance`, `fixed_margin`, `risk_based`)
- effective quantity per trade
- entry / exit / TP / SL / trailing / breakeven logic
- `enable_loss_scaling` behavior (remains disabled by default)
- live vs paper mode behavior

All Stage 1 changes are: tests, observability, CI, and documentation only.
