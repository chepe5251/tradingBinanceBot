# Binance Futures Scalping Bot (USDT-M)

Algorithmic trading bot for **Binance USDT-M Futures** with:
- full-universe multi-pair scanning (all available USDT-M perpetual symbols),
- Telegram signal broadcasting,
- automatic execution with only 1 active trade at a time,
- TP/SL protection and continuous monitoring.

## Important Notice
This software can open and close real positions. Use it at your own risk. Start in **testnet** or **paper mode** first.

## Core Features
- Full USDT perpetual symbol universe scanning (no hard symbol cap by default).
- Signal strategy on `M15` with strict `1H` bias alignment.
- Sends **all** valid signals to Telegram.
- Executes only the first valid signal when:
  - `RiskManager` allows trading, and
  - no open position exists.
- Limit entry with market fallback.
- Mandatory TP/SL with automatic recovery if protection orders are lost.
- Floating-loss scaling (levels defined in current logic).
- WebSocket heartbeat and automatic restart.

## Architecture
- `main.py`: orchestration (stream, signals, execution, monitoring).
- `strategy.py`: signal engine.
- `execution.py`: order execution, Binance filter rounding, TP/SL, OCO monitor.
- `data_stream.py`: historical load + WebSocket + candle cache.
- `risk.py`: cooldown, daily drawdown guard, loss pause logic.
- `config.py`: `Settings` model and `.env` loading.
- `indicators.py`: indicator helpers (used by auxiliary flows if needed).
- `test_trade.py`: manual script to validate minimal order placement.

## Requirements
- Python 3.10+
- Binance Futures account (testnet or live)
- Dependencies:

```bash
pip install -r requirements.txt
```

## Configuration
Create a `.env` file in the project root:

```env
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret

# Trading endpoint
BINANCE_TESTNET=true
BINANCE_DATA_TESTNET=false

# Optional: Telegram alerts
TELEGRAM_BOT_TOKEN=xxxxxxxx
TELEGRAM_CHAT_ID=123456789

# Optional: simulated trading
USE_PAPER_TRADING=false
PAPER_START_BALANCE=25

# Optional: risk controls
FIXED_MARGIN_PER_TRADE_USDT=5
DAILY_DRAWDOWN_LIMIT_USDT=6
ANTI_LIQ_TRIGGER_R=1.1
```

## Key Parameters (`config.py`)
Most relevant runtime settings:

- `main_interval` (default `15m`)
- `context_interval` (default `1h`)
- `leverage` (default `20`)
- `fixed_margin_per_trade_usdt` (default `5.0`)
- `tp_rr` (default `1.8`)
- `stop_atr_mult` (default `1.2`)
- `cooldown_sec` (default `180`)
- `max_consecutive_losses` (default `2`)
- `daily_drawdown_limit_usdt` (default `6.0`)
- `history_candles_main` / `history_candles_context`

## Run
```bash
python main.py
```

## Operational Flow (Summary)
1. Load configuration and historical candles.
2. Start WebSocket multiplexer in chunks.
3. On each main candle close:
   - evaluate signals across all symbols,
   - send valid signals to Telegram,
   - execute only one signal if allowed.
4. After execution:
   - place TP/SL,
   - start a protection/monitoring thread,
   - apply exit/scale rules based on runtime state.

## Logs
- Console: status, heartbeat, warnings, and errors.
- File: `logs/trades.log` (validation, entry, exit, and monitor events).

## Recommended Practices
- Start with `BINANCE_TESTNET=true`.
- Use `USE_PAPER_TRADING=true` to validate logic with no market risk.
- Never commit your `.env` file.
- Review `logs/trades.log` before tuning parameters.

## GitHub Workflow
Standard push flow:

```bash
git add .
git commit -m "update readme"
git push
```
