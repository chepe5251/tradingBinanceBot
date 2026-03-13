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
- Signal strategy on `M15` with strict `1H` trend-strength alignment.
- Sends **all** valid signals to Telegram.
- Executes only the first valid signal when:
  - `RiskManager` allows trading, and
  - no open position exists.
- Limit entry with market fallback.
- Mandatory TP/SL with automatic recovery if protection orders are lost.
- Floating-loss scaling (levels defined in current logic).
- Telegram anti-spam protection with rate-limit handling (`HTTP 429` retry).
- WebSocket heartbeat and automatic restart.

## Strategy Profile (Current)
The strategy is selective and continuation-focused, designed for earlier entries
near the end of pullbacks (not late expansion candles).

Entry filters include:
- Strict 1H directional filter:
  - LONG: `close > EMA50` and `EMA50 rising`
  - SHORT: `close < EMA50` and `EMA50 falling`
- M15 trend alignment:
  - LONG: `EMA7 > EMA25` and `DIF > 0`
  - SHORT: `EMA7 < EMA25` and `DIF < 0`
- MACD histogram expanding in trade direction:
  - LONG: current histogram bar `> previous histogram bar`
  - SHORT: current histogram bar `< previous histogram bar`
- Structured small pullback requirement:
  - only `1` or `2` opposite candles allowed
  - **all** pullback candles must have `body/range <= 0.6` (both candles validated for 2-candle pullbacks)
  - LONG pullback low must hold above EMA25
  - SHORT pullback high must hold below EMA25
- Early entry trigger:
  - LONG: current candle breaks pullback high and closes above pullback close
  - SHORT: current candle breaks pullback low and closes below pullback close
- Late-entry blockers:
  - reject if current range `> 1.6 * ATR`
  - reject if distance from EMA7 `> 0.3 * ATR`
  - reject if current `body/range > 0.85`
- Volume quality gate:
  - current volume must be at least `1.3 * avg volume(5)` (strong confirmation required)
- Anti-chop range filter:
  - reject if EMA7/EMA25 crossed `3` or more times over last `15` candles
- Score-based ranking (minimum `3.0` to pass):
  - `h1_slope_strength`: 1H EMA50 momentum (0–3 pts)
  - `pullback_quality`: cleaner/smaller pullback body (0–2 pts)
  - `rr_vs_atr`: risk distance in ATR units (0–2 pts)
  - `volume_strength`: volume boost above average (0–1.5 pts)
  - `late_penalty`: penalizes large entry candles (up to −2 pts)
  - `atr_regime_penalty`: penalizes high-volatility regimes where ATR > 1.3× its 20-bar average (up to −1.5 pts)
  - the engine sorts signals and executes only the top score each cycle

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
