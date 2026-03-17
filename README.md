# Binance Futures Scalping Bot

> Algorithmic trading bot for **Binance USDT-M Perpetual Futures**.
> Scans 529+ pairs in real time, detects high-probability **Liquidity Sweep Reversals** on M15, and executes one position at a time with automatic TP/SL protection, trailing stop, and loss-based scaling.

---

> **Risk Warning**
> This software places and cancels real orders on Binance Futures. Futures trading with leverage carries a high risk of loss, including total loss of deposited capital. Start with `BINANCE_TESTNET=true` and validate with `USE_PAPER_TRADING=true` before using real capital. Past performance of a strategy in testing is not indicative of future results.

---

## Table of Contents

- [Features](#features)
- [Strategy](#strategy)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Bot](#running-the-bot)
- [Trade Lifecycle](#trade-lifecycle)
- [Loss-Based Scaling](#loss-based-scaling)
- [Risk Management](#risk-management)
- [Telegram Alerts](#telegram-alerts)
- [Logs](#logs)
- [Project Structure](#project-structure)
- [Disclaimer](#disclaimer)

---

## Features

| Feature | Detail |
|---|---|
| Symbol universe | All 529+ USDT-M perpetual pairs loaded at startup |
| Signal timeframe | M15 with 1H trend alignment |
| Entry | Limit order with automatic market fallback after 6 s |
| Protection | Mandatory TP + SL with auto-recovery if orders disappear |
| Breakeven | SL moved to entry once +0.5 % profit is reached |
| Trailing stop | ATR-based trail activates after breakeven |
| Scaling | Up to 5 scale-in levels on adverse moves |
| Risk controls | Cooldown, consecutive-loss pause, daily drawdown cap |
| Alerts | Telegram notifications for every signal and trade event |
| WebSocket | Chunked multiplexer with stale detection and auto-restart |
| Paper mode | Full simulation without touching the exchange |

---

## Strategy

### Liquidity Sweep Reversal (M15)

The bot targets **stop-hunt reversals**: candles that briefly break a key 20-bar high or low but close back inside the prior range, signalling that the breakout was absorbed by institutional orders. A second closed candle is required to confirm the move before any signal is emitted.

#### LONG Setup — all 7 conditions must pass in order

| # | Filter | Condition |
|---|--------|-----------|
| 1 | **Trend bullish** | `close > EMA200` AND `EMA50 > EMA200` |
| 2 | **Bearish sweep** | Sweep candle `low < lowest_low_20` |
| 3 | **False breakout** | Sweep candle `close > lowest_low_20` |
| 4 | **Absorption wick** | `lower_wick / range ≥ 0.6` |
| 5 | **Volume spike** | Sweep candle `volume ≥ 1.3 × avg_vol_20` |
| 6 | **Size filter** | Sweep candle `range < 2 × ATR` |
| 7 | **Reversal confirm** | Next candle `high > sweep_high` AND `close > sweep_close` |

#### SHORT Setup

Exact mirror of the LONG setup:
- `close < EMA200` AND `EMA50 < EMA200`
- Sweep candle `high > highest_high_20`, closes back below
- `upper_wick / range ≥ 0.6`
- Confirmation candle `low < sweep_low` AND `close < sweep_close`

#### Levels

```
Entry  = close of confirmation candle
SL     = low/high of sweep candle
TP     = entry ± (risk × 2.0)
```

#### Signal Score

Signals are ranked by score when multiple setups appear simultaneously. Only the top-scoring signal is executed.

| Component | Formula | Max |
|-----------|---------|-----|
| Wick quality | `(wick_ratio − 0.6) / 0.4 × 3` | 3.0 |
| Volume boost | `vol / avg_vol − 1.3` | 2.0 |
| **Total** | | **5.0** |

---

## Architecture

```
tradingPython/
├── main.py          # Orchestrator: stream, signals, execution, monitoring
├── strategy.py      # Signal engine: 7-filter sweep detector + scorer
├── execution.py     # Order router: limit/market, TP/SL, OCO monitor, trailing
├── data_stream.py   # WebSocket multiplexer + in-memory candle cache
├── risk.py          # Cooldown, loss pause, daily drawdown guard
├── config.py        # Settings dataclass + .env loader
├── indicators.py    # Shared indicator helpers (EMA, ATR)
├── test_trade.py    # Manual script to validate minimal order placement
├── .env.example     # Configuration template (copy to .env)
└── requirements.txt # Pinned Python dependencies
```

### Component Diagram

```
          .env
            │
            ▼
        config.py  ──► Settings
            │
       ┌────┴──────────────────────────────────┐
       │                                       │
       ▼                                       ▼
  data_stream.py                         risk.py
  (WebSocket + cache)                (RiskManager)
       │                                       │
       │  candle close                         │ can_trade?
       ▼                                       │
  strategy.py ──► signal dict ────────────────►│
  (evaluate_signal)                            │
                                               ▼
                                          main.py
                                     (on_main_close)
                                               │
                                               ▼
                                        execution.py
                                     (FuturesExecutor)
                                    limit → TP/SL → trail
                                               │
                                               ▼
                                       Binance API
```

---

## Requirements

- Python 3.10+
- Binance Futures account (testnet or live)

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/chepe5251/tradingBinance.git
cd tradingBinance

# 2. Create and activate a virtual environment
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

# 3. Install pinned dependencies
pip install -r requirements.txt

# 4. Copy the configuration template
cp .env.example .env
```

---

## Configuration

Edit `.env` with your credentials and preferred parameters:

```env
# Binance API credentials
BINANCE_API_KEY=your_api_key_here
BINANCE_API_SECRET=your_api_secret_here

# Start in testnet — change to false only when ready for live trading
BINANCE_TESTNET=true
BINANCE_DATA_TESTNET=false

# Telegram alerts (optional)
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# Simulate trades without touching the exchange
USE_PAPER_TRADING=true
PAPER_START_BALANCE=25

# Capital controls
FIXED_MARGIN_PER_TRADE_USDT=5
DAILY_DRAWDOWN_LIMIT_USDT=6

# Scaling levels (set to 0 to disable)
SCALE_LEVEL1_MARGIN_USDT=5
SCALE_LEVEL2_MARGIN_USDT=0
```

See [`.env.example`](.env.example) for the full list of available options.

### Key Parameters

| Variable | Default | Description |
|----------|---------|-------------|
| `BINANCE_TESTNET` | `true` | Route execution to testnet |
| `FIXED_MARGIN_PER_TRADE_USDT` | `5.0` | USDT margin per trade entry |
| `DAILY_DRAWDOWN_LIMIT_USDT` | `6.0` | Bot pauses when daily loss exceeds this |
| `MARGIN_UTILIZATION` | `0.95` | Fraction of balance available as margin |
| `SCALE_LEVEL1_MARGIN_USDT` | `5.0` | Extra margin added at first scale level |
| `USE_PAPER_TRADING` | `false` | Full simulation mode |
| `SYMBOLS` | *(all)* | Comma-separated symbol filter (empty = all 529+) |

All defaults are defined in [`config.py`](config.py). Settings are validated and bounded at startup.

---

## Running the Bot

```bash
python main.py
```

The bot will:

1. Load all USDT-M perpetual symbols from the exchange.
2. Bootstrap historical candles (600 × M15, 400 × 1H per symbol).
3. Open WebSocket streams in chunks of 50.
4. Begin evaluating signals on every M15 candle close.

To stop the bot, press `Ctrl+C`. Open TP/SL orders remain active on the exchange and must be cancelled manually if desired.

---

## Trade Lifecycle

```
M15 candle closes
        │
        ▼
evaluate_signal() ── all 7 filters pass? ──► score signal
        │
        ▼ (all valid signals sorted by score)
RiskManager.can_trade()  AND  no open position?
        │
        ├─ NO  ──► broadcast signal to Telegram, skip execution
        │
        └─ YES ──► place_limit_with_market_fallback()
                        │
                        ├─ Limit fills within 6 s  ──► "MAKER"
                        └─ Timeout ──► cancel + market remaining ──► "TAKER"
                                │
                                ▼
                        place_tp_sl()  (TAKE_PROFIT + STOP, reduceOnly)
                                │
                                ▼
                        protect_and_monitor() thread
                        ┌───────────────────────────┐
                        │  every 0.5 s:             │
                        │  • check TP / SL status   │
                        │  • recover missing orders │
                        │  • breakeven @ +0.5 %     │
                        │  • trail after breakeven  │
                        │  • scale-in on drawdown   │
                        │  • early exit on EMA cross│
                        └───────────────────────────┘
                                │
                        TP / SL / EARLY exit
                                │
                        RiskManager.update_trade(pnl)
```

---

## Loss-Based Scaling

When a position moves against the entry, the bot can add margin at predefined drawdown thresholds to lower the average entry price.

| Level | Floating Loss Trigger | Additional Margin | Cumulative Exposure |
|-------|-----------------------|------------------|---------------------|
| Entry | — | $5 | $5 |
| L1 | −50 % of $5 = $2.50 | $5 | $10 |
| L2 | −100 % of $5 = $5.00 | $10 | $20 |
| L3 | −200 % of $5 = $10.00 | $20 | $40 |
| L4 | −400 % of $5 = $20.00 | $40 | $80 |
| L5 | −800 % of $5 = $40.00 | $80 | $160 |

> Set `SCALE_LEVEL*_MARGIN_USDT=0` to disable any individual level.
> Be aware that scaling significantly increases total risk exposure.

---

## Risk Management

The `RiskManager` enforces three independent guards:

| Guard | Default | Behaviour |
|-------|---------|-----------|
| **Cooldown** | 180 s | Minimum gap between entry signals |
| **Consecutive losses** | 2 | After 2 losses in a row, pause trading for 1 hour |
| **Daily drawdown** | $6 USDT | Bot pauses for the rest of the UTC day |

All counters reset at UTC midnight.

---

## Telegram Alerts

When `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set, the bot sends:

- **Signal alert** — for every valid setup detected across all 529+ symbols, including entry, SL, TP, R:R, and quality rating.
- **Trade opened** — confirmation with fill price and execution type (MAKER / TAKER).
- **Breakeven activated** — when SL is moved to entry.
- **Trailing stop updated** — each time the trail advances.
- **Scale-in added** — when a new position layer is added.
- **Trade closed** — result (TP / SL / EARLY) with PnL.

Rate limiting is handled automatically (HTTP 429 retry with back-off).

---

## Logs

| Destination | Content |
|-------------|---------|
| Console (`INFO`) | Heartbeat, stream events, warnings, errors |
| `logs/trades.log` | Every signal, skip reason, entry, exit, scale, and monitor event |

```bash
# tail live trades log
tail -f logs/trades.log
```

---

## Project Structure

```
main.py          Application entry point and orchestration loop (1 300+ lines)
strategy.py      Liquidity Sweep Reversal signal engine (210 lines)
execution.py     Order routing, rounding, OCO monitor, trailing stop (725 lines)
data_stream.py   WebSocket kline multiplexer with auto-restart (350 lines)
risk.py          RiskManager: cooldown, loss pause, drawdown guard (124 lines)
config.py        Settings dataclass and .env loader (259 lines)
indicators.py    Shared EMA / ATR helpers (47 lines)
test_trade.py    Manual order validation script (82 lines)
```

---

## Disclaimer

This project is provided for **educational and research purposes only**.
The authors are not responsible for financial losses resulting from its use.
Always perform your own due diligence before deploying any automated trading system with real capital.
Cryptocurrency futures trading is highly speculative and not suitable for all investors.
