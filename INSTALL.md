# Installation Guide

## Requirements
- Python 3.11+
- Binance Futures account (testnet or live)
- Git

## 1. Clone and install
```bash
git clone https://github.com/chepe5251/tradingBinance.git
cd tradingBinance
python -m venv .venv
```

Activate virtualenv:
- Windows PowerShell: `.venv\Scripts\activate`
- macOS/Linux: `source .venv/bin/activate`

Install deps:
```bash
pip install -r requirements.txt
```

## 2. Configure environment
```bash
cp .env.example .env
```

Set at least:
- `BINANCE_API_KEY`
- `BINANCE_API_SECRET`
- `BINANCE_TESTNET=true` for initial validation

For paper mode:
- `USE_PAPER_TRADING=true`
- optional `PAPER_START_BALANCE`

## 3. Run bot
```bash
python main.py
```

Logs:
- Console runtime logs
- `logs/trades.log`

## 4. Run backtest
```bash
python backtest/backtest.py
```

Output CSV files are generated in `backtest/results/`.

## 5. Common checks
- Missing API keys in live mode: bot fails fast at startup
- Scheduler health: heartbeat updates `logs/.alive`
- Symbol universe is controlled by `USE_TOP_VOLUME_SYMBOLS`, `TOP_VOLUME_SYMBOLS_COUNT`, `SYMBOLS`, `EXTRA_SYMBOLS`
