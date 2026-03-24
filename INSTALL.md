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

Key dependencies (pinned in `requirements.txt`):
- `python-binance==1.0.34`
- `pandas==3.0.0`
- `numpy==2.4.2`
- `requests==2.32.5`
- `websockets==16.0`
- `aiohttp==3.13.3`
- `pytest==9.0.2`

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
- `ENABLE_OPERATIONAL_KILL_SWITCHES=false` (default; keeps entry behavior unchanged)
- `ENABLE_OPERATIONAL_ALERTS=false` (default; no extra Telegram operational alerts)
- `KILL_SWITCH_MAX_API_ERRORS=0` (0 disables API degradation suspension rule)

## 3. Run bot
```bash
python main.py
```

Logs:
- Console runtime logs
- `logs/trades.log`
- `logs/risk_state.json` — persisted risk state, reloaded on restart
- `logs/ops_status.json` - operational snapshot (JSON)
- `logs/ops_summary.md` - operational summary (markdown)
- `logs/ops_state.json` - persisted operational counters

Operational snapshot includes:
- health state (`healthy/degraded/paused`)
- scheduler/polling status
- metrics (`signals_detected`, `signals_rejected`, `entries_attempted`, etc.)
- recent signals/entries/exits/errors with traceability IDs

## 4. Run backtest
```bash
python backtest/backtest.py
```

Output CSV files are generated in `backtest/results/`.

## 5. Run tests
```bash
# All tests
python -m pytest tests/ -v

# Unit tests only (pure logic, no network)
python -m pytest tests/ -v -m unit

# Integration tests only
python -m pytest tests/ -v -m integration

# With coverage report for core modules
python -m pytest tests/ \
  --cov=risk --cov=strategy --cov=sizing \
  --cov=execution --cov=monitor_logic --cov=indicators \
  --cov-report=term-missing
```

All tests run without a Binance API key. `pyproject.toml` adds the repo root to `sys.path`
automatically via `pythonpath = ["."]`.

## 6. Common checks
- Missing API keys in live mode: bot fails fast at startup
- Scheduler health: heartbeat updates `logs/.alive`
- Symbol universe: top **300** USDT perpetual symbols by 24h quote volume (hardcoded;
  `USE_TOP_VOLUME_SYMBOLS`, `EXTRA_SYMBOLS`, and `SYMBOLS` can filter/extend the set)
- Operational health: inspect `logs/ops_summary.md` for healthy/degraded/paused status
