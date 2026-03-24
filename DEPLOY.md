# VPS Deployment (Docker)

## Prerequisites
- Ubuntu/Debian VPS
- Docker + Docker Compose
- Binance Futures API key with proper permissions

## 1. Clone and configure
```bash
git clone https://github.com/chepe5251/tradingBinance.git
cd tradingBinance
cp .env.example .env
nano .env
```

Minimum `.env` for live:
```env
BINANCE_API_KEY=...
BINANCE_API_SECRET=...
BINANCE_TESTNET=false
USE_PAPER_TRADING=false
```

## 2. Start container
```bash
docker compose up -d --build
```

## 3. Useful commands
```bash
docker logs -f binance_bot
docker logs --tail 200 binance_bot
docker compose restart
docker compose down
```

## 4. Health expectations

Typical startup log pattern (verify your `.env` here — no secrets exposed):
```text
INFO | Startup | mode=LIVE leverage=20x sizing=pct_balance pct=5% max_pos=2 symbols=300 intervals=['15m', '1h', '4h'] hold=50 candles
INFO | Strategy config | ema=20/50/200 atr=14 rsi=48-68 vol=1.05-1.50x body>=0.35 score>=1.5 rr=2.0
INFO | Loaded 300 symbols by top-volume filter.
INFO | Starting scheduler symbols=300 intervals=['15m', '1h', '4h']
INFO | Heartbeat: bot alive | polls=... scheduler=True
```

Container healthcheck is based on `logs/.alive` (updated every `LOG_HEARTBEAT_SEC` seconds).

Risk state is persisted to `logs/risk_state.json` and reloaded automatically on restart.

### Validating `.env` without exposing secrets
Check the `Startup |` log line immediately after launch. It shows mode, leverage, sizing, max
positions, symbol count, and intervals — everything needed to confirm configuration is loaded
correctly, without echoing any API keys or secrets.

## 5. Safety
- Start with `BINANCE_TESTNET=true` first.
- Use API key IP whitelist.
- Keep `MAX_POSITIONS` conservative until validated in your environment.
- `ENABLE_LOSS_SCALING=false` by default — do not enable without backtesting.
