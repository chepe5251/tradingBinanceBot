# Security Policy

## Credential Handling

This project reads API credentials from a local `.env` file at startup.

- **Never commit `.env` to version control.** A `.gitignore` entry is provided.
- Binance API keys should be scoped to **Futures trading only** — disable Spot, Margin, and Withdrawal permissions.
- Enable **IP whitelist** on your Binance API key to restrict access to your server's IP.
- Rotate API keys immediately if you suspect they have been exposed.

## Testnet First

Always start with `BINANCE_TESTNET=true` and `USE_PAPER_TRADING=true`.
Only disable these settings after verifying all configuration parameters produce the expected behaviour.

## Known Risks

| Risk | Mitigation |
|------|-----------|
| API key exposure | Use `.env`, never hardcode secrets, enable IP whitelist |
| Runaway scaling | Set `SCALE_LEVEL*_MARGIN_USDT=0` to disable scaling levels |
| Network failure mid-trade | TP/SL orders are placed on-exchange and persist independently of the bot process |
| Exchange rate limits | Chunked WebSocket streams and Telegram back-off are built-in |

## Reporting a Vulnerability

If you discover a security issue in this project, please open a private GitHub Security Advisory rather than a public issue.

Provide:
1. A description of the vulnerability.
2. Steps to reproduce.
3. Potential impact.
4. Suggested remediation, if known.

You can expect an acknowledgement within 48 hours.
