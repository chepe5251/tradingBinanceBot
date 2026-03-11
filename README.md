# Binance Futures Scalping Bot (USDT-M)

Bot algorítmico para **Binance Futures USDT-M** con:
- escaneo multi-par (hasta 200 símbolos),
- generación de señales en Telegram,
- ejecución automática de 1 operación activa a la vez,
- protección TP/SL y monitoreo continuo.

## Aviso importante
Este software puede abrir/cerrar operaciones reales. Úsalo bajo tu propio riesgo, idealmente primero en **testnet** o **paper mode**.

## Características principales
- Escaneo de universo USDT perpetual.
- Estrategia de señal en `M15` con sesgo estricto `1H`.
- Envío de **todas** las señales válidas a Telegram.
- Ejecución de solo la primera señal cuando:
  - `RiskManager` permite operar, y
  - no hay posición abierta.
- Entrada limit con fallback a market.
- TP/SL obligatorios con reposición automática si se pierden.
- Escalado por pérdida flotante (niveles configurados en lógica actual).
- Heartbeat y auto-restart del stream WebSocket.

## Arquitectura
- `main.py`: orquestación general (stream, señales, ejecución, monitoreo).
- `strategy.py`: motor de señales.
- `execution.py`: ejecución, redondeo por filtros Binance, TP/SL, monitor OCO.
- `data_stream.py`: carga histórica + WebSocket + caché de velas.
- `risk.py`: cooldown, drawdown diario, pausa por pérdidas.
- `config.py`: modelo `Settings` y carga desde `.env`.
- `indicators.py`: utilidades de indicadores (si se usan en flujos auxiliares).
- `test_trade.py`: script manual para validar envío de orden mínima.

## Requisitos
- Python 3.10+
- Cuenta Binance Futures (testnet o real)
- Dependencias:

```bash
pip install -r requirements.txt
```

## Configuración
Crear archivo `.env` en la raíz:

```env
BINANCE_API_KEY=tu_api_key
BINANCE_API_SECRET=tu_api_secret

# Trading endpoint
BINANCE_TESTNET=true
BINANCE_DATA_TESTNET=false

# Opcional: alertas Telegram
TELEGRAM_BOT_TOKEN=xxxxxxxx
TELEGRAM_CHAT_ID=123456789

# Opcional: operación simulada
USE_PAPER_TRADING=false
PAPER_START_BALANCE=25

# Opcional: riesgo
FIXED_MARGIN_PER_TRADE_USDT=5
DAILY_DRAWDOWN_LIMIT_USDT=6
ANTI_LIQ_TRIGGER_R=1.1
```

## Parámetros clave (config.py)
Estos son los más relevantes para operación:

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

## Ejecución
```bash
python main.py
```

## Flujo operativo (resumen)
1. Carga configuración y velas históricas.
2. Inicia WebSocket multiplexer por chunks.
3. En cada cierre de vela principal:
   - evalúa señales en todos los símbolos,
   - manda señales válidas por Telegram,
   - ejecuta solo una si está permitido.
4. Tras ejecutar:
   - coloca TP/SL,
   - inicia thread de protección/monitoreo,
   - aplica reglas de salida/escala según estado.

## Logs
- Consola: estado, heartbeat, warnings y errores.
- Archivo: `logs/trades.log` (eventos de validación, entrada, salida, monitoreo).

## Buenas prácticas
- Empieza en `BINANCE_TESTNET=true`.
- Usa `USE_PAPER_TRADING=true` para validar lógica sin riesgo.
- No subas `.env` al repositorio.
- Revisa `logs/trades.log` antes de ajustar parámetros.

## Publicación en GitHub
Este repositorio está preparado para push directo con:

```bash
git add .
git commit -m "update readme"
git push
```

