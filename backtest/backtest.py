"""Backtest — EMA Pullback momentum strategy.

Downloads historical Binance Futures klines, runs evaluate_signal candle-by-candle
for M15 / 1H / 4H across the top 50 USDT-M pairs, calculates metrics, and saves
two CSV files: one with individual trades and one with aggregated analysis.

Usage (from repo root):
    python backtest/backtest.py
"""
from __future__ import annotations

import csv
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
from binance import Client

# ── strategy import (one level up) ───────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from strategy import evaluate_signal  # noqa: E402

# ── configuration ─────────────────────────────────────────────────────────────
BACKTEST_DAYS = 30
TOP_SYMBOLS = 150
INTERVALS = ["15m", "1h", "4h"]
CANDLES_PER_INTERVAL: dict[str, int] = {"15m": 1500, "1h": 720, "4h": 500}
INITIAL_CAPITAL = 500.0
MARGIN_PER_TRADE = 5.0
LEVERAGE = 20
COMMISSION_PCT = 0.0004   # 0.04 % per side (taker)
ATR_PERIOD = 14

# evaluate_signal parameters — same defaults as main.py
_EVAL_KWARGS: dict = dict(
    ema_trend=200,
    ema_fast=20,
    ema_mid=50,
    atr_period=ATR_PERIOD,
    atr_avg_window=20,
    volume_avg_window=20,
    rsi_period=14,
    rsi_long_min=40.0,
    rsi_long_max=70.0,
    rsi_short_min=30.0,
    rsi_short_max=60.0,
    volume_min_ratio=1.0,
)

MAX_CANDLES_HOLD = 50   # close at market after this many candles
SKIP_AFTER_SIGNAL = 10  # skip candles after a signal to avoid overlap
MAX_WORKERS = 20        # parallel download threads


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_env() -> dict[str, str]:
    """Load key=value pairs from ../.env relative to this script."""
    env: dict[str, str] = {}
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
    env_path = os.path.normpath(env_path)
    if not os.path.exists(env_path):
        return env
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            env[key.strip()] = val.strip().strip('"').strip("'")
    return env


def _load_symbols(client: Client) -> list[str]:
    """Return top TOP_SYMBOLS USDT-M perpetual symbols by 24h quote volume."""
    try:
        info = client.futures_exchange_info()
    except Exception as exc:
        print(f"[ERROR] futures_exchange_info: {exc}")
        return []

    perp: set[str] = set()
    for s in info.get("symbols", []):
        if (
            s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and str(s.get("symbol", "")).endswith("USDT")
        ):
            perp.add(s["symbol"])

    try:
        tickers = client.futures_ticker()
    except Exception as exc:
        print(f"[ERROR] futures_ticker: {exc}")
        return sorted(perp)[:TOP_SYMBOLS]

    vol_map: dict[str, float] = {}
    for t in tickers:
        sym = t.get("symbol", "")
        if sym in perp:
            try:
                vol_map[sym] = float(t.get("quoteVolume", 0) or 0)
            except Exception:
                vol_map[sym] = 0.0

    ranked = sorted(perp, key=lambda s: vol_map.get(s, 0.0), reverse=True)
    return ranked[:TOP_SYMBOLS]


def _fetch_klines(client: Client, symbol: str, interval: str, limit: int) -> pd.DataFrame:
    """Download klines and return a clean DataFrame."""
    max_retries = 3
    klines = None
    for attempt in range(max_retries):
        try:
            klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
            time.sleep(0.3)  # pausa entre requests exitosas
            break
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 5 * (attempt + 1)  # 5s, 10s, 15s
                print(f"  [RETRY {attempt+1}/{max_retries}] {symbol} {interval}: esperando {wait}s...")
                time.sleep(wait)
            else:
                raise e
    rows = [
        {
            "open_time": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "close_time": int(k[6]),
        }
        for k in klines
    ]
    df = pd.DataFrame(rows)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df


# ── local indicator helpers (for extra CSV fields) ────────────────────────────

def _ema_col(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _atr_col(df: pd.DataFrame, period: int) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def _rsi_col(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


# ── simulation ────────────────────────────────────────────────────────────────

def _simulate_trades(df: pd.DataFrame, symbol: str, interval: str) -> tuple[list[dict], int, int]:
    """Walk through df candle-by-candle and simulate every signal.

    Returns (trades, skipped_4h_sell, skipped_low_score).
    """
    # Precompute indicators once for extra CSV fields
    ema20  = _ema_col(df["close"], 20)
    ema50  = _ema_col(df["close"], 50)
    ema200 = _ema_col(df["close"], 200)
    atr    = _atr_col(df, ATR_PERIOD)
    avg_vol = df["volume"].rolling(20).mean()
    rsi    = _rsi_col(df["close"], 14)

    trades: list[dict] = []
    skipped_4h_sell = 0
    skipped_low_score = 0
    i = min(230, max(20, len(df) // 2))  # warm-up: use half the data or 230, min 20
    n = len(df)

    while i < n - 1:
        sub = df.iloc[: i + 1]
        try:
            signal = evaluate_signal(sub, pd.DataFrame(), **_EVAL_KWARGS)
        except Exception:
            i += 1
            continue

        if signal is None:
            i += 1
            continue

        # Mirror production filters exactly
        if interval == "4h" and signal.get("side") == "SELL":
            skipped_4h_sell += 1
            i += 1
            continue

        if float(signal.get("score") or 0) < 1.0:
            skipped_low_score += 1
            i += 1
            continue

        entry_price = float(signal["price"])
        stop_price  = float(signal["stop_price"])
        tp_price    = float(signal["tp_price"])
        side  = signal["side"]
        score = float(signal.get("score") or 0.0)

        if entry_price <= 0 or stop_price <= 0 or tp_price <= 0:
            i += 1
            continue

        # ── extra fields from precomputed indicators ──────────────────────────
        # Signal candle = sub.iloc[-2] = df.iloc[i-1]
        sig_idx = i - 1
        def _safe(series: pd.Series, idx: int) -> float:
            v = series.iloc[idx]
            return float(v) if not pd.isna(v) else 0.0

        e20     = _safe(ema20,   sig_idx)
        e50     = _safe(ema50,   sig_idx)
        e200    = _safe(ema200,  sig_idx)
        atr_sig = _safe(atr,     sig_idx)
        avv_sig = _safe(avg_vol, sig_idx)
        rsi_sig = _safe(rsi,     sig_idx)

        sig_row = df.iloc[sig_idx]
        s_high  = float(sig_row["high"])
        s_low   = float(sig_row["low"])
        s_open  = float(sig_row["open"])
        s_close = float(sig_row["close"])
        s_vol   = float(sig_row["volume"])

        rng          = s_high - s_low
        body         = abs(s_close - s_open)
        body_ratio   = body / rng if rng > 0 else 0.0
        vol_ratio    = s_vol / avv_sig if avv_sig > 0 else 0.0
        ema_spread   = (e20 - e50) / atr_sig if atr_sig > 0 else 0.0
        dist_tp      = abs(tp_price - entry_price)
        dist_sl      = abs(entry_price - stop_price)
        rr_planned   = dist_tp / dist_sl if dist_sl > 0 else 0.0

        if e20 > e50 and e50 > e200:
            market_phase = "UPTREND"
        elif e20 < e50 and e50 < e200:
            market_phase = "DOWNTREND"
        else:
            market_phase = "MIXED"

        qty        = (MARGIN_PER_TRADE * LEVERAGE) / entry_price
        commission = qty * entry_price * COMMISSION_PCT * 2

        # Simulate: scan next candles for SL/TP hit
        result      = "TIMEOUT"
        exit_price  = float(df.iloc[min(i + MAX_CANDLES_HOLD, n - 1)]["close"])
        candles_held = 0

        for j in range(i + 1, min(i + MAX_CANDLES_HOLD + 1, n)):
            candle = df.iloc[j]
            candles_held = j - i
            c_high = float(candle["high"])
            c_low  = float(candle["low"])

            if side == "BUY":
                if c_high >= tp_price and c_low <= stop_price:
                    result = "LOSS"
                    exit_price = stop_price
                    break
                if c_high >= tp_price:
                    result = "WIN"
                    exit_price = tp_price
                    break
                if c_low <= stop_price:
                    result = "LOSS"
                    exit_price = stop_price
                    break
            else:  # SELL
                if c_low <= tp_price and c_high >= stop_price:
                    result = "LOSS"
                    exit_price = stop_price
                    break
                if c_low <= tp_price:
                    result = "WIN"
                    exit_price = tp_price
                    break
                if c_high >= stop_price:
                    result = "LOSS"
                    exit_price = stop_price
                    break

        if side == "BUY":
            gross_pnl = (exit_price - entry_price) * qty
        else:
            gross_pnl = (entry_price - exit_price) * qty
        pnl_usdt = gross_pnl - commission

        trades.append({
            "symbol":        symbol,
            "interval":      interval,
            "side":          side,
            "entry_price":   round(entry_price, 8),
            "exit_price":    round(exit_price, 8),
            "stop_price":    round(stop_price, 8),
            "tp_price":      round(tp_price, 8),
            "pnl_usdt":      round(pnl_usdt, 4),
            "result":        result,
            "candles_held":  candles_held,
            "score":         score,
            "signal_candle": i,
            # Extra fields
            "ema_spread":       round(ema_spread, 4),
            "rsi_at_signal":    round(rsi_sig, 2),
            "vol_ratio":        round(vol_ratio, 3),
            "body_ratio":       round(body_ratio, 3),
            "distance_to_tp":   round(dist_tp, 8),
            "distance_to_sl":   round(dist_sl, 8),
            "rr_planned":       round(rr_planned, 3),
            "market_phase":     market_phase,
        })

        i += SKIP_AFTER_SIGNAL

    return trades, skipped_4h_sell, skipped_low_score


# ── stats helpers ─────────────────────────────────────────────────────────────

def _compute_stats(trades: list[dict]) -> dict:
    """Return aggregated statistics for a list of trades."""
    if not trades:
        return {
            "total": 0, "wins": 0, "losses": 0, "timeouts": 0,
            "winrate": 0.0, "total_pnl": 0.0, "avg_pnl": 0.0,
            "avg_win": 0.0, "avg_loss": 0.0, "rr_real": 0.0,
        }
    wins    = [t for t in trades if t["result"] == "WIN"]
    losses  = [t for t in trades if t["result"] == "LOSS"]
    timeouts = [t for t in trades if t["result"] == "TIMEOUT"]
    total   = len(trades)
    total_pnl = sum(t["pnl_usdt"] for t in trades)
    avg_pnl   = total_pnl / total
    avg_win   = sum(t["pnl_usdt"] for t in wins)   / len(wins)   if wins   else 0.0
    avg_loss  = sum(t["pnl_usdt"] for t in losses) / len(losses) if losses else 0.0
    rr_real   = avg_win / abs(avg_loss) if losses and avg_loss != 0 else 0.0
    return {
        "total":     total,
        "wins":      len(wins),
        "losses":    len(losses),
        "timeouts":  len(timeouts),
        "winrate":   len(wins) / total * 100,
        "total_pnl": total_pnl,
        "avg_pnl":   avg_pnl,
        "avg_win":   avg_win,
        "avg_loss":  avg_loss,
        "rr_real":   rr_real,
    }


def _score_range(t: dict) -> str:
    s = float(t["score"])
    if s < 1:
        return "0-1"
    if s < 2:
        return "1-2"
    if s < 3:
        return "2-3"
    if s < 4:
        return "3-4"
    return "4+"


def _hold_range(t: dict) -> str:
    h = int(t["candles_held"])
    if h <= 5:
        return "0-5"
    if h <= 10:
        return "5-10"
    if h <= 20:
        return "10-20"
    if h <= 35:
        return "20-35"
    return "35+"


def _vol_range(t: dict) -> str:
    v = float(t.get("vol_ratio", 0))
    if v < 1.5:
        return "<1.5"
    if v < 2.0:
        return "1.5-2.0"
    if v < 3.0:
        return "2.0-3.0"
    return ">3.0"


def _rsi_range(t: dict) -> str:
    r = float(t.get("rsi_at_signal", 0))
    if r < 40:
        return "<40"
    if r < 50:
        return "40-50"
    if r < 60:
        return "50-60"
    return ">60"


def _group_by(trades: list[dict], key_fn) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for t in trades:
        k = key_fn(t)
        groups.setdefault(k, []).append(t)
    return groups


# ── CSV output ────────────────────────────────────────────────────────────────

def _save_csv(all_trades: list[dict]) -> str:
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(results_dir, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(results_dir, f"backtest_{ts}.csv")
    if not all_trades:
        return path
    fieldnames = list(all_trades[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_trades)
    return path


def _save_analysis_csv(all_trades: list[dict]) -> str:
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(results_dir, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(results_dir, f"analysis_{ts}.csv")

    fieldnames = [
        "categoria", "valor", "total_trades", "wins", "losses", "timeouts",
        "winrate", "total_pnl", "avg_pnl", "avg_win", "avg_loss", "rr_real",
    ]

    def _rows_for(categoria: str, groups: dict[str, list[dict]]) -> list[dict]:
        rows = []
        for valor, trades in sorted(groups.items()):
            s = _compute_stats(trades)
            rows.append({
                "categoria":    categoria,
                "valor":        valor,
                "total_trades": s["total"],
                "wins":         s["wins"],
                "losses":       s["losses"],
                "timeouts":     s["timeouts"],
                "winrate":      round(s["winrate"], 2),
                "total_pnl":    round(s["total_pnl"], 4),
                "avg_pnl":      round(s["avg_pnl"], 4),
                "avg_win":      round(s["avg_win"], 4),
                "avg_loss":     round(s["avg_loss"], 4),
                "rr_real":      round(s["rr_real"], 3),
            })
        return rows

    all_rows: list[dict] = []
    all_rows += _rows_for("interval_side",
                          _group_by(all_trades, lambda t: f"{t['interval']}_{t['side']}"))
    all_rows += _rows_for("score_range",
                          _group_by(all_trades, _score_range))
    all_rows += _rows_for("candles_held",
                          _group_by(all_trades, _hold_range))
    all_rows += _rows_for("market_phase",
                          _group_by(all_trades, lambda t: str(t.get("market_phase", "MIXED"))))
    all_rows += _rows_for("vol_ratio",
                          _group_by(all_trades, _vol_range))
    all_rows += _rows_for("rsi_at_signal",
                          _group_by(all_trades, _rsi_range))

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    return path


# ── console report ────────────────────────────────────────────────────────────

def _print_report(
    all_trades: list[dict],
    skipped_4h_sell: int,
    skipped_low_score: int,
    csv_path: str,
    analysis_path: str,
) -> None:
    SEP = "=" * 60

    def _usd(v: float) -> str:
        return f"+${v:.2f}" if v >= 0 else f"-${abs(v):.2f}"

    def _section(title: str) -> None:
        print(f"\n{SEP}")
        print(f"  {title}")
        print(SEP)

    def _print_group(groups: dict[str, list[dict]], order: list[str] | None = None) -> None:
        keys = order if order else sorted(groups.keys())
        for k in keys:
            if k not in groups:
                continue
            s = _compute_stats(groups[k])
            print(
                f"  {k:<20} | {s['total']:>5} trades | "
                f"WR: {s['winrate']:>5.1f}% | "
                f"PnL: {_usd(s['total_pnl'])}"
            )

    # ── Section 1: General summary ────────────────────────────────────────────
    _section("1. RESUMEN GENERAL")
    g = _compute_stats(all_trades)
    if g["total"] == 0:
        print("  Sin trades.")
    else:
        be_wr = 1 / (1 + g["rr_real"]) * 100 if g["rr_real"] > 0 else 0.0
        print(f"  Total trades : {g['total']}")
        print(f"  Wins / Losses / Timeouts : {g['wins']} / {g['losses']} / {g['timeouts']}")
        print(f"  Winrate      : {g['winrate']:.2f}%")
        print(f"  PnL total    : {_usd(g['total_pnl'])}")
        print(f"  Avg PnL      : {_usd(g['avg_pnl'])}")
        print(f"  Avg WIN      : {_usd(g['avg_win'])}")
        print(f"  Avg LOSS     : {_usd(g['avg_loss'])}")
        print(f"  RR real      : {g['rr_real']:.2f}")
        print(f"  WR breakeven : {be_wr:.1f}%")

    # ── Section 2: By timeframe + side ───────────────────────────────────────
    _section("2. POR TIMEFRAME Y SIDE")
    tf_side = _group_by(all_trades, lambda t: f"{t['interval']} {t['side']}")
    order2 = ["15m BUY", "15m SELL", "1h BUY", "1h SELL", "4h BUY", "4h SELL"]
    _print_group(tf_side, order2)

    # ── Section 3: By score range ─────────────────────────────────────────────
    _section("3. POR SCORE")
    _print_group(_group_by(all_trades, _score_range), ["0-1", "1-2", "2-3", "3-4", "4+"])

    # ── Section 4: By candles held ────────────────────────────────────────────
    _section("4. POR DURACION (candles held)")
    _print_group(_group_by(all_trades, _hold_range), ["0-5", "5-10", "10-20", "20-35", "35+"])

    # ── Section 5: By market phase ────────────────────────────────────────────
    _section("5. POR MARKET PHASE")
    _print_group(_group_by(all_trades, lambda t: str(t.get("market_phase", "MIXED"))),
                 ["UPTREND", "DOWNTREND", "MIXED"])

    # ── Section 6: By vol_ratio ───────────────────────────────────────────────
    _section("6. POR VOLUMEN (vol_ratio)")
    _print_group(_group_by(all_trades, _vol_range), ["<1.5", "1.5-2.0", "2.0-3.0", ">3.0"])

    # ── Section 7: By RSI ─────────────────────────────────────────────────────
    _section("7. POR RSI EN SEÑAL")
    _print_group(_group_by(all_trades, _rsi_range), ["<40", "40-50", "50-60", ">60"])

    # ── Section 8: Top 5 best / worst symbols ────────────────────────────────
    _section("8. TOP 5 MEJORES Y PEORES PARES")
    sym_groups = _group_by(all_trades, lambda t: t["symbol"])
    sym_stats = {sym: _compute_stats(ts) for sym, ts in sym_groups.items()}
    by_pnl = sorted(sym_stats.items(), key=lambda x: x[1]["total_pnl"], reverse=True)

    print("  MEJORES:")
    for sym, s in by_pnl[:5]:
        print(f"    {sym:<15} | {s['total']:>4} trades | WR: {s['winrate']:>5.1f}% | PnL: {_usd(s['total_pnl'])}")

    print("  PEORES:")
    for sym, s in by_pnl[-5:]:
        print(f"    {sym:<15} | {s['total']:>4} trades | WR: {s['winrate']:>5.1f}% | PnL: {_usd(s['total_pnl'])}")

    # ── Section 9: Discarded trades ───────────────────────────────────────────
    _section("9. TRADES DESCARTADOS")
    print(f"  4H SELL filtrados  : {skipped_4h_sell}")
    print(f"  Score < 1.0        : {skipped_low_score}")

    # ── Section 10: Files generated ───────────────────────────────────────────
    _section("10. ARCHIVOS GENERADOS")
    print(f"  Trades CSV   : {csv_path}")
    print(f"  Analysis CSV : {analysis_path}")
    print()


# ── parallel worker ───────────────────────────────────────────────────────────

_thread_local = threading.local()


def _get_client(api_key: str, api_secret: str) -> Client:
    """Return a per-thread Binance client (created once per thread)."""
    if not hasattr(_thread_local, "client"):
        _thread_local.client = Client(api_key, api_secret)
    return _thread_local.client


def _process_task(
    api_key: str, api_secret: str, sym: str, interval: str
) -> tuple[str, str, list[dict], int, int, str | None]:
    """Fetch klines and simulate trades for one (symbol, interval) pair."""
    client = _get_client(api_key, api_secret)
    limit = CANDLES_PER_INTERVAL[interval]
    try:
        df = _fetch_klines(client, sym, interval, limit)
    except Exception as exc:
        return sym, interval, [], 0, 0, f"error: {exc}"
    if len(df) < 10:
        return sym, interval, [], 0, 0, f"datos insuficientes ({len(df)} velas)"
    trades, s4h, ssc = _simulate_trades(df, sym, interval)
    return sym, interval, trades, s4h, ssc, None


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    env = _load_env()
    api_key    = env.get("BINANCE_API_KEY", "")
    api_secret = env.get("BINANCE_API_SECRET", "")

    client = Client(api_key, api_secret)

    print("Cargando símbolos...")
    symbols = _load_symbols(client)
    if not symbols:
        print("[ERROR] No se pudieron cargar los símbolos.")
        return
    print(f"Símbolos seleccionados: {len(symbols)}")

    all_trades: list[dict] = []
    total_skipped_4h_sell  = 0
    total_skipped_low_score = 0
    tasks = [(api_key, api_secret, sym, interval)
             for sym in symbols for interval in INTERVALS]
    total_tasks = len(tasks)
    task_num = 0

    print(f"Iniciando con {MAX_WORKERS} workers paralelos...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_process_task, *t): t for t in tasks}
        for future in as_completed(futures):
            task_num += 1
            sym, interval, trades, s4h, ssc, skip_reason = future.result()
            if skip_reason:
                print(f"  [SKIP] {sym} {interval}: {skip_reason} ({task_num}/{total_tasks})")
            else:
                all_trades.extend(trades)
                total_skipped_4h_sell  += s4h
                total_skipped_low_score += ssc
                print(f"  {sym} {interval} → {len(trades)} señales ({task_num}/{total_tasks})")

    csv_path      = _save_csv(all_trades)
    analysis_path = _save_analysis_csv(all_trades)

    _print_report(
        all_trades,
        total_skipped_4h_sell,
        total_skipped_low_score,
        csv_path,
        analysis_path,
    )


if __name__ == "__main__":
    main()
