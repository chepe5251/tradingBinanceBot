"""Backtest - EMA Pullback Long-Only strategy (Binance USDT-M Futures).

Downloads historical klines from Binance Futures, runs evaluate_signal candle-by-candle
for M15 / 1H / 4H across the top 300 USDT-M perpetual pairs by volume, simulates trades
using the exact same filters as the live bot, and saves three CSV files:

  - backtest_YYYYMMDD_HHMMSS.csv   : one row per simulated trade with full metadata
  - analysis_YYYYMMDD_HHMMSS.csv   : aggregated stats grouped by interval, score, RSI, etc.
  - equity_YYYYMMDD_HHMMSS.csv     : equity curve with cumulative PnL and drawdown per trade

Architecture - two phases:
  Phase 1 (I/O bound)  : ThreadPoolExecutor(60 workers) downloads all klines in parallel.
                         A RateLimiter class enforces the Binance 2300 weight/min budget.
  Phase 2 (CPU bound)  : ProcessPoolExecutor(cpu_count) simulates trades across all cores.
                         DataFrames are serialized to dicts for cross-process pickling.

Simulation details:
  - Starts at candle 230 (or len/2 for short series) to ensure EMA200 is warm.
  - Checks SL/TP/TIMEOUT candle-by-candle up to MAX_CANDLES_HOLD (50).
  - Mirrors all production filters: 4h SELL block, score < 1.0 block.
  - Indicators are precomputed once per DataFrame and reused by evaluate_signal
    (strategy.py skips recalculation when columns already exist).

Configuration constants (edit at top of file):
  TOP_SYMBOLS          : number of pairs to test (top N by 24h quote volume)
  CANDLES_PER_INTERVAL : klines to fetch per interval (controls time range and API weight)
  MARGIN_PER_TRADE     : USDT margin per trade (default 10)
  LEVERAGE             : leverage applied to each trade (default 20x)
  MAX_CANDLES_HOLD     : max candles before force-closing a trade as TIMEOUT
  SKIP_AFTER_SIGNAL    : candles to skip after a signal to avoid overlapping trades
  MAX_DL_WORKERS       : parallel threads for Phase 1 download
  SIM_WORKERS          : parallel processes for Phase 2 simulation (default 10)

Usage (from repo root):
    python backtest/backtest.py
"""
from __future__ import annotations

import csv
import multiprocessing
import os
import re
import statistics
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
from binance import Client

# -- strategy import (one level up) --
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import from_env  # noqa: E402
from indicators import atr_series, ema, rsi  # noqa: E402
from services.bootstrap_service import DEFAULT_HTF_MAP, build_interval_plan  # noqa: E402
from strategy import StrategyConfig, evaluate_signal  # noqa: E402

# -- configuration --
APP_SETTINGS = from_env()


def _build_backtest_interval_plan(settings) -> tuple[list[str], dict[str, str]]:
    """Reuse runtime interval plan and extend one extra HTF level for backtest coverage."""
    intervals, context_map = build_interval_plan(settings)
    if intervals:
        highest = intervals[-1]
        next_higher = DEFAULT_HTF_MAP.get(highest)
        if next_higher and next_higher not in intervals:
            intervals.append(next_higher)
            context_map[highest] = next_higher
    return intervals, context_map


ENTRY_INTERVALS, ENTRY_CONTEXT_MAP = _build_backtest_interval_plan(APP_SETTINGS)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name, "")
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name, "")
    if not value:
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


TOP_SYMBOLS = _env_int("BACKTEST_TOP_SYMBOLS", 300)
# Import the same hierarchy used by the live bot - ensures backtest
# and production always simulate the exact same intervals and contexts.
INTERVALS: list[str] = list(ENTRY_INTERVALS)  # ["15m", "1h", "4h", "1d"]

CANDLES_PER_INTERVAL: dict[str, int] = {
    "15m": _env_int("BACKTEST_CANDLES_15M", 10000),
    "1h":  _env_int("BACKTEST_CANDLES_1H", 8000),
    "4h":  _env_int("BACKTEST_CANDLES_4H", 3000),
    "1d":  _env_int("BACKTEST_CANDLES_1D", 1000),
}

INITIAL_CAPITAL = 500.0
MARGIN_PER_TRADE = APP_SETTINGS.fixed_margin_per_trade_usdt
LEVERAGE = APP_SETTINGS.leverage
COMMISSION_PCT = 0.0004   # 0.04 % per side (taker)
ATR_PERIOD = APP_SETTINGS.atr_period

# Strategy configuration - mirrors live runtime settings exactly.
_STRATEGY_CFG = StrategyConfig(
    ema_trend=APP_SETTINGS.ema_trend,
    ema_fast=APP_SETTINGS.ema_fast,
    ema_mid=APP_SETTINGS.ema_mid,
    atr_period=ATR_PERIOD,
    atr_avg_window=APP_SETTINGS.atr_avg_window,
    volume_avg_window=APP_SETTINGS.volume_avg_window,
    rsi_period=APP_SETTINGS.rsi_period,
    rsi_long_min=APP_SETTINGS.rsi_long_min,
    rsi_long_max=APP_SETTINGS.rsi_long_max,
    volume_min_ratio=APP_SETTINGS.volume_min_ratio,
    volume_max_ratio=APP_SETTINGS.volume_max_ratio,
    pullback_tolerance_atr=APP_SETTINGS.pullback_tolerance_atr,
    min_ema_spread_atr=APP_SETTINGS.min_ema_spread_atr,
    max_ema_spread_atr=APP_SETTINGS.max_ema_spread_atr,
    min_body_ratio=APP_SETTINGS.min_body_ratio,
    rr_target=APP_SETTINGS.rr_target,
    min_risk_atr=APP_SETTINGS.min_risk_atr,
    max_risk_atr=APP_SETTINGS.max_risk_atr,
    min_score=APP_SETTINGS.min_score,
    max_atr_avg_ratio=APP_SETTINGS.max_atr_avg_ratio,
)
MAX_CANDLES_HOLD = 50   # close at market after this many candles
SKIP_AFTER_SIGNAL = 10  # skip candles after a signal to avoid overlap
MAX_DL_WORKERS = _env_int("BACKTEST_DL_WORKERS", 10)  # I/O download threads
SIM_WORKERS    = _env_int("BACKTEST_SIM_WORKERS", multiprocessing.cpu_count())  # CPU simulation workers
RATE_LIMIT_PER_MIN = _env_int("BACKTEST_RATE_LIMIT_PER_MIN", 1200)
USE_KLINE_CACHE = os.getenv("BACKTEST_USE_CACHE", "1").strip().lower() not in {"0", "false", "no"}
KLINE_CACHE_MAX_AGE_MIN = _env_int("BACKTEST_CACHE_MAX_AGE_MIN", 180)
EVAL_WINDOW_ROWS = _env_int("BACKTEST_EVAL_WINDOW_ROWS", max(260, APP_SETTINGS.ema_trend + 30))
CONTEXT_WINDOW_ROWS = _env_int("BACKTEST_CONTEXT_WINDOW_ROWS", max(260, APP_SETTINGS.ema_trend + 30))
DOWNLOAD_JITTER_BASE_SEC = _env_float("BACKTEST_DOWNLOAD_JITTER_BASE_SEC", 0.05)
DOWNLOAD_JITTER_STEP_SEC = _env_float("BACKTEST_DOWNLOAD_JITTER_STEP_SEC", 0.02)


# -- rate limiter --

class RateLimiter:
    """Thread-safe rate limiter for Binance API weight budget."""

    def __init__(self, max_weight_per_minute: int = RATE_LIMIT_PER_MIN) -> None:
        # Configurable budget under Binance 2400/min hard cap.
        self._max = max_weight_per_minute
        self._lock = threading.Lock()
        self._timestamps: list[tuple[float, int]] = []  # (time, weight)

    def _weight_for_limit(self, limit: int) -> int:
        if limit < 100:
            return 1
        if limit < 500:
            return 2
        if limit < 1000:
            return 5
        return 10

    def acquire(self, limit: int) -> None:
        weight = self._weight_for_limit(limit)
        while True:
            with self._lock:
                now = time.time()
                self._timestamps = [(t, w) for t, w in self._timestamps if now - t < 60]
                current_weight = sum(w for _, w in self._timestamps)
                if current_weight + weight <= self._max:
                    self._timestamps.append((now, weight))
                    return
            time.sleep(0.05)  # Poll every 50ms - aggressive but low CPU


_rate_limiter = RateLimiter(RATE_LIMIT_PER_MIN)


# -- helpers --

def _ban_wait_seconds_from_error(exc: Exception) -> float | None:
    """Parse Binance ban-until timestamp and return remaining seconds."""
    match = re.search(r"banned until\s+(\d+)", str(exc))
    if not match:
        return None
    try:
        ban_until_ms = int(match.group(1))
    except ValueError:
        return None
    remaining = (ban_until_ms / 1000.0) - time.time()
    return max(0.0, remaining + 1.0)


def _sleep_if_banned(exc: Exception, *, max_wait_sec: float = 300.0) -> bool:
    """Sleep until temporary IP ban expires (capped)."""
    wait_sec = _ban_wait_seconds_from_error(exc)
    if wait_sec is None:
        return False
    wait_sec = min(wait_sec, max_wait_sec)
    if wait_sec > 0:
        print(f"[WARN] Rate-limit ban detected; waiting {wait_sec:.1f}s before retry...")
        time.sleep(wait_sec)
    return True


def _klines_cache_path(symbol: str, interval: str, limit: int) -> str:
    cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
    os.makedirs(cache_dir, exist_ok=True)
    safe_symbol   = re.sub(r"[^A-Z0-9_]", "_", symbol.upper())
    safe_interval = re.sub(r"[^A-Z0-9_]", "_", interval.upper())
    cfg_sig = (
        f"e{APP_SETTINGS.ema_fast}"
        f"m{APP_SETTINGS.ema_mid}"
        f"t{APP_SETTINGS.ema_trend}"
        f"a{APP_SETTINGS.atr_period}"
        f"r{APP_SETTINGS.rsi_period}"
    )
    return os.path.join(cache_dir, f"{safe_symbol}_{safe_interval}_{limit}_{cfg_sig}.pkl")


def _load_klines_cache(
    symbol: str,
    interval: str,
    limit: int,
    *,
    allow_stale: bool = False,
) -> pd.DataFrame | None:
    if not USE_KLINE_CACHE:
        return None
    path = _klines_cache_path(symbol, interval, limit)
    if not os.path.exists(path):
        return None
    max_age_sec = (KLINE_CACHE_MAX_AGE_MIN * 60) if not allow_stale else 0
    if max_age_sec > 0:
        age_sec = time.time() - os.path.getmtime(path)
        if age_sec > max_age_sec:
            return None
    try:
        cached = pd.read_pickle(path)
    except Exception:
        return None
    if cached.empty:
        return None
    required_cols = {"open_time", "open", "high", "low", "close", "volume", "close_time"}
    if not required_cols.issubset(set(cached.columns)):
        return None
    return cached


def _save_klines_cache(symbol: str, interval: str, limit: int, df: pd.DataFrame) -> None:
    if not USE_KLINE_CACHE or df.empty:
        return
    path = _klines_cache_path(symbol, interval, limit)
    try:
        df.to_pickle(path)
    except Exception:
        return


def _load_symbols(client: Client) -> list[str]:
    """Return top TOP_SYMBOLS USDT-M perpetual symbols by 24h quote volume."""
    symbol_pattern = re.compile(r"^[A-Z0-9]{2,20}USDT$")
    configured_symbols = [
        symbol
        for symbol in dict.fromkeys(
            s.strip().upper()
            for s in (APP_SETTINGS.symbols + APP_SETTINGS.extra_symbols)
            if isinstance(s, str) and s.strip()
        )
        if symbol_pattern.match(symbol)
    ]
    if configured_symbols and not APP_SETTINGS.use_top_volume_symbols:
        if TOP_SYMBOLS > 0:
            return configured_symbols[:TOP_SYMBOLS]
        return configured_symbols
    info = None
    for attempt in range(2):
        try:
            info = client.futures_exchange_info()
            break
        except Exception as exc:
            print(f"[ERROR] futures_exchange_info: {exc}")
            if attempt == 0 and _sleep_if_banned(exc):
                continue
            fallback_symbols = configured_symbols or [str(APP_SETTINGS.symbol).upper()]
            fallback_symbols = [s for s in fallback_symbols if symbol_pattern.match(s)]
            if TOP_SYMBOLS > 0:
                fallback_symbols = fallback_symbols[:TOP_SYMBOLS]
            if fallback_symbols:
                print(f"[WARN] Using configured-symbol fallback: {len(fallback_symbols)}")
            return fallback_symbols
    if info is None:
        return []

    perp: set[str] = set()
    for s in info.get("symbols", []):
        if (
            s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and symbol_pattern.match(str(s.get("symbol", "")).upper())
        ):
            perp.add(s["symbol"])
    tickers = None
    for attempt in range(2):
        try:
            tickers = client.futures_ticker()
            break
        except Exception as exc:
            print(f"[ERROR] futures_ticker: {exc}")
            if attempt == 0 and _sleep_if_banned(exc):
                continue
            ranked_fallback = sorted(perp)
            if not ranked_fallback:
                ranked_fallback = configured_symbols or [str(APP_SETTINGS.symbol).upper()]
            return ranked_fallback[:TOP_SYMBOLS] if TOP_SYMBOLS > 0 else ranked_fallback
    if tickers is None:
        ranked_fallback = sorted(perp)
        if not ranked_fallback:
            ranked_fallback = configured_symbols or [str(APP_SETTINGS.symbol).upper()]
        return ranked_fallback[:TOP_SYMBOLS] if TOP_SYMBOLS > 0 else ranked_fallback

    vol_map: dict[str, float] = {}
    price_map: dict[str, float] = {}
    for t in tickers:
        sym = t.get("symbol", "")
        if sym in perp:
            try:
                vol_map[sym] = float(t.get("quoteVolume", 0) or 0)
                price_map[sym] = float(t.get("lastPrice", 0) or 0)
            except Exception:
                vol_map[sym] = 0.0
                price_map[sym] = 0.0

    ranked = sorted(perp, key=lambda s: vol_map.get(s, 0.0), reverse=True)
    if APP_SETTINGS.top_volume_min_quote_volume > 0 or APP_SETTINGS.top_volume_min_price > 0:
        ranked = [
            symbol
            for symbol in ranked
            if vol_map.get(symbol, 0.0) >= APP_SETTINGS.top_volume_min_quote_volume
            and price_map.get(symbol, 0.0) >= APP_SETTINGS.top_volume_min_price
        ] or ranked

    top_symbols = ranked[:TOP_SYMBOLS] if TOP_SYMBOLS > 0 else ranked
    for symbol in (
        s.strip().upper()
        for s in APP_SETTINGS.top_volume_allowlist
        if isinstance(s, str) and s.strip()
    ):
        if symbol_pattern.match(symbol) and symbol in perp and symbol not in top_symbols:
            top_symbols.append(symbol)
    return top_symbols


def _fetch_klines(client: Client, symbol: str, interval: str, limit: int) -> pd.DataFrame:
    """Download klines with automatic pagination for limits > 1500."""
    cached = _load_klines_cache(symbol, interval, limit)
    if cached is not None:
        return cached

    MAX_PER_REQUEST = 1500
    all_rows: list[dict] = []
    remaining = limit
    end_time: int | None = None
    max_retries = 4

    while remaining > 0:
        fetch_limit = min(remaining, MAX_PER_REQUEST)
        # Small jitter to spread burst across workers.
        time.sleep(DOWNLOAD_JITTER_BASE_SEC + (hash((symbol, interval, remaining)) % 5) * DOWNLOAD_JITTER_STEP_SEC)
        _rate_limiter.acquire(fetch_limit)
        klines = None
        delay = 1.0
        for attempt in range(max_retries):
            try:
                kwargs: dict = dict(symbol=symbol, interval=interval, limit=fetch_limit)
                if end_time is not None:
                    kwargs["endTime"] = end_time
                klines = client.futures_klines(**kwargs)
                break
            except Exception as exc:
                if _sleep_if_banned(exc, max_wait_sec=600.0):
                    delay = min(delay * 1.5, 10.0)
                    continue
                if attempt < max_retries - 1:
                    print(f"  [RETRY {attempt+1}/{max_retries}] {symbol} {interval}: waiting {delay}s...")
                    time.sleep(delay)
                    delay = min(delay * 2.0, 10.0)
                else:
                    raise exc

        if not klines:
            break

        rows = [
            {
                "open_time":  int(k[0]),
                "open":       float(k[1]),
                "high":       float(k[2]),
                "low":        float(k[3]),
                "close":      float(k[4]),
                "volume":     float(k[5]),
                "close_time": int(k[6]),
            }
            for k in klines
        ]
        all_rows = rows + all_rows  # prepend - keeps chronological order
        remaining -= len(klines)

        if len(klines) < fetch_limit:
            break  # Binance returned fewer rows - no more history available

        end_time = int(klines[0][0]) - 1  # next page ends before the first candle in this batch

    if not all_rows:
        stale = _load_klines_cache(symbol, interval, limit, allow_stale=True)
        return stale if stale is not None else pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time").reset_index(drop=True)
    df["open_time"]  = pd.to_datetime(df["open_time"],  unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    # Precalcular indicadores con la config activa antes de cachear.
    # strategy.py reutiliza columnas existentes y no las recalcula en Phase 2.
    df["ema_fast"]  = ema(df["close"], APP_SETTINGS.ema_fast)
    df["ema_mid"]   = ema(df["close"], APP_SETTINGS.ema_mid)
    df["ema_trend"] = ema(df["close"], APP_SETTINGS.ema_trend)
    df["atr"]       = atr_series(df, ATR_PERIOD)
    df["atr_avg"]   = df["atr"].rolling(max(1, APP_SETTINGS.atr_avg_window)).mean()
    df["avg_vol"]   = df["volume"].rolling(max(1, APP_SETTINGS.volume_avg_window)).mean()
    df["rsi"]       = rsi(df["close"], APP_SETTINGS.rsi_period)

    _save_klines_cache(symbol, interval, limit, df)
    return df


def _fmt_ts(ts) -> str:
    """Format a close_time value to a readable UTC string."""
    if isinstance(ts, pd.Timestamp):
        return ts.strftime("%Y-%m-%d %H:%M UTC")
    return str(ts)


# -- local indicator helpers (for extra CSV fields) --

def _ema_col(series: pd.Series, period: int) -> pd.Series:
    return ema(series, period)


def _atr_col(df: pd.DataFrame, period: int) -> pd.Series:
    return atr_series(df, period)


def _rsi_col(series: pd.Series, period: int = 14) -> pd.Series:
    return rsi(series, period)


# -- simulation --

def _simulate_trades(
    df: pd.DataFrame,
    symbol: str,
    interval: str,
    context_df: pd.DataFrame | None = None,
) -> tuple[list[dict], int, int, dict[str, int], dict[str, int]]:
    """Walk through df candle-by-candle and simulate every signal.

    Returns (trades, skipped_4h_sell, skipped_low_score, rejects, diagnostics).
    """
    # Reusar indicadores precalculados si ya vienen en el DataFrame (cache hit).
    # Solo recalcular columnas que falten.
    df = df.copy()
    ema20   = df["ema_fast"]  if "ema_fast"  in df.columns else _ema_col(df["close"], APP_SETTINGS.ema_fast)
    ema50   = df["ema_mid"]   if "ema_mid"   in df.columns else _ema_col(df["close"], APP_SETTINGS.ema_mid)
    ema200  = df["ema_trend"] if "ema_trend" in df.columns else _ema_col(df["close"], APP_SETTINGS.ema_trend)
    atr     = df["atr"]       if "atr"       in df.columns else _atr_col(df, ATR_PERIOD)
    atr_avg = df["atr_avg"]   if "atr_avg"   in df.columns else atr.rolling(max(1, APP_SETTINGS.atr_avg_window)).mean()
    avg_vol = df["avg_vol"]   if "avg_vol"   in df.columns else df["volume"].rolling(APP_SETTINGS.volume_avg_window).mean()
    rsi     = df["rsi"]       if "rsi"       in df.columns else _rsi_col(df["close"], APP_SETTINGS.rsi_period)
    for col, series in [
        ("ema_fast",  ema20),
        ("ema_mid",   ema50),
        ("ema_trend", ema200),
        ("atr",       atr),
        ("atr_avg",   atr_avg),
        ("avg_vol",   avg_vol),
        ("rsi",       rsi),
    ]:
        if col not in df.columns:
            df[col] = series

    trades: list[dict] = []
    rejects: dict[str, int] = {}
    diagnostics = {"eval_calls": 0, "signals": 0, "eval_exceptions": 0}
    skipped_4h_sell = 0
    skipped_low_score = 0
    i = min(230, max(20, len(df) // 2))  # warm-up: use half the data or 230, min 20
    n = len(df)
    high_arr = df["high"].to_numpy()
    low_arr = df["low"].to_numpy()
    open_arr = df["open"].to_numpy()
    close_arr = df["close"].to_numpy()
    vol_arr = df["volume"].to_numpy()
    close_time_arr = df["close_time"].to_numpy()
    close_time_ns = df["close_time"].astype("int64").to_numpy()
    close_index_by_ns = {int(ts): idx for idx, ts in enumerate(close_time_ns)}
    empty_context = pd.DataFrame()
    context_sub = empty_context
    prev_ctx_pos = -1
    has_context = context_df is not None and not context_df.empty
    context_close_ns = None
    if has_context:
        context_df = context_df.copy()
        if "ema_mid" not in context_df.columns:
            context_df["ema_mid"] = ema(context_df["close"], APP_SETTINGS.ema_mid)
        if "ema_trend" not in context_df.columns:
            context_df["ema_trend"] = ema(context_df["close"], APP_SETTINGS.ema_trend)
        context_close_ns = context_df["close_time"].astype("int64").to_numpy()

    def _safe(series: pd.Series, idx: int) -> float:
        v = series.iloc[idx]
        return float(v) if not pd.isna(v) else 0.0

    while i < n - 1:
        sub_start = max(0, i - EVAL_WINDOW_ROWS + 1)
        sub = df.iloc[sub_start : i + 1]
        if has_context and context_close_ns is not None:
            cutoff_ns = int(close_time_ns[i])
            ctx_pos = int(context_close_ns.searchsorted(cutoff_ns, side="right"))
            if ctx_pos != prev_ctx_pos:
                ctx_start = max(0, ctx_pos - CONTEXT_WINDOW_ROWS)
                context_sub = context_df.iloc[ctx_start:ctx_pos] if ctx_pos > 0 else context_df.iloc[:0]
                prev_ctx_pos = ctx_pos
        else:
            context_sub = empty_context
        diagnostics["eval_calls"] += 1
        try:
            signal = evaluate_signal(
                sub,
                context_sub,
                _STRATEGY_CFG,
                interval=interval,
                rejects=rejects,
            )
        except Exception:
            diagnostics["eval_exceptions"] += 1
            i += 1
            continue

        # Second pass: try mean-reversion short on 15m when long fails.
        if signal is None and interval == "15m":
            try:
                signal = evaluate_signal(
                    sub,
                    context_sub,
                    _STRATEGY_CFG,
                    interval="15m_short",
                    rejects=rejects,
                )
            except Exception:
                diagnostics["eval_exceptions"] += 1

        if signal is None:
            i += 1
            continue
        diagnostics["signals"] += 1

        # Mirror production filters exactly
        if interval in APP_SETTINGS.block_sell_on_intervals and signal.get("side") == "SELL":
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

        # entry_time: close_time of confirmation candle (df.iloc[i])
        entry_time = _fmt_ts(close_time_arr[i])

        # -- extra fields from precomputed indicators --
        # Determine setup candle index for CSV metadata.
        strategy_name = signal.get("strategy", "ema_pullback_long")
        sig_idx = i - 1  # default: confirmation-based entry (EMA pullback and NR4)
        if strategy_name == "bos_retest_4h":
            breakout_raw = signal.get("breakout_time")
            if breakout_raw:
                breakout_ts = pd.to_datetime(str(breakout_raw), utc=True, errors="coerce")
                if not pd.isna(breakout_ts):
                    breakout_ns = int(breakout_ts.value)
                    mapped_idx = close_index_by_ns.get(breakout_ns)
                    if mapped_idx is not None:
                        sig_idx = int(mapped_idx)
                    else:
                        # Fallback to nearest candle around the entry window.
                        w_start = max(0, i - 8)
                        w_ns = close_time_ns[w_start : i + 1]
                        if len(w_ns) > 0:
                            nearest_local = int((abs(w_ns - breakout_ns)).argmin())
                            nearest_abs = w_start + nearest_local
                            if abs(int(close_time_ns[nearest_abs]) - breakout_ns) <= int(pd.Timedelta(hours=8).value):
                                sig_idx = nearest_abs
        elif strategy_name == "nr4_breakout_1d":
            sig_idx = i - 1  # signal candle is the breakout candle
        if sig_idx < 0 or sig_idx >= len(df):
            sig_idx = max(0, i - 1)

        e20     = _safe(ema20,   sig_idx)
        e50     = _safe(ema50,   sig_idx)
        e200    = _safe(ema200,  sig_idx)
        atr_sig = _safe(atr,     sig_idx)
        avv_sig = _safe(avg_vol, sig_idx)
        rsi_sig = _safe(rsi,     sig_idx)

        s_high  = float(high_arr[sig_idx])
        s_low   = float(low_arr[sig_idx])
        s_open  = float(open_arr[sig_idx])
        s_close = float(close_arr[sig_idx])
        s_vol   = float(vol_arr[sig_idx])

        rng        = s_high - s_low
        body       = abs(s_close - s_open)
        body_ratio = body / rng if rng > 0 else 0.0
        vol_ratio  = s_vol / avv_sig if avv_sig > 0 else 0.0
        ema_spread = (e20 - e50) / atr_sig if atr_sig > 0 else 0.0
        dist_tp    = abs(tp_price - entry_price)
        dist_sl    = abs(entry_price - stop_price)
        rr_planned = dist_tp / dist_sl if dist_sl > 0 else 0.0

        if e20 > e50 and e50 > e200:
            market_phase = "UPTREND"
        elif e20 < e50 and e50 < e200:
            market_phase = "DOWNTREND"
        else:
            market_phase = "MIXED"

        qty        = (MARGIN_PER_TRADE * LEVERAGE) / entry_price
        commission = qty * entry_price * COMMISSION_PCT * 2

        # Simulate: scan next candles for SL/TP hit
        result     = "TIMEOUT"
        timeout_j  = min(i + MAX_CANDLES_HOLD, n - 1)
        exit_price = float(close_arr[timeout_j])
        exit_j     = timeout_j
        candles_held = 0

        for j in range(i + 1, min(i + MAX_CANDLES_HOLD + 1, n)):
            candles_held = j - i
            c_high = float(high_arr[j])
            c_low  = float(low_arr[j])

            if side == "BUY":
                if c_high >= tp_price and c_low <= stop_price:
                    result = "LOSS"
                    exit_price = stop_price
                    exit_j = j
                    break
                if c_high >= tp_price:
                    result = "WIN"
                    exit_price = tp_price
                    exit_j = j
                    break
                if c_low <= stop_price:
                    result = "LOSS"
                    exit_price = stop_price
                    exit_j = j
                    break
            else:  # SELL
                if c_low <= tp_price and c_high >= stop_price:
                    result = "LOSS"
                    exit_price = stop_price
                    exit_j = j
                    break
                if c_low <= tp_price:
                    result = "WIN"
                    exit_price = tp_price
                    exit_j = j
                    break
                if c_high >= stop_price:
                    result = "LOSS"
                    exit_price = stop_price
                    exit_j = j
                    break

        exit_time = _fmt_ts(close_time_arr[exit_j])

        if side == "BUY":
            gross_pnl = (exit_price - entry_price) * qty
        else:
            gross_pnl = (entry_price - exit_price) * qty
        pnl_usdt = gross_pnl - commission

        trades.append({
            "symbol":        symbol,
            "interval":      interval,
            "side":          side,
            "entry_time":    entry_time,
            "exit_time":     exit_time,
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
            "ema_spread":      round(ema_spread, 4),
            "rsi_at_signal":   round(rsi_sig, 2),
            "vol_ratio":       round(vol_ratio, 3),
            "body_ratio":      round(body_ratio, 3),
            "distance_to_tp":  round(dist_tp, 8),
            "distance_to_sl":  round(dist_sl, 8),
            "rr_planned":      round(rr_planned, 3),
            "market_phase":    market_phase,
        })

        i += SKIP_AFTER_SIGNAL

    return trades, skipped_4h_sell, skipped_low_score, rejects, diagnostics


# -- stats helpers --

def _compute_stats(trades: list[dict]) -> dict:
    """Return aggregated statistics for a list of trades."""
    if not trades:
        return {
            "total": 0, "wins": 0, "losses": 0, "timeouts": 0,
            "winrate": 0.0, "total_pnl": 0.0, "avg_pnl": 0.0,
            "median_pnl": 0.0, "avg_win": 0.0, "avg_loss": 0.0,
            "rr_real": 0.0, "profit_factor": 0.0,
            "expectancy": 0.0, "best_trade": 0.0, "worst_trade": 0.0,
        }
    wins     = [t for t in trades if t["result"] == "WIN"]
    losses   = [t for t in trades if t["result"] == "LOSS"]
    timeouts = [t for t in trades if t["result"] == "TIMEOUT"]
    total    = len(trades)
    pnls         = [t["pnl_usdt"] for t in trades]
    total_pnl    = sum(pnls)
    avg_pnl      = total_pnl / total
    median_pnl   = statistics.median(pnls)
    best_trade   = max(pnls)
    worst_trade  = min(pnls)
    avg_win      = sum(t["pnl_usdt"] for t in wins)   / len(wins)   if wins   else 0.0
    avg_loss     = sum(t["pnl_usdt"] for t in losses) / len(losses) if losses else 0.0
    rr_real      = avg_win / abs(avg_loss) if losses and avg_loss != 0 else 0.0
    gross_profit = sum(p for p in pnls if p > 0)
    gross_loss   = abs(sum(p for p in pnls if p < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0
    winrate_frac  = len(wins) / total
    expectancy    = winrate_frac * avg_win + (1.0 - winrate_frac) * avg_loss
    return {
        "total":         total,
        "wins":          len(wins),
        "losses":        len(losses),
        "timeouts":      len(timeouts),
        "winrate":       winrate_frac * 100,
        "total_pnl":     total_pnl,
        "avg_pnl":       avg_pnl,
        "median_pnl":    median_pnl,
        "avg_win":       avg_win,
        "avg_loss":      avg_loss,
        "rr_real":       rr_real,
        "profit_factor": profit_factor,
        "expectancy":    expectancy,
        "best_trade":    best_trade,
        "worst_trade":   worst_trade,
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
    if r < 48:
        return "<48"
    if r < 52:
        return "48-52"
    if r < 56:
        return "52-56"
    if r < 60:
        return "56-60"
    if r < 64:
        return "60-64"
    if r < 68:
        return "64-68"
    return ">=68"


def _spread_range(t: dict) -> str:
    s = abs(float(t.get("ema_spread", 0)))
    if s < 0.30:
        return "<0.30"
    if s < 0.50:
        return "0.30-0.50"
    if s < 0.65:
        return "0.50-0.65"
    if s < 0.80:
        return "0.65-0.80"
    if s < 1.00:
        return "0.80-1.00"
    return ">=1.00"


def _body_range(t: dict) -> str:
    b = float(t.get("body_ratio", 0))
    if b < 0.45:
        return "<0.45"
    if b < 0.60:
        return "0.45-0.60"
    if b < 0.75:
        return "0.60-0.75"
    if b < 0.90:
        return "0.75-0.90"
    return ">=0.90"


def _hour_range(t: dict) -> str:
    try:
        h = int(t.get("entry_time", "00:00")[-9:-7])
    except (ValueError, IndexError):
        return "unknown"
    if h < 6:
        return "00-06"
    if h < 12:
        return "06-12"
    if h < 18:
        return "12-18"
    return "18-24"


def _weekday_range(t: dict) -> str:
    try:
        dt = datetime.strptime(t.get("entry_time", "")[:10], "%Y-%m-%d")
        return dt.strftime("%a")
    except (ValueError, IndexError):
        return "unknown"


def _group_by(trades: list[dict], key_fn) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for t in trades:
        k = key_fn(t)
        groups.setdefault(k, []).append(t)
    return groups


# -- CSV output --

def _save_csv(all_trades: list[dict], ts: str) -> str:
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(results_dir, exist_ok=True)
    path = os.path.join(results_dir, f"backtest_{ts}.csv")
    default_fieldnames = [
        "symbol", "interval", "side", "entry_time", "exit_time",
        "entry_price", "exit_price", "stop_price", "tp_price",
        "pnl_usdt", "result", "candles_held", "score", "signal_candle",
        "ema_spread", "rsi_at_signal", "vol_ratio", "body_ratio",
        "distance_to_tp", "distance_to_sl", "rr_planned", "market_phase",
    ]
    fieldnames = list(all_trades[0].keys()) if all_trades else default_fieldnames
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_trades)
    return path


def _save_analysis_csv(all_trades: list[dict], ts: str) -> str:
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(results_dir, exist_ok=True)
    path = os.path.join(results_dir, f"analysis_{ts}.csv")

    fieldnames = [
        "categoria", "valor", "total_trades", "wins", "losses", "timeouts",
        "winrate", "total_pnl", "avg_pnl", "median_pnl", "avg_win", "avg_loss",
        "rr_real", "profit_factor", "expectancy", "best_trade", "worst_trade",
    ]

    def _rows_for(categoria: str, groups: dict[str, list[dict]]) -> list[dict]:
        rows = []
        for valor, trades in sorted(groups.items()):
            s = _compute_stats(trades)
            rows.append({
                "categoria":     categoria,
                "valor":         valor,
                "total_trades":  s["total"],
                "wins":          s["wins"],
                "losses":        s["losses"],
                "timeouts":      s["timeouts"],
                "winrate":       round(s["winrate"], 2),
                "total_pnl":     round(s["total_pnl"], 4),
                "avg_pnl":       round(s["avg_pnl"], 4),
                "median_pnl":    round(s["median_pnl"], 4),
                "avg_win":       round(s["avg_win"], 4),
                "avg_loss":      round(s["avg_loss"], 4),
                "rr_real":       round(s["rr_real"], 3),
                "profit_factor": round(s["profit_factor"], 3),
                "expectancy":    round(s["expectancy"], 4),
                "best_trade":    round(s["best_trade"], 4),
                "worst_trade":   round(s["worst_trade"], 4),
            })
        return rows

    all_rows: list[dict] = []
    all_rows += _rows_for("interval",
                          _group_by(all_trades, lambda t: t["interval"]))
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
    all_rows += _rows_for("ema_spread",
                          _group_by(all_trades, _spread_range))
    all_rows += _rows_for("body_ratio",
                          _group_by(all_trades, _body_range))
    all_rows += _rows_for("hour_utc",
                          _group_by(all_trades, _hour_range))
    all_rows += _rows_for("weekday",
                          _group_by(all_trades, _weekday_range))

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    return path


def _save_equity_csv(all_trades: list[dict], ts: str) -> str:
    """Save equity curve CSV with cumulative PnL and drawdown per trade."""
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(results_dir, exist_ok=True)
    path = os.path.join(results_dir, f"equity_{ts}.csv")

    fieldnames = ["trade_number", "entry_time", "symbol", "interval",
                  "pnl_usdt", "cumulative_pnl", "peak", "drawdown"]

    rows = []
    cum_pnl = 0.0
    peak    = 0.0
    for n, t in enumerate(all_trades, start=1):
        cum_pnl += t["pnl_usdt"]
        peak     = max(peak, cum_pnl)
        drawdown = cum_pnl - peak
        rows.append({
            "trade_number":  n,
            "entry_time":    t.get("entry_time", ""),
            "symbol":        t["symbol"],
            "interval":      t["interval"],
            "pnl_usdt":      round(t["pnl_usdt"], 4),
            "cumulative_pnl": round(cum_pnl, 4),
            "peak":          round(peak, 4),
            "drawdown":      round(drawdown, 4),
        })

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


# -- console report --

def _print_report(
    all_trades: list[dict],
    skipped_4h_sell: int,
    skipped_low_score: int,
    csv_path: str,
    analysis_path: str,
    equity_path: str,
) -> None:
    SEP = "=" * 65

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
                f"  {k:<22} | {s['total']:>5} trades | "
                f"WR: {s['winrate']:>5.1f}% | "
                f"PF: {s['profit_factor']:>4.2f} | "
                f"PnL: {_usd(s['total_pnl'])}"
            )

    # -- Section 1: General summary --
    _section("1. OVERALL SUMMARY")
    g = _compute_stats(all_trades)
    if g["total"] == 0:
        print("  No trades.")
    else:
        be_wr = 1 / (1 + g["rr_real"]) * 100 if g["rr_real"] > 0 else 0.0
        print(f"  Total trades   : {g['total']}")
        print(f"  Wins / Losses / Timeouts : {g['wins']} / {g['losses']} / {g['timeouts']}")
        print(f"  Winrate        : {g['winrate']:.2f}%")
        print(f"  PnL total      : {_usd(g['total_pnl'])}")
        print(f"  Avg PnL        : {_usd(g['avg_pnl'])}")
        print(f"  Median PnL     : {_usd(g['median_pnl'])}")
        print(f"  Expectancy     : {_usd(g['expectancy'])}")
        print(f"  Best trade     : {_usd(g['best_trade'])}")
        print(f"  Worst trade    : {_usd(g['worst_trade'])}")
        print(f"  Avg WIN        : {_usd(g['avg_win'])}")
        print(f"  Avg LOSS       : {_usd(g['avg_loss'])}")
        print(f"  RR real        : {g['rr_real']:.2f}")
        print(f"  Profit Factor  : {g['profit_factor']:.2f}")
        print(f"  WR breakeven   : {be_wr:.1f}%")

    # -- Section 2: By timeframe + side --
    _section("2. BY TIMEFRAME AND SIDE")
    tf_side = _group_by(all_trades, lambda t: f"{t['interval']} {t['side']}")
    order2 = ["15m BUY", "15m SELL", "1h BUY", "1h SELL", "4h BUY", "4h SELL"]
    _print_group(tf_side, order2)

    # -- Section 3: By score range --
    _section("3. BY SCORE")
    _print_group(_group_by(all_trades, _score_range), ["0-1", "1-2", "2-3", "3-4", "4+"])

    # -- Section 4: By candles held --
    _section("4. BY DURATION (candles held)")
    _print_group(_group_by(all_trades, _hold_range), ["0-5", "5-10", "10-20", "20-35", "35+"])

    # -- Section 5: By market phase --
    _section("5. BY MARKET PHASE")
    _print_group(_group_by(all_trades, lambda t: str(t.get("market_phase", "MIXED"))),
                 ["UPTREND", "DOWNTREND", "MIXED"])

    # -- Section 6: By vol_ratio --
    _section("6. BY VOLUME (vol_ratio)")
    _print_group(_group_by(all_trades, _vol_range), ["<1.5", "1.5-2.0", "2.0-3.0", ">3.0"])

    # -- Section 7: By RSI --
    _section("7. BY RSI AT SIGNAL")
    _print_group(_group_by(all_trades, _rsi_range),
                 ["<48", "48-52", "52-56", "56-60", "60-64", "64-68", ">=68"])

    # -- Section 8: Top 5 best / worst symbols --
    _section("8. TOP 5 BEST AND WORST PAIRS")
    sym_groups = _group_by(all_trades, lambda t: t["symbol"])
    sym_stats  = {sym: _compute_stats(ts) for sym, ts in sym_groups.items()}
    by_pnl     = sorted(sym_stats.items(), key=lambda x: x[1]["total_pnl"], reverse=True)

    print("  BEST:")
    for sym, s in by_pnl[:5]:
        print(f"    {sym:<15} | {s['total']:>4} trades | WR: {s['winrate']:>5.1f}% | "
              f"PF: {s['profit_factor']:>4.2f} | PnL: {_usd(s['total_pnl'])}")

    print("  WORST:")
    for sym, s in by_pnl[-5:]:
        print(f"    {sym:<15} | {s['total']:>4} trades | WR: {s['winrate']:>5.1f}% | "
              f"PF: {s['profit_factor']:>4.2f} | PnL: {_usd(s['total_pnl'])}")

    # -- Section 9: Discarded trades --
    _section("9. FILTERED TRADES")
    print(f"  4H SELL filtered   : {skipped_4h_sell}")
    print(f"  Score < 1.0        : {skipped_low_score}")

    # -- Section 10: Equity curve and drawdown --
    _section("10. EQUITY CURVE AND DRAWDOWN")
    if all_trades:
        cum_pnl    = 0.0
        peak       = 0.0
        max_dd      = 0.0
        max_dd_time = ""
        in_drawdown = False
        dd_peak     = 0.0

        max_cons_wins = max_cons_losses = 0
        streak_w = streak_l = 0

        for t in all_trades:
            cum_pnl += t["pnl_usdt"]
            peak     = max(peak, cum_pnl)
            dd       = cum_pnl - peak
            if dd < max_dd:
                max_dd      = dd
                max_dd_time = t.get("entry_time", "")
                dd_peak     = peak
                in_drawdown = True

            if in_drawdown and cum_pnl >= dd_peak:
                in_drawdown = False

            # consecutive streaks
            if t["result"] == "WIN":
                streak_w += 1
                streak_l  = 0
            elif t["result"] == "LOSS":
                streak_l += 1
                streak_w  = 0
            max_cons_wins   = max(max_cons_wins,  streak_w)
            max_cons_losses = max(max_cons_losses, streak_l)

        max_dd_pct = (max_dd / dd_peak * 100) if dd_peak > 0 else 0.0
        calmar     = g["total_pnl"] / abs(max_dd) if max_dd != 0 else 0.0
        recovered  = not in_drawdown

        print(f"  Max drawdown (USDT)  : {_usd(max_dd)}")
        print(f"  Max drawdown (%)     : {max_dd_pct:.2f}%")
        print(f"  Drawdown at          : {max_dd_time}")
        print(f"  Recovered            : {'Yes' if recovered else 'No (still open at end)'}")
        print(f"  Calmar ratio         : {calmar:.2f}")
        print(f"  Max cons. wins       : {max_cons_wins}")
        print(f"  Max cons. losses     : {max_cons_losses}")

    # -- Section 11: Trade frequency --
    _section("11. TRADE FREQUENCY")
    if all_trades:
        days_count: dict[str, int] = {}
        days_by_interval: dict[str, dict[str, int]] = {iv: {} for iv in INTERVALS}

        for t in all_trades:
            day = t.get("entry_time", "")[:10]
            if not day:
                continue
            days_count[day] = days_count.get(day, 0) + 1
            iv = t["interval"]
            if iv in days_by_interval:
                days_by_interval[iv][day] = days_by_interval[iv].get(day, 0) + 1

        counts = sorted(days_count.values())
        n_days = len(counts)
        if n_days:
            avg_trades = sum(counts) / n_days
            med_idx    = n_days // 2
            med_trades = counts[med_idx]
            print(f"  Active days          : {n_days}")
            print(f"  Avg trades/day       : {avg_trades:.1f}")
            print(f"  Median trades/day    : {med_trades}")
            print(f"  Min trades/day       : {counts[0]}")
            print(f"  Max trades/day       : {counts[-1]}")
            print()
            for iv in INTERVALS:
                iv_counts = sorted(days_by_interval[iv].values())
                if iv_counts:
                    avg_iv = sum(iv_counts) / len(iv_counts)
                    print(f"  {iv} -> {avg_iv:.1f} avg trades/day ({len(iv_counts)} active days)")

    # -- Section 12: Top-winner concentration --
    _section("12. TOP WINNER CONCENTRATION")
    if all_trades:
        total_pnl_all = sum(t["pnl_usdt"] for t in all_trades)
        sorted_by_pnl = sorted(all_trades, key=lambda t: t["pnl_usdt"], reverse=True)
        for n in (1, 5, 10, 20):
            n_eff = min(n, len(all_trades))
            top_n = sorted_by_pnl[:n_eff]
            rest  = sorted_by_pnl[n_eff:]
            top_pnl = sum(t["pnl_usdt"] for t in top_n)
            pct = (top_pnl / total_pnl_all * 100) if total_pnl_all != 0 else 0.0
            rest_s = _compute_stats(rest)
            print(
                f"  Top {n_eff:<3} trades -> {_usd(top_pnl):>10}  ({pct:>+6.1f}% of total PnL) | "
                f"Rest: WR {rest_s['winrate']:>5.1f}%  PF {rest_s['profit_factor']:>4.2f}  "
                f"Exp {rest_s['expectancy']:>+.3f}"
            )
        print()
        print("  Interpretation: if top-5 trades concentrate >70% of PnL,")
        print("  statistical edge may be fragile (outlier dependency).")

    # -- Section 13: Files generated --
    _section("13. GENERATED FILES")
    print(f"  Trades CSV   : {csv_path}")
    print(f"  Analysis CSV : {analysis_path}")
    print(f"  Equity CSV   : {equity_path}")
    print()


# -- parallel worker --

_thread_local = threading.local()


def _get_client(api_key: str, api_secret: str) -> Client:
    """Return a per-thread Binance client (created once per thread)."""
    if not hasattr(_thread_local, "client"):
        _thread_local.client = Client(api_key, api_secret)
    return _thread_local.client


def _download_one(args: tuple) -> tuple[str, str, pd.DataFrame | None, str | None]:
    """Download klines for one (sym, interval). Used in Phase 1."""
    sym, interval, api_key, api_secret = args
    client = _get_client(api_key, api_secret)
    limit = CANDLES_PER_INTERVAL[interval]
    try:
        df = _fetch_klines(client, sym, interval, limit)
        if len(df) < 10:
            return sym, interval, None, f"insufficient data ({len(df)} candles)"
        return sym, interval, df, None
    except Exception as exc:
        stale = _load_klines_cache(sym, interval, limit, allow_stale=True)
        if stale is not None and len(stale) >= 10:
            return sym, interval, stale, None
        return sym, interval, None, f"error: {exc}"


# -- simulation task (top-level for pickle / ProcessPoolExecutor) --

def _simulate_task(args: tuple) -> tuple[str, str, list[dict], int, int, dict[str, int], dict[str, int]]:
    """Pickleable wrapper for ProcessPoolExecutor - Phase 2."""
    df_dict, context_dict, sym, interval = args
    df = pd.DataFrame(df_dict)
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], utc=True)

    context_df = pd.DataFrame()
    if context_dict:
        context_df = pd.DataFrame(context_dict)
        context_df["open_time"] = pd.to_datetime(context_df["open_time"], utc=True)
        context_df["close_time"] = pd.to_datetime(context_df["close_time"], utc=True)

    trades, s4h, ssc, rejects, diagnostics = _simulate_trades(df, sym, interval, context_df=context_df)
    return sym, interval, trades, s4h, ssc, rejects, diagnostics


def _simulate_task_by_symbol(args: tuple) -> tuple[str, list[dict], int, int, dict[str, int], dict[str, dict[str, int]], dict[str, int]]:
    """Procesa todos los intervalos de un símbolo en un solo worker."""
    sym, intervals_data = args
    # intervals_data = {interval: (df_dict, context_dict)}

    all_trades: list[dict] = []
    total_s4h = 0
    total_ssc = 0
    all_rejects: dict[str, int] = {}
    rejects_by_interval: dict[str, dict[str, int]] = {}
    all_diagnostics: dict[str, int] = {"eval_calls": 0, "signals": 0, "eval_exceptions": 0}

    for interval, (df_dict, context_dict) in intervals_data.items():
        df = pd.DataFrame(df_dict)
        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], utc=True)

        context_df = pd.DataFrame()
        if context_dict:
            context_df = pd.DataFrame(context_dict)
            context_df["open_time"] = pd.to_datetime(context_df["open_time"], utc=True)
            context_df["close_time"] = pd.to_datetime(context_df["close_time"], utc=True)

        trades, s4h, ssc, rejects, diagnostics = _simulate_trades(df, sym, interval, context_df=context_df)
        all_trades.extend(trades)
        total_s4h += s4h
        total_ssc += ssc
        rejects_by_interval[interval] = dict(rejects)
        for k, v in rejects.items():
            all_rejects[k] = all_rejects.get(k, 0) + v
        for k, v in diagnostics.items():
            all_diagnostics[k] = all_diagnostics.get(k, 0) + int(v)

    return sym, all_trades, total_s4h, total_ssc, all_rejects, rejects_by_interval, all_diagnostics


# -- main --

def main() -> None:
    api_key = os.getenv("BINANCE_API_KEY", "")
    api_secret = os.getenv("BINANCE_API_SECRET", "")

    client = Client(api_key, api_secret)

    print("Loading symbols...")
    symbols = _load_symbols(client)
    if not symbols:
        print("[ERROR] Could not load symbols.")
        return
    print(f"Selected symbols: {len(symbols)}")

    download_tasks = [
        (sym, interval, api_key, api_secret)
        for sym in symbols for interval in INTERVALS
    ]
    total_dl = len(download_tasks)

    # Phase 1: Parallel download
    print(f"\nPhase 1: Downloading {total_dl} datasets with {MAX_DL_WORKERS} workers...")
    kline_data: dict[tuple[str, str], pd.DataFrame] = {}
    dl_done = 0
    dl_errors = 0
    t0 = time.time()
    interrupted_phase1 = False

    executor = ThreadPoolExecutor(max_workers=MAX_DL_WORKERS)
    futures = {}
    try:
        futures = {executor.submit(_download_one, t): t for t in download_tasks}
        for future in as_completed(futures):
            dl_done += 1
            sym, interval, df, err = future.result()
            if err:
                dl_errors += 1
            else:
                kline_data[(sym, interval)] = df  # type: ignore[index]
            if dl_done % 100 == 0 or dl_done == total_dl:
                elapsed   = time.time() - t0
                rate      = dl_done / elapsed if elapsed > 0 else 1
                remaining = (total_dl - dl_done) / rate if rate > 0 else 0
                print(f"  Downloaded: {dl_done}/{total_dl} | "
                      f"{elapsed:.0f}s elapsed | ~{remaining:.0f}s remaining | "
                      f"{dl_errors} errors")
    except KeyboardInterrupt:
        interrupted_phase1 = True
        print("\n[WARN] Interrupted during download phase. Continuing with downloaded datasets...")
    finally:
        executor.shutdown(wait=not interrupted_phase1, cancel_futures=interrupted_phase1)

    _dl_end = time.time()
    print(f"Phase 1 complete: {len(kline_data)} datasets OK, {dl_errors} errors "
          f"in {_dl_end - t0:.0f}s")

    # Phase 2: Parallel simulation (CPU-bound -> ProcessPoolExecutor)
    all_trades: list[dict] = []
    total_skipped_4h_sell   = 0
    total_skipped_low_score = 0
    reject_totals: dict[str, int] = {}
    reject_by_interval: dict[str, dict[str, int]] = {iv: {} for iv in INTERVALS}
    eval_calls_total = 0
    eval_signals_total = 0
    eval_exceptions_total = 0
    sim_done  = 0
    interrupted_phase2 = False

    print(f"\n{'=' * 60}")
    print("  PHASE 2: TRADE SIMULATION")
    print(f"{'=' * 60}")
    print(f"  Using {SIM_WORKERS} parallel processes")

    # Agrupar por símbolo: 1 tarea por símbolo en lugar de 1 por (símbolo × intervalo).
    # Reduce IPC de ~1200 tareas a ~300.
    context_by_interval: dict[str, str] = ENTRY_CONTEXT_MAP

    sym_args: dict[str, dict] = {}
    for (sym, interval), df in kline_data.items():
        df_send = df.copy()
        df_send["open_time"]  = df_send["open_time"].astype(str)
        df_send["close_time"] = df_send["close_time"].astype(str)

        context_dict: dict = {}
        context_interval = context_by_interval.get(interval)
        if context_interval:
            context_df = kline_data.get((sym, context_interval))
            if context_df is not None and not context_df.empty:
                ctx_send = context_df.copy()
                ctx_send["open_time"]  = ctx_send["open_time"].astype(str)
                ctx_send["close_time"] = ctx_send["close_time"].astype(str)
                context_dict = ctx_send.to_dict("list")

        sym_args.setdefault(sym, {})[interval] = (df_send.to_dict("list"), context_dict)

    sim_args = list(sym_args.items())  # [(sym, {interval: (df_dict, ctx_dict)}), ...]
    total_sim = len(sim_args)
    del kline_data  # free memory

    t1 = time.time()
    executor = ProcessPoolExecutor(max_workers=SIM_WORKERS)
    futures = {}
    try:
        futures = {
            executor.submit(_simulate_task_by_symbol, args): args[0]
            for args in sim_args
        }
        for future in as_completed(futures):
            sim_done += 1
            sym = futures[future]
            try:
                sym, trades, s4h, ssc, rejects, rejects_by_iv, diagnostics = future.result()
                all_trades.extend(trades)
                total_skipped_4h_sell   += s4h
                total_skipped_low_score += ssc
                eval_calls_total      += int(diagnostics.get("eval_calls", 0))
                eval_signals_total    += int(diagnostics.get("signals", 0))
                eval_exceptions_total += int(diagnostics.get("eval_exceptions", 0))
                for k, v in rejects.items():
                    reject_totals[k] = reject_totals.get(k, 0) + int(v)
                for iv, iv_rejects in rejects_by_iv.items():
                    dest = reject_by_interval.setdefault(iv, {})
                    for k, v in iv_rejects.items():
                        dest[k] = dest.get(k, 0) + int(v)
            except Exception as exc:
                print(f"  [SIM ERROR] {sym}: {exc}")
            if sim_done % 50 == 0 or sim_done == total_sim:
                elapsed   = time.time() - t1
                rate      = sim_done / elapsed if elapsed > 0 else 1
                remaining = (total_sim - sim_done) / rate if rate > 0 else 0
                print(f"  Simulated: {sim_done}/{total_sim} | "
                      f"{elapsed:.0f}s | ~{remaining:.0f}s remaining")
    except KeyboardInterrupt:
        interrupted_phase2 = True
        print("\n[WARN] Interrupted during simulation phase. Saving partial results...")
    finally:
        executor.shutdown(wait=not interrupted_phase2, cancel_futures=interrupted_phase2)

    del sim_args  # free memory

    _total = time.time() - t0
    print(f"\n{'=' * 60}")
    print("  EXECUTION SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Download:   {_dl_end - t0:.0f}s")
    print(f"  Simulation: {time.time() - t1:.0f}s")
    print(f"  TOTAL:      {_total:.0f}s ({_total / 60:.1f} min)")
    print(f"  evaluate_signal calls: {eval_calls_total}")
    print(f"  Signals emitted (pre-trade): {eval_signals_total}")
    print(f"  Exceptions in evaluate_signal: {eval_exceptions_total}")
    print(f"  Trades generated: {len(all_trades)}")
    if interrupted_phase1 or interrupted_phase2:
        print("  NOTE: partial run (interrupted by user)")

    reject_keys = [
        "reject_trend",
        "reject_spread_min",
        "reject_spread_max",
        "reject_spread_dead_zone",
        "reject_pullback",
        "reject_structure",
        "reject_rsi",
        "reject_body",
        "reject_volume",
        "reject_volume_extreme",
        "reject_confirmation",
        "reject_extension",
        "reject_bos_trend",
        "reject_bos_missing",
        "reject_bos_volume",
        "reject_bos_retest_zone",
        "reject_bos_retest_body",
        "reject_bos_retest_rsi",
        "reject_bos_atr_spike",
        "reject_bos_risk",
        "reject_bos_score",
        "reject_bos_htf",
        "reject_nr4_trend",
        "reject_nr4_compression",
        "reject_nr4_breakout",
        "reject_nr4_signal_body",
        "reject_nr4_signal_volume",
        "reject_nr4_signal_rsi",
        "reject_nr4_confirmation",
        "reject_nr4_htf",
        "reject_nr4_risk",
        "reject_nr4_score",
        "reject_nr4_atr_spike",
        "reject_htf",
        "reject_risk",
        "reject_score",
        "reject_atr_spike",
    ]
    print(f"\n{'=' * 60}")
    print("  FILTER DIAGNOSTIC (reject_*)")
    print(f"{'=' * 60}")
    total_rejects = sum(reject_totals.values())
    print(f"  Total rejects counted: {total_rejects}")
    for key in reject_keys:
        print(f"  {key:<24}: {reject_totals.get(key, 0)}")

    print(f"\n{'=' * 60}")
    print("  REJECTS BY INTERVAL")
    print(f"{'=' * 60}")
    for iv in INTERVALS:
        iv_counts = reject_by_interval.get(iv, {})
        iv_total = sum(iv_counts.values())
        print(f"  {iv}: {iv_total}")
        for key in reject_keys:
            value = iv_counts.get(key, 0)
            if value > 0:
                print(f"    {key}: {value}")

    if not all_trades:
        print("\n[WARN] No trades were generated; review the filter diagnostic above.")

    # Sort chronologically before saving - ensures equity curve is meaningful
    all_trades.sort(key=lambda t: t.get("entry_time", ""))

    # Shared timestamp for all output files
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path      = _save_csv(all_trades, ts)
    analysis_path = _save_analysis_csv(all_trades, ts)
    equity_path   = _save_equity_csv(all_trades, ts)

    _print_report(
        all_trades,
        total_skipped_4h_sell,
        total_skipped_low_score,
        csv_path,
        analysis_path,
        equity_path,
    )


if __name__ == "__main__":
    main()



