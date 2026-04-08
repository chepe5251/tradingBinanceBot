from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from strategy import StrategyConfig, evaluate_signal

pytestmark = pytest.mark.unit


def _fixtures_dir() -> Path:
    return Path(__file__).resolve().parent / "fixtures"


def _load_fixture(name: str) -> dict:
    with (_fixtures_dir() / name).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _build_golden_dataframe() -> pd.DataFrame:
    n = 80
    idx = pd.date_range("2025-01-01", periods=n, freq="1h", tz="UTC")
    base = 100 + (0.25 * np.arange(n))
    open_ = base - 0.15
    close = base + 0.15
    high = close + 0.10
    low = open_ - 0.40
    volume = np.full(n, 120.0)

    return pd.DataFrame(
        {
            "open_time": idx,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "close_time": idx,
            # Precomputed indicators to avoid env-dependent recomputation in backtest.
            "ema_fast": close - 0.20,
            "ema_mid": close - 0.50,
            "ema_trend": close - 1.00,
            "atr": np.full(n, 1.0),
            "atr_avg": np.full(n, 1.0),
            "avg_vol": np.full(n, 100.0),
            "rsi": np.full(n, 55.0),
        }
    )


def _golden_cfg() -> StrategyConfig:
    return StrategyConfig(
        ema_fast=3,
        ema_mid=5,
        ema_trend=8,
        atr_period=3,
        atr_avg_window=3,
        volume_avg_window=3,
        rsi_period=3,
        rsi_long_min=0.0,
        rsi_long_max=100.0,
        volume_min_ratio=0.5,
        volume_max_ratio=10.0,
        pullback_tolerance_atr=2.0,
        min_ema_spread_atr=0.1,
        max_ema_spread_atr=10.0,
        min_body_ratio=0.2,
        rr_target=2.0,
        min_risk_atr=0.2,
        max_risk_atr=5.0,
        min_score=0.0,
        context_missing_penalty=0.0,
        max_atr_avg_ratio=10.0,
    )


def _canonical_signal(signal: dict) -> dict:
    return {
        "side": signal["side"],
        "price": round(float(signal["price"]), 8),
        "stop_price": round(float(signal["stop_price"]), 8),
        "tp_price": round(float(signal["tp_price"]), 8),
        "risk_per_unit": round(float(signal["risk_per_unit"]), 8),
        "rr_target": round(float(signal["rr_target"]), 8),
        "atr": round(float(signal["atr"]), 8),
        "score": round(float(signal["score"]), 8),
        "htf_bias": str(signal["htf_bias"]),
        "strategy": str(signal["strategy"]),
        "confirm_m15": str(signal["confirm_m15"]),
        "breakout_time": str(signal["breakout_time"]),
    }


@contextmanager
def _patched_backtest_globals():
    import backtest.backtest as bt

    original = {
        "_STRATEGY_CFG": bt._STRATEGY_CFG,
        "SKIP_AFTER_SIGNAL": bt.SKIP_AFTER_SIGNAL,
        "MARGIN_PER_TRADE": bt.MARGIN_PER_TRADE,
        "LEVERAGE": bt.LEVERAGE,
        "COMMISSION_PCT": bt.COMMISSION_PCT,
        "EVAL_WINDOW_ROWS": bt.EVAL_WINDOW_ROWS,
        "CONTEXT_WINDOW_ROWS": bt.CONTEXT_WINDOW_ROWS,
    }
    try:
        bt._STRATEGY_CFG = _golden_cfg()
        bt.SKIP_AFTER_SIGNAL = 5
        bt.MARGIN_PER_TRADE = 10.0
        bt.LEVERAGE = 20
        bt.COMMISSION_PCT = 0.0004
        bt.EVAL_WINDOW_ROWS = 120
        bt.CONTEXT_WINDOW_ROWS = 120
        yield bt
    finally:
        for key, value in original.items():
            setattr(bt, key, value)


def _canonical_simulation_payload(
    trades: list[dict],
    skipped_4h_sell: int,
    skipped_low_score: int,
    rejects: dict[str, int],
    diagnostics: dict[str, int],
) -> dict:
    return {
        "trade_count": len(trades),
        "skipped_4h_sell": int(skipped_4h_sell),
        "skipped_low_score": int(skipped_low_score),
        "rejects": dict(rejects),
        "diagnostics": {k: int(v) for k, v in diagnostics.items()},
        "total_pnl": round(sum(float(t["pnl_usdt"]) for t in trades), 4),
        "wins": sum(1 for t in trades if t["result"] == "WIN"),
        "losses": sum(1 for t in trades if t["result"] == "LOSS"),
        "timeouts": sum(1 for t in trades if t["result"] == "TIMEOUT"),
        "trades": trades,
    }


def test_strategy_signal_matches_golden_fixture() -> None:
    df = _build_golden_dataframe()
    signal = evaluate_signal(df.iloc[:41], pd.DataFrame(), _golden_cfg(), interval="1h")
    assert signal is not None

    expected = _load_fixture("golden_strategy_signal_1h.json")
    actual = _canonical_signal(signal)
    assert actual == expected


def test_backtest_simulator_matches_golden_fixture() -> None:
    df = _build_golden_dataframe()
    with _patched_backtest_globals() as bt:
        trades, s4h, ssc, rejects, diagnostics = bt._simulate_trades(
            df=df,
            symbol="TESTUSDT",
            interval="1h",
            context_df=pd.DataFrame(),
        )
    actual = _canonical_simulation_payload(trades, s4h, ssc, rejects, diagnostics)
    expected = _load_fixture("golden_backtest_sim_1h.json")
    assert actual == expected

