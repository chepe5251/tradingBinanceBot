"""Unit tests for offline filter-experiment framework."""
from __future__ import annotations

import pytest

from analysis.filter_experiments import (
    apply_filter,
    combine_filters,
    compare_variants,
    filter_exclude_hours,
    filter_exclude_symbols,
    filter_exclude_weekdays,
    filter_interval,
    filter_market_phase,
    filter_min_score,
    render_experiment_markdown,
)


def _trade(
    score: float,
    pnl: float,
    entry_time: str,
    symbol: str = "BTCUSDT",
    interval: str = "15m",
    market_phase: str = "UPTREND",
) -> dict:
    return {
        "score": score,
        "pnl_usdt": pnl,
        "entry_time": entry_time,
        "symbol": symbol,
        "interval": interval,
        "market_phase": market_phase,
        "result": "WIN" if pnl > 0 else "LOSS",
        "vol_ratio": 1.2,
        "body_ratio": 0.5,
        "ema_spread": 0.4,
    }


def _trades() -> list[dict]:
    return [
        _trade(2.5, 8.0, "2026-01-05 10:00 UTC", symbol="BTCUSDT", interval="15m"),
        _trade(1.2, -4.0, "2026-01-05 02:00 UTC", symbol="ETHUSDT", interval="15m"),
        _trade(2.0, 5.0, "2026-01-06 14:00 UTC", symbol="BTCUSDT", interval="1h"),
        _trade(0.9, -3.0, "2026-01-10 09:00 UTC", symbol="XRPUSDT", interval="15m", market_phase="MIXED"),
    ]


@pytest.mark.unit
def test_apply_filter_and_compare_variants() -> None:
    trades = _trades()
    min_score_filter = filter_min_score(2.0)
    kept = apply_filter(trades, min_score_filter)
    assert len(kept) == 2

    variants = {
        "baseline": None,
        "score>=2": min_score_filter,
        "btc_only": filter_exclude_symbols({"ETHUSDT", "XRPUSDT"}),
    }
    results = compare_variants(trades, variants)
    assert results["baseline"]["n_trades"] == 4
    assert results["score>=2"]["n_trades"] == 2
    assert results["btc_only"]["n_trades"] == 2


@pytest.mark.unit
def test_filter_combinations_hours_days_phase_interval() -> None:
    trades = _trades()
    combined = combine_filters(
        [
            filter_min_score(1.5),
            filter_interval({"15m"}),
            filter_market_phase({"UPTREND"}),
            filter_exclude_hours({2}),
            filter_exclude_weekdays({"Sat", "Sun"}),
        ]
    )
    results = compare_variants(trades, {"baseline": None, "combo": combined})
    assert results["combo"]["n_trades"] <= results["baseline"]["n_trades"]


@pytest.mark.unit
def test_render_experiment_markdown() -> None:
    results = compare_variants(_trades(), {"baseline": None, "score>=2": filter_min_score(2.0)})
    md = render_experiment_markdown(results)
    assert "| Variant | Trades | Kept % | Winrate % | PF | Expectancy | PnL | Max DD % | Top5 % |" in md
    assert "baseline" in md
