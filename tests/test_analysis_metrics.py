"""Unit tests for Stage 2 pure analysis modules."""
from __future__ import annotations

import pytest

from analysis.metrics import (
    classify_oos_stability,
    compare_period_stats,
    compute_stats,
    compute_wf_summary,
    exclude_top_winners,
    oos_split,
    rolling_windows,
    split_by_dates,
    top_winner_concentration,
)
from analysis.regime import (
    add_regime_labels,
    classify_trend_regime,
    classify_volatility_regime,
    regime_analysis,
)


def _trade(
    pnl: float,
    result: str,
    entry_time: str,
    market_phase: str = "UPTREND",
    ema_spread: float = 0.5,
    vol_ratio: float = 1.2,
    body_ratio: float = 0.5,
) -> dict:
    return {
        "pnl_usdt": pnl,
        "result": result,
        "entry_time": entry_time,
        "market_phase": market_phase,
        "ema_spread": ema_spread,
        "vol_ratio": vol_ratio,
        "body_ratio": body_ratio,
        "symbol": "BTCUSDT",
        "interval": "15m",
        "score": 2.0,
    }


def _sample_trades() -> list[dict]:
    return [
        _trade(10.0, "WIN", "2026-01-01 10:00 UTC", market_phase="UPTREND", ema_spread=0.7),
        _trade(-5.0, "LOSS", "2026-01-05 10:00 UTC", market_phase="MIXED", ema_spread=0.1),
        _trade(6.0, "WIN", "2026-02-01 10:00 UTC", market_phase="UPTREND", ema_spread=0.5),
        _trade(-2.0, "TIMEOUT", "2026-03-01 10:00 UTC", market_phase="DOWNTREND", ema_spread=0.4),
        _trade(4.0, "WIN", "2026-04-01 10:00 UTC", market_phase="UPTREND", ema_spread=0.6),
    ]


@pytest.mark.unit
def test_compute_stats_basic() -> None:
    stats = compute_stats(_sample_trades())
    assert stats["total"] == 5
    assert stats["wins"] == 3
    assert stats["losses"] == 1
    assert stats["timeouts"] == 1
    assert stats["total_pnl"] == pytest.approx(13.0)
    assert stats["profit_factor"] > 1.0


@pytest.mark.unit
def test_top_winner_concentration_and_exclusion() -> None:
    trades = _sample_trades()
    rows = top_winner_concentration(trades, ns=[1, 3])
    assert rows[0]["n"] == 1
    assert rows[0]["top_pnl"] == pytest.approx(10.0)
    assert rows[1]["n"] == 3

    remaining = exclude_top_winners(trades, n=1)
    assert len(remaining) == len(trades) - 1
    assert max(t["pnl_usdt"] for t in remaining) <= 6.0


@pytest.mark.unit
def test_rolling_windows_and_summary() -> None:
    windows = rolling_windows(_sample_trades(), window_days=45, step_days=30)
    assert windows
    assert all("top5_pct" in window for window in windows)

    summary = compute_wf_summary(windows)
    assert summary["total_windows"] >= 1
    assert 0.0 <= summary["pct_pf_gt_1"] <= 100.0
    assert summary["verdict"] in {"stable", "mixed", "unstable", "no_data"}


@pytest.mark.unit
def test_oos_split_by_date_and_pct() -> None:
    trades = _sample_trades()
    ins, oos = oos_split(trades, split_date="2026-03-01")
    assert len(ins) == 3
    assert len(oos) == 2

    ins_pct, oos_pct = oos_split(trades, split_pct=0.6)
    assert len(ins_pct) == 3
    assert len(oos_pct) == 2


@pytest.mark.unit
def test_multi_cut_split_by_dates() -> None:
    trades = _sample_trades()
    periods = split_by_dates(trades, ["2026-02-01", "2026-03-01"])
    assert len(periods) == 3
    assert periods[0][0].startswith("is_before_")
    assert periods[1][0].startswith("oos_1_")
    assert periods[2][0].startswith("oos_2_")


@pytest.mark.unit
def test_compare_period_stats_and_stability_classification() -> None:
    baseline = compute_stats(
        [
            _trade(8.0, "WIN", "2026-01-01 00:00 UTC"),
            _trade(6.0, "WIN", "2026-01-02 00:00 UTC"),
            _trade(-2.0, "LOSS", "2026-01-03 00:00 UTC"),
        ]
    )
    degraded = compute_stats(
        [
            _trade(-5.0, "LOSS", "2026-02-01 00:00 UTC"),
            _trade(-3.0, "LOSS", "2026-02-02 00:00 UTC"),
            _trade(1.0, "WIN", "2026-02-03 00:00 UTC"),
        ]
    )
    cmp = compare_period_stats(baseline, degraded)
    assert cmp["delta_total_pnl"] < 0
    assert cmp["delta_profit_factor"] < 0

    verdict = classify_oos_stability(baseline, degraded)
    assert verdict in {"degradacion_leve", "degradacion_moderada", "degradacion_severa"}


@pytest.mark.unit
def test_regime_classification_and_aggregation() -> None:
    assert classify_trend_regime("UPTREND", 0.8).startswith("trending_up")
    assert classify_trend_regime("DOWNTREND", 0.7).startswith("trending_down")
    assert classify_trend_regime("MIXED", 0.1) == "ranging"

    assert classify_volatility_regime(2.2, 0.6) == "high_volatility"
    assert classify_volatility_regime(0.9, 0.2) == "low_volatility"

    trades = _sample_trades()
    add_regime_labels(trades)
    assert "trend_regime" in trades[0]
    assert "volatility_regime" in trades[0]
    assert "combined_regime" in trades[0]

    report = regime_analysis(_sample_trades())
    assert "by_trend" in report
    assert "by_volatility" in report
    assert "by_combined" in report
