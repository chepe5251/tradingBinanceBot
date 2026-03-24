"""Deterministic market-regime classification for Stage 2 offline analysis.

This module labels each trade with regime categories using fields already
present in backtest CSV rows (`market_phase`, `ema_spread`, `vol_ratio`,
`body_ratio`).

It is strictly an offline analysis layer. It does NOT change live runtime
logic, position sizing, leverage, exposure, or entry/exit behavior.

Method limits:
- Classification is based on signal-time snapshots, not full OHLC history.
- `ema_spread` is used as a trend-strength proxy (normalized by ATR upstream).
- `vol_ratio` is an activity/relative-volatility proxy, not raw ATR.
"""
from __future__ import annotations

from analysis.metrics import segment_trades

# Trend strength thresholds on |ema_spread|.
TREND_STRONG_SPREAD = 0.45
TREND_WEAK_SPREAD = 0.18

# Relative volatility thresholds on vol_ratio/body_ratio.
VOL_HIGH_RATIO = 1.8
VOL_LOW_RATIO = 1.0
VOL_HIGH_BODY = 0.70
VOL_LOW_BODY = 0.30

# Compression / expansion thresholds.
COMP_BODY_MAX = 0.35
COMP_VOL_MAX = 1.05
EXP_BODY_MIN = 0.62
EXP_SPREAD_MIN = 0.45
EXP_VOL_MIN = 1.35


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def classify_trend_regime(market_phase: str, ema_spread: float) -> str:
    """Classify directional regime.

    Returns one of:
    - `trending_up_strong`
    - `trending_up`
    - `trending_down_strong`
    - `trending_down`
    - `ranging`
    - `transitioning`
    """
    phase = (market_phase or "MIXED").upper()
    spread = abs(_to_float(ema_spread))

    if phase == "UPTREND":
        if spread >= TREND_STRONG_SPREAD:
            return "trending_up_strong"
        if spread >= TREND_WEAK_SPREAD:
            return "trending_up"
        return "ranging"

    if phase == "DOWNTREND":
        if spread >= TREND_STRONG_SPREAD:
            return "trending_down_strong"
        if spread >= TREND_WEAK_SPREAD:
            return "trending_down"
        return "ranging"

    # MIXED and any unknown phase.
    if spread >= TREND_STRONG_SPREAD:
        return "transitioning"
    return "ranging"


def classify_volatility_regime(vol_ratio: float, body_ratio: float) -> str:
    """Classify relative volatility/activity from vol_ratio + candle body quality.

    Returns one of:
    - `high_volatility`
    - `normal_volatility`
    - `low_volatility`
    """
    vr = _to_float(vol_ratio)
    br = _to_float(body_ratio)

    if vr >= VOL_HIGH_RATIO or br >= VOL_HIGH_BODY:
        return "high_volatility"
    if vr <= VOL_LOW_RATIO and br <= VOL_LOW_BODY:
        return "low_volatility"
    return "normal_volatility"


def classify_structure_regime(body_ratio: float, ema_spread: float, vol_ratio: float) -> str:
    """Classify local structure as compression/expansion/balanced.

    This is a simple deterministic proxy for range behavior using available
    signal-time fields.
    """
    br = _to_float(body_ratio)
    spread = abs(_to_float(ema_spread))
    vr = _to_float(vol_ratio)

    if br <= COMP_BODY_MAX and vr <= COMP_VOL_MAX and spread <= EXP_SPREAD_MIN:
        return "compression"
    if br >= EXP_BODY_MIN and (spread >= EXP_SPREAD_MIN or vr >= EXP_VOL_MIN):
        return "expansion"
    return "balanced"


def classify_ema_regime(ema_fast: float, ema_mid: float, ema_trend: float) -> str:
    """Legacy helper kept for compatibility with existing code/tests."""
    if ema_fast > ema_mid > ema_trend:
        return "UPTREND"
    if ema_fast < ema_mid < ema_trend:
        return "DOWNTREND"
    return "MIXED"


def classify_volatility(vol_ratio: float, high_threshold: float = 1.5) -> str:
    """Legacy two-bucket volatility helper kept for compatibility.

    Returns `HIGH_VOL` if `vol_ratio >= high_threshold`, otherwise `LOW_VOL`.
    """
    return "HIGH_VOL" if _to_float(vol_ratio) >= high_threshold else "LOW_VOL"


def add_regime_labels(trades: list[dict]) -> list[dict]:
    """Attach regime labels to each trade dict in-place and return the same list."""
    for trade in trades:
        market_phase = str(trade.get("market_phase", "MIXED"))
        ema_spread = _to_float(trade.get("ema_spread", 0.0))
        vol_ratio = _to_float(trade.get("vol_ratio", 0.0))
        body_ratio = _to_float(trade.get("body_ratio", 0.0))

        trend_regime = classify_trend_regime(market_phase, ema_spread)
        volatility_regime = classify_volatility_regime(vol_ratio, body_ratio)
        structure_regime = classify_structure_regime(body_ratio, ema_spread, vol_ratio)

        trade["trend_regime"] = trend_regime
        trade["volatility_regime"] = volatility_regime
        trade["structure_regime"] = structure_regime
        trade["combined_regime"] = f"{trend_regime}|{volatility_regime}"

        # Legacy compatibility fields.
        trade["vol_regime"] = volatility_regime

    return trades


def regime_analysis(trades: list[dict]) -> dict[str, dict]:
    """Aggregate stats by regime dimensions.

    Returns both new keys and legacy aliases for backwards compatibility.
    """
    add_regime_labels(trades)

    by_trend = segment_trades(trades, lambda t: str(t.get("trend_regime", "unknown")))
    by_volatility = segment_trades(
        trades,
        lambda t: str(t.get("volatility_regime", "unknown")),
    )
    by_structure = segment_trades(trades, lambda t: str(t.get("structure_regime", "unknown")))
    by_combined = segment_trades(trades, lambda t: str(t.get("combined_regime", "unknown")))
    by_market_phase = segment_trades(trades, lambda t: str(t.get("market_phase", "MIXED")))

    return {
        "by_trend": by_trend,
        "by_volatility": by_volatility,
        "by_structure": by_structure,
        "by_combined": by_combined,
        # Legacy aliases.
        "by_market_phase": by_market_phase,
        "by_vol_regime": by_volatility,
        "by_combo": by_combined,
    }


def print_regime_report(report: dict[str, dict]) -> None:
    """Print compact regime report for CLI usage."""

    def _print_section(title: str, section: dict[str, dict]) -> None:
        print(f"\n  {title}")
        print(
            f"  {'label':<34} {'N':>5} {'WR%':>7} {'PF':>6} {'Exp':>9} {'PnL':>10} {'DD%':>7}"
        )
        print("  " + "-" * 84)
        for label, stats in sorted(section.items()):
            if stats.get("total", 0) == 0:
                continue
            print(
                f"  {label:<34} {stats['total']:>5} {stats['winrate']:>6.1f} "
                f"{stats['profit_factor']:>6.2f} {stats['expectancy']:>+9.4f} "
                f"{stats['total_pnl']:>+10.2f} {stats['max_dd_pct']:>6.1f}"
            )

    _print_section("By trend", report.get("by_trend", {}))
    _print_section("By volatility", report.get("by_volatility", {}))
    _print_section("By structure", report.get("by_structure", {}))
    _print_section("By trend+volatility", report.get("by_combined", {}))
