"""Offline filter-candidate framework for Stage 2 experiments.

This module is analysis-only. It does not alter live runtime behaviour.
"""
from __future__ import annotations

import csv
from datetime import datetime
from typing import Callable

from analysis.metrics import compute_stats, parse_entry_datetime, top_winner_concentration

FilterFn = Callable[[dict], bool]


def apply_filter(trades: list[dict], fn: FilterFn | None) -> list[dict]:
    """Return trades that pass the filter, or all trades for baseline (`fn=None`)."""
    if fn is None:
        return list(trades)
    return [trade for trade in trades if fn(trade)]


def combine_filters(filters: list[FilterFn]) -> FilterFn:
    """AND-combine filters: a trade is kept only when all filters pass."""

    def _combined(trade: dict) -> bool:
        return all(filter_fn(trade) for filter_fn in filters)

    return _combined


def any_filter(filters: list[FilterFn]) -> FilterFn:
    """OR-combine filters: a trade is kept when any filter passes."""

    def _combined(trade: dict) -> bool:
        return any(filter_fn(trade) for filter_fn in filters)

    return _combined


def compare_variants(trades: list[dict], variants: dict[str, FilterFn | None]) -> dict[str, dict]:
    """Compute comparable metrics for baseline and filter variants."""
    total = len(trades)
    results: dict[str, dict] = {}
    for name, filter_fn in variants.items():
        kept = apply_filter(trades, filter_fn)
        stats = compute_stats(kept)
        concentration = top_winner_concentration(kept, ns=[1, 5, 10])
        top5_pct = concentration[1]["pct_of_total"] if len(concentration) > 1 else 0.0
        results[name] = {
            "n_trades": len(kept),
            "n_removed": max(0, total - len(kept)),
            "pct_remaining": (len(kept) / total * 100.0) if total else 0.0,
            "stats": stats,
            "top_winners": concentration,
            "top5_pct": top5_pct,
        }
    return results


def filter_min_score(min_score: float) -> FilterFn:
    """Keep trades with score >= min_score."""
    return lambda trade: float(trade.get("score", 0.0)) >= min_score


def filter_max_score(max_score: float) -> FilterFn:
    """Keep trades with score <= max_score."""
    return lambda trade: float(trade.get("score", 0.0)) <= max_score


def filter_interval(intervals: set[str]) -> FilterFn:
    """Keep trades whose interval is in `intervals`."""
    allowed = {value.lower() for value in intervals}
    return lambda trade: str(trade.get("interval", "")).lower() in allowed


def filter_exclude_symbols(symbols: set[str]) -> FilterFn:
    """Exclude listed symbols."""
    banned = {value.upper() for value in symbols}
    return lambda trade: str(trade.get("symbol", "")).upper() not in banned


def filter_include_symbols(symbols: set[str]) -> FilterFn:
    """Keep only listed symbols."""
    allowed = {value.upper() for value in symbols}
    return lambda trade: str(trade.get("symbol", "")).upper() in allowed


def filter_include_hours(hours: set[int]) -> FilterFn:
    """Keep trades whose UTC entry hour is in `hours`."""
    allowed = set(hours)

    def _filter(trade: dict) -> bool:
        dt = parse_entry_datetime(trade)
        if dt is None:
            return True
        return dt.hour in allowed

    return _filter


def filter_exclude_hours(hours: set[int]) -> FilterFn:
    """Exclude trades whose UTC entry hour is in `hours`."""
    banned = set(hours)

    def _filter(trade: dict) -> bool:
        dt = parse_entry_datetime(trade)
        if dt is None:
            return True
        return dt.hour not in banned

    return _filter


def filter_include_weekdays(days: set[str]) -> FilterFn:
    """Keep trades on given weekday abbreviations (Mon..Sun)."""
    allowed = {value.title()[:3] for value in days}

    def _filter(trade: dict) -> bool:
        dt = parse_entry_datetime(trade)
        if dt is None:
            return True
        return dt.strftime("%a") in allowed

    return _filter


def filter_exclude_weekdays(days: set[str]) -> FilterFn:
    """Exclude trades on given weekday abbreviations (Mon..Sun)."""
    banned = {value.title()[:3] for value in days}

    def _filter(trade: dict) -> bool:
        dt = parse_entry_datetime(trade)
        if dt is None:
            return True
        return dt.strftime("%a") not in banned

    return _filter


def filter_market_phase(phases: set[str]) -> FilterFn:
    """Keep trades in selected `market_phase` values."""
    allowed = {value.upper() for value in phases}
    return lambda trade: str(trade.get("market_phase", "")).upper() in allowed


def filter_trend_regime(regimes: set[str]) -> FilterFn:
    """Keep trades in selected trend regimes (from analysis.regime labels)."""
    allowed = {value.lower() for value in regimes}
    return lambda trade: str(trade.get("trend_regime", "")).lower() in allowed


def filter_combined_regime(regimes: set[str]) -> FilterFn:
    """Keep trades in selected combined regimes (trend|volatility)."""
    allowed = {value.lower() for value in regimes}
    return lambda trade: str(trade.get("combined_regime", "")).lower() in allowed


def _ratio(value: float, baseline: float) -> float:
    if abs(baseline) < 1e-12:
        return 0.0
    return value / baseline * 100.0


def print_experiment_report(results: dict[str, dict]) -> None:
    """Print compact baseline-vs-variants report."""
    if not results:
        print("No experiment results.")
        return

    baseline_key = "baseline" if "baseline" in results else next(iter(results))
    baseline = results[baseline_key]
    baseline_pnl = baseline["stats"]["total_pnl"]

    print(
        f"{'variant':<28} {'N':>6} {'kept%':>7} {'WR%':>7} {'PF':>6} "
        f"{'Exp':>9} {'PnL':>10} {'PnLvsBase':>10} {'Top5%':>7} {'DD%':>7}"
    )
    print("-" * 105)

    for name, row in results.items():
        stats = row["stats"]
        pnl = stats["total_pnl"]
        pnl_vs_base = _ratio(pnl, baseline_pnl) if name != baseline_key else 100.0
        print(
            f"{name:<28} {stats['total']:>6} {row['pct_remaining']:>6.1f}% {stats['winrate']:>6.1f}% "
            f"{stats['profit_factor']:>6.2f} {stats['expectancy']:>+9.4f} {pnl:>+10.2f} "
            f"{pnl_vs_base:>9.1f}% {row['top5_pct']:>6.1f}% {stats['max_dd_pct']:>6.1f}%"
        )


def save_experiment_csv(results: dict[str, dict], output_path: str) -> None:
    """Persist experiment variants to CSV."""
    fieldnames = [
        "variant",
        "n_trades",
        "n_removed",
        "pct_remaining",
        "winrate",
        "profit_factor",
        "expectancy",
        "total_pnl",
        "avg_pnl",
        "median_pnl",
        "max_dd_pct",
        "top5_pct",
    ]
    with open(output_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for name, row in results.items():
            stats = row["stats"]
            writer.writerow(
                {
                    "variant": name,
                    "n_trades": row["n_trades"],
                    "n_removed": row["n_removed"],
                    "pct_remaining": round(row["pct_remaining"], 2),
                    "winrate": round(stats["winrate"], 4),
                    "profit_factor": round(stats["profit_factor"], 4),
                    "expectancy": round(stats["expectancy"], 6),
                    "total_pnl": round(stats["total_pnl"], 6),
                    "avg_pnl": round(stats["avg_pnl"], 6),
                    "median_pnl": round(stats["median_pnl"], 6),
                    "max_dd_pct": round(stats["max_dd_pct"], 6),
                    "top5_pct": round(row["top5_pct"], 4),
                }
            )


def render_experiment_markdown(results: dict[str, dict]) -> str:
    """Render experiment summary as a Markdown table."""
    lines = [
        "| Variant | Trades | Kept % | Winrate % | PF | Expectancy | PnL | Max DD % | Top5 % |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, row in results.items():
        stats = row["stats"]
        lines.append(
            "| "
            + f"{name} | {stats['total']} | {row['pct_remaining']:.1f} | {stats['winrate']:.2f} | "
            + f"{stats['profit_factor']:.3f} | {stats['expectancy']:+.4f} | {stats['total_pnl']:+.2f} | "
            + f"{stats['max_dd_pct']:.2f} | {row['top5_pct']:.1f} |"
        )
    return "\n".join(lines)


def save_experiment_markdown(results: dict[str, dict], output_path: str) -> None:
    """Persist Markdown experiment summary."""
    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write("# Filter Experiments\n\n")
        handle.write(f"Generated: {datetime.utcnow().isoformat()}Z\n\n")
        handle.write(render_experiment_markdown(results))
