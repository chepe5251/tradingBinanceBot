"""Pure metric-computation utilities for Stage 2 backtest analysis.

All functions in this module are stateless and deterministic. They are safe to
use in offline analysis scripts and tests, and they do not modify live runtime
behaviour.
"""
from __future__ import annotations

import statistics
from datetime import datetime, timedelta
from typing import Callable


def parse_entry_datetime(trade: dict) -> datetime | None:
    """Parse `entry_time` from a trade dict using common backtest formats."""
    raw = str(trade.get("entry_time", "")).strip()
    if not raw:
        return None
    for fmt, length in (
        ("%Y-%m-%d %H:%M UTC", 20),
        ("%Y-%m-%d %H:%M", 16),
        ("%Y-%m-%d", 10),
    ):
        try:
            return datetime.strptime(raw[:length], fmt)
        except ValueError:
            continue
    # Fallback for malformed strings where only date is usable.
    try:
        return datetime.strptime(raw[:10], "%Y-%m-%d")
    except ValueError:
        return None


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def compute_stats(trades: list[dict]) -> dict:
    """Return core performance statistics for a sequence of trades."""
    zero = {
        "total": 0,
        "wins": 0,
        "losses": 0,
        "timeouts": 0,
        "winrate": 0.0,
        "total_pnl": 0.0,
        "avg_pnl": 0.0,
        "median_pnl": 0.0,
        "std_pnl": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "rr_real": 0.0,
        "profit_factor": 0.0,
        "expectancy": 0.0,
        "best_trade": 0.0,
        "worst_trade": 0.0,
        "max_drawdown": 0.0,
        "max_dd_pct": 0.0,
        "max_win_streak": 0,
        "max_loss_streak": 0,
        "underwater_trades": 0,
    }
    if not trades:
        return zero

    pnls = [_safe_float(t.get("pnl_usdt", 0.0)) for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    total = len(trades)

    win_count = sum(1 for t in trades if str(t.get("result", "")).upper() == "WIN")
    loss_count = sum(1 for t in trades if str(t.get("result", "")).upper() == "LOSS")
    timeout_count = sum(1 for t in trades if str(t.get("result", "")).upper() == "TIMEOUT")

    total_pnl = sum(pnls)
    avg_pnl = total_pnl / total
    median_pnl = statistics.median(pnls)
    std_pnl = statistics.stdev(pnls) if total > 1 else 0.0
    best_trade = max(pnls)
    worst_trade = min(pnls)

    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = sum(losses) / len(losses) if losses else 0.0
    rr_real = (avg_win / abs(avg_loss)) if avg_loss < 0 else 0.0

    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 0.0

    winrate = (win_count / total) * 100.0
    expectancy = (win_count / total) * avg_win + ((total - win_count) / total) * avg_loss

    cum_pnl = 0.0
    peak = 0.0
    max_drawdown = 0.0
    max_dd_peak = 0.0
    underwater = 0
    streak_win = 0
    streak_loss = 0
    max_win_streak = 0
    max_loss_streak = 0
    for trade in trades:
        pnl = _safe_float(trade.get("pnl_usdt", 0.0))
        result = str(trade.get("result", "")).upper()
        cum_pnl += pnl
        if cum_pnl > peak:
            peak = cum_pnl
        dd = cum_pnl - peak
        if dd < max_drawdown:
            max_drawdown = dd
            max_dd_peak = peak
        if dd < 0:
            underwater += 1

        if result == "WIN":
            streak_win += 1
            streak_loss = 0
        elif result == "LOSS":
            streak_loss += 1
            streak_win = 0
        max_win_streak = max(max_win_streak, streak_win)
        max_loss_streak = max(max_loss_streak, streak_loss)

    max_dd_pct = (abs(max_drawdown) / max_dd_peak * 100.0) if max_dd_peak > 0 else 0.0

    return {
        "total": total,
        "wins": win_count,
        "losses": loss_count,
        "timeouts": timeout_count,
        "winrate": winrate,
        "total_pnl": total_pnl,
        "avg_pnl": avg_pnl,
        "median_pnl": median_pnl,
        "std_pnl": std_pnl,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "rr_real": rr_real,
        "profit_factor": profit_factor,
        "expectancy": expectancy,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "max_drawdown": max_drawdown,
        "max_dd_pct": max_dd_pct,
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
        "underwater_trades": underwater,
    }


def exclude_top_winners(trades: list[dict], n: int) -> list[dict]:
    """Return trades with the top-N PnL winners removed."""
    if n <= 0 or not trades:
        return list(trades)
    ordered = sorted(trades, key=lambda t: _safe_float(t.get("pnl_usdt", 0.0)), reverse=True)
    n_capped = min(n, len(ordered))
    return ordered[n_capped:]


def top_winner_concentration(trades: list[dict], ns: list[int] | None = None) -> list[dict]:
    """Measure PnL concentration in top-N winners for each N in `ns`."""
    if ns is None:
        ns = [1, 5, 10, 20]
    if not trades:
        return []

    total_pnl = sum(_safe_float(t.get("pnl_usdt", 0.0)) for t in trades)
    ordered = sorted(trades, key=lambda t: _safe_float(t.get("pnl_usdt", 0.0)), reverse=True)
    rows: list[dict] = []
    for n in ns:
        n_capped = min(max(0, n), len(ordered))
        top = ordered[:n_capped]
        rest = ordered[n_capped:]
        top_pnl = sum(_safe_float(t.get("pnl_usdt", 0.0)) for t in top)
        pct = (top_pnl / total_pnl * 100.0) if abs(total_pnl) > 1e-12 else 0.0
        rows.append(
            {
                "n": n_capped,
                "top_pnl": top_pnl,
                "total_pnl": total_pnl,
                "pct_of_total": pct,
                "stats_excl": compute_stats(rest),
            }
        )
    return rows


def segment_trades(trades: list[dict], key_fn: Callable[[dict], str]) -> dict[str, dict]:
    """Group trades by key and compute stats per group."""
    grouped: dict[str, list[dict]] = {}
    for trade in trades:
        key = key_fn(trade)
        grouped.setdefault(key, []).append(trade)
    return {key: compute_stats(items) for key, items in grouped.items()}


def _window_top5_pct(window_trades: list[dict]) -> float:
    rows = top_winner_concentration(window_trades, ns=[5])
    return rows[0]["pct_of_total"] if rows else 0.0


def rolling_windows(trades: list[dict], window_days: int, step_days: int) -> list[dict]:
    """Compute rolling windows over chronological trades."""
    if not trades:
        return []
    if window_days <= 0 or step_days <= 0:
        raise ValueError("window_days and step_days must be > 0")

    dated = [(trade, parse_entry_datetime(trade)) for trade in trades]
    dated = [(trade, dt) for trade, dt in dated if dt is not None]
    if not dated:
        return []

    dated.sort(key=lambda pair: pair[1])
    first = dated[0][1].replace(hour=0, minute=0, second=0, microsecond=0)
    last = dated[-1][1]

    cursor = first
    window_td = timedelta(days=window_days)
    step_td = timedelta(days=step_days)
    windows: list[dict] = []
    while cursor <= last:
        end = cursor + window_td
        window_trades = [trade for trade, dt in dated if cursor <= dt < end]
        stats = compute_stats(window_trades)
        windows.append(
            {
                "window_start": cursor.strftime("%Y-%m-%d"),
                "window_end": (end - timedelta(seconds=1)).strftime("%Y-%m-%d"),
                "n_trades": len(window_trades),
                "stats": stats,
                "top5_pct": _window_top5_pct(window_trades),
            }
        )
        cursor += step_td
    return windows


def expanding_windows(trades: list[dict], step_days: int) -> list[dict]:
    """Compute expanding windows from first trade to moving end date."""
    if not trades:
        return []
    if step_days <= 0:
        raise ValueError("step_days must be > 0")

    dated = [(trade, parse_entry_datetime(trade)) for trade in trades]
    dated = [(trade, dt) for trade, dt in dated if dt is not None]
    if not dated:
        return []

    dated.sort(key=lambda pair: pair[1])
    first = dated[0][1].replace(hour=0, minute=0, second=0, microsecond=0)
    last = dated[-1][1]

    end = first + timedelta(days=step_days)
    windows: list[dict] = []
    while end <= last + timedelta(days=1):
        window_trades = [trade for trade, dt in dated if dt < end]
        stats = compute_stats(window_trades)
        windows.append(
            {
                "window_start": first.strftime("%Y-%m-%d"),
                "window_end": (end - timedelta(seconds=1)).strftime("%Y-%m-%d"),
                "n_trades": len(window_trades),
                "stats": stats,
                "top5_pct": _window_top5_pct(window_trades),
            }
        )
        end += timedelta(days=step_days)
    return windows


def compute_wf_summary(windows: list[dict]) -> dict:
    """Aggregate walk-forward windows into consistency metrics."""
    active = [window for window in windows if _safe_int(window.get("n_trades", 0)) > 0]
    if not active:
        return {
            "total_windows": len(windows),
            "active_windows": 0,
            "pct_positive_pnl": 0.0,
            "pct_pf_gt_1": 0.0,
            "pct_positive_expectancy": 0.0,
            "pct_high_drawdown": 0.0,
            "pnl_mean": 0.0,
            "pnl_stdev": 0.0,
            "pf_mean": 0.0,
            "pf_stdev": 0.0,
            "expectancy_mean": 0.0,
            "expectancy_stdev": 0.0,
            "winrate_mean": 0.0,
            "winrate_stdev": 0.0,
            "top5_concentration_mean": 0.0,
            "best_window_pnl": 0.0,
            "worst_window_pnl": 0.0,
            "high_dd_windows": [],
            "verdict": "no_data",
        }

    pnls = [window["stats"]["total_pnl"] for window in active]
    pfs = [window["stats"]["profit_factor"] for window in active]
    exps = [window["stats"]["expectancy"] for window in active]
    wrs = [window["stats"]["winrate"] for window in active]
    dds = [window["stats"]["max_dd_pct"] for window in active]
    top5 = [_safe_float(window.get("top5_pct", 0.0)) for window in active]
    n = len(active)

    def _pct(condition: Callable[[dict], bool]) -> float:
        return sum(1 for window in active if condition(window)) / n * 100.0

    pct_pf = _pct(lambda window: window["stats"]["profit_factor"] > 1.0)
    pct_exp = _pct(lambda window: window["stats"]["expectancy"] > 0.0)
    pct_pos_pnl = _pct(lambda window: window["stats"]["total_pnl"] > 0.0)
    pct_high_dd = _pct(lambda window: window["stats"]["max_dd_pct"] >= 20.0)

    if pct_pf >= 70.0 and pct_exp >= 70.0 and pct_high_dd <= 25.0:
        verdict = "stable"
    elif pct_pf >= 50.0 and pct_exp >= 50.0 and pct_high_dd <= 40.0:
        verdict = "mixed"
    else:
        verdict = "unstable"

    high_dd_windows = [
        f"{window['window_start']}..{window['window_end']}"
        for window in active
        if window["stats"]["max_dd_pct"] >= 20.0
    ]

    return {
        "total_windows": len(windows),
        "active_windows": n,
        "pct_positive_pnl": round(pct_pos_pnl, 1),
        "pct_pf_gt_1": round(pct_pf, 1),
        "pct_positive_expectancy": round(pct_exp, 1),
        "pct_high_drawdown": round(pct_high_dd, 1),
        "pnl_mean": round(statistics.mean(pnls), 4),
        "pnl_stdev": round(statistics.stdev(pnls), 4) if n > 1 else 0.0,
        "pf_mean": round(statistics.mean(pfs), 4),
        "pf_stdev": round(statistics.stdev(pfs), 4) if n > 1 else 0.0,
        "expectancy_mean": round(statistics.mean(exps), 4),
        "expectancy_stdev": round(statistics.stdev(exps), 4) if n > 1 else 0.0,
        "winrate_mean": round(statistics.mean(wrs), 4),
        "winrate_stdev": round(statistics.stdev(wrs), 4) if n > 1 else 0.0,
        "top5_concentration_mean": round(statistics.mean(top5), 2),
        "best_window_pnl": round(max(pnls), 4),
        "worst_window_pnl": round(min(pnls), 4),
        "high_dd_windows": high_dd_windows,
        "verdict": verdict,
    }


def oos_split(
    trades: list[dict],
    split_date: str | None = None,
    split_pct: float | None = None,
) -> tuple[list[dict], list[dict]]:
    """Split trades into in-sample and out-of-sample partitions."""
    if not trades:
        return [], []
    if split_date is None and split_pct is None:
        raise ValueError("Provide split_date or split_pct")
    if split_date is not None and split_pct is not None:
        raise ValueError("Provide only one split mode")

    dated = [(trade, parse_entry_datetime(trade)) for trade in trades]
    dated = [(trade, dt) for trade, dt in dated if dt is not None]
    dated.sort(key=lambda pair: pair[1])
    ordered = [trade for trade, _ in dated]
    if not ordered:
        return [], []

    if split_date is not None:
        cutoff = datetime.strptime(split_date[:10], "%Y-%m-%d")
        ins = [trade for trade, dt in dated if dt.date() < cutoff.date()]
        oos = [trade for trade, dt in dated if dt.date() >= cutoff.date()]
        return ins, oos

    pct = _safe_float(split_pct, 0.0)
    if pct <= 0 or pct > 1:
        raise ValueError("split_pct must be in (0, 1]")
    idx = int(len(ordered) * pct)
    idx = max(1, min(idx, len(ordered)))
    return ordered[:idx], ordered[idx:]


def split_by_dates(trades: list[dict], dates: list[str]) -> list[tuple[str, list[dict]]]:
    """Split trades into IS + multiple OOS periods using sorted cut dates."""
    if not trades:
        return []
    if len(dates) < 2:
        raise ValueError("Provide at least 2 cut dates for multi-cut split")

    cutoffs = sorted(datetime.strptime(date[:10], "%Y-%m-%d").date() for date in dates)
    dated = [(trade, parse_entry_datetime(trade)) for trade in trades]
    dated = [(trade, dt.date()) for trade, dt in dated if dt is not None]
    dated.sort(key=lambda pair: pair[1])

    periods: list[tuple[str, list[dict]]] = []
    first_cut = cutoffs[0]
    periods.append((f"is_before_{first_cut}", [trade for trade, day in dated if day < first_cut]))

    for idx in range(len(cutoffs) - 1):
        start = cutoffs[idx]
        end = cutoffs[idx + 1]
        label = f"oos_{idx + 1}_{start}_to_{end}"
        period_trades = [trade for trade, day in dated if start <= day < end]
        periods.append((label, period_trades))

    last_cut = cutoffs[-1]
    periods.append((f"oos_{len(cutoffs)}_from_{last_cut}", [trade for trade, day in dated if day >= last_cut]))
    return periods


def compare_period_stats(reference: dict, candidate: dict) -> dict:
    """Compare candidate metrics against reference metrics."""
    keys = ("total_pnl", "winrate", "expectancy", "profit_factor", "max_dd_pct", "total")
    comparison: dict[str, float | None] = {}
    for key in keys:
        ref_val = _safe_float(reference.get(key, 0.0))
        cand_val = _safe_float(candidate.get(key, 0.0))
        comparison[f"delta_{key}"] = cand_val - ref_val
        comparison[f"ratio_{key}_pct"] = None if abs(ref_val) < 1e-12 else (cand_val / ref_val) * 100.0
    return comparison


def classify_oos_stability(reference: dict, candidate: dict) -> str:
    """Classify OOS degradation versus IS baseline."""
    if _safe_int(candidate.get("total", 0)) == 0:
        return "no_oos_trades"

    def _drop(ref_key: str) -> float:
        ref = _safe_float(reference.get(ref_key, 0.0))
        cand = _safe_float(candidate.get(ref_key, 0.0))
        if abs(ref) < 1e-12:
            return 0.0
        return (ref - cand) / abs(ref)

    wr_drop = _drop("winrate")
    pf_drop = _drop("profit_factor")
    exp_drop = _drop("expectancy")

    drops = [wr_drop, pf_drop, exp_drop]
    n_drop_10 = sum(1 for drop in drops if drop > 0.10)
    n_drop_25 = sum(1 for drop in drops if drop > 0.25)
    n_drop_40 = sum(1 for drop in drops if drop > 0.40)

    dd_ref = _safe_float(reference.get("max_dd_pct", 0.0))
    dd_cand = _safe_float(candidate.get("max_dd_pct", 0.0))
    dd_worse = dd_cand > (dd_ref + 10.0)

    if n_drop_40 >= 1 or n_drop_10 >= 3 or (dd_worse and n_drop_25 >= 1):
        return "degradacion_severa"
    if n_drop_25 >= 1 or n_drop_10 >= 2 or dd_worse:
        return "degradacion_moderada"
    if n_drop_10 >= 1:
        return "degradacion_leve"
    return "estabilidad_aceptable"
