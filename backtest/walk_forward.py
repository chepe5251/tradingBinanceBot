#!/usr/bin/env python3
"""Walk-forward stability analysis for a backtest trade CSV.

This script is offline-only and does not alter live bot behaviour.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from analysis.metrics import (  # noqa: E402
    compute_stats,
    compute_wf_summary,
    expanding_windows,
    rolling_windows,
)
from analysis.reporting import markdown_table  # noqa: E402


def _load_trades(csv_path: str) -> list[dict]:
    trades: list[dict] = []
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row["pnl_usdt"] = float(row.get("pnl_usdt", 0.0) or 0.0)
            row["score"] = float(row.get("score", 0.0) or 0.0)
            row["vol_ratio"] = float(row.get("vol_ratio", 0.0) or 0.0)
            row["body_ratio"] = float(row.get("body_ratio", 0.0) or 0.0)
            row["ema_spread"] = float(row.get("ema_spread", 0.0) or 0.0)
            trades.append(row)
    return trades


def _print_windows_table(windows: list[dict]) -> None:
    print(
        f"{'window_start':<12} {'window_end':<12} {'N':>6} {'WR%':>7} {'PF':>7} "
        f"{'Exp':>10} {'PnL':>10} {'DD%':>7} {'Top5%':>8}"
    )
    print("-" * 88)
    for window in windows:
        stats = window["stats"]
        if stats["total"] == 0:
            print(
                f"{window['window_start']:<12} {window['window_end']:<12} {0:>6} {'-':>7} {'-':>7} "
                f"{'-':>10} {'-':>10} {'-':>7} {'-':>8}"
            )
            continue
        print(
            f"{window['window_start']:<12} {window['window_end']:<12} {stats['total']:>6} "
            f"{stats['winrate']:>6.1f} {stats['profit_factor']:>7.3f} "
            f"{stats['expectancy']:>+10.4f} {stats['total_pnl']:>+10.2f} "
            f"{stats['max_dd_pct']:>6.1f} {window.get('top5_pct', 0.0):>7.1f}"
        )


def _print_summary(summary: dict) -> None:
    print("\nWalk-forward summary")
    print("-" * 40)
    print(f"total_windows           : {summary['total_windows']}")
    print(f"active_windows          : {summary['active_windows']}")
    print(f"pct_positive_pnl        : {summary['pct_positive_pnl']:.1f}%")
    print(f"pct_pf_gt_1             : {summary['pct_pf_gt_1']:.1f}%")
    print(f"pct_positive_expectancy : {summary['pct_positive_expectancy']:.1f}%")
    print(f"pct_high_drawdown       : {summary['pct_high_drawdown']:.1f}%")
    print(f"pnl_stdev               : {summary['pnl_stdev']:.4f}")
    print(f"pf_stdev                : {summary['pf_stdev']:.4f}")
    print(f"expectancy_stdev        : {summary['expectancy_stdev']:.4f}")
    print(f"top5_concentration_mean : {summary['top5_concentration_mean']:.2f}%")
    print(f"best_window_pnl         : {summary['best_window_pnl']:+.2f}")
    print(f"worst_window_pnl        : {summary['worst_window_pnl']:+.2f}")
    print(f"verdict                 : {summary['verdict']}")
    if summary["high_dd_windows"]:
        print(f"high_dd_windows         : {', '.join(summary['high_dd_windows'])}")


def _save_windows_csv(windows: list[dict], mode: str, output_path: str) -> None:
    fields = [
        "mode",
        "window_start",
        "window_end",
        "n_trades",
        "winrate",
        "profit_factor",
        "expectancy",
        "total_pnl",
        "max_dd_pct",
        "top5_pct",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for window in windows:
            stats = window["stats"]
            writer.writerow(
                {
                    "mode": mode,
                    "window_start": window["window_start"],
                    "window_end": window["window_end"],
                    "n_trades": stats["total"],
                    "winrate": round(stats["winrate"], 6),
                    "profit_factor": round(stats["profit_factor"], 6),
                    "expectancy": round(stats["expectancy"], 8),
                    "total_pnl": round(stats["total_pnl"], 8),
                    "max_dd_pct": round(stats["max_dd_pct"], 8),
                    "top5_pct": round(window.get("top5_pct", 0.0), 6),
                }
            )


def _save_markdown(
    markdown_path: str,
    mode: str,
    baseline: dict,
    summary: dict,
    windows: list[dict],
) -> None:
    rows = []
    for window in windows:
        stats = window["stats"]
        rows.append(
            [
                window["window_start"],
                window["window_end"],
                str(stats["total"]),
                f"{stats['winrate']:.2f}",
                f"{stats['profit_factor']:.3f}",
                f"{stats['expectancy']:+.4f}",
                f"{stats['total_pnl']:+.2f}",
                f"{stats['max_dd_pct']:.2f}",
                f"{window.get('top5_pct', 0.0):.1f}",
            ]
        )

    with open(markdown_path, "w", encoding="utf-8") as handle:
        handle.write("# Walk-Forward Analysis\n\n")
        handle.write(f"Mode: `{mode}`\n\n")
        handle.write(
            f"Baseline: trades={baseline['total']} wr={baseline['winrate']:.2f}% "
            f"pf={baseline['profit_factor']:.3f} exp={baseline['expectancy']:+.4f} "
            f"pnl={baseline['total_pnl']:+.2f}\n\n"
        )
        handle.write(
            markdown_table(
                [
                    "Start",
                    "End",
                    "Trades",
                    "WR %",
                    "PF",
                    "Expectancy",
                    "PnL",
                    "Max DD %",
                    "Top5 %",
                ],
                rows,
            )
        )
        handle.write("\n\n## Summary\n\n")
        handle.write(
            markdown_table(
                ["Metric", "Value"],
                [
                    ["active_windows", str(summary["active_windows"])],
                    ["pct_positive_pnl", f"{summary['pct_positive_pnl']:.1f}%"],
                    ["pct_pf_gt_1", f"{summary['pct_pf_gt_1']:.1f}%"],
                    ["pct_positive_expectancy", f"{summary['pct_positive_expectancy']:.1f}%"],
                    ["pct_high_drawdown", f"{summary['pct_high_drawdown']:.1f}%"],
                    ["pnl_stdev", f"{summary['pnl_stdev']:.4f}"],
                    ["pf_stdev", f"{summary['pf_stdev']:.4f}"],
                    ["expectancy_stdev", f"{summary['expectancy_stdev']:.4f}"],
                    ["top5_concentration_mean", f"{summary['top5_concentration_mean']:.2f}%"],
                    ["verdict", str(summary["verdict"])],
                ],
            )
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Walk-forward stability analysis")
    parser.add_argument("--csv", required=True, help="Backtest trade CSV path")
    parser.add_argument("--window", type=int, default=30, help="Rolling window size in days")
    parser.add_argument("--step", type=int, default=15, help="Window step in days")
    parser.add_argument(
        "--mode",
        choices=["rolling", "expanding"],
        default="rolling",
        help="Windowing mode",
    )
    parser.add_argument("--output", default=None, help="Output CSV path")
    parser.add_argument("--markdown", default=None, help="Optional markdown summary output")
    args = parser.parse_args()

    trades = _load_trades(args.csv)
    if not trades:
        print("No trades loaded.")
        return

    baseline = compute_stats(trades)
    if args.mode == "rolling":
        windows = rolling_windows(trades, window_days=args.window, step_days=args.step)
    else:
        windows = expanding_windows(trades, step_days=args.step)
    summary = compute_wf_summary(windows)

    print(
        f"Baseline: trades={baseline['total']} wr={baseline['winrate']:.2f}% "
        f"pf={baseline['profit_factor']:.3f} exp={baseline['expectancy']:+.4f} "
        f"pnl={baseline['total_pnl']:+.2f}"
    )
    print()
    _print_windows_table(windows)
    _print_summary(summary)

    if args.output:
        output_path = args.output
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"walk_forward_{ts}.csv")
    _save_windows_csv(windows, mode=args.mode, output_path=output_path)
    print(f"\nSaved CSV: {output_path}")

    if args.markdown:
        _save_markdown(args.markdown, args.mode, baseline, summary, windows)
        print(f"Saved markdown: {args.markdown}")


if __name__ == "__main__":
    main()
