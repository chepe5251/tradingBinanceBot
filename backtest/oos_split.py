#!/usr/bin/env python3
"""Out-of-sample (OOS) validation for backtest trade CSV files.

Offline-only analysis tool. It does not alter live runtime behaviour.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from analysis.metrics import (  # noqa: E402
    classify_oos_stability,
    compare_period_stats,
    compute_stats,
    oos_split,
    split_by_dates,
    top_winner_concentration,
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


def _period_rows(periods: list[tuple[str, list[dict]]]) -> list[dict]:
    rows: list[dict] = []
    if not periods:
        return rows
    baseline_stats = compute_stats(periods[0][1])
    for index, (label, period_trades) in enumerate(periods):
        stats = compute_stats(period_trades)
        top_rows = top_winner_concentration(period_trades, ns=[5])
        top5_pct = top_rows[0]["pct_of_total"] if top_rows else 0.0
        if index == 0:
            verdict = "baseline"
            comparison = compare_period_stats(baseline_stats, stats)
        else:
            verdict = classify_oos_stability(baseline_stats, stats)
            comparison = compare_period_stats(baseline_stats, stats)
        rows.append(
            {
                "period": label,
                "trades": stats["total"],
                "winrate": stats["winrate"],
                "profit_factor": stats["profit_factor"],
                "expectancy": stats["expectancy"],
                "total_pnl": stats["total_pnl"],
                "max_dd_pct": stats["max_dd_pct"],
                "top5_pct": top5_pct,
                "verdict": verdict,
                "comparison": comparison,
            }
        )
    return rows


def _print_rows(rows: list[dict]) -> None:
    print(
        f"{'period':<32} {'N':>6} {'WR%':>7} {'PF':>7} {'Exp':>10} "
        f"{'PnL':>10} {'DD%':>7} {'Top5%':>8} {'verdict':>24}"
    )
    print("-" * 124)
    for row in rows:
        print(
            f"{row['period']:<32} {row['trades']:>6} {row['winrate']:>6.2f} {row['profit_factor']:>7.3f} "
            f"{row['expectancy']:>+10.4f} {row['total_pnl']:>+10.2f} {row['max_dd_pct']:>6.2f} "
            f"{row['top5_pct']:>7.1f} {row['verdict']:>24}"
        )

    if len(rows) > 1:
        print("\nComparison vs baseline (delta):")
        print(
            f"{'period':<32} {'dWR':>9} {'dPF':>9} {'dExp':>11} {'dPnL':>11} {'dDD%':>9} {'PF_ratio%':>11}"
        )
        print("-" * 96)
        for row in rows[1:]:
            cmp = row["comparison"]
            pf_ratio = cmp.get("ratio_profit_factor_pct")
            pf_ratio_txt = "-" if pf_ratio is None else f"{pf_ratio:>10.1f}"
            print(
                f"{row['period']:<32} {cmp['delta_winrate']:>+9.2f} {cmp['delta_profit_factor']:>+9.3f} "
                f"{cmp['delta_expectancy']:>+11.4f} {cmp['delta_total_pnl']:>+11.2f} "
                f"{cmp['delta_max_dd_pct']:>+9.2f} {pf_ratio_txt:>11}"
            )


def _save_csv(rows: list[dict], output_path: str) -> None:
    fields = [
        "period",
        "trades",
        "winrate",
        "profit_factor",
        "expectancy",
        "total_pnl",
        "max_dd_pct",
        "top5_pct",
        "verdict",
        "delta_winrate",
        "delta_profit_factor",
        "delta_expectancy",
        "delta_total_pnl",
        "delta_max_dd_pct",
        "ratio_profit_factor_pct",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            cmp = row["comparison"]
            writer.writerow(
                {
                    "period": row["period"],
                    "trades": row["trades"],
                    "winrate": round(row["winrate"], 6),
                    "profit_factor": round(row["profit_factor"], 6),
                    "expectancy": round(row["expectancy"], 8),
                    "total_pnl": round(row["total_pnl"], 8),
                    "max_dd_pct": round(row["max_dd_pct"], 8),
                    "top5_pct": round(row["top5_pct"], 6),
                    "verdict": row["verdict"],
                    "delta_winrate": round(cmp.get("delta_winrate", 0.0), 6),
                    "delta_profit_factor": round(cmp.get("delta_profit_factor", 0.0), 6),
                    "delta_expectancy": round(cmp.get("delta_expectancy", 0.0), 8),
                    "delta_total_pnl": round(cmp.get("delta_total_pnl", 0.0), 8),
                    "delta_max_dd_pct": round(cmp.get("delta_max_dd_pct", 0.0), 8),
                    "ratio_profit_factor_pct": cmp.get("ratio_profit_factor_pct"),
                }
            )


def _save_markdown(rows: list[dict], output_path: str) -> None:
    table_rows = [
        [
            row["period"],
            str(row["trades"]),
            f"{row['winrate']:.2f}",
            f"{row['profit_factor']:.3f}",
            f"{row['expectancy']:+.4f}",
            f"{row['total_pnl']:+.2f}",
            f"{row['max_dd_pct']:.2f}",
            f"{row['top5_pct']:.1f}",
            row["verdict"],
        ]
        for row in rows
    ]
    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write("# OOS Validation\n\n")
        handle.write(
            markdown_table(
                ["Period", "Trades", "WR %", "PF", "Expectancy", "PnL", "Max DD %", "Top5 %", "Verdict"],
                table_rows,
            )
        )
        if len(rows) > 1:
            handle.write("\n\n## Delta vs baseline\n\n")
            delta_rows = []
            for row in rows[1:]:
                cmp = row["comparison"]
                pf_ratio = cmp.get("ratio_profit_factor_pct")
                delta_rows.append(
                    [
                        row["period"],
                        f"{cmp['delta_winrate']:+.2f}",
                        f"{cmp['delta_profit_factor']:+.3f}",
                        f"{cmp['delta_expectancy']:+.4f}",
                        f"{cmp['delta_total_pnl']:+.2f}",
                        f"{cmp['delta_max_dd_pct']:+.2f}",
                        "-" if pf_ratio is None else f"{pf_ratio:.1f}",
                    ]
                )
            handle.write(
                markdown_table(
                    ["Period", "dWR", "dPF", "dExp", "dPnL", "dDD%", "PF ratio %"],
                    delta_rows,
                )
            )


def _build_periods(args, trades: list[dict]) -> list[tuple[str, list[dict]]]:
    if args.splits:
        return split_by_dates(trades, args.splits)
    if args.split:
        ins, oos = oos_split(trades, split_date=args.split)
        return [("in_sample", ins), ("out_of_sample", oos)]
    ins, oos = oos_split(trades, split_pct=args.pct)
    return [("in_sample", ins), ("out_of_sample", oos)]


def main() -> None:
    parser = argparse.ArgumentParser(description="OOS validation for backtest trades")
    parser.add_argument("--csv", required=True, help="Backtest trade CSV path")
    split_group = parser.add_mutually_exclusive_group(required=True)
    split_group.add_argument("--split", help="Date split YYYY-MM-DD")
    split_group.add_argument("--pct", type=float, help="IS fraction in (0,1]")
    split_group.add_argument("--splits", nargs="+", help="Multiple date cuts (2+)")
    parser.add_argument("--output", default=None, help="Optional output CSV path")
    parser.add_argument("--markdown", default=None, help="Optional markdown output path")
    args = parser.parse_args()

    trades = _load_trades(args.csv)
    if not trades:
        print("No trades loaded.")
        return
    periods = _build_periods(args, trades)
    rows = _period_rows(periods)
    _print_rows(rows)

    if args.output:
        output_path = args.output
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"oos_summary_{ts}.csv")
    _save_csv(rows, output_path)
    print(f"\nSaved CSV: {output_path}")

    if args.markdown:
        _save_markdown(rows, args.markdown)
        print(f"Saved markdown: {args.markdown}")


if __name__ == "__main__":
    main()
