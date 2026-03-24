"""Pure reporting helpers for Stage 2 analysis outputs."""
from __future__ import annotations

from typing import Iterable


def markdown_table(headers: list[str], rows: Iterable[list[str]]) -> str:
    """Render a markdown table from string headers/rows."""
    header = "| " + " | ".join(headers) + " |"
    separator = "| " + " | ".join("---" for _ in headers) + " |"
    lines = [header, separator]
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return "\n".join(lines)


def stage2_overview_markdown(
    baseline_stats: dict,
    wf_summary: dict,
    oos_rows: list[dict],
    filter_rows: list[dict],
) -> str:
    """Build compact markdown overview for baseline + WF + OOS + filters."""
    sections: list[str] = []

    sections.append("## Baseline")
    sections.append(
        markdown_table(
            [
                "Trades",
                "Winrate %",
                "PF",
                "Expectancy",
                "PnL",
                "Max DD %",
            ],
            [
                [
                    str(baseline_stats.get("total", 0)),
                    f"{baseline_stats.get('winrate', 0.0):.2f}",
                    f"{baseline_stats.get('profit_factor', 0.0):.3f}",
                    f"{baseline_stats.get('expectancy', 0.0):+.4f}",
                    f"{baseline_stats.get('total_pnl', 0.0):+.2f}",
                    f"{baseline_stats.get('max_dd_pct', 0.0):.2f}",
                ]
            ],
        )
    )

    sections.append("\n## Walk-Forward Summary")
    sections.append(
        markdown_table(
            [
                "Active windows",
                "PF>1 %",
                "Exp>0 %",
                "Positive PnL %",
                "High DD %",
                "Verdict",
            ],
            [
                [
                    str(wf_summary.get("active_windows", 0)),
                    f"{wf_summary.get('pct_pf_gt_1', 0.0):.1f}",
                    f"{wf_summary.get('pct_positive_expectancy', 0.0):.1f}",
                    f"{wf_summary.get('pct_positive_pnl', 0.0):.1f}",
                    f"{wf_summary.get('pct_high_drawdown', 0.0):.1f}",
                    str(wf_summary.get("verdict", "no_data")),
                ]
            ],
        )
    )

    sections.append("\n## OOS Comparison")
    if oos_rows:
        sections.append(
            markdown_table(
                ["Period", "Trades", "WR %", "PF", "Expectancy", "PnL", "Verdict"],
                [
                    [
                        str(row.get("period", "")),
                        str(row.get("trades", 0)),
                        f"{row.get('winrate', 0.0):.2f}",
                        f"{row.get('profit_factor', 0.0):.3f}",
                        f"{row.get('expectancy', 0.0):+.4f}",
                        f"{row.get('total_pnl', 0.0):+.2f}",
                        str(row.get("verdict", "")),
                    ]
                    for row in oos_rows
                ],
            )
        )
    else:
        sections.append("_No OOS rows._")

    sections.append("\n## Filter Variants")
    if filter_rows:
        sections.append(
            markdown_table(
                ["Variant", "Trades", "Kept %", "WR %", "PF", "Expectancy", "PnL", "DD %"],
                [
                    [
                        str(row.get("variant", "")),
                        str(row.get("trades", 0)),
                        f"{row.get('kept_pct', 0.0):.1f}",
                        f"{row.get('winrate', 0.0):.2f}",
                        f"{row.get('profit_factor', 0.0):.3f}",
                        f"{row.get('expectancy', 0.0):+.4f}",
                        f"{row.get('total_pnl', 0.0):+.2f}",
                        f"{row.get('max_dd_pct', 0.0):.2f}",
                    ]
                    for row in filter_rows
                ],
            )
        )
    else:
        sections.append("_No filter rows._")

    return "\n".join(sections).strip() + "\n"
