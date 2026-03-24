"""Unit tests for pure reporting helpers."""
from __future__ import annotations

import pytest

from analysis.reporting import markdown_table, stage2_overview_markdown


@pytest.mark.unit
def test_markdown_table_render() -> None:
    output = markdown_table(["A", "B"], [["x", "1"], ["y", "2"]])
    assert "| A | B |" in output
    assert "| x | 1 |" in output
    assert "| y | 2 |" in output


@pytest.mark.unit
def test_stage2_overview_markdown_contains_sections() -> None:
    baseline = {
        "total": 10,
        "winrate": 55.0,
        "profit_factor": 1.2,
        "expectancy": 0.2,
        "total_pnl": 2.0,
        "max_dd_pct": 12.0,
    }
    wf_summary = {
        "active_windows": 5,
        "pct_pf_gt_1": 60.0,
        "pct_positive_expectancy": 60.0,
        "pct_positive_pnl": 60.0,
        "pct_high_drawdown": 20.0,
        "verdict": "mixed",
    }
    oos_rows = [
        {
            "period": "in_sample",
            "trades": 8,
            "winrate": 57.0,
            "profit_factor": 1.3,
            "expectancy": 0.25,
            "total_pnl": 2.1,
            "verdict": "baseline",
        },
        {
            "period": "out_of_sample",
            "trades": 4,
            "winrate": 45.0,
            "profit_factor": 0.9,
            "expectancy": -0.1,
            "total_pnl": -0.4,
            "verdict": "degradacion_moderada",
        },
    ]
    filter_rows = [
        {
            "variant": "baseline",
            "trades": 10,
            "kept_pct": 100.0,
            "winrate": 55.0,
            "profit_factor": 1.2,
            "expectancy": 0.2,
            "total_pnl": 2.0,
            "max_dd_pct": 12.0,
        }
    ]

    md = stage2_overview_markdown(baseline, wf_summary, oos_rows, filter_rows)
    assert "## Baseline" in md
    assert "## Walk-Forward Summary" in md
    assert "## OOS Comparison" in md
    assert "## Filter Variants" in md
