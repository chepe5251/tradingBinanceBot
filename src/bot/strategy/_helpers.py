"""Internal helpers shared by all strategy engines."""
from __future__ import annotations

import pandas as pd


def _bump_reject(rejects: dict | None, key: str) -> None:
    if rejects is not None:
        rejects[key] = rejects.get(key, 0) + 1


def _format_breakout_time(ts: object) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S UTC") if isinstance(ts, pd.Timestamp) else str(ts)


def _is_pivot_high(highs: pd.Series, idx: int) -> bool:
    if idx < 2 or idx + 2 >= len(highs):
        return False
    pivot = float(highs.iloc[idx])
    return (
        pivot > float(highs.iloc[idx - 1])
        and pivot > float(highs.iloc[idx - 2])
        and pivot > float(highs.iloc[idx + 1])
        and pivot > float(highs.iloc[idx + 2])
    )
