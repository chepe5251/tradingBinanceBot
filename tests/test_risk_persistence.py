from __future__ import annotations

from pathlib import Path

import pytest
from bot.risk import RiskManager


@pytest.mark.unit
def test_risk_load_quarantines_corrupt_json(tmp_path: Path) -> None:
    path = tmp_path / "risk_state.json"
    path.write_text("{bad-json", encoding="utf-8")

    risk = RiskManager(
        cooldown_sec=0,
        max_consecutive_losses=3,
        daily_drawdown_limit=0.2,
        daily_drawdown_limit_usdt=10.0,
        loss_pause_sec=60,
        volatility_pause=False,
        volatility_threshold=0.0,
    )
    risk.load(str(path))

    bad_files = list(tmp_path.glob("risk_state.json.bad-*"))
    assert bad_files, "Corrupt risk state file should be quarantined as .bad-*"

