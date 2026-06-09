from __future__ import annotations

from pathlib import Path

import pytest
from bot.risk import JsonRiskRepository


@pytest.mark.unit
def test_risk_load_quarantines_corrupt_json(tmp_path: Path) -> None:
    path = tmp_path / "risk_state.json"
    path.write_text("{bad-json", encoding="utf-8")

    repo = JsonRiskRepository(path)
    repo.load()

    bad_files = list(tmp_path.glob("risk_state.json.bad-*"))
    assert bad_files, "Corrupt risk state file should be quarantined as .bad-*"

