"""Atomic persistence helpers for runtime state files."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: str, content: str) -> None:
    """Write text atomically using tmp+replace semantics."""
    target = Path(path)
    _ensure_parent(target)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, target)


def atomic_write_json(path: str, payload: dict[str, Any]) -> None:
    """Write JSON atomically using tmp+replace semantics."""
    atomic_write_text(path, json.dumps(payload, ensure_ascii=True, indent=2))


def load_json_safe(
    path: str,
    *,
    on_corrupt: Callable[[str], None] | None = None,
) -> dict[str, Any] | None:
    """Load a JSON file, quarantining corrupt payloads as `*.bad-*`."""
    target = Path(path)
    if not target.exists():
        return None
    try:
        with target.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        bad_name = (
            f"{target.name}.bad-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        )
        bad_path = target.with_name(bad_name)
        try:
            os.replace(target, bad_path)
        except OSError:
            # If quarantine fails, keep original file in place and report corruption.
            pass
        if on_corrupt:
            on_corrupt(f"{exc}")
        return None
    if not isinstance(data, dict):
        if on_corrupt:
            on_corrupt("JSON root is not an object")
        return None
    return data

