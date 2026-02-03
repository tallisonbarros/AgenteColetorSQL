from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
import os
import tempfile
from typing import Any, Dict


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"sources": {}}
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            return {"sources": {}}
        if "sources" not in data or not isinstance(data["sources"], dict):
            data["sources"] = {}
        return data
    except (OSError, ValueError, json.JSONDecodeError):
        return {"sources": {}}


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", delete=False, encoding="utf-8", dir=str(path.parent)
    ) as handle:
        json.dump(state, handle, separators=(",", ":"))
        temp_name = handle.name
    os.replace(temp_name, path)


def get_source_state(state: Dict[str, Any], source: str) -> Dict[str, Any]:
    sources = state.setdefault("sources", {})
    return sources.setdefault(source, {})


def normalize_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        return datetime.fromisoformat(value)
    return datetime(1900, 1, 1)


def update_source_state(
    state: Dict[str, Any],
    source: str,
    last_id: int,
    last_ts: datetime | None,
    last_tie: int | None,
) -> None:
    payload: Dict[str, Any] = {"last_id": int(last_id)}
    if last_ts is not None:
        payload["last_ts"] = last_ts.isoformat()
    if last_tie is not None:
        payload["last_tie"] = int(last_tie)
    state.setdefault("sources", {})[source] = payload
