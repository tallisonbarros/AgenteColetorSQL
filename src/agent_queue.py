from __future__ import annotations

from pathlib import Path
import json
import os
import tempfile
from typing import Dict, Iterable, List


QueueItem = Dict[str, object]


def load_queue(path: Path) -> List[QueueItem]:
    if not path.exists():
        return []
    items: List[QueueItem] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                items.append(item)
    return items


def _can_append(path: Path, max_mb: int) -> bool:
    if max_mb <= 0:
        return True
    if not path.exists():
        return True
    size_mb = path.stat().st_size / (1024 * 1024)
    return size_mb < max_mb


def append_queue(path: Path, item: QueueItem, max_mb: int) -> bool:
    if not _can_append(path, max_mb):
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(item, separators=(",", ":"), default=str))
        handle.write("\n")
    return True


def rewrite_queue(path: Path, items: Iterable[QueueItem]) -> None:
    items = list(items)
    if not items:
        if path.exists():
            path.unlink()
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", delete=False, encoding="utf-8", dir=str(path.parent)
    ) as handle:
        for item in items:
            handle.write(json.dumps(item, separators=(",", ":")))
            handle.write("\n")
        temp_name = handle.name
    os.replace(temp_name, path)
