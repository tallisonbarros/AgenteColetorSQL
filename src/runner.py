from __future__ import annotations

from pathlib import Path
from threading import Event, Thread
from typing import Optional

from main import run_from_path

_thread: Optional[Thread] = None
_stop_event: Optional[Event] = None
_last_error: Optional[str] = None
_config_path: Optional[Path] = None


def start(config_path: Path) -> bool:
    global _thread, _stop_event, _last_error, _config_path
    if _thread and _thread.is_alive():
        return False
    _last_error = None
    _config_path = config_path
    _stop_event = Event()
    _thread = Thread(
        target=_run_safe,
        args=(config_path, _stop_event),
        daemon=True,
    )
    _thread.start()
    return True


def _run_safe(config_path: Path, stop_event: Event) -> None:
    global _last_error
    try:
        run_from_path(config_path, stop_event=stop_event)
    except Exception as exc:
        _last_error = str(exc)


def stop() -> bool:
    global _thread, _stop_event
    if not _thread or not _thread.is_alive():
        return False
    if _stop_event:
        _stop_event.set()
    _thread.join(timeout=5)
    return True


def status() -> dict:
    running = bool(_thread and _thread.is_alive())
    return {
        "running": running,
        "last_error": _last_error,
        "config_path": str(_config_path) if _config_path else "",
    }
