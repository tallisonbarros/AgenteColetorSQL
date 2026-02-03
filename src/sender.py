from __future__ import annotations

from typing import Sequence
import logging
from datetime import date, datetime

import requests

from config import SinkConfig


_LAST_SEND: dict = {}


def _normalize_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _normalize_batch(batch: Sequence[dict]) -> list[dict]:
    normalized: list[dict] = []
    for row in batch:
        normalized.append({key: _normalize_value(val) for key, val in row.items()})
    return normalized


def send_batch(sink: SinkConfig, batch: Sequence[dict]) -> bool:
    if not batch:
        return True
    headers = {
        "Authorization": f"Bearer {sink.token}",
        "Content-Type": "application/json",
    }
    try:
        payload = _normalize_batch(batch)
        _LAST_SEND.clear()
        _LAST_SEND.update(
            {
                "count": len(payload),
                "sample": payload[:2],
                "status": None,
                "error": None,
            }
        )
        response = requests.post(
            sink.api_url,
            json=payload,
            headers=headers,
            timeout=sink.timeout,
            verify=sink.verify_ssl,
        )
        _LAST_SEND["status"] = response.status_code
        _LAST_SEND["response_preview"] = response.text[:500]
        if 200 <= response.status_code < 300:
            return True
        logging.error(
            "HTTP error status=%s body=%s",
            response.status_code,
            response.text[:500],
        )
        return False
    except requests.RequestException as exc:
        logging.error("HTTP request failed: %s", exc)
        _LAST_SEND["error"] = str(exc)
        return False


def get_last_send() -> dict:
    return dict(_LAST_SEND)
