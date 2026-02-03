from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from threading import Event

from collector import fetch_rows
from config import Config, build_connection_string, load_config, normalize_timestamp
from agent_queue import append_queue, load_queue, rewrite_queue
from sender import send_batch
from state import get_source_state, load_state, save_state, update_source_state


def _setup_logging(log_path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def _watermark_from_batch(
    batch: list[dict],
    mode: str,
    id_column: str,
    ts_column: str,
    tie_breaker: str,
) -> tuple[int, datetime | None, int | None]:
    if not batch:
        return 0, None, None

    def _payload(row: dict) -> dict:
        return row.get("payload") if isinstance(row.get("payload"), dict) else row

    if mode == "id":
        max_id = max(int(_payload(row)[id_column]) for row in batch)
        return max_id, None, None

    best_ts: datetime | None = None
    best_tie = 0
    for row in batch:
        payload = _payload(row)
        ts = payload[ts_column]
        ts_value = normalize_timestamp(ts)
        tie = int(payload[tie_breaker])
        if best_ts is None or ts_value > best_ts or (ts_value == best_ts and tie > best_tie):
            best_ts = ts_value
            best_tie = tie
    return int(best_tie), best_ts, best_tie


def _get_watermark(state, source):
    source_state = get_source_state(state, source.name)
    last_id = int(source_state.get("last_id", 0))
    last_ts = source_state.get("last_ts")
    last_tie = int(source_state.get("last_tie", last_id))
    if source.incremental.mode == "ts":
        last_ts = normalize_timestamp(last_ts)
    else:
        last_ts = datetime(1900, 1, 1)
    return last_id, last_ts, last_tie


def _apply_start_from(source, last_ts: datetime) -> datetime:
    if source.incremental.mode != "ts":
        return last_ts
    if not source.incremental.start_from:
        return last_ts
    start_ts = normalize_timestamp(source.incremental.start_from)
    return max(last_ts, start_ts)


def _apply_lookback(last_ts: datetime, lookback_minutes: int) -> datetime:
    if lookback_minutes <= 0:
        return last_ts
    return last_ts - timedelta(minutes=lookback_minutes)


def _normalize_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _normalize_row(row: dict) -> dict:
    return {key: _normalize_value(val) for key, val in row.items()}


def _build_records(config: Config, source, rows: list[dict]) -> list[dict]:
    records: list[dict] = []
    for row in rows:
        payload = _normalize_row(row)
        if source.incremental.mode == "id":
            identity_value = payload.get(source.incremental.id_column)
        else:
            ts_value = payload.get(source.incremental.ts_column)
            tie_value = payload.get(source.incremental.tie_breaker)
            identity_value = f"{ts_value}:{tie_value}"
        source_id = (
            f"{config.identity.client_id}:"
            f"{config.identity.agent_id}:"
            f"{source.name}:"
            f"{identity_value}"
        )
        records.append(
            {
                "source_id": source_id,
                "client_id": config.identity.client_id,
                "agent_id": config.identity.agent_id,
                "source": source.name,
                "payload": payload,
            }
        )
    return records


def run(config: Config, stop_event: Event | None = None) -> None:
    _setup_logging(config.paths.log)
    stop_event = stop_event or Event()

    sql_conn = build_connection_string(config.sql)
    state = load_state(config.paths.state)
    next_reprocess_at = datetime.now()

    logging.info("Agent started sources=%s", len(config.sources))

    while not stop_event.is_set():
        try:
            pending = load_queue(config.paths.queue)
            if pending:
                logging.info("Retrying %s queued item(s)", len(pending))
                remaining = []
                for index, item in enumerate(pending):
                    source_name = str(item.get("source", ""))
                    rows = item.get("rows", [])
                    source = next((s for s in config.sources if s.name == source_name), None)
                    if not source or not isinstance(rows, list):
                        logging.warning("Skipping invalid queued item source=%s", source_name)
                        continue
                    if send_batch(config.sink, rows):
                        last_id, last_ts, last_tie = _watermark_from_batch(
                            rows,
                            source.incremental.mode,
                            source.incremental.id_column,
                            source.incremental.ts_column,
                            source.incremental.tie_breaker,
                        )
                        update_source_state(state, source.name, last_id, last_ts, last_tie)
                        save_state(config.paths.state, state)
                        logging.info("Queued batch sent source=%s", source.name)
                    else:
                        remaining.extend(pending[index:])
                        break
                rewrite_queue(config.paths.queue, remaining)
                if remaining:
                    time.sleep(config.runtime.retry_backoff)
                    continue

            for source in config.sources:
                last_id, last_ts, last_tie = _get_watermark(state, source)
                last_ts = _apply_start_from(source, last_ts)
                last_ts = _apply_lookback(last_ts, config.runtime.lookback_minutes)

                if (
                    config.runtime.reprocess_every_minutes > 0
                    and datetime.now() >= next_reprocess_at
                    and config.runtime.reprocess_window_minutes > 0
                    and source.incremental.mode == "ts"
                ):
                    reprocess_from = datetime.now() - timedelta(
                        minutes=config.runtime.reprocess_window_minutes
                    )
                    last_ts = min(last_ts, reprocess_from)
                rows = fetch_rows(
                    sql_conn,
                    source,
                    last_id,
                    last_ts,
                    last_tie,
                    config.runtime.batch_size,
                )
                if rows:
                    logging.info("Fetched %s row(s) source=%s", len(rows), source.name)
                    records = _build_records(config, source, rows)
                    if send_batch(config.sink, records):
                        last_id, last_ts, last_tie = _watermark_from_batch(
                            rows,
                            source.incremental.mode,
                            source.incremental.id_column,
                            source.incremental.ts_column,
                            source.incremental.tie_breaker,
                        )
                        update_source_state(state, source.name, last_id, last_ts, last_tie)
                        save_state(config.paths.state, state)
                        logging.info("Batch sent source=%s", source.name)
                    else:
                        item = {"source": source.name, "rows": records}
                        if append_queue(config.paths.queue, item, config.runtime.queue_max_mb):
                            logging.warning("Batch queued source=%s", source.name)
                        else:
                            logging.error("Queue full, dropping batch source=%s", source.name)
                else:
                    logging.info("No new rows source=%s", source.name)
            if (
                config.runtime.reprocess_every_minutes > 0
                and datetime.now() >= next_reprocess_at
            ):
                next_reprocess_at = datetime.now() + timedelta(
                    minutes=config.runtime.reprocess_every_minutes
                )
        except Exception:
            logging.exception("Unexpected error in main loop")

        time.sleep(config.runtime.interval)


def run_from_path(config_path=None, stop_event: Event | None = None) -> None:
    config = load_config(config_path)
    run(config, stop_event=stop_event)


if __name__ == "__main__":
    run_from_path()
