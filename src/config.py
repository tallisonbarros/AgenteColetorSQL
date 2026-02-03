from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Iterable, Optional
import os

import yaml


def _base_dir() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


BASE_DIR = _base_dir()
DEFAULT_CONFIG_PATH = BASE_DIR / "config.yaml"


@dataclass(frozen=True)
class SQLConfig:
    driver: str
    server: str
    database: str
    auth: str
    user: str
    password: str
    timeout: int


@dataclass(frozen=True)
class IncrementalConfig:
    mode: str
    id_column: str
    ts_column: str
    tie_breaker: str
    start_from: str


@dataclass(frozen=True)
class SourceConfig:
    name: str
    kind: str
    table: str
    query: str
    select: list[str]
    filter: str
    incremental: IncrementalConfig


@dataclass(frozen=True)
class SinkConfig:
    api_url: str
    token: str
    verify_ssl: bool
    timeout: float


@dataclass(frozen=True)
class RuntimeConfig:
    interval: int
    batch_size: int
    retry_backoff: int
    queue_max_mb: int
    lookback_minutes: int
    reprocess_every_minutes: int
    reprocess_window_minutes: int


@dataclass(frozen=True)
class PathsConfig:
    state: Path
    queue: Path
    log: Path


@dataclass(frozen=True)
class IdentityConfig:
    client_id: str
    agent_id: str


@dataclass(frozen=True)
class Config:
    sql: SQLConfig
    sources: list[SourceConfig]
    sink: SinkConfig
    runtime: RuntimeConfig
    paths: PathsConfig
    identity: IdentityConfig


def _require(value: Any, name: str) -> Any:
    if value is None or value == "":
        raise ValueError(f"Missing required config: {name}")
    return value


def _as_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("Invalid config format (expected mapping at root).")
    return data


def _parse_sql(data: dict) -> SQLConfig:
    driver = _require(data.get("driver"), "sql.driver")
    server = _require(data.get("server"), "sql.server")
    database = _require(data.get("database"), "sql.database")
    auth = data.get("auth", "windows")
    user = data.get("user", "")
    password = data.get("password", "")
    timeout = int(data.get("timeout", 5))
    if timeout <= 0:
        raise ValueError("sql.timeout must be > 0")
    if auth not in {"windows", "sql"}:
        raise ValueError("sql.auth must be 'windows' or 'sql'")
    if auth == "sql" and (not user or not password):
        raise ValueError("sql.user and sql.password required for sql auth")
    return SQLConfig(
        driver=str(driver),
        server=str(server),
        database=str(database),
        auth=str(auth),
        user=str(user),
        password=str(password),
        timeout=timeout,
    )


def _parse_incremental(data: dict) -> IncrementalConfig:
    mode = data.get("mode", "id")
    if mode not in {"id", "ts"}:
        raise ValueError("incremental.mode must be 'id' or 'ts'")
    id_column = data.get("id_column", "ID")
    ts_column = data.get("ts_column", "")
    tie_breaker = data.get("tie_breaker", id_column)
    if mode == "ts" and not ts_column:
        raise ValueError("incremental.ts_column required when mode=ts")
    start_from = str(data.get("start_from", "") or "")
    return IncrementalConfig(
        mode=str(mode),
        id_column=str(id_column),
        ts_column=str(ts_column),
        tie_breaker=str(tie_breaker),
        start_from=start_from,
    )


def _parse_source(data: dict) -> SourceConfig:
    name = _require(data.get("name"), "sources[].name")
    kind = data.get("kind", "table")
    if kind not in {"table", "query"}:
        raise ValueError("sources[].kind must be 'table' or 'query'")
    table = data.get("table", "")
    query = data.get("query", "")
    if kind == "table" and not table:
        raise ValueError("sources[].table required for kind=table")
    if kind == "query" and not query:
        raise ValueError("sources[].query required for kind=query")
    select = [str(col) for col in _as_list(data.get("select")) if str(col).strip()]
    filter_value = str(data.get("filter", "") or "")
    incremental = _parse_incremental(data.get("incremental", {}))
    return SourceConfig(
        name=str(name),
        kind=str(kind),
        table=str(table),
        query=str(query),
        select=select,
        filter=filter_value,
        incremental=incremental,
    )


def _parse_sink(data: dict) -> SinkConfig:
    api_url = _require(data.get("api_url"), "sink.api_url")
    token = _require(data.get("token"), "sink.token")
    verify_ssl = bool(data.get("verify_ssl", True))
    timeout = float(data.get("timeout", 10.0))
    if timeout <= 0:
        raise ValueError("sink.timeout must be > 0")
    return SinkConfig(
        api_url=str(api_url),
        token=str(token),
        verify_ssl=verify_ssl,
        timeout=timeout,
    )


def _parse_runtime(data: dict) -> RuntimeConfig:
    interval = int(data.get("interval", 5))
    batch_size = int(data.get("batch_size", 100))
    retry_backoff = int(data.get("retry_backoff", interval))
    queue_max_mb = int(data.get("queue_max_mb", 200))
    lookback_minutes = int(data.get("lookback_minutes", 0))
    reprocess_every_minutes = int(data.get("reprocess_every_minutes", 0))
    reprocess_window_minutes = int(data.get("reprocess_window_minutes", 0))
    if interval <= 0:
        raise ValueError("runtime.interval must be > 0")
    if batch_size <= 0:
        raise ValueError("runtime.batch_size must be > 0")
    if retry_backoff <= 0:
        raise ValueError("runtime.retry_backoff must be > 0")
    if queue_max_mb < 0:
        raise ValueError("runtime.queue_max_mb must be >= 0")
    if lookback_minutes < 0:
        raise ValueError("runtime.lookback_minutes must be >= 0")
    if reprocess_every_minutes < 0:
        raise ValueError("runtime.reprocess_every_minutes must be >= 0")
    if reprocess_window_minutes < 0:
        raise ValueError("runtime.reprocess_window_minutes must be >= 0")
    return RuntimeConfig(
        interval=interval,
        batch_size=batch_size,
        retry_backoff=retry_backoff,
        queue_max_mb=queue_max_mb,
        lookback_minutes=lookback_minutes,
        reprocess_every_minutes=reprocess_every_minutes,
        reprocess_window_minutes=reprocess_window_minutes,
    )


def _parse_paths(data: dict) -> PathsConfig:
    state = Path(data.get("state", str(BASE_DIR / "state.json")))
    queue = Path(data.get("queue", str(BASE_DIR / "queue.jsonl")))
    log = Path(data.get("log", str(BASE_DIR / "agent.log")))
    return PathsConfig(state=state, queue=queue, log=log)


def _parse_identity(data: dict) -> IdentityConfig:
    client_id = _require(data.get("client_id"), "identity.client_id")
    agent_id = _require(data.get("agent_id"), "identity.agent_id")
    return IdentityConfig(client_id=str(client_id), agent_id=str(agent_id))


def load_config(path: Optional[Path] = None) -> Config:
    config_path = path or Path(os.getenv("CONFIG_PATH", str(DEFAULT_CONFIG_PATH)))
    data = _read_yaml(config_path)
    return load_config_from_dict(data)


def load_config_from_dict(data: dict) -> Config:
    sql = _parse_sql(data.get("sql", {}))
    sources_data = data.get("sources", [])
    if not isinstance(sources_data, list) or not sources_data:
        raise ValueError("sources must be a non-empty list")
    sources = [_parse_source(item or {}) for item in sources_data]
    sink = _parse_sink(data.get("sink", {}))
    runtime = _parse_runtime(data.get("runtime", {}))
    paths = _parse_paths(data.get("paths", {}))
    identity = _parse_identity(data.get("identity", {}))
    return Config(
        sql=sql,
        sources=sources,
        sink=sink,
        runtime=runtime,
        paths=paths,
        identity=identity,
    )


def build_connection_string(sql: SQLConfig) -> str:
    if sql.auth == "windows":
        return (
            f"DRIVER={{{sql.driver}}};"
            f"SERVER={sql.server};"
            f"DATABASE={sql.database};"
            "Trusted_Connection=yes;"
            f"Connection Timeout={sql.timeout};"
        )
    return (
        f"DRIVER={{{sql.driver}}};"
        f"SERVER={sql.server};"
        f"DATABASE={sql.database};"
        f"UID={sql.user};"
        f"PWD={sql.password};"
        f"Connection Timeout={sql.timeout};"
        "Trusted_Connection=no;"
    )


def normalize_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        return datetime.fromisoformat(value)
    return datetime(1900, 1, 1)
