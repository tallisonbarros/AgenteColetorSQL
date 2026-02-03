from __future__ import annotations

from datetime import datetime, date
from typing import List
import pyodbc

from config import IncrementalConfig, SourceConfig


_LAST_QUERY: dict[str, dict] = {}
_LAST_SAMPLE: dict[str, list[dict]] = {}


def _quote_identifier(name: str) -> str:
    parts = name.split(".")
    quoted_parts = []
    for part in parts:
        if not part or not part.replace("_", "").isalnum():
            raise ValueError(f"Invalid SQL identifier: {name}")
        quoted_parts.append(f"[{part}]")
    return ".".join(quoted_parts)


def _qualified(alias: str, column: str) -> str:
    return f"{alias}.{_quote_identifier(column)}"


def _build_select(source: SourceConfig) -> str:
    if not source.select:
        return "*"
    columns = [_quote_identifier(col) for col in source.select]
    return ", ".join(columns)


def _build_where(
    incremental: IncrementalConfig,
    last_id: int,
    last_ts: datetime,
    last_tie: int,
    alias: str,
) -> tuple[str, list]:
    clauses = []
    params: list = []
    if incremental.mode == "id":
        id_sql = _qualified(alias, incremental.id_column)
        clauses.append(f"{id_sql} > ?")
        params.append(last_id)
        return " AND ".join(clauses), params

    ts_sql = _qualified(alias, incremental.ts_column)
    tie_sql = _qualified(alias, incremental.tie_breaker)
    clauses.append(f"({ts_sql} > ?) OR ({ts_sql} = ? AND {tie_sql} > ?)")
    params.extend([last_ts, last_ts, last_tie])
    return " AND ".join(clauses), params


def _format_param(value) -> str:
    if isinstance(value, (datetime, date)):
        return f"'{value.isoformat(sep=' ')}'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if value is None:
        return "NULL"
    return str(value)


def _format_query(query: str, params: list) -> str:
    formatted = query
    for param in params:
        formatted = formatted.replace("?", _format_param(param), 1)
    return formatted


def get_last_query(source_name: str) -> dict:
    return _LAST_QUERY.get(source_name, {})


def get_last_sample(source_name: str) -> list[dict]:
    return _LAST_SAMPLE.get(source_name, [])


def fetch_rows(
    sql_conn: str,
    source: SourceConfig,
    last_id: int,
    last_ts: datetime,
    last_tie: int,
    batch_size: int,
) -> List[dict]:
    incremental = source.incremental
    params: list = []

    if source.kind == "table":
        select_sql = _build_select(source)
        table_sql = _quote_identifier(source.table)
        where_clauses = []
        where_sql, where_params = _build_where(
            incremental, last_id, last_ts, last_tie, alias="t"
        )
        if where_sql:
            where_clauses.append(where_sql)
            params.extend(where_params)
        if source.filter:
            where_clauses.append(f"({source.filter})")

        query = f"SELECT TOP ({batch_size}) {select_sql} FROM {table_sql} AS t"
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        if incremental.mode == "id":
            order_by = _qualified("t", incremental.id_column)
        else:
            order_by = (
                f"{_qualified('t', incremental.ts_column)} ASC, "
                f"{_qualified('t', incremental.tie_breaker)} ASC"
            )
        query += f" ORDER BY {order_by}"
    else:
        query = f"SELECT TOP ({batch_size}) * FROM ({source.query}) AS q"
        where_clauses = []
        where_sql, where_params = _build_where(
            incremental, last_id, last_ts, last_tie, alias="q"
        )
        if where_sql:
            where_clauses.append(where_sql)
            params.extend(where_params)
        if source.filter:
            where_clauses.append(f"({source.filter})")
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        if incremental.mode == "id":
            order_by = _qualified("q", incremental.id_column)
        else:
            order_by = (
                f"{_qualified('q', incremental.ts_column)} ASC, "
                f"{_qualified('q', incremental.tie_breaker)} ASC"
            )
        query += f" ORDER BY {order_by}"

    rows: List[dict] = []
    _LAST_QUERY[source.name] = {
        "query": query,
        "params": list(params),
        "query_with_params": _format_query(query, list(params)),
    }
    with pyodbc.connect(sql_conn) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        for row in cursor.fetchall():
            rows.append(dict(zip(columns, row)))
    _LAST_SAMPLE[source.name] = rows[:5]
    return rows
