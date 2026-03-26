import os
from typing import Any, Dict, Iterable, List

import psycopg
from psycopg import sql
from psycopg.rows import dict_row


def extract(config: Dict[str, Any]) -> Any:
    dsn = _dsn_from_config(config)
    query = config.get("query")
    params = config.get("params", [])
    if not query:
        raise ValueError("postgres extract requires 'query'")

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    return rows


def load(payload: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    dsn = _dsn_from_config(config)
    table = config.get("table")
    if not table:
        raise ValueError("postgres load requires 'table'")

    rows = _normalize_rows(payload)
    if not rows:
        return {"table": table, "inserted": 0}

    columns = sorted(rows[0].keys())
    for row in rows:
        missing = [c for c in columns if c not in row]
        if missing:
            raise ValueError(f"row missing columns: {missing}")

    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table),
        sql.SQL(",").join(sql.Identifier(c) for c in columns),
        sql.SQL(",").join(sql.Placeholder() for _ in columns),
    )

    values: List[Iterable[Any]] = [[row.get(c) for c in columns] for row in rows]

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.executemany(query, values)
        conn.commit()

    return {"table": table, "inserted": len(rows)}


def _dsn_from_config(config: Dict[str, Any]) -> str:
    return (
        config.get("dsn")
        or os.getenv("PG_DSN")
        or os.getenv("DATABASE_URL")
        or "postgresql:///postgres"
    )


def _normalize_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        rows = [x for x in payload if isinstance(x, dict)]
        if len(rows) != len(payload):
            raise ValueError("postgres load expects list[dict]")
        return rows
    if isinstance(payload, dict):
        return [payload]
    raise ValueError("postgres load expects dict or list[dict]")
