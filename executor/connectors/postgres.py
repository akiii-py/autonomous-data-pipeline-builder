"""PostgreSQL connector.

Credentials are never taken from the step config. The config names an
environment variable via ``dsn_ref``; the worker resolves it, and the reference
must be in the registry's allowlist (rule 6.4, D-05).
"""

import os
import re
from typing import Any, Dict, Iterable, List, Optional

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

try:
    from executor.worker.errors import NotAllowedError, PermanentError, TransientError
except ImportError:  # pragma: no cover - import shim for PYTHONPATH=executor
    from worker.errors import NotAllowedError, PermanentError, TransientError


def extract(config: Dict[str, Any], allowlist: Any = None) -> Any:
    dsn = _dsn_from_config(config, allowlist)
    query = config.get("query")
    params = config.get("params", [])
    if not query:
        raise PermanentError("postgres extract requires 'query'")

    _reject_non_select(query)

    try:
        with psycopg.connect(dsn, row_factory=dict_row) as conn:
            # The keyword guard above is the fast, informative check. This is
            # the one the database enforces: a read-only transaction rejects
            # any write the guard did not anticipate.
            conn.read_only = True
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
        return rows
    except psycopg.OperationalError as exc:
        raise TransientError(f"postgres extract connection failed: {exc}") from exc
    except psycopg.ProgrammingError as exc:
        raise PermanentError(f"postgres extract rejected: {exc}") from exc


def load(payload: Any, config: Dict[str, Any], allowlist: Any = None) -> Dict[str, Any]:
    dsn = _dsn_from_config(config, allowlist)
    table = config.get("table")
    if not table:
        raise PermanentError("postgres load requires 'table'")

    rows = _normalize_rows(payload)
    if not rows:
        return {"table": table, "inserted": 0}

    columns = sorted(rows[0].keys())
    for row in rows:
        missing = [c for c in columns if c not in row]
        if missing:
            raise PermanentError(f"row missing columns: {missing}")

    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table),
        sql.SQL(",").join(sql.Identifier(c) for c in columns),
        sql.SQL(",").join(sql.Placeholder() for _ in columns),
    )

    values: List[Iterable[Any]] = [[row.get(c) for c in columns] for row in rows]

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(query, values)
            conn.commit()
    except psycopg.OperationalError as exc:
        raise TransientError(f"postgres load connection failed: {exc}") from exc
    except psycopg.ProgrammingError as exc:
        raise PermanentError(f"postgres load rejected: {exc}") from exc

    return {"table": table, "inserted": len(rows)}


# Keywords that make a statement something other than a read. Checked as whole
# tokens after comments, string literals and quoted identifiers are removed, so
# a CTE wrapping an INSERT or a stacked statement is caught, while a column
# called "update" or a string containing "delete" is not.
_WRITE_KEYWORDS = frozenset(
    {
        "insert", "update", "delete", "merge", "truncate", "copy",
        "create", "alter", "drop", "grant", "revoke", "comment",
        "call", "do", "execute", "prepare", "deallocate",
        "set", "reset", "lock", "vacuum", "analyze", "analyse",
        "cluster", "reindex", "refresh", "listen", "notify",
        "begin", "commit", "rollback", "savepoint", "release",
        "security", "into",
    }
)

_SQL_NOISE = re.compile(
    r"""
    --[^\n]*             # line comment
  | /\*.*?\*/            # block comment
  | '(?:[^']|'')*'       # string literal
  | "(?:[^"]|"")*"       # quoted identifier
  | \$([A-Za-z_]*)\$.*?\$\1\$   # dollar-quoted string
    """,
    re.VERBOSE | re.DOTALL,
)


def _reject_non_select(query: str) -> None:
    """Extract queries are reads. This is a textual guard, not a parser: it
    keeps the obvious cases (DML, DDL, stacked statements, writes hidden in a
    CTE) out with a clear error. The read-only transaction in extract() is
    what actually enforces it."""
    bare = _SQL_NOISE.sub(" ", query).strip()
    lowered = bare.lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise PermanentError("postgres extract is restricted to SELECT")

    # Only one statement. A trailing semicolon is fine; a second statement is not.
    if ";" in lowered.rstrip(" ;\t\n"):
        raise PermanentError("postgres extract accepts a single statement")

    tokens = set(re.findall(r"[a-z_]+", lowered))
    found = sorted(tokens & _WRITE_KEYWORDS)
    if found:
        raise PermanentError(
            f"postgres extract is restricted to SELECT; found {', '.join(found)}"
        )


def _dsn_from_config(config: Dict[str, Any], allowlist: Any) -> str:
    if config.get("dsn"):
        raise NotAllowedError(
            "plaintext config.dsn is not accepted; use dsn_ref naming an environment variable"
        )

    ref = config.get("dsn_ref")
    if not ref:
        raise PermanentError("postgres connector requires 'dsn_ref'")

    if allowlist is not None:
        allowlist.check_dsn_ref(ref)

    dsn: Optional[str] = os.getenv(ref)
    if not dsn:
        raise PermanentError(f"dsn_ref {ref!r} is not set in the worker environment")
    return dsn


def _normalize_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        rows = [x for x in payload if isinstance(x, dict)]
        if len(rows) != len(payload):
            raise PermanentError("postgres load expects list[dict]")
        return rows
    if isinstance(payload, dict):
        return [payload]
    raise PermanentError("postgres load expects dict or list[dict]")
