"""Artifact storage.

Artifacts are keyed by (run_id, step_key) in storage that outlives the worker
process and is visible to more than one worker (D-06). The module-level dict this
replaces was the load-bearing constraint behind three separate problems: the
worker could not be restarted, could not be scaled horizontally, and leaked the
payload of every run it had ever executed.

The same module holds the execution ledger (D-03): the worker's durable record of
what it has already done, so a retry after a dispatcher timeout replays the
stored result instead of re-running a load.
"""

import json
import threading
from typing import Any, Dict, Optional, Protocol

try:
    from executor.worker.errors import TransientError
    from executor.worker.models import StepResult
except ImportError:  # pragma: no cover - import shim for PYTHONPATH=executor
    from worker.errors import TransientError
    from worker.models import StepResult


class ArtifactStore(Protocol):
    """The storage contract. Production uses Postgres; tests may substitute the
    in-memory implementation explicitly."""

    def put(self, run_id: str, step_key: str, value: Any) -> None: ...

    def get(self, run_id: str, step_key: str) -> Optional[Any]: ...

    def delete_run(self, run_id: str) -> None: ...


class ExecutionLedger(Protocol):
    """Durable record of completed work, keyed by the logical unit."""

    def find(self, run_id: str, step_key: str) -> Optional[StepResult]: ...

    def record(self, result: StepResult) -> None: ...


class InMemoryArtifactStore:
    """Test-only artifact store.

    This exists so the unit suite can run without a database. It must never be
    wired into the server — process-local state is exactly what D-06 removes.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: Dict[str, Dict[str, Any]] = {}

    def put(self, run_id: str, step_key: str, value: Any) -> None:
        with self._lock:
            self._data.setdefault(run_id, {})[step_key] = value

    def get(self, run_id: str, step_key: str) -> Optional[Any]:
        with self._lock:
            return self._data.get(run_id, {}).get(step_key)

    def delete_run(self, run_id: str) -> None:
        with self._lock:
            self._data.pop(run_id, None)


class InMemoryExecutionLedger:
    """Test-only execution ledger. See InMemoryArtifactStore."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: Dict[str, StepResult] = {}

    def find(self, run_id: str, step_key: str) -> Optional[StepResult]:
        with self._lock:
            return self._data.get(f"{run_id}:{step_key}")

    def record(self, result: StepResult) -> None:
        with self._lock:
            self._data[f"{result.run_id}:{result.step_key}"] = result


class PostgresArtifactStore:
    """Artifacts in the database the orchestrator already owns.

    Writes are upserts on (run_id, step_key), so a replayed step overwrites
    rather than duplicating. Cleanup on run completion is performed by the
    orchestrator inside the run's terminal transaction.
    """

    def __init__(self, dsn: str):
        self._dsn = dsn

    def put(self, run_id: str, step_key: str, value: Any) -> None:
        payload = json.dumps(value)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO run_artifacts (run_id, step_key, payload, byte_size)
                    VALUES (%s, %s, %s::jsonb, %s)
                    ON CONFLICT (run_id, step_key)
                    DO UPDATE SET payload = EXCLUDED.payload,
                                  byte_size = EXCLUDED.byte_size,
                                  created_at = NOW()
                    """,
                    (run_id, step_key, payload, len(payload)),
                )
            conn.commit()

    def get(self, run_id: str, step_key: str) -> Optional[Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT payload FROM run_artifacts WHERE run_id = %s AND step_key = %s",
                    (run_id, step_key),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return row[0]

    def delete_run(self, run_id: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM run_artifacts WHERE run_id = %s", (run_id,))
            conn.commit()

    def _connect(self):
        import psycopg

        try:
            return psycopg.connect(self._dsn)
        except psycopg.OperationalError as exc:
            raise TransientError(f"artifact store unavailable: {exc}") from exc


class PostgresExecutionLedger:
    """The worker's durable record of completed work (D-03).

    Keyed by (run_id, step_key) — the logical unit — not by the per-attempt
    idempotency key. That is the whole point: a dispatcher timeout produces a
    retry with a new attempt number, and keying on the attempt would mean the
    ledger never matched and the load ran twice.
    """

    def __init__(self, dsn: str):
        self._dsn = dsn

    def find(self, run_id: str, step_key: str) -> Optional[StepResult]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT result FROM step_executions WHERE run_id = %s AND step_key = %s",
                    (run_id, step_key),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return StepResult.from_dict(row[0])

    def record(self, result: StepResult) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO step_executions
                        (run_id, step_key, idempotency_key, attempt, result)
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (run_id, step_key) DO NOTHING
                    """,
                    (
                        result.run_id,
                        result.step_key,
                        result.idempotency_key,
                        result.attempt,
                        json.dumps(result.to_dict()),
                    ),
                )
            conn.commit()

    def _connect(self):
        import psycopg

        try:
            return psycopg.connect(self._dsn)
        except psycopg.OperationalError as exc:
            raise TransientError(f"execution ledger unavailable: {exc}") from exc
