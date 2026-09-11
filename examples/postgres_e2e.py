"""Local extract → transform → load walkthrough against PostgreSQL.

This drives the worker's StepRunner directly, without the orchestrator, so it
shows what one run's worth of steps does to real data.

Run from the repo root:
    PG_DSN=postgresql:///postgres .venv/bin/python examples/postgres_e2e.py

Two things to notice, because they are the point:

  * Credentials are never in the step config. The config names an environment
    variable via `dsn_ref`, and that name has to be in the registry's allowlist.
  * The runner consults its ledger before executing. Re-running a step that
    already completed replays the recorded result instead of inserting again —
    which is what stops a dispatcher timeout from duplicating a load.

This example uses in-memory storage for brevity. The deployed worker uses
Postgres-backed storage so artifacts survive a restart and are visible to more
than one worker.
"""

import os
import sys
import uuid
from pathlib import Path

import psycopg

# Ensure imports work when running as a standalone script from the repo root.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from executor.connectors.registry import build_registry
from executor.worker.artifacts import InMemoryArtifactStore, InMemoryExecutionLedger
from executor.worker.models import ExecuteRequest
from executor.worker.runner import StepRunner

DSN_REF = "PG_DSN"


def step(run_id: str, key: str, step_type: str, config: dict, attempt: int = 1) -> ExecuteRequest:
    return ExecuteRequest.parse(
        {
            "run_id": run_id,
            "attempt": attempt,
            "idempotency_key": f"{run_id}:{key}:{attempt}",
            "step": {"key": key, "type": step_type, "config": config},
        }
    )


def report(label: str, result) -> None:
    detail = f" detail={result.detail}" if result.detail else ""
    status = "FAILED" if result.failed else "ok"
    replayed = " (replayed)" if result.replayed else ""
    print(f"  {label:<16} {status}{replayed} rows={result.rows_processed}{detail}")
    if result.failed:
        print(f"                   {result.error_class}: {result.error_message}")


def main() -> None:
    dsn = os.getenv(DSN_REF) or "postgresql:///postgres"
    os.environ[DSN_REF] = dsn

    run_id = f"e2e-{uuid.uuid4().hex[:8]}"
    src_table = f"e2e_src_{uuid.uuid4().hex[:8]}"
    out_table = f"e2e_out_{uuid.uuid4().hex[:8]}"

    # The registry owns connector lookup and the allowlist. Only PG_DSN may be
    # resolved; any other dsn_ref is rejected before a connection is attempted.
    runner = StepRunner(
        registry=build_registry(allowed_dsn_refs=[DSN_REF]),
        artifacts=InMemoryArtifactStore(),
        ledger=InMemoryExecutionLedger(),
    )

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE {src_table} (
                    id SERIAL PRIMARY KEY,
                    region TEXT NOT NULL,
                    amount INT NOT NULL
                );
                INSERT INTO {src_table}(region, amount) VALUES
                    ('APAC', 10), ('APAC', 5), ('EMEA', 7);

                CREATE TABLE {out_table} (
                    region TEXT NOT NULL,
                    sum_amount DOUBLE PRECISION NOT NULL
                );
                """
            )
        conn.commit()

    try:
        print(f"run {run_id}")

        report("extract", runner.execute(step(run_id, "extract_sales", "extract", {
            "connector": "postgres",
            "dsn_ref": DSN_REF,
            "query": f"SELECT region, amount FROM {src_table} ORDER BY id",
        })))

        report("transform", runner.execute(step(run_id, "agg_sales", "transform", {
            "input_from": "extract_sales",
            "op": "aggregate_sum",
            "group_by": "region",
            "field": "amount",
        })))

        load_config = {
            "connector": "postgres",
            "dsn_ref": DSN_REF,
            "input_from": "agg_sales",
            "table": out_table,
        }
        report("load", runner.execute(step(run_id, "load_sales", "load", load_config)))

        # A retry of a step that already succeeded. Attempt 2 is a different
        # idempotency key, but the ledger keys on (run_id, step_key), so this
        # replays rather than inserting the rows a second time.
        report("load (retry)", runner.execute(step(run_id, "load_sales", "load", load_config, attempt=2)))

        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT region, sum_amount FROM {out_table} ORDER BY region")
                rows = cur.fetchall()

        print(f"\noutput rows: {rows}")
        print(f"row count:   {len(rows)}  <- 2, not 4: the retry replayed")
    finally:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {src_table}; DROP TABLE IF EXISTS {out_table};")
            conn.commit()


if __name__ == "__main__":
    main()
