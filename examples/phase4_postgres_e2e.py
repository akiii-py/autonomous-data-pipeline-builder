"""Phase 4 local E2E script using Homebrew PostgreSQL.

Run from repo root:
    .venv/bin/python examples/phase4_postgres_e2e.py

Optional:
    PG_DSN=postgresql:///postgres .venv/bin/python examples/phase4_postgres_e2e.py
"""

import os
import sys
import uuid
from pathlib import Path

import psycopg

# Ensure imports work when running as a standalone script from repo root.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from executor.worker.runner import execute_step


def main() -> None:
    dsn = os.getenv("PG_DSN") or "postgresql:///postgres"
    run_id = f"phase4-e2e-{uuid.uuid4().hex[:8]}"

    src_table = f"phase4_src_{uuid.uuid4().hex[:8]}"
    out_table = f"phase4_out_{uuid.uuid4().hex[:8]}"

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
                    ('APAC', 10),
                    ('APAC', 5),
                    ('EMEA', 7);

                CREATE TABLE {out_table} (
                    region TEXT NOT NULL,
                    sum_amount DOUBLE PRECISION NOT NULL
                );
                """
            )
        conn.commit()

    try:
        print("Running steps...")

        print(
            execute_step(
                run_id,
                {
                    "key": "extract_sales",
                    "type": "extract",
                    "config": {
                        "connector": "postgres",
                        "dsn": dsn,
                        "query": f"SELECT region, amount FROM {src_table} ORDER BY id",
                    },
                },
            )
        )

        print(
            execute_step(
                run_id,
                {
                    "key": "agg_sales",
                    "type": "transform",
                    "config": {
                        "input_from": "extract_sales",
                        "op": "aggregate_sum",
                        "group_by": "region",
                        "field": "amount",
                    },
                },
            )
        )

        print(
            execute_step(
                run_id,
                {
                    "key": "load_sales",
                    "type": "load",
                    "config": {
                        "connector": "postgres",
                        "dsn": dsn,
                        "input_from": "agg_sales",
                        "table": out_table,
                    },
                },
            )
        )

        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT region, sum_amount FROM {out_table} ORDER BY region")
                rows = cur.fetchall()
        print("Output rows:", rows)
    finally:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {src_table}; DROP TABLE IF EXISTS {out_table};")
            conn.commit()


if __name__ == "__main__":
    main()
