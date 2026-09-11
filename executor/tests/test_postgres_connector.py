"""Postgres connector tests.

These need a live database. They skip rather than error when one is not
configured, so the suite is runnable on a laptop and in CI without a matrix.
"""

import os
import unittest
import uuid

from executor.connectors import postgres
from executor.connectors.registry import Allowlist
from executor.worker.errors import NotAllowedError, PermanentError

DSN_ENV = "PG_TEST_DSN"


def _dsn():
    return os.getenv(DSN_ENV)


class PostgresConfigTest(unittest.TestCase):
    """Config handling needs no database."""

    def test_plaintext_dsn_is_rejected(self):
        with self.assertRaises(NotAllowedError):
            postgres.extract({"dsn": "postgresql:///x", "query": "SELECT 1"})

    def test_dsn_ref_must_be_allowlisted(self):
        allow = Allowlist(dsn_refs=["PG_ALLOWED"])
        with self.assertRaises(NotAllowedError):
            postgres.extract({"dsn_ref": "PG_OTHER", "query": "SELECT 1"}, allow)

    def test_dsn_ref_must_be_set_in_environment(self):
        allow = Allowlist(dsn_refs=["PG_DEFINITELY_UNSET"])
        with self.assertRaises(PermanentError):
            postgres.extract({"dsn_ref": "PG_DEFINITELY_UNSET", "query": "SELECT 1"}, allow)

    def test_extract_is_restricted_to_select(self):
        os.environ["PG_TEST_REF"] = "postgresql:///postgres"
        allow = Allowlist(dsn_refs=["PG_TEST_REF"])
        try:
            with self.assertRaises(PermanentError) as ctx:
                postgres.extract({"dsn_ref": "PG_TEST_REF", "query": "DROP TABLE users"}, allow)
            self.assertIn("restricted to SELECT", str(ctx.exception))
        finally:
            del os.environ["PG_TEST_REF"]


@unittest.skipUnless(_dsn(), f"set {DSN_ENV} to run live Postgres tests")
class PostgresConnectorTest(unittest.TestCase):
    def setUp(self):
        os.environ["PG_TEST_REF"] = _dsn()
        self.allow = Allowlist(dsn_refs=["PG_TEST_REF"])
        self.src_table = f"phase4_src_{uuid.uuid4().hex[:8]}"
        self.dst_table = f"phase4_dst_{uuid.uuid4().hex[:8]}"

        import psycopg

        with psycopg.connect(_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE {self.src_table} (
                        id SERIAL PRIMARY KEY,
                        region TEXT NOT NULL,
                        amount INT NOT NULL
                    );
                    INSERT INTO {self.src_table}(region, amount) VALUES
                        ('APAC', 10), ('APAC', 5), ('EMEA', 7);
                    CREATE TABLE {self.dst_table} (
                        region TEXT NOT NULL,
                        amount INT NOT NULL
                    );
                    """
                )
            conn.commit()

    def tearDown(self):
        import psycopg

        with psycopg.connect(_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {self.src_table}; DROP TABLE IF EXISTS {self.dst_table};")
            conn.commit()
        os.environ.pop("PG_TEST_REF", None)

    def test_extract_and_load(self):
        rows = postgres.extract(
            {
                "dsn_ref": "PG_TEST_REF",
                "query": f"SELECT region, amount FROM {self.src_table} ORDER BY id",
            },
            self.allow,
        )
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["region"], "APAC")

        load_result = postgres.load(
            {"region": "NA", "amount": 99},
            {"dsn_ref": "PG_TEST_REF", "table": self.dst_table},
            self.allow,
        )
        self.assertEqual(load_result["inserted"], 1)

        verify = postgres.extract(
            {"dsn_ref": "PG_TEST_REF", "query": f"SELECT region, amount FROM {self.dst_table}"},
            self.allow,
        )
        self.assertEqual(len(verify), 1)
        self.assertEqual(verify[0]["region"], "NA")


if __name__ == "__main__":
    unittest.main()
