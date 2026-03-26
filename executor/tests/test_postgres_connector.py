import os
import uuid
import unittest

from executor.connectors import postgres


def _dsn() -> str:
    return os.getenv("PG_DSN") or "postgresql:///postgres"


class PostgresConnectorTest(unittest.TestCase):
    def test_extract_and_load(self):
        dsn = _dsn()
        src_table = f"phase4_src_{uuid.uuid4().hex[:8]}"
        dst_table = f"phase4_dst_{uuid.uuid4().hex[:8]}"

        setup_sql = f"""
        CREATE TABLE {src_table} (
            id SERIAL PRIMARY KEY,
            region TEXT NOT NULL,
            amount INT NOT NULL
        );
        INSERT INTO {src_table}(region, amount) VALUES
            ('APAC', 10),
            ('APAC', 5),
            ('EMEA', 7);

        CREATE TABLE {dst_table} (
            region TEXT NOT NULL,
            amount INT NOT NULL
        );
        """

        cleanup_sql = f"""
        DROP TABLE IF EXISTS {src_table};
        DROP TABLE IF EXISTS {dst_table};
        """

        import psycopg

        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(setup_sql)
            conn.commit()

        try:
            rows = postgres.extract({"dsn": dsn, "query": f"SELECT region, amount FROM {src_table} ORDER BY id"})
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0]["region"], "APAC")

            load_result = postgres.load({"region": "NA", "amount": 99}, {"dsn": dsn, "table": dst_table})
            self.assertEqual(load_result["inserted"], 1)

            verify = postgres.extract({"dsn": dsn, "query": f"SELECT region, amount FROM {dst_table}"})
            self.assertEqual(len(verify), 1)
            self.assertEqual(verify[0]["region"], "NA")
        finally:
            with psycopg.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(cleanup_sql)
                conn.commit()


if __name__ == "__main__":
    unittest.main()
