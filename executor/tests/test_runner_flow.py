import json
import os
import tempfile
import unittest

from executor.worker.artifacts import get_artifact
from executor.worker.runner import execute_step


class RunnerFlowTest(unittest.TestCase):
    def test_extract_transform_load_with_file_connector(self):
        run_id = "test-runner-flow"

        src = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        src.write(
            json.dumps(
                [
                    {"region": "APAC", "amount": 10},
                    {"region": "APAC", "amount": 5},
                    {"region": "EMEA", "amount": 7},
                ]
            ).encode("utf-8")
        )
        src.close()

        out = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        out.close()

        try:
            extract_res = execute_step(
                run_id,
                {
                    "key": "extract_sales",
                    "type": "extract",
                    "config": {"connector": "file", "path": src.name, "format": "json"},
                },
            )
            self.assertEqual(extract_res["rows"], 3)

            transform_res = execute_step(
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
            self.assertEqual(transform_res["rows"], 2)

            load_res = execute_step(
                run_id,
                {
                    "key": "load_output",
                    "type": "load",
                    "config": {
                        "connector": "file",
                        "input_from": "agg_sales",
                        "path": out.name,
                        "format": "json",
                    },
                },
            )
            self.assertEqual(load_res["result"]["format"], "json")

            final = get_artifact(run_id, "agg_sales")
            self.assertIsInstance(final, list)
            self.assertEqual(len(final), 2)
        finally:
            os.unlink(src.name)
            os.unlink(out.name)


if __name__ == "__main__":
    unittest.main()
