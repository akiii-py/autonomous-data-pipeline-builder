import json
import os
import tempfile
import unittest
from pathlib import Path

from executor.connectors.registry import build_registry
from executor.worker.artifacts import InMemoryArtifactStore, InMemoryExecutionLedger
from executor.worker.models import ExecuteRequest
from executor.worker.runner import StepRunner


def _request(run_id: str, key: str, step_type: str, config: dict, attempt: int = 1) -> ExecuteRequest:
    return ExecuteRequest.parse(
        {
            "run_id": run_id,
            "attempt": attempt,
            "idempotency_key": f"{run_id}:{key}:{attempt}",
            "step": {"key": key, "type": step_type, "config": config},
        }
    )


class RunnerFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        # The allowlist is enforced by the registry, so tests must opt into the
        # directory they use rather than the connector accepting any path.
        self.registry = build_registry(allowed_paths=[str(Path(self.tmpdir).resolve())])
        self.artifacts = InMemoryArtifactStore()
        self.ledger = InMemoryExecutionLedger()
        self.runner = StepRunner(self.registry, self.artifacts, self.ledger)

        self.src = os.path.join(self.tmpdir, "src.json")
        self.out = os.path.join(self.tmpdir, "out.json")
        Path(self.src).write_text(
            json.dumps(
                [
                    {"region": "APAC", "amount": 10},
                    {"region": "APAC", "amount": 5},
                    {"region": "EMEA", "amount": 7},
                ]
            )
        )

    def test_extract_transform_load_with_file_connector(self):
        run_id = "test-runner-flow"

        extract = self.runner.execute(
            _request(run_id, "extract_sales", "extract",
                     {"connector": "file", "path": self.src, "format": "json"})
        )
        self.assertFalse(extract.failed, extract.error_message)
        self.assertEqual(extract.rows_processed, 3)

        transform = self.runner.execute(
            _request(run_id, "agg_sales", "transform", {
                "input_from": "extract_sales",
                "op": "aggregate_sum",
                "group_by": "region",
                "field": "amount",
            })
        )
        self.assertFalse(transform.failed, transform.error_message)
        self.assertEqual(transform.rows_processed, 2)

        load = self.runner.execute(
            _request(run_id, "load_output", "load", {
                "connector": "file",
                "input_from": "agg_sales",
                "path": self.out,
                "format": "json",
            })
        )
        self.assertFalse(load.failed, load.error_message)
        self.assertEqual(load.detail["format"], "json")

        final = self.artifacts.get(run_id, "agg_sales")
        self.assertIsInstance(final, list)
        self.assertEqual(len(final), 2)

    def test_retry_after_completion_replays_instead_of_re_executing(self):
        """A dispatcher timeout does not mean the work did not happen.

        The second attempt carries a different attempt number, so the ledger has
        to key on the logical unit for this to work at all (D-03).
        """
        run_id = "test-replay"

        first = self.runner.execute(
            _request(run_id, "extract_sales", "extract",
                     {"connector": "file", "path": self.src, "format": "json"}, attempt=1)
        )
        self.assertFalse(first.replayed)

        # Remove the source: a genuine re-execution would now fail.
        os.unlink(self.src)

        second = self.runner.execute(
            _request(run_id, "extract_sales", "extract",
                     {"connector": "file", "path": self.src, "format": "json"}, attempt=2)
        )
        self.assertTrue(second.replayed, "retry must replay the recorded result")
        self.assertFalse(second.failed)
        self.assertEqual(second.rows_processed, 3)
        self.assertEqual(second.attempt, 2)

    def test_missing_upstream_artifact_is_permanent(self):
        result = self.runner.execute(
            _request("run-x", "agg", "transform", {"input_from": "nope", "op": "select"})
        )
        self.assertTrue(result.failed)
        self.assertEqual(result.error_class, "permanent")

    def test_unsupported_transform_op_is_permanent(self):
        run_id = "run-op"
        self.runner.execute(
            _request(run_id, "extract_sales", "extract",
                     {"connector": "file", "path": self.src, "format": "json"})
        )
        result = self.runner.execute(
            _request(run_id, "agg", "transform", {"input_from": "extract_sales", "op": "aggregate"})
        )
        self.assertTrue(result.failed)
        self.assertEqual(result.error_class, "permanent")

    def test_path_outside_allowlist_is_rejected(self):
        result = self.runner.execute(
            _request("run-deny", "extract_etc", "extract",
                     {"connector": "file", "path": "/etc/passwd", "format": "json"})
        )
        self.assertTrue(result.failed)
        self.assertEqual(result.error_class, "permanent")
        self.assertIn("allowed prefix", result.error_message)

    def test_unknown_connector_is_rejected(self):
        result = self.runner.execute(
            _request("run-deny", "extract_x", "extract", {"connector": "s3", "path": self.src})
        )
        self.assertTrue(result.failed)
        self.assertIn("unsupported connector", result.error_message)

    def test_failed_load_on_non_idempotent_connector_is_never_retryable(self):
        run_id = "run-load"
        self.artifacts.put(run_id, "rows", [{"a": 1}])

        result = self.runner.execute(
            _request(run_id, "load_http", "load", {
                "connector": "http",
                "input_from": "rows",
                "url": "https://blocked.example.com/ingest",
            })
        )
        self.assertTrue(result.failed)
        # Even a transient-looking failure must not be retried when replaying the
        # load cannot be made safe (rule 1.3).
        self.assertEqual(result.error_class, "permanent")


class WireContractTest(unittest.TestCase):
    def test_request_requires_run_id_and_step(self):
        from executor.worker.errors import PermanentError

        with self.assertRaises(PermanentError):
            ExecuteRequest.parse({"step": {"key": "a", "type": "extract"}})
        with self.assertRaises(PermanentError):
            ExecuteRequest.parse({"run_id": "r1"})
        with self.assertRaises(PermanentError):
            ExecuteRequest.parse({"run_id": "r1", "step": {"key": "a"}})

    def test_idempotency_key_defaults_to_logical_unit(self):
        req = ExecuteRequest.parse(
            {"run_id": "r1", "attempt": 3, "step": {"key": "s", "type": "extract"}}
        )
        self.assertEqual(req.idempotency_key, "r1:s:3")


if __name__ == "__main__":
    unittest.main()
