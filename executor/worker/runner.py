"""Step execution.

Every path returns a StepResult (D-01). Connectors come from the registry, never
constructed inline (D-05). Before doing any work the runner consults the
execution ledger, so a retry after a dispatcher timeout replays the prior result
rather than running a load a second time (D-03).
"""

from typing import Any, Optional

try:
    from executor.connectors.registry import Connector, Registry
    from executor.transformations.ops import apply_transform
    from executor.worker.artifacts import ArtifactStore, ExecutionLedger
    from executor.worker.errors import PERMANENT, PermanentError, WorkerExecutionError, classify
    from executor.worker.models import ExecuteRequest, StepResult
except ImportError:  # pragma: no cover - import shim for PYTHONPATH=executor
    from connectors.registry import Connector, Registry
    from transformations.ops import apply_transform
    from worker.artifacts import ArtifactStore, ExecutionLedger
    from worker.errors import PERMANENT, PermanentError, WorkerExecutionError, classify
    from worker.models import ExecuteRequest, StepResult


class StepRunner:
    """Executes one step against injected collaborators.

    Dependencies are constructed by the server, not looked up here, so tests can
    substitute in-memory storage without the production path ever depending on
    process memory (D-06).
    """

    def __init__(
        self,
        registry: Registry,
        artifacts: ArtifactStore,
        ledger: Optional[ExecutionLedger] = None,
    ):
        self._registry = registry
        self._artifacts = artifacts
        self._ledger = ledger

    def execute(self, req: ExecuteRequest) -> StepResult:
        # Execution identity is the logical unit, so a replayed attempt is
        # recognised however many times it has been retried (D-03).
        if self._ledger is not None:
            prior = self._ledger.find(req.run_id, req.step.key)
            if prior is not None:
                prior.replayed = True
                prior.attempt = req.attempt
                prior.idempotency_key = req.idempotency_key
                return prior

        try:
            result = self._dispatch(req)
        except WorkerExecutionError as exc:
            return self._failure(req, str(exc), exc.error_class)
        except Exception as exc:  # noqa: BLE001 - classified, never swallowed
            return self._failure(req, str(exc), classify(exc))

        if self._ledger is not None and not result.failed:
            self._ledger.record(result)

        return result

    def _dispatch(self, req: ExecuteRequest) -> StepResult:
        step = req.step
        config = step.config

        if step.type == "extract":
            connector = self._connector(config)
            data = connector.extract_fn()(config, connector.allowlist)
            self._artifacts.put(req.run_id, step.key, data)
            return self._success(req, rows=_row_count(data))

        if step.type == "transform":
            data = self._upstream(req)
            result = apply_transform(data, config)
            self._artifacts.put(req.run_id, step.key, result)
            return self._success(req, rows=_row_count(result))

        if step.type == "load":
            data = self._upstream(req)
            connector = self._connector(config)
            detail = connector.load_fn()(data, config, connector.allowlist)
            self._artifacts.put(req.run_id, step.key, detail)
            return self._success(req, rows=_row_count(data), detail=detail, connector=connector)

        raise PermanentError(f"unsupported step type: {step.type}")

    def _connector(self, config: dict) -> Connector:
        return self._registry.get(config.get("connector") or "file")

    def _upstream(self, req: ExecuteRequest) -> Any:
        source = req.step.config.get("input_from")
        if not source:
            raise PermanentError(f"{req.step.type} requires config.input_from")

        data = self._artifacts.get(req.run_id, source)
        if data is None:
            raise PermanentError(f"missing upstream artifact: {source}")
        return data

    def _success(
        self,
        req: ExecuteRequest,
        rows: int = 0,
        detail: Optional[dict] = None,
        connector: Optional[Connector] = None,
    ) -> StepResult:
        return StepResult(
            run_id=req.run_id,
            step_key=req.step.key,
            idempotency_key=req.idempotency_key,
            attempt=req.attempt,
            rows_processed=rows,
            detail=detail,
        )

    def _failure(self, req: ExecuteRequest, message: str, error_class: str) -> StepResult:
        # A connector whose load cannot be replayed safely is never retried,
        # whatever the underlying classification says (rule 1.3).
        if req.step.type == "load":
            connector = self._registry_or_none(req.step.config)
            if connector is not None and not connector.idempotent_load:
                error_class = PERMANENT

        return StepResult(
            run_id=req.run_id,
            step_key=req.step.key,
            idempotency_key=req.idempotency_key,
            attempt=req.attempt,
            error_class=error_class,
            error_message=message,
        )

    def _registry_or_none(self, config: dict) -> Optional[Connector]:
        try:
            return self._connector(config)
        except WorkerExecutionError:
            return None


def _row_count(value: Any) -> int:
    return len(value) if isinstance(value, list) else 1
