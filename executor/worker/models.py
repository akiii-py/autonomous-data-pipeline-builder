"""The worker's wire contract.

These dataclasses are the single definition of the request and response shapes
(D-04). Parsing happens once, at the HTTP boundary; nothing downstream reaches
into raw dicts with .get(), and nothing re-validates a field the parse already
guaranteed (rule 1.4).
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

try:
    from executor.worker.errors import PermanentError
except ImportError:  # pragma: no cover - import shim for PYTHONPATH=executor
    from worker.errors import PermanentError


@dataclass
class StepPayload:
    key: str
    type: str
    config: Dict[str, Any] = field(default_factory=dict)
    id: str = ""
    name: str = ""
    depends_on: List[str] = field(default_factory=list)

    @classmethod
    def parse(cls, raw: Any) -> "StepPayload":
        if not isinstance(raw, dict):
            raise PermanentError("step must be an object")

        key = _require_str(raw, "key")
        step_type = _require_str(raw, "type")

        config = raw.get("config") or {}
        if not isinstance(config, dict):
            raise PermanentError("step.config must be an object")

        depends_on = raw.get("depends_on") or []
        if not isinstance(depends_on, list):
            raise PermanentError("step.depends_on must be a list")

        return cls(
            key=key,
            type=step_type,
            config=config,
            id=str(raw.get("id") or ""),
            name=str(raw.get("name") or ""),
            depends_on=[str(d) for d in depends_on],
        )


@dataclass
class ExecuteRequest:
    run_id: str
    step: StepPayload
    pipeline_id: str = ""
    attempt: int = 1
    idempotency_key: str = ""

    @classmethod
    def parse(cls, raw: Any) -> "ExecuteRequest":
        if not isinstance(raw, dict):
            raise PermanentError("request body must be an object")

        run_id = _require_str(raw, "run_id")
        step = StepPayload.parse(raw.get("step"))

        attempt = raw.get("attempt", 1)
        if not isinstance(attempt, int) or attempt < 1:
            raise PermanentError("attempt must be a positive integer")

        # The orchestrator mints this. Falling back to the logical unit keeps a
        # hand-made request working without weakening dedup (D-03).
        idempotency_key = str(raw.get("idempotency_key") or f"{run_id}:{step.key}:{attempt}")

        return cls(
            run_id=run_id,
            step=step,
            pipeline_id=str(raw.get("pipeline_id") or ""),
            attempt=attempt,
            idempotency_key=idempotency_key,
        )


@dataclass
class StepResult:
    """The single value a step execution returns (D-01). Everything the
    orchestrator needs to know travels here — never in a side channel."""

    run_id: str
    step_key: str
    idempotency_key: str = ""
    attempt: int = 0
    rows_processed: int = 0
    simulated: bool = False
    replayed: bool = False
    error_class: str = ""
    error_message: str = ""
    detail: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "run_id": self.run_id,
            "step_key": self.step_key,
            "idempotency_key": self.idempotency_key,
            "attempt": self.attempt,
            "rows_processed": self.rows_processed,
            "simulated": self.simulated,
            "replayed": self.replayed,
        }
        if self.error_class:
            out["error_class"] = self.error_class
        if self.error_message:
            out["error_message"] = self.error_message
        if self.detail is not None:
            out["detail"] = self.detail
        return out

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "StepResult":
        return cls(
            run_id=str(raw.get("run_id") or ""),
            step_key=str(raw.get("step_key") or ""),
            idempotency_key=str(raw.get("idempotency_key") or ""),
            attempt=int(raw.get("attempt") or 0),
            rows_processed=int(raw.get("rows_processed") or 0),
            simulated=bool(raw.get("simulated")),
            replayed=bool(raw.get("replayed")),
            error_class=str(raw.get("error_class") or ""),
            error_message=str(raw.get("error_message") or ""),
            detail=raw.get("detail"),
        )

    @property
    def failed(self) -> bool:
        return bool(self.error_message)


@dataclass
class ExecuteResponse:
    status: str
    result: Optional[StepResult] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"status": self.status}
        if self.result is not None:
            out["result"] = self.result.to_dict()
        if self.error:
            out["error"] = self.error
        return out


def _require_str(raw: Dict[str, Any], field_name: str) -> str:
    value = raw.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise PermanentError(f"{field_name} is required")
    return value.strip()
