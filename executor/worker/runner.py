from typing import Any, Dict

try:
    from executor.connectors import file_io, http_api, postgres
    from executor.transformations.ops import apply_transform
    from executor.worker.artifacts import get_artifact, put_artifact
    from executor.worker.errors import WorkerExecutionError
except ImportError:
    from connectors import file_io, http_api, postgres
    from transformations.ops import apply_transform
    from worker.artifacts import get_artifact, put_artifact
    from worker.errors import WorkerExecutionError


def execute_step(run_id: str, step: Dict[str, Any]) -> Dict[str, Any]:
    step_key = step.get("key")
    step_type = step.get("type")
    config = step.get("config") or {}

    if not step_key:
        raise WorkerExecutionError("step.key is required")

    if step_type == "extract":
        connector = _connector(config)
        try:
            result = connector["extract"](config)
        except Exception as exc:
            raise WorkerExecutionError(f"extract failed for {step_key}: {exc}") from exc
        put_artifact(run_id, step_key, result)
        return {"step_key": step_key, "rows": _len_if_list(result)}

    if step_type == "transform":
        source = config.get("input_from")
        if not source:
            raise WorkerExecutionError("transform requires config.input_from")
        data = get_artifact(run_id, source)
        if data is None:
            raise WorkerExecutionError(f"missing upstream artifact: {source}")
        try:
            result = apply_transform(data, config)
        except Exception as exc:
            raise WorkerExecutionError(f"transform failed for {step_key}: {exc}") from exc
        put_artifact(run_id, step_key, result)
        return {"step_key": step_key, "rows": _len_if_list(result)}

    if step_type == "load":
        source = config.get("input_from")
        if not source:
            raise WorkerExecutionError("load requires config.input_from")
        data = get_artifact(run_id, source)
        if data is None:
            raise WorkerExecutionError(f"missing upstream artifact: {source}")
        connector = _connector(config)
        try:
            result = connector["load"](data, config)
        except Exception as exc:
            raise WorkerExecutionError(f"load failed for {step_key}: {exc}") from exc
        put_artifact(run_id, step_key, result)
        return {"step_key": step_key, "result": result}

    raise WorkerExecutionError(f"unsupported step type: {step_type}")


def _connector(config: Dict[str, Any]):
    name = config.get("connector", "file")
    if name == "file":
        return {"extract": file_io.extract, "load": file_io.load}
    if name == "http":
        return {"extract": http_api.extract, "load": http_api.load}
    if name == "postgres":
        return {"extract": postgres.extract, "load": postgres.load}
    raise WorkerExecutionError(f"unsupported connector: {name}")


def _len_if_list(x: Any) -> int:
    return len(x) if isinstance(x, list) else 1
