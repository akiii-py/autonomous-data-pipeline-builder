"""Transformation operations.

Transform failures are always permanent: an unsupported op or a malformed row
will fail identically on a second attempt, so they must not consume retry budget
(D-02).
"""

from typing import Any, Dict, List

try:
    from executor.worker.errors import PermanentError
except ImportError:  # pragma: no cover - import shim for PYTHONPATH=executor
    from worker.errors import PermanentError

SUPPORTED_OPS = ("select", "filter_eq", "aggregate_sum")


def apply_transform(data: Any, config: Dict[str, Any]) -> Any:
    op = config.get("op")

    if op == "select":
        fields = config.get("fields", [])
        rows = _as_rows(data)
        return [{k: row.get(k) for k in fields} for row in rows]

    if op == "filter_eq":
        field = config.get("field")
        value = config.get("value")
        rows = _as_rows(data)
        return [row for row in rows if row.get(field) == value]

    if op == "aggregate_sum":
        group_by = config.get("group_by")
        field = config.get("field")
        rows = _as_rows(data)
        acc: Dict[str, float] = {}
        for row in rows:
            key = str(row.get(group_by))
            try:
                acc[key] = acc.get(key, 0.0) + float(row.get(field, 0) or 0)
            except (TypeError, ValueError) as exc:
                raise PermanentError(
                    f"aggregate_sum: field {field!r} is not numeric in row {key!r}"
                ) from exc
        return [{group_by: k, f"sum_{field}": v} for k, v in acc.items()]

    raise PermanentError(f"unsupported transform op: {op!r} (supported: {list(SUPPORTED_OPS)})")


def _as_rows(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        return [data]
    raise PermanentError("transform input must be object or list of objects")
