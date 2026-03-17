from typing import Any, Dict, List


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
            acc[key] = acc.get(key, 0.0) + float(row.get(field, 0) or 0)
        return [{group_by: k, f"sum_{field}": v} for k, v in acc.items()]

    raise ValueError(f"unsupported transform op: {op}")


def _as_rows(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        return [data]
    raise ValueError("transform input must be object or list of objects")
