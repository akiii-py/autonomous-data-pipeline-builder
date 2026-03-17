import csv
import json
from pathlib import Path
from typing import Any, Dict, List


def extract(config: Dict[str, Any]) -> Any:
    path = config.get("path")
    fmt = config.get("format", "json")
    if not path:
        raise ValueError("file connector requires 'path'")

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    if fmt == "json":
        return json.loads(p.read_text())
    if fmt == "csv":
        with p.open(newline="") as f:
            return list(csv.DictReader(f))

    raise ValueError(f"unsupported file format: {fmt}")


def load(payload: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    path = config.get("path")
    fmt = config.get("format", "json")
    if not path:
        raise ValueError("file connector requires 'path'")

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "json":
        p.write_text(json.dumps(payload, indent=2))
        return {"written": str(p), "format": fmt}

    if fmt == "csv":
        rows: List[Dict[str, Any]] = payload if isinstance(payload, list) else []
        headers = sorted(rows[0].keys()) if rows else []
        with p.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if headers:
                writer.writeheader()
                writer.writerows(rows)
        return {"written": str(p), "format": fmt, "rows": len(rows)}

    raise ValueError(f"unsupported file format: {fmt}")
