"""Filesystem connector.

Paths are checked against the registry's allowlist before any read or write
(D-05). Loads are whole-file writes, which is why the registry marks this
connector's load idempotent.
"""

import csv
import json
from pathlib import Path
from typing import Any, Dict, List

try:
    from executor.worker.errors import PermanentError, TransientError
except ImportError:  # pragma: no cover - import shim for PYTHONPATH=executor
    from worker.errors import PermanentError, TransientError


def extract(config: Dict[str, Any], allowlist: Any = None) -> Any:
    p = _checked_path(config, allowlist)
    fmt = config.get("format", "json")

    if not p.exists():
        raise PermanentError(f"file not found: {p}")

    try:
        if fmt == "json":
            return json.loads(p.read_text())
        if fmt == "csv":
            with p.open(newline="") as f:
                return list(csv.DictReader(f))
    except json.JSONDecodeError as exc:
        raise PermanentError(f"file is not valid json: {exc}") from exc
    except OSError as exc:
        raise TransientError(f"file read failed: {exc}") from exc

    raise PermanentError(f"unsupported file format: {fmt}")


def load(payload: Any, config: Dict[str, Any], allowlist: Any = None) -> Dict[str, Any]:
    p = _checked_path(config, allowlist)
    fmt = config.get("format", "json")

    try:
        p.parent.mkdir(parents=True, exist_ok=True)

        if fmt == "json":
            p.write_text(json.dumps(payload, indent=2))
            return {"written": str(p), "format": fmt, "rows": _row_count(payload)}

        if fmt == "csv":
            rows: List[Dict[str, Any]] = payload if isinstance(payload, list) else []
            headers = sorted(rows[0].keys()) if rows else []
            with p.open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if headers:
                    writer.writeheader()
                    writer.writerows(rows)
            return {"written": str(p), "format": fmt, "rows": len(rows)}
    except OSError as exc:
        raise TransientError(f"file write failed: {exc}") from exc

    raise PermanentError(f"unsupported file format: {fmt}")


def _checked_path(config: Dict[str, Any], allowlist: Any) -> Path:
    path = config.get("path")
    if not path:
        raise PermanentError("file connector requires 'path'")

    resolved = Path(path).expanduser()
    if allowlist is not None:
        # Resolve before checking so that ../ cannot escape an allowed prefix.
        allowlist.check_path(str(resolved.resolve()))
    return resolved


def _row_count(payload: Any) -> int:
    return len(payload) if isinstance(payload, list) else 1
