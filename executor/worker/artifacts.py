import threading
from typing import Any, Dict, Optional


_lock = threading.Lock()
_data: Dict[str, Dict[str, Any]] = {}


def put_artifact(run_id: str, step_key: str, value: Any) -> None:
    with _lock:
        _data.setdefault(run_id, {})[step_key] = value


def get_artifact(run_id: str, step_key: str) -> Optional[Any]:
    with _lock:
        return _data.get(run_id, {}).get(step_key)
