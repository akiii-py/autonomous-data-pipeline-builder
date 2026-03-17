import json
import urllib.request
from typing import Any, Dict


def extract(config: Dict[str, Any]) -> Any:
    url = config.get("url")
    if not url:
        raise ValueError("http connector requires 'url'")

    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        body = resp.read().decode("utf-8")
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return {"raw": body}


def load(payload: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    url = config.get("url")
    if not url:
        raise ValueError("http connector requires 'url'")

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        code = resp.status
    return {"posted_to": url, "status_code": code}
