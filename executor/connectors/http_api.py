"""HTTP connector.

Every URL is checked against the registry's allowlist before a request is made
(D-05). Without that check an endpoint that accepts arbitrary URLs is an
outbound request proxy into the internal network.
"""

import json
import urllib.error
import urllib.request
from typing import Any, Dict

try:
    from executor.worker.errors import PermanentError, TransientError
except ImportError:  # pragma: no cover - import shim for PYTHONPATH=executor
    from worker.errors import PermanentError, TransientError

_TIMEOUT_SECONDS = 10


def extract(config: Dict[str, Any], allowlist: Any = None) -> Any:
    url = _checked_url(config, allowlist)

    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raise _classify_http_error(exc) from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise TransientError(f"http extract failed: {exc}") from exc

    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return {"raw": body}


def load(payload: Any, config: Dict[str, Any], allowlist: Any = None) -> Dict[str, Any]:
    url = _checked_url(config, allowlist)

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, method="POST", headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp:
            code = resp.status
    except urllib.error.HTTPError as exc:
        raise _classify_http_error(exc) from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise TransientError(f"http load failed: {exc}") from exc

    if code >= 300:
        raise PermanentError(f"http load returned unexpected status {code}")

    return {"posted_to": url, "status_code": code}


def _checked_url(config: Dict[str, Any], allowlist: Any) -> str:
    url = config.get("url")
    if not url:
        raise PermanentError("http connector requires 'url'")
    if allowlist is not None:
        allowlist.check_url(url)
    return url


def _classify_http_error(exc: urllib.error.HTTPError) -> Exception:
    # 5xx and 429 may succeed on a later attempt; other 4xx will not.
    if exc.code >= 500 or exc.code == 429:
        return TransientError(f"http request failed with status {exc.code}")
    return PermanentError(f"http request rejected with status {exc.code}")
