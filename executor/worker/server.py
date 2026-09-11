"""Worker HTTP server.

Threaded, because parallelism across steps belongs to the scheduler but the
worker has to be able to accept the concurrent dispatches that produces —
parallelising only one end converts a design win into queueing latency
(D-15, rule 4.1).

Parsing happens once here, into the dataclasses that define the wire contract
(D-04). Nothing downstream reads raw dicts.
"""

import hmac
import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional

try:
    from executor.connectors.registry import build_registry
    from executor.worker import config as worker_config
    from executor.worker.artifacts import (
        InMemoryArtifactStore,
        InMemoryExecutionLedger,
        PostgresArtifactStore,
        PostgresExecutionLedger,
    )
    from executor.worker.errors import WorkerExecutionError
    from executor.worker.models import ExecuteRequest, ExecuteResponse
    from executor.worker.runner import StepRunner
except ImportError:  # pragma: no cover - import shim for PYTHONPATH=executor
    from connectors.registry import build_registry
    from worker import config as worker_config
    from worker.artifacts import (
        InMemoryArtifactStore,
        InMemoryExecutionLedger,
        PostgresArtifactStore,
        PostgresExecutionLedger,
    )
    from worker.errors import WorkerExecutionError
    from worker.models import ExecuteRequest, ExecuteResponse
    from worker.runner import StepRunner

logger = logging.getLogger("executor.worker")

MAX_BODY_BYTES = 8 * 1024 * 1024


class WorkerHandler(BaseHTTPRequestHandler):
    # Set by build_server. Shared across threads; StepRunner holds no mutable
    # per-request state.
    runner: Optional[StepRunner] = None
    auth_token: str = ""

    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"status": "healthy", "service": "executor-worker"})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/execute":
            self._send(404, {"error": "not found"})
            return

        if not self._authorized():
            self._send(401, {"status": "error", "error": "unauthorized"})
            return

        payload: Any = None
        try:
            payload = self._json_body()
            req = ExecuteRequest.parse(payload)
        except WorkerExecutionError as exc:
            self._send(400, ExecuteResponse.error_for(payload, str(exc)).to_dict())
            return
        except (ValueError, UnicodeDecodeError) as exc:
            self._send(400, ExecuteResponse.error_for(payload, f"invalid body: {exc}").to_dict())
            return

        if self.runner is None:
            self._send(503, ExecuteResponse.error_for(payload, "worker not initialised").to_dict())
            return

        # execute() classifies and returns rather than raising, so a failed step
        # is a 200 carrying a failed StepResult. The orchestrator reads the
        # classification off the result (D-02).
        result = self.runner.execute(req)
        response = ExecuteResponse(
            status="error" if result.failed else "ok",
            result=result,
            error=result.error_message or None,
            run_id=result.run_id,
            step_key=result.step_key,
        )
        self._send(200, response.to_dict())

    def log_message(self, format: str, *args: Any) -> None:
        logger.debug("%s - %s", self.address_string(), format % args)

    def _authorized(self) -> bool:
        if not self.auth_token:
            return True
        provided = self.headers.get("X-Worker-Token", "")
        return hmac.compare_digest(provided, self.auth_token)

    def _json_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length > MAX_BODY_BYTES:
            raise ValueError("request body too large")
        raw = self.rfile.read(length).decode("utf-8") if length else ""
        return json.loads(raw) if raw else {}

    def _send(self, code: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def build_runner(cfg: worker_config.WorkerConfig) -> StepRunner:
    """Assemble the runner's collaborators.

    Durable storage is the production path. The in-memory fallback exists only so
    a developer can start the worker without a database; it is announced loudly
    because it reintroduces exactly the constraint D-06 removed.
    """
    registry = build_registry(
        allowed_hosts=cfg.allowed_hosts,
        allowed_schemes=cfg.allowed_schemes,
        allowed_paths=cfg.allowed_paths,
        allowed_dsn_refs=cfg.allowed_dsn_refs,
        unrestricted=cfg.unrestricted_connectors,
    )

    if cfg.durable_storage_configured:
        return StepRunner(
            registry=registry,
            artifacts=PostgresArtifactStore(cfg.database_url),
            ledger=PostgresExecutionLedger(cfg.database_url),
        )

    logger.warning(
        "WORKER_DATABASE_URL is not set: using in-process artifact storage. "
        "The worker cannot be restarted or scaled in this mode, and retries are "
        "not deduplicated."
    )
    return StepRunner(
        registry=registry,
        artifacts=InMemoryArtifactStore(),
        ledger=InMemoryExecutionLedger(),
    )


def build_server(cfg: Optional[worker_config.WorkerConfig] = None) -> ThreadingHTTPServer:
    cfg = cfg or worker_config.load()
    worker_config.validate(cfg)

    handler = type(
        "ConfiguredWorkerHandler",
        (WorkerHandler,),
        {"runner": build_runner(cfg), "auth_token": cfg.auth_token},
    )

    server = ThreadingHTTPServer((cfg.host, cfg.port), handler)
    server.daemon_threads = True
    return server


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = worker_config.load()
    try:
        worker_config.validate(cfg)
    except worker_config.InsecureConfigError as exc:
        logger.error("refusing to start: %s", exc)
        raise SystemExit(2) from exc

    if not cfg.auth_token:
        logger.warning("WORKER_INSECURE_DEV is set: /execute is unauthenticated")
    if cfg.unrestricted_connectors:
        logger.warning("WORKER_UNRESTRICTED_CONNECTORS is set: connector allowlists are disabled")

    server = build_server(cfg)
    logger.info("worker listening on %s:%d", cfg.host, cfg.port)
    server.serve_forever()


if __name__ == "__main__":
    run()
