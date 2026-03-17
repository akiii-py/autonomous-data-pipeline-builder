import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict

from worker.errors import WorkerExecutionError
from worker.runner import execute_step


class WorkerHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"status": "healthy", "service": "executor-worker"})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/execute":
            self._send(404, {"error": "not found"})
            return

        try:
            payload = self._json_body()
            run_id = payload.get("run_id")
            step = payload.get("step")
            if not run_id or not isinstance(step, dict):
                raise WorkerExecutionError("run_id and step are required")

            result = execute_step(run_id, step)
            self._send(200, {"status": "ok", "result": result})
        except WorkerExecutionError as exc:
            self._send(400, {"status": "error", "error": str(exc)})
        except Exception as exc:
            self._send(500, {"status": "error", "error": str(exc)})

    def log_message(self, format: str, *args: Any) -> None:
        # Keep worker logs clean for now.
        return

    def _json_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8")
        return json.loads(raw) if raw else {}

    def _send(self, code: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run(host: str = "0.0.0.0", port: int = 8090) -> None:
    server = HTTPServer((host, port), WorkerHandler)
    print(f"worker listening on {host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run()
