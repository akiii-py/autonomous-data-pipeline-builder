"""Worker configuration.

This is the only place in the executor that reads the environment, mirroring the
orchestrator's single-composition-root rule.
"""

import os
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class WorkerConfig:
    host: str
    port: int
    auth_token: str
    database_url: str
    allowed_hosts: List[str]
    allowed_schemes: List[str]
    allowed_paths: List[str]
    allowed_dsn_refs: List[str]
    unrestricted_connectors: bool
    # Explicit opt-in to run without WORKER_TOKEN. The worker refuses to start
    # unauthenticated unless this is set, so an unconfigured deployment fails
    # loudly rather than exposing /execute (item 20).
    insecure_dev: bool
    max_threads: int

    @property
    def durable_storage_configured(self) -> bool:
        return bool(self.database_url)


def load() -> WorkerConfig:
    return WorkerConfig(
        host=os.getenv("WORKER_HOST", "127.0.0.1"),
        port=_int("WORKER_PORT", 8090),
        auth_token=os.getenv("WORKER_TOKEN", ""),
        database_url=os.getenv("WORKER_DATABASE_URL") or os.getenv("DATABASE_URL", ""),
        allowed_hosts=_list("WORKER_ALLOWED_HOSTS"),
        allowed_schemes=_list("WORKER_ALLOWED_SCHEMES") or ["https"],
        allowed_paths=_list("WORKER_ALLOWED_PATHS"),
        allowed_dsn_refs=_list("WORKER_ALLOWED_DSN_REFS"),
        unrestricted_connectors=_bool("WORKER_UNRESTRICTED_CONNECTORS", False),
        insecure_dev=_bool("WORKER_INSECURE_DEV", False),
        max_threads=_int("WORKER_MAX_THREADS", 8),
    )


class InsecureConfigError(RuntimeError):
    """Raised when the worker would start in a configuration that exposes
    /execute without an explicit opt-in."""


def validate(cfg: WorkerConfig) -> None:
    if not cfg.auth_token and not cfg.insecure_dev:
        raise InsecureConfigError(
            "WORKER_TOKEN is not set. /execute would accept unauthenticated requests "
            "that can reach arbitrary hosts and databases. Set WORKER_TOKEN, or set "
            "WORKER_INSECURE_DEV=1 to run unauthenticated on a local machine only."
        )
    if cfg.unrestricted_connectors and not cfg.insecure_dev:
        raise InsecureConfigError(
            "WORKER_UNRESTRICTED_CONNECTORS disables every allowlist and requires "
            "WORKER_INSECURE_DEV=1 as an explicit acknowledgement."
        )


def _int(key: str, fallback: int) -> int:
    raw = os.getenv(key)
    if not raw:
        return fallback
    try:
        return int(raw)
    except ValueError:
        return fallback


def _bool(key: str, fallback: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return fallback
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _list(key: str) -> List[str]:
    raw: Optional[str] = os.getenv(key)
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]
