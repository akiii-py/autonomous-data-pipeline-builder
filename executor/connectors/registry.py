"""Connector registry.

One place owns connector lookup and declares each connector's capabilities:
whether its load is idempotent, whether it accepts credentials, and what it is
allowed to reach (D-05). The abstraction and the allowlist were reaching for the
same concept; building them separately gives three files to edit when a connector
is added.

Adding a connector means one entry here (rule 1.5).
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

try:
    from executor.connectors import file_io, http_api, postgres
    from executor.worker.errors import NotAllowedError, PermanentError
except ImportError:  # pragma: no cover - import shim for PYTHONPATH=executor
    from connectors import file_io, http_api, postgres
    from worker.errors import NotAllowedError, PermanentError


@dataclass
class Allowlist:
    """What a connector instance may reach. An empty list denies everything —
    fail closed, so an unconfigured worker cannot be pointed anywhere."""

    hosts: List[str] = field(default_factory=list)
    schemes: List[str] = field(default_factory=lambda: ["https"])
    path_prefixes: List[str] = field(default_factory=list)
    dsn_refs: List[str] = field(default_factory=list)
    # Set explicitly to disable allowlisting for local development. Never in a
    # deployed configuration.
    unrestricted: bool = False

    def check_url(self, url: str) -> None:
        if self.unrestricted:
            return

        parsed = urlparse(url)
        if parsed.scheme.lower() not in [s.lower() for s in self.schemes]:
            raise NotAllowedError(
                f"scheme {parsed.scheme!r} is not allowed (permitted: {self.schemes})"
            )
        host = (parsed.hostname or "").lower()
        if host not in [h.lower() for h in self.hosts]:
            raise NotAllowedError(f"host {host!r} is not in the worker's allowlist")

    def check_path(self, path: str) -> None:
        if self.unrestricted:
            return
        if not any(path.startswith(prefix) for prefix in self.path_prefixes):
            raise NotAllowedError(f"path {path!r} is not under an allowed prefix")

    def check_dsn_ref(self, ref: str) -> None:
        if self.unrestricted:
            return
        if ref not in self.dsn_refs:
            raise NotAllowedError(f"dsn_ref {ref!r} is not in the worker's allowlist")


@dataclass
class Connector:
    """One connector and everything the worker needs to know about it."""

    name: str
    extract: Optional[Callable[..., Any]] = None
    load: Optional[Callable[..., Any]] = None
    # False when a repeated load cannot be made safe. Such a step is reported as
    # permanently failed rather than retried (rule 1.3).
    idempotent_load: bool = False
    accepts_credentials: bool = False
    allowlist: Allowlist = field(default_factory=Allowlist)

    def extract_fn(self) -> Callable[..., Any]:
        if self.extract is None:
            raise PermanentError(f"connector {self.name!r} does not support extract")
        return self.extract

    def load_fn(self) -> Callable[..., Any]:
        if self.load is None:
            raise PermanentError(f"connector {self.name!r} does not support load")
        return self.load


class Registry:
    def __init__(self, connectors: Dict[str, Connector]):
        self._connectors = connectors

    def get(self, name: str) -> Connector:
        connector = self._connectors.get(name)
        if connector is None:
            raise NotAllowedError(
                f"unsupported connector: {name!r} (available: {sorted(self._connectors)})"
            )
        return connector

    def names(self) -> List[str]:
        return sorted(self._connectors)


def build_registry(
    allowed_hosts: Optional[List[str]] = None,
    allowed_schemes: Optional[List[str]] = None,
    allowed_paths: Optional[List[str]] = None,
    allowed_dsn_refs: Optional[List[str]] = None,
    unrestricted: bool = False,
) -> Registry:
    """Assemble the registry from the worker's configuration."""

    http_allow = Allowlist(
        hosts=allowed_hosts or [],
        schemes=allowed_schemes or ["https"],
        unrestricted=unrestricted,
    )
    file_allow = Allowlist(path_prefixes=allowed_paths or [], unrestricted=unrestricted)
    pg_allow = Allowlist(dsn_refs=allowed_dsn_refs or [], unrestricted=unrestricted)

    return Registry(
        {
            # Writing a file is a whole-file overwrite, so replaying it lands the
            # same bytes.
            "file": Connector(
                name="file",
                extract=file_io.extract,
                load=file_io.load,
                idempotent_load=True,
                allowlist=file_allow,
            ),
            # A POST cannot be assumed safe to repeat: the worker cannot know
            # whether a request that timed out was in fact applied.
            "http": Connector(
                name="http",
                extract=http_api.extract,
                load=http_api.load,
                idempotent_load=False,
                allowlist=http_allow,
            ),
            # INSERT is not idempotent. The execution ledger covers the common
            # timeout case; this flag covers the gap where the worker died after
            # the write but before recording it.
            "postgres": Connector(
                name="postgres",
                extract=postgres.extract,
                load=postgres.load,
                idempotent_load=False,
                accepts_credentials=True,
                allowlist=pg_allow,
            ),
        }
    )
