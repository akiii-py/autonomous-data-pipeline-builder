"""Error taxonomy for the worker.

The classification is produced here, where the knowledge is, and consumed by the
scheduler's retry decision (D-02). It crosses the wire as StepResult.error_class,
so the orchestrator never has to guess retryability from a message string.
"""

TRANSIENT = "transient"
PERMANENT = "permanent"


class WorkerExecutionError(Exception):
    """Base error. Defaults to permanent: an unclassified failure is one we have
    no reason to believe will succeed on a second identical attempt, and
    retrying it is exactly the tight loop D-02 exists to remove."""

    error_class = PERMANENT

    def __init__(self, message: str, error_class: str | None = None):
        super().__init__(message)
        if error_class is not None:
            self.error_class = error_class

    @property
    def transient(self) -> bool:
        return self.error_class == TRANSIENT


class TransientError(WorkerExecutionError):
    """A failure worth retrying: a timeout, a dropped connection, a locked
    table. The same request may well succeed shortly."""

    error_class = TRANSIENT


class PermanentError(WorkerExecutionError):
    """A failure that will recur identically: malformed SQL, a missing file, a
    value outside an allowlist."""

    error_class = PERMANENT


class NotAllowedError(PermanentError):
    """The request named a connector, host, path or credential reference that is
    not in the worker's allowlist."""


def classify(exc: BaseException) -> str:
    """Best-effort classification of an error raised outside our own taxonomy."""
    if isinstance(exc, WorkerExecutionError):
        return exc.error_class

    transient_types = (TimeoutError, ConnectionError, BrokenPipeError, InterruptedError)
    if isinstance(exc, transient_types):
        return TRANSIENT

    # OSError covers socket and filesystem faults; errno tells the two apart well
    # enough to be useful, and anything unrecognised stays permanent.
    if isinstance(exc, OSError) and exc.errno in (11, 16, 35, 110, 111, 112):
        return TRANSIENT

    return PERMANENT
