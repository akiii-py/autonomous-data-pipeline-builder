## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).

---

# System design rules

These are binding on all new and modified code. They were derived from
`SYSTEM_DESIGN_CHANGES.md` and the audit in `AUDIT_FINDINGS.md`. Each rule names
the design change it enforces. If a change genuinely requires breaking one, say
so explicitly and record the reason — do not work around it silently.

## 1. Contracts between orchestrator and worker

**1.1 — Step execution returns a value, never a bare error.** `Dispatcher.ExecuteStep`
returns `(*models.StepResult, error)`. `StepResult` is the single carrier for
rows processed, error classification, idempotency key, and simulated/real
provenance. Never add a side channel (an extra return, a field on the scheduler,
a package-level map) for information that belongs on `StepResult`. *(D-01)*

**1.2 — Error classification is produced where the knowledge is and consumed where
the decision is.** Connectors classify (`transient` vs `permanent`); the worker
serializes the class; the scheduler's retry decision reads it. Never infer
retryability from error text at any layer. *(D-02, D-16)*

**1.3 — Execution identity is `(run_id, step_key, attempt)`.** The orchestrator mints
the idempotency key and sends it. The worker records completion durably against
it and replays the stored result rather than re-executing. A connector that
cannot be made idempotent declares `idempotent = False` in the registry and is
never retried. *(D-03)*

**1.4 — The worker's wire contract lives in `executor/worker/models.py`.** Parse into
those dataclasses at the HTTP boundary. No `.get()`-based field access on request
payloads outside that parse, and no re-validating a field the dataclass already
guarantees. *(D-04)*

**1.5 — Connectors are obtained from the registry, never constructed inline.**
`executor/connectors/registry.py` owns lookup and declares each connector's
capabilities: idempotency, whether it accepts credentials, and its allowlist.
Adding a connector means one registry entry, not edits in three files. *(D-05)*

## 2. State and storage

**2.1 — No execution state in process memory.** Artifacts, run ownership, and queue
position all live in Postgres. A module-level dict holding anything that outlives
a single function call is a design violation. *(D-06)*

**2.2 — The run row owns the run, not the goroutine.** A process executes runs it has
claimed (`claimed_by`, `claim_expires_at`) and heartbeats to hold them. An expired
claim is reclaimable by any process. Never start a detached `go func()` that is
the sole owner of durable work. *(D-07)*

**2.3 — Postgres is the work queue.** Claim with `FOR UPDATE SKIP LOCKED`. Do not add
a broker, a Redis queue, or a Python-side scheduler shim. *(D-08)*

**2.4 — A run records how it executed.** `exec_mode`, `simulated`, and `worker_url`
are written at claim time and carried through every read path that reports an
outcome — status, run history, metrics, failure breakdown. A new endpoint that
reports run outcomes without provenance is incomplete. *(D-09)*

**2.5 — History outlives definitions.** `pipelines` is soft-deleted; run history and
events carry no FK cascade from it. Never re-attach an audit table to a
definition table's lifecycle. *(D-10)*

**2.6 — Schema changes are versioned migrations.** Append a numbered entry to
`internal/database/migrations.go`; never edit an applied one, and never rely on
`IF NOT EXISTS` as the idempotence mechanism. `schema_migrations` is the record of
what ran. *(D-11)*

## 3. Lifecycle and trust boundary

**3.1 — Irreversible pipelines require an approval transition before they can run.**
A pipeline containing a `load` step or credential-bearing config is created
`requires_approval`. `POST /run` refuses it until `POST /approve` has moved it.
Approval is a state transition on the pipeline, never a query parameter or a
header on the run call. *(D-12)*

**3.2 — Validation is two stages with different inputs.** Structural validation takes
only the draft (shape, keys, types, acyclicity). Semantic validation takes the
draft plus the `catalog.Context` it was generated against (schema, connector
allowlist, destination allowlist, transform ops). Extend the semantic stage;
leave the structural one alone. *(D-13)*

**3.3 — Whatever is sent to the model is what the response is validated against.**
The interpret request carries the catalog context, and the same context object
validates the reply. Never validate against a hardcoded list that could drift
from what was sent. *(D-14)*

## 4. Concurrency

**4.1 — Parallelism across steps belongs to the scheduler; fan-out within a step
belongs to the worker.** The scheduler is the only component that knows the DAG.
Never parallelize one end without the other — it converts a design win into
queueing latency. *(D-15)*

**4.2 — Sibling cancellation is decided and recorded: when one step in a ready set
fails, in-flight siblings are cancelled** via context, marked `cancelled`, and each
emits a terminal event. This is deliberate, not incidental — a partially-applied
ready set must be visible in the event log. *(D-15)*

**4.3 — Concurrency is always bounded.** Read the limit from config
(`SCHEDULER_MAX_CONCURRENCY`). No unbounded goroutine fan-out.

## 5. Errors and signals

**5.1 — Propagate typed errors; never match on error text.** Use `errors.As` against
`dag.ValidationError` and friends. `strings.Contains(err.Error(), ...)` as
control flow is a defect. *(D-16)*

**5.2 — Validation returns every error, not the first.** The caller is a user fixing a
rejected draft. Builders return `ValidationErrors`, not `errs[0]`. *(D-16)*

**5.3 — The event log is AUTHORITATIVE.** This is the decision; it is not open.
Event writes join the same transaction as the status write they describe, via the
`*Tx` store methods. A failed emit fails the transition. Never call a status
update and an event emit as two independent statements, and never discard an emit
error. *(D-17)*

**5.4 — Every terminal path emits a terminal event.** Enumerate them when you touch
the scheduler: completion, step failure, retry exhaustion, panic, cancellation,
lease expiry. A path that returns without an event is a defect even when the
status write succeeded. *(D-17)*

**5.5 — Degradation is an event, not a response field.** Every interpret fallback
writes a `degradation_events` row with its reason and is surfaced in `/metrics`.
A new degradation path adds a reason constant and an emit. *(D-18)*

## 6. Carried forward from the original architecture

**6.1 — `router.New` is the only composition root.** No handler, scheduler, or store
reads config or constructs its own dependencies. `os.Getenv` appears only in
`internal/config`.

**6.2 — Layering is handler → service → store, one direction.** No `database/sql` or
`pq` import under `api/handlers/`. The scheduler never writes an HTTP response.

**6.3 — `Dispatcher` is a pure interface boundary.** The scheduler never type-asserts
an implementation and never reads `EXEC_MODE`.

**6.4 — Credentials are never persisted in plaintext.** Step config carries a secret
*reference* (`dsn_ref`), resolved worker-side from the environment. Config is
redacted on every read path.

**6.5 — A real `context.Context` threads from run claim to the worker HTTP call.**
`context.Background()` in the execution path is a defect.

## Checklist before finishing any change here

- [ ] `cd orchestrator && go build ./... && go test ./...` passes
- [ ] `PYTHONPATH="$PWD" .venv/bin/python -m unittest discover -s executor/tests -p "test_*.py"` passes
- [ ] New status writes go through a `*Tx` method with a paired event (5.3)
- [ ] New config read added to `internal/config` only (6.1)
- [ ] Schema change appended as a new numbered migration (2.6)
- [ ] `graphify update .` run after the change
