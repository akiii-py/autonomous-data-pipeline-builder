# System Design Changes

Structural changes only — things that alter a contract, a boundary, an ownership
model, or where a concept lives. Bug fixes, missing validation, config defaults,
CI, Docker, and test coverage are excluded even where the audit rated them HIGH.

18 items, grouped by what they change.

---

## A. Contracts between orchestrator and worker

### D-01 — Introduce a `StepResult` value type
**Now:** `Dispatcher.ExecuteStep` returns a bare `error`. `executeResponse`
declares only `Status` and `Error`. The worker already sends row counts and load
results; the orchestrator discards them at the boundary.

**Change:** define a result type carrying at minimum — rows processed, a
retryability classification, an idempotency token, and whether execution was
simulated. Return it from `ExecuteStep` and serialize it from the worker.

**Why it's design:** three separate findings (error classification, retry
dedup, simulated-run marking) are all downstream of one missing value type.
Fixing them individually produces three ad-hoc side channels.

**Unblocks:** D-02, D-03, D-09.

---

### D-02 — Make the error taxonomy cross the wire
**Now:** `WorkerExecutionError` carries a `transient` flag. No call site sets
it, and the server serializes only `str(exc)`. The concept exists on one side of
the boundary and does not exist on the other.

**Change:** classify errors at the connector level (transient vs permanent),
carry the classification in `StepResult`, and let the scheduler's retry decision
read it rather than retrying everything identically.

**Why it's design:** retry policy currently has no input other than "an error
occurred." The classification has to be produced where the knowledge is (the
connector) and consumed where the decision is (the scheduler).

---

### D-03 — Define an idempotency contract for step execution
**Now:** nothing. A dispatcher timeout is indistinguishable from a failure, the
worker deduplicates nothing, and load connectors INSERT.

**Change:** make `(run_id, step_key, attempt)` — or a token minted by the
orchestrator — the unit of execution identity. The worker records completion
durably and returns the prior result on replay. Declare in the connector
interface whether a connector is idempotent, and refuse to retry the ones that
aren't.

**Why it's design:** this is a contract, not a patch. It determines what "at
least once" means in this system and who is responsible for honoring it.

---

### D-04 — Replace raw dicts with the typed worker contract that already exists
**Now:** `executor/worker/models.py` defines `StepPayload`, `ExecuteRequest`,
`ExecuteResponse`. Nothing imports them. `server.py` and `runner.py` pass raw
dicts and use `.get()`, which is why `runner.execute_step` re-validates
`step.key` by hand.

**Change:** parse into the dataclasses at the HTTP boundary, and make them the
single definition of the worker's request/response shape.

**Why it's design:** the contract is currently implicit and enforced by scattered
`.get()` calls. Two representations of the same thing, one of them unused.

---

### D-05 — Make connectors a registry, not a dict literal
**Now:** `connectors/base.py` declares a `Connector` class nothing subclasses.
All three connectors are module-level function pairs assembled into a dict
inside `runner._connector`.

**Change:** one registry that owns connector lookup, declares each connector's
capabilities (idempotent? accepts credentials? what schemes/hosts are allowed?),
and is the single place an allowlist can be enforced.

**Why it's design:** the abstraction and the allowlist are reaching for the same
concept. Building them separately gives you three places to update when a
connector is added.

---

## B. State and storage model

### D-06 — Move artifacts to durable shared storage
**Now:** a module-level dict behind a lock, with `put` and `get` and no delete.

**Change:** artifacts keyed by `(run_id, step_key)` in storage that outlives the
process and is visible to more than one worker, with deletion at run completion.

**Why it's design:** this single decision determines whether the worker can be
restarted, whether it can be scaled horizontally, and whether runs can ever
resume. It is the load-bearing constraint behind D-07 and D-08.

---

### D-07 — Change who owns a running run
**Now:** a detached `go func()` inside the orchestrator process. No queue, no
claim, no lease. The process *is* the run's only owner.

**Change:** the run row becomes the owner of record, with a claim column, an
owner identity, and an expiry. A process executes runs it has claimed and
heartbeats to hold them.

**Why it's design:** it moves run ownership from process memory into the
database. Everything else — restart recovery, cancellation, multiple
orchestrators — follows from that relocation rather than being added separately.

**Depends on:** D-06.

---

### D-08 — Use Postgres as the work queue rather than adding a broker
**Now:** dispatch is a direct in-process function call.

**Change:**
```sql
SELECT ... FROM step_runs WHERE status = 'queued'
ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1;
```
Multiple workers claim different rows atomically without blocking. Combined with
D-07's claim columns, this is the whole queue.

**Why it's design, and why not Celery:** Celery is Python and the scheduler is
Go, so Go-side enqueueing means either a fragile compat library or a Python HTTP
shim — which is a broker service, with the failure mode of a process nobody
started and jobs that return 200 then sit forever. Postgres is already in the
architecture.

---

### D-09 — Record execution provenance on the run
**Now:** nothing distinguishes a run that touched data from one executed by the
no-op dispatcher.

**Change:** the run row records *how* it executed — which dispatcher, which
worker, simulated or real — and every read path that reports outcomes carries it
through.

**Why it's design:** a run is a record of something that happened. Omitting how
it happened makes the record incomplete, and makes every aggregate computed from
it ambiguous.

---

### D-10 — Separate execution-history lifecycle from definition lifecycle
**Now:** `pipeline_run_events` cascades from `pipelines`. Deleting a pipeline
erases its entire audit trail.

**Change:** history outlives the definition. Soft-delete pipelines, or detach
events from the cascade.

**Why it's design:** an audit log destroyable by the same key that can run things
is not an audit log. The two lifecycles are genuinely independent and the schema
currently ties them together.

---

### D-11 — Make schema a versioned artifact
**Now:** three Go string constants re-executed on every boot. Idempotence rests
entirely on `IF NOT EXISTS` holding forever.

**Change:** a `schema_migrations` table with applied-at records, or a migration
tool. Once present, non-idempotent migrations (backfills, type changes) become
possible at all.

**Why it's design:** it determines whether the schema has a knowable version.
Today no environment can answer "what schema am I at?"

---

## C. Lifecycle and trust boundary

### D-12 — Add an authorization stage between create and run
**Now:** the only gate is a confidence threshold, and it sits on `/interpret` —
which executes nothing. `POST /run` executes whatever was stored.

**Change:** a distinct stage in the pipeline lifecycle where a pipeline
containing irreversible operations (any `load`, any config carrying credentials)
becomes runnable. Confirmation is a state transition on the pipeline, not a
parameter on the run call.

**Why it's design:** the current lifecycle is draft → stored → executed with no
step where a human affirms the irreversible part. That's a missing stage, not a
missing check.

---

### D-13 — Split validation into structural and semantic stages
**Now:** one `validateDraft` doing name, step keys, step type, and DAG
buildability. Semantic checks are absent.

**Change:** two stages with different inputs. Structural validation needs only
the draft. Semantic validation needs the draft *plus* the context it was
generated against — the schema, the connector allowlist, the destination
allowlist. Give them separate call sites so the second can be extended without
touching the first.

**Why it's design:** the project's core bet is that the DAG engine disposes of
what the LLM proposes. Right now it disposes on shape only. Making meaning
checkable requires a stage that has the context to check it — which is D-14.

---

### D-14 — Change the interpret request contract to carry schema context
**Now:** `interpreter.Request` has `query` plus two free-text hints. No schema
is sent to the model.

**Change:** the request carries the target schema, the connector allowlist, and
the available transform ops. The response is validated against the same context
that was sent.

**Why it's design:** this is a contract change and it is a hard prerequisite.
Without it, "referenced tables must exist" is not unimplemented — it is
impossible, because the system never told the model what tables exist and has
nothing to check against.

**Blocks:** D-13's semantic half.

---

## D. Concurrency model

### D-15 — Decide where parallelism lives, at both ends at once
**Now:** the scheduler computes the full ready set and executes it sequentially.
Separately, the worker uses `HTTPServer`, so `/execute` handles one step at a
time process-wide.

**Change:** bounded concurrency over the ready set in the scheduler, **and** a
concurrent worker. Plus an explicit, recorded decision on sibling cancellation
when one step in a ready set fails.

**Why it's design, and why it's one item:** parallelizing only the scheduler
converts a design win into queueing latency — concurrent dispatches that the
worker serializes. The two ends are one decision. Fan-out *within* a step
belongs to the worker; parallelism *across* steps belongs to the scheduler,
which is the only component that knows the DAG.

**Depends on:** D-06.

---

## E. Error and signal propagation

### D-16 — Propagate typed errors instead of matching on error text
**Now:** `dag.ValidationError` exists with a `Code` field. The one consumer that
needs it — the handler choosing between 400 and 500 — does
`strings.Contains(err.Error(), "invalid pipeline dag")`.

**Change:** `errors.As` against the existing type. Also: validation collects all
errors and both builders return only the first, so a user fixing a rejected
draft resubmits once per problem — return the set.

**Why it's design:** the handler is currently coupled to the store's
error-wrapping string. The typed error was built for exactly this and isn't
wired up.

---

### D-17 — Decide whether the event log is authoritative or advisory
**Now:** `emitEvent` discards every error it receives. The log is load-bearing
for `/metrics` and `/failure-breakdown` but written best-effort, and one
terminal path returns without emitting at all.

**Change:** pick one and build for it. If authoritative, event writes join the
transaction that writes the status, and a failed emit is a failed transition. If
advisory, say so and log failures — but then metrics derived from it are
approximate by design.

**Why it's design:** the current state is an authoritative log implemented as an
advisory one, which is the worst of both.

---

### D-18 — Make degradation observable, not just reported
**Now:** all five interpret fallback reasons return 200 with `fallback_reason`
in the response body. Nothing is persisted, counted, or emitted.

**Change:** degradation is a system event, not a field in a reply. Emit it with
its reason and surface the breakdown alongside other metrics.

**Why it's design:** the endpoint degrades by design, which is correct. But a
design that degrades silently and invisibly can't be operated — "the NLP service
is dead" and "the model scored 0.68" are currently the same observation from
outside.

---

## Sequencing

**First, because other items depend on them:**
D-01 → D-06 → D-14

**Then the ones those unlock:**
D-02, D-03, D-09 (all downstream of D-01)
D-07 → D-08 (downstream of D-06)
D-13 (downstream of D-14)
D-15 (downstream of D-06, and must land at both ends together)

**Independent, do whenever:**
D-04, D-05, D-10, D-11, D-12, D-16, D-17, D-18

**If you do only three:** D-01, D-06, D-14. They are the three that other
changes are currently blocked behind, and each one removes an entire category of
awkwardness rather than a single symptom.
