# ARCHITECTURE_AUDIT.md

**Purpose:** this file is a prompt. Give it to Claude Code running inside the
repo. It defines the design rules this system is supposed to follow, tells
Claude how to verify the code against them, and specifies the report format.

---

## 0. Your task

You are auditing the **Autonomous Data Pipeline Builder** codebase (Go
orchestrator + Python executor worker + a planned NLP service) against the
design rules in Section 3.

Produce a findings report in the format given in Section 7. **Do not change any
code.** This is a read-and-report pass. If the user wants fixes, they will ask
in a follow-up.

---

## 1. Ground rules for this audit

These override any assumption you might otherwise make.

**1.1 — Verify against source, never against documentation.**
Every claim in this file, in `PROJECT_UNDERSTANDING.md`, or in any memory or
notes you have, is a *hypothesis to check*, not a fact. Documentation on this
project has already been wrong once. If the code disagrees with a document, the
code wins and you report the discrepancy as a finding.

**1.2 — Cite evidence.** Every finding must carry `file:line` or
`file::function`. A finding with no citation is not a finding.

**1.3 — Say "not found" when you can't verify.** Do not infer that something
exists because it would be reasonable for it to exist. If you looked for
per-step timeouts and found none, report "no timeout handling found in
`scheduler/*.go`" — not "timeouts appear to be missing."

**1.4 — Prefer the knowledge graph over grep.** The repo has one in
`graphify-out/`. Use it first:
```
graphify query "how does the scheduler retry a failed step"
graphify path "PipelineHandler.RunPipeline" "execute_step"
graphify explain "Dispatcher"
```
Fall back to grep/read for line-level confirmation. Hub nodes worth knowing:
`PipelineStore`, `PipelineHandler`, `execute_step`, `router.New`, `dag.Graph`.
`router.New` is the composition root — every wiring decision is made there.

**1.5 — Judge the rule, not the style.** Do not report formatting, naming
preferences, or idiom choices unless they cause a rule violation. This audit is
about correctness, safety, and design integrity.

**1.6 — Distinguish "missing" from "wrong".** A gap that is known and documented
is lower severity than code that claims to do something it doesn't. Silent
wrongness is the worst category.

---

## 2. What this system is (orientation only — verify everything)

A user describes a data job in plain English. The system turns it into a
validated DAG pipeline, persists it, runs it with dependency ordering and
retries, and reports what happened.

```
client
  │ POST /api/v1/interpret          NL → draft
  │ POST /api/v1/pipelines          draft → stored pipeline
  │ POST /api/v1/pipelines/{id}/run
  ▼
Go orchestrator (:8080)
  middleware: CORS → Recovery → Logger → APIKey
  router.New  ← composition root
  handlers → scheduler.ExecuteRun → store.PipelineStore → Postgres
                                  → dispatcher.Dispatcher
                                      ├─ LocalDispatcher (EXEC_MODE=local)
                                      └─ HTTPDispatcher  (EXEC_MODE=worker)
                                             ▼
                              Python worker (:8090)
                                POST /execute → runner.execute_step
                                connectors: file / http / postgres
                                transformations/ops.py
                                artifacts.py
```

Tables: `pipelines`, `pipeline_steps`, `pipeline_runs`, `step_runs`,
`pipeline_run_events`.

**Core design bet:** *the LLM proposes, the DAG engine disposes.* No generated
pipeline reaches execution without passing the same validation a hand-written
pipeline passes.

**Split rationale:** correctness and control (validation, ordering, retries,
state, audit) live in Go; data work (connectors, transforms) lives in Python.

---

## 3. Design rules

Each rule has a statement, a rationale, and a verification instruction. Check
every one. Report each as PASS, FAIL, PARTIAL, or NOT VERIFIABLE.

### A. Architecture and layering

**A1 — `router.New` is the only composition root.**
All dependency wiring happens in one place. No handler, scheduler, or store
constructs its own dependencies or reads config directly.
*Why:* single place to swap implementations; it's what makes the dispatcher
pluggable.
*Verify:* search for `os.Getenv` and struct construction outside `router.New`
and `config`. Any dependency built inside a handler is a finding.

**A2 — Layering is handler → service → store, one direction only.**
Handlers do not touch the database directly. Stores do not call handlers.
Scheduler does not construct HTTP responses.
*Verify:* check imports. A `database/sql` or `pgx` import inside `handlers/` is
a finding.

**A3 — `Dispatcher` remains a pure interface boundary.**
The scheduler must not branch on which dispatcher implementation it has.
*Why:* Strategy pattern is what keeps local/worker swappable by config.
*Verify:* search the scheduler for type assertions or `EXEC_MODE` reads. Any
`if _, ok := d.(*LocalDispatcher)` is a finding.

**A4 — No new services without amortization.**
A separate deployable is justified only when it serves more than one consumer or
has a genuinely different runtime profile. A generic "router" service in front
of a single LLM task is not justified.
*Verify:* if new services have appeared, report what justifies each.

### B. DAG and scheduling

**B1 — Every pipeline is validated as a DAG before it is persisted.**
Cycles and unknown dependencies are rejected at write time with HTTP 400, not
discovered at run time.
*Why:* a cycle means steps that can never start; a saved cyclic pipeline is a
guaranteed hung run.
*Verify:* confirm `PipelineStore.Create` validates before insert. Confirm both
rejection paths return 400. Confirm tests exist for cycle and unknown-dep.

**B2 — Validation is duplicated at `/interpret` and at create, deliberately.**
These are separate calls and a draft can be hand-edited between them. Do not
report this duplication as redundancy.
*Verify:* confirm both call sites still validate.

**B3 — The ready set is computed from completion state, not from a
precomputed static order.**
A step is ready when *all* its dependencies have completed.
*Why:* this is Kahn's algorithm applied incrementally; a precomputed linear
order loses the parallelism information.
*Verify:* read the scheduler loop. Confirm the ready set is recomputed each
pass.

**B4 — Independent steps in the same ready set should execute concurrently.**
*Known gap:* the scheduler computes the ready set correctly, then executes it
sequentially in one goroutine.
*Verify:* confirm whether this is still true. If concurrency was added, check
for (a) a bounded concurrency limit, (b) a documented decision about whether
sibling steps are cancelled when one fails.

**B5 — A step exhausting its retries fails the whole run.**
Partial-success semantics are not supported and must not be silently
introduced.
*Verify:* trace the failure path out of the retry loop.

### C. Execution, durability, and retries

**C1 — A real `context.Context` is threaded from run start to the worker HTTP
call.**
*Why:* this is the single mechanism for cancellation, per-step timeouts, and
graceful shutdown. `context.Background()` in the execution path means none of
those are possible.
*Verify:* grep for `context.Background()` and `context.TODO()` outside `main`
and tests. Each occurrence in the run path is a finding. Confirm the context
reaches the dispatcher's HTTP request.

**C2 — Retries use exponential backoff with jitter.**
*Why:* immediate tight-loop retries fail three times in 30ms against a database
that needed two seconds. Jitter prevents a thundering herd when many steps fail
at once.
*Verify:* read the retry loop. No sleep at all is a finding. A fixed sleep with
no jitter is a partial.

**C3 — Failures are classified retryable vs non-retryable.**
A network timeout is worth retrying. Malformed SQL will fail identically
forever.
*Verify:* look for error classification before the retry decision. Absence is a
finding.

**C4 — Step execution is idempotent, or protected by a dedup key.**
*Why:* a dispatcher timeout does **not** mean the worker didn't do the work. It
may have completed the extract, written the artifact, and responded slowly.
Retrying re-runs it. For a `load` step that is duplicate rows in production.
*Verify:* check whether the worker deduplicates on `(run_id, step_key)`, or
whether load connectors upsert rather than insert. If neither, this is a HIGH
severity finding regardless of what the docs say.

**C5 — No run is left undriven.**
If the orchestrator restarts mid-run, rows must not remain in `running` with
nobody executing them.
*Verify:* look for a startup sweep that marks orphaned runs failed, or a
lease/claim mechanism. `go func()` with no recovery path is a finding.

**C6 — A panicking step does not silently kill the run.**
*Verify:* confirm recover() in the execution goroutine, and that a panic writes
a terminal event and status.

### D. State and observability

**D1 — Every state transition emits an event.**
`pipeline_run_events` is the audit trail behind `/metrics` and
`/failure-breakdown`.
*Verify:* list every place `step_runs.status` or `pipeline_runs.status` is
written. Each one must have an adjacent event emit. Report any write with no
paired event.

**D2 — Every terminal path emits a terminal event.**
Not just the happy path. Enumerate: normal completion, step failure, retry
exhaustion, panic, step timeout, run cancellation, orchestrator restart.
*Why:* this exact class of bug has bitten a sibling project — a status channel
was removed and the async completion path was left with no notification at all,
so successful work was never reported. Fixing the happy path and forgetting a
second terminal path is the failure mode.
*Verify:* trace each terminal path independently. Report any that ends without
an event.

**D3 — Runs that did not touch data must be marked as such.**
`EXEC_MODE=local` uses a no-op dispatcher. A run executed that way must be
visibly distinguishable — a `simulated` flag on the run, propagated into events
and every metrics endpoint.
*Why:* otherwise a green run proves only that the scheduler walked a graph, and
every metric is measuring a simulator.
*Verify:* check whether the run row carries this flag and whether
`/metrics` and `/failure-breakdown` surface it. If not, HIGH severity.

**D4 — Fallback reasons are counted separately.**
`/interpret` degrades to `manual_fallback` for at least five distinct reasons
(`interpreter_not_configured`, `nlp_unavailable`, `invalid_nlp_response`,
`low_confidence`, `invalid_pipeline`). Metrics must distinguish them.
*Why:* otherwise "the NLP service is down" and "the NLP service answered but
scored 0.68" are indistinguishable in production.
*Verify:* check whether the reason is recorded in a counter or event field.

**D5 — Pagination and filters exist on all list endpoints.**
`limit`/`offset`, plus status/level/event_type filters where applicable.
*Verify:* check `/runs`, `/events`, `/metrics`, `/failure-breakdown`, and the
audit endpoints. Report any unbounded list query.

### E. LLM integration and the trust boundary

**E1 — NL output is a draft, never an executable pipeline.**
`/interpret` returns a draft. It does not create, and does not run.
*Verify:* confirm no write-to-`pipelines` path exists inside the interpret
handler unless explicitly gated behind a flag (see E6).

**E2 — Every gate failure degrades to a deterministic fallback, never an
error.**
The five conditions in D4 each produce a safe manual draft.
*Verify:* confirm each branch returns a fallback, not a 5xx.

**E3 — Structural validation is not sufficient, and the code must not pretend
it is.**
A draft can be perfectly acyclic and completely wrong: wrong table, dropped
filter, writing to production. Required semantic checks:
- connector value is in an allowlist
- referenced tables/columns exist in the schema that was supplied to the model
- `input_from` names a real upstream step
- destination is in an allowlist
*Verify:* check what validation actually runs on a draft. Report each missing
check.

**E4 — Any `load` step, and any config containing a DSN, requires explicit
human confirmation before execution — regardless of confidence score.**
*Why:* confidence is a model signal, not a safety guarantee. The irreversible
operation is the one that needs a human.
*Verify:* look for a confirmation gate. Absence is HIGH severity.

**E5 — Model and prompt configuration lives in data, not in code.**
`{provider, model, sys_prompt, temperature, max_tokens}` as a row with an ID,
and the ID recorded against each generated draft.
*Why:* prompt iteration without redeploy, and — more importantly — the ability
to attribute a generated pipeline to the exact config that produced it, which is
what makes prompt comparison measurable.
*Verify:* check whether the prompt is a string literal in the NLP service. If
config is cached, check for a version/bust mechanism; a cache that requires a
process restart to refresh is a finding.

**E6 — Confidence gating must be honest about what it measures.**
Self-reported LLM confidence tracks fluency, not correctness. If the only
signal is a number the model emitted about itself, the threshold is
decorative.
*Verify:* identify which confidence signal is in use. Report it plainly. If
alternatives (log-probs, self-consistency, validation-derived scoring) have been
implemented, report which and whether any evaluation compares them.

**E7 — Structured output is schema-constrained, not prose-requested.**
Use the provider's JSON-schema / tool mode. "Please reply in JSON" is a finding.
*Verify:* read the LLM call site.

**E8 — Generated SQL is constrained.**
Restrict to SELECT for extract steps; validate identifiers against the known
schema; parameterize where possible.
*Verify:* trace how a generated `query` field reaches the database driver.

### F. Security

**F1 — The worker's `/execute` is authenticated.**
*Verify:* check for any auth on the worker. Absence is HIGH severity.

**F2 — The `http` connector has a host/scheme allowlist; the `postgres`
connector has a DSN allowlist.**
*Why:* an unauthenticated endpoint that accepts arbitrary URLs and DSNs is SSRF
plus arbitrary outbound database connections from inside the network. This is
not merely "missing auth" — it is a usable primitive.
*Verify:* read both connectors. Report exactly what an attacker controls.

**F3 — Credentials are not stored in plaintext in `pipeline_steps`.**
Connection strings must be env-var references, encrypted at rest, or held in an
external store.
*Verify:* read how a postgres connector config is persisted and retrieved.

**F4 — The orchestrator's API key check exempts only `/health` and CORS
preflight.**
*Verify:* read the middleware chain. Confirm ordering is CORS → Recovery →
Logger → APIKey, and that no route unintentionally bypasses auth.

**F5 — Secrets and internal URLs are not hardcoded.**
*Verify:* grep for `localhost`, `http://`, and credential-shaped literals
outside config and tests.

### G. Data and persistence

**G1 — Artifacts are not stored in process memory.**
Artifacts must be keyed by `(run_id, step_key)` in durable storage, with
cleanup on run completion.
*Why:* a module-level dict means worker restart loses in-flight data, two
workers cannot share artifacts, and completed runs leak memory forever.
*Verify:* read `artifacts.py`. If it is still a module-level dict, report all
three consequences.

**G2 — Definition and execution are separate.**
`pipelines`/`pipeline_steps` are templates; `pipeline_runs`/`step_runs` are
instances. Run state must never be written back into the definition tables.
*Verify:* check for updates to definition tables inside the scheduler.

**G3 — The event log is append-only.**
No updates or deletes on `pipeline_run_events`.
*Verify:* grep for UPDATE/DELETE against that table.

**G4 — Migrations are versioned.**
Hand-maintained SQL constants applied on startup with no version tracking is a
finding.
*Verify:* check `internal/database/database.go`.

**G5 — Concurrent writes to `step_runs` are safe.**
Once steps run in parallel, any read-modify-write on run/step state must be
atomic.
*Verify:* look for read-then-write sequences without a transaction or a
single-statement update.

### H. Testing and operations

**H1 — There is at least one real end-to-end test.**
interpret → create → run → assert data actually landed, with
`EXEC_MODE=worker`.
*Why:* a sibling project shipped a full async pipeline that had never once been
run end-to-end. Unit tests passing is not the same signal.
*Verify:* look in `tests/` and the Go test files. Report honestly if only unit
tests exist.

**H2 — CI runs both suites.**
`go test ./...` and the Python unittest discovery.
*Verify:* check `.github/workflows`.

**H3 — Local dev can be started in one command.**
Dockerfile(s) and a compose file covering orchestrator + worker + Postgres
(+ NLP service).
*Verify:* check for their existence.

**H4 — No operational step exists that a person can silently forget.**
Any component that must be started separately, and whose absence produces a
success-looking response followed by silence, is a design finding — not just an
ops note.
*Verify:* enumerate every process that must be running. For each, describe what
happens if it isn't. Flag any that fail silently.

---

## 4. Anti-patterns to flag on sight

- `context.Background()` anywhere in the run execution path
- A `go func()` with no recover and no lifecycle owner
- Retry logic in two layers (e.g. dispatcher retries *and* scheduler retries) —
  this multiplies attempts and makes failure reports meaningless
- Any config cache that requires a process restart to refresh
- A status/notification mechanism emitted on the success path only
- Business logic inside a middleware
- `panic()` used for control flow in request handling
- Silent `except:` / `catch` that swallows an error without an event
- A validation function that returns early on the first error when the caller
  needs all errors
- Hardcoded `localhost` outside config and tests
- Any TODO/FIXME comment older than the file's last significant change

---

## 5. Known gaps — verify current status of each

These were documented as of commit `997a8ed`. **Do not assume they are still
true, and do not assume they have been fixed.** Check each and report its actual
current state.

| # | Gap | Rule |
|---|---|---|
| 1 | `LocalDispatcher` is a 10ms sleep; default `EXEC_MODE` is `local` | D3 |
| 2 | No NLP service exists; every interpret call falls back | E1–E7 |
| 3 | `artifacts.py` is an in-memory dict, never freed | G1 |
| 4 | Runs are bare `go func()` + `context.Background()` | C1, C5 |
| 5 | Ready set executes sequentially | B4 |
| 6 | Retries have no backoff | C2 |
| 7 | One shared static API key; worker `/execute` has no auth | F1, F4 |
| 8 | `ui/`, `docs/`, `tests/`, `.github/` empty; no Docker; unversioned migrations | G4, H1–H3 |

---

## 6. Questions to answer explicitly in your report

1. Does `EXEC_MODE` still default to a no-op dispatcher?
2. Can a run be cancelled? If yes, trace the mechanism end to end.
3. What happens, concretely, if the orchestrator is killed mid-run?
4. What happens if the worker responds slowly and the dispatcher retries a step
   that actually succeeded?
5. What exactly does an attacker who can reach `:8090` control?
6. Where do database credentials for the postgres connector physically live?
7. Which confidence signal gates the interpret flow, and what evidence exists
   that it correlates with correctness?
8. List every process that must be running for a real run to complete. For each,
   state the observable symptom if it is not running.
9. Is there any path where work completes successfully but nothing records that
   it completed?
10. Where does `PROJECT_UNDERSTANDING.md` disagree with the code?

---

## 7. Report format

Produce exactly this structure. Nothing else.

### 7.1 Summary
Three to five sentences. State the overall health and the single most urgent
finding. No preamble.

### 7.2 Rule compliance table

| Rule | Status | Evidence | Note |
|---|---|---|---|
| A1 | PASS | `internal/router/router.go:42` | — |
| C1 | FAIL | `internal/scheduler/scheduler.go:88` | `context.Background()` in ExecuteRun |

Status is one of PASS / FAIL / PARTIAL / NOT VERIFIABLE. Every rule in Section 3
gets a row.

### 7.3 Findings

One block per finding, ordered by severity.

```
### F-01 · HIGH · Step retry can double-execute a load
Rule: C4
Evidence: internal/scheduler/scheduler.go:112, executor/worker/runner.py:34
What: The dispatcher retries on timeout. The worker has no dedup on
      (run_id, step_key) and the postgres connector's load() issues INSERT.
Impact: A slow-but-successful load followed by a retry writes duplicate rows.
Fix: <one or two sentences>
Effort: S | M | L
```

Severity definitions:
- **HIGH** — data loss, data corruption, a security primitive, or a green signal
  that is not true
- **MEDIUM** — a design rule violated with real consequences under load or
  failure
- **LOW** — a gap that is known, contained, and currently harmless

### 7.4 Documentation discrepancies
Anything where `PROJECT_UNDERSTANDING.md` or this file disagrees with the code.
State which is correct.

### 7.5 Structural observations
Things that are not rule violations but are worth the owner's attention:
coupling that will hurt later, abstractions that are not earning their keep,
duplication that indicates a missing concept.

### 7.6 What you could not verify
Be explicit. List what you looked for and did not find, and what you could not
reach.

---

## 8. Do not

- Do not modify code, config, or any file in this pass
- Do not fix findings inline "while you're there"
- Do not report style, naming, or formatting
- Do not restate a rule's rationale back to the user — they wrote it
- Do not pad the report with items you could not verify
- Do not soften a HIGH finding because the gap is documented; documented and
  dangerous is still dangerous
- Do not assume a fix exists because it would be sensible
