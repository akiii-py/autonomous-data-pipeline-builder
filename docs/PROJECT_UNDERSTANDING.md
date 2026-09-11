# Project Understanding — Autonomous Data Pipeline Builder

Status snapshot: through the structural design changes (commit `4425c5d`, branch
`feat/structural-design-changes`). Written for anyone joining the project cold —
what it is, why it exists, how it currently works, what is real versus stubbed,
and what to do next.

If you are changing code here, read the system design rules in `CLAUDE.md` first.
They are binding, and each one names the design decision it enforces.

---

## 1. What we are building and why

**Goal:** a user describes a data job in plain English ("pull sales rows from
Postgres, aggregate by region, write to a warehouse table") and the platform
turns that into a validated DAG pipeline, persists it, runs it with dependency
ordering and retries, and reports what happened.

**Why it is structured as two services:**

- Correctness and control (validation, ordering, retries, state, audit) is
  orchestration work — it lives in Go, where a long-running server, typed models,
  and a real database fit naturally.
- Data work (connectors, transforms) is Python work — that is where the data
  ecosystem lives (psycopg, pandas-style ops later).
- Natural language is untrusted input, so it never becomes an executable pipeline
  directly. NLP output is a *draft* that must clear a confidence threshold, a
  structural check, **and** a semantic check against the catalog it was generated
  from. Anything irreversible then needs a human approval transition on top.

The design bet: **the LLM proposes, the DAG engine disposes.** No generated
pipeline reaches execution without passing the same validation a hand-written
pipeline passes.

---

## 2. System shape

```
   client
     │  POST /api/v1/interpret          (natural language → draft)
     │  POST /api/v1/pipelines          (draft → stored pipeline)
     │  POST /api/v1/pipelines/{id}/approve   (required if it writes anywhere)
     │  POST /api/v1/pipelines/{id}/run
     ▼
┌─────────────────────────── Go orchestrator (:8080) ────────────────────────────┐
│ middleware: CORS → Recovery → Logger → APIKey                                   │
│ router.New builds the handler AND the run loop — single composition root        │
│                                                                                 │
│ handlers ── interpret.go ──► interpreter.HTTPClient ──► NLP service (:8091)     │
│          │                     (request carries catalog.Context)                │
│          └─ pipeline.go / monitoring.go                                         │
│                    │                                                            │
│                    ├──► store.PipelineStore ──► PostgreSQL                      │
│                    │      POST /run inserts an UNCLAIMED run row and returns    │
│                    ▼                                                            │
│           runner.Loop  (background, owns execution)                             │
│                    │  claims a run: FOR UPDATE SKIP LOCKED + lease + heartbeat  │
│                    ▼                                                            │
│           scheduler.ExecuteRun                                                  │
│                    │  ready set → step_runs 'queued' → bounded concurrent claim │
│                    ├──► store (status + event in ONE transaction)               │
│                    └──► dispatcher.Dispatcher                                    │
│                            ├── LocalDispatcher  (EXEC_MODE=local, default)       │
│                            │     simulated=true on every result                  │
│                            └── HTTPDispatcher   (EXEC_MODE=worker) ──┐           │
└──────────────────────────────────────────────────────────────────────┼──────────┘
                                                                       ▼
                                              ┌── Python executor worker (:8090) ──┐
                                              │ ThreadingHTTPServer, X-Worker-Token │
                                              │ POST /execute → StepRunner.execute  │
                                              │  ExecuteRequest → StepResult        │
                                              │  registry: file / http / postgres   │
                                              │   (capabilities + allowlists)       │
                                              │  artifacts + ledger in Postgres     │
                                              └─────────────────────────────────────┘
```

**Persistence** (versioned migrations in `internal/database/migrations.go`,
applied once each and recorded in `schema_migrations`):

| Table | Holds |
| --- | --- |
| `pipelines` | definitions; soft-deleted, carries `requires_approval` / `approved_at` |
| `pipeline_steps` | step definitions; config is JSONB, credentials by reference only |
| `pipeline_runs` | run instances; ownership (`claimed_by`, `claim_expires_at`) and provenance (`exec_mode`, `simulated`) |
| `step_runs` | step instances; also the work queue (`status = 'queued'`) |
| `pipeline_run_events` | audit log; no FK cascade from `pipelines` |
| `run_artifacts` | data passed between steps, keyed `(run_id, step_key)` |
| `step_executions` | the worker's idempotency ledger, keyed `(run_id, step_key)` |
| `degradation_events` | every interpret fallback, with its reason |
| `schema_migrations` | applied version + checksum |

---

## 3. How the main flows actually work

### 3.1 Natural language → draft (`POST /api/v1/interpret`)

A gate chain that degrades to a safe manual draft rather than erroring:

| Condition | Result |
| --- | --- |
| Interpreter or catalog not configured | `manual_fallback`, `interpreter_not_configured` |
| NLP call errored | `manual_fallback`, `nlp_unavailable` |
| Empty NLP response | `manual_fallback`, `invalid_nlp_response` |
| `confidence < NLP_MIN_CONFIDENCE` (default 0.70) | `manual_fallback`, `low_confidence` |
| Structural **or** semantic validation failed | `manual_fallback`, `invalid_pipeline` |
| All checks pass | `mode: auto` + normalized `pipeline_draft` |

Two things changed here and both matter:

**Every fallback writes a `degradation_events` row** with its reason, surfaced in
`GET /api/v1/metrics` under `degradations`. "The NLP service is dead" and "the
model scored 0.68" are no longer the same observation from outside.

**Validation is two stages** (`internal/validation`):

- *Structural* takes only the draft — name, step keys, step types, acyclicity,
  parseable config. It returns **every** problem, not the first, so fixing a
  rejected draft is one round trip.
- *Semantic* takes the draft **plus** the `catalog.Context` that was sent to the
  model. It checks the connector allowlist, the destination allowlist, that
  `input_from` names a real upstream step *and* is declared in `depends_on`, that
  referenced tables exist in the described schema, that the transform op is
  supported, that extract queries are SELECT-only and not stacked, and that no
  plaintext `dsn` is present.

The catalog is the same object on both legs — sent in the request, used to
validate the reply. Without that, "referenced tables must exist" is not
unimplemented, it is impossible.

**The endpoint still returns a draft only.** The client POSTs it to
`/api/v1/pipelines` itself.

### 3.2 Create → approve → run → observe

1. **`POST /api/v1/pipelines`** — `PipelineStore.Create` validates the DAG before
   insert; failures come back as HTTP 400 with the full `errors` list and
   machine-readable `codes`. The handler matches the typed
   `dag.ValidationErrors` with `errors.As`, not on message text.

   Create also computes `requires_approval`: true if any step is a `load` or any
   config carries credentials. Such a pipeline is stored `pending_approval`.

2. **`POST /api/v1/pipelines/{id}/approve`** — the lifecycle transition that makes
   an irreversible pipeline runnable. Confirmation is a state change on the
   pipeline, not a flag on the run call. `POST /run` on an unapproved pipeline
   returns **409** with the approve URL.

3. **`POST /api/v1/pipelines/{id}/run`** — inserts an **unclaimed** run row plus its
   step rows and a `run_queued` event, then returns 202. It does **not** execute
   anything and spawns no goroutine.

4. **`runner.Loop`** (started by `main`, one per process) claims a run:

   ```sql
   UPDATE pipeline_runs SET claimed_by = $me, claim_expires_at = now() + lease ...
   WHERE id = (SELECT id FROM pipeline_runs
               WHERE status IN ('pending','running')
                 AND (claimed_by = '' OR claim_expires_at < now())
               ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1)
   ```

   It heartbeats to hold the claim. Losing the claim, or an operator calling
   `POST /runs/{run_id}/cancel`, cancels the run's context. **This is why restart
   recovery needs no sweep:** a dead process's claims simply expire and another
   process picks the runs up.

5. **`scheduler.ExecuteRun`** rebuilds the DAG, then loops: compute the ready set
   from completion state, move those `step_runs` to `queued`, and let up to
   `SCHEDULER_MAX_CONCURRENCY` goroutines claim them — again with
   `FOR UPDATE SKIP LOCKED`, so the same step is never taken twice.

   Retries consult the error classification: only `transient` failures retry, with
   exponential backoff and jitter. A `permanent` failure burns no budget.

   When one step fails, its in-flight siblings are cancelled, marked `cancelled`,
   and each emits its own terminal event.

6. **Observability** reads that history back: `/runs`, `/events`, `/metrics`,
   `/failure-breakdown`, plus global `/api/v1/metrics`. All paginated and
   filterable — including `/api/v1/pipelines`, which used to be unbounded.

### 3.3 Step execution

`LocalDispatcher` simulates and marks every result `simulated`. `HTTPDispatcher`
POSTs a `StepRequest` to the worker's `/execute` with `X-Worker-Token`.

In the worker, `StepRunner.execute`:

1. **Checks the ledger first.** If `(run_id, step_key)` is already recorded, the
   stored result is returned with `replayed = true` and nothing re-executes.
2. Otherwise dispatches on step type, taking the connector **from the registry**
   — which enforces that connector's allowlist.
3. Records the result in the ledger on success.

Artifacts live in `run_artifacts`, keyed `(run_id, step_key)`, deleted inside the
run's terminal transaction.

### 3.4 Two invariants worth knowing before you touch the scheduler

**The event log is authoritative.** A status write and its event share one
transaction (`TransitionRun` / `TransitionStep` / `FinishRun`). A failed emit
fails the transition. There is no longer a path on which work completes and
nothing records it.

**Execution identity is `(run_id, step_key)` — not the attempt.** The idempotency
key sent on the wire includes the attempt for traceability, but the ledger keys
on the logical unit. Keying on the attempt would mean a retry after a timeout
mints a new key, never matches, and re-runs the load that had in fact succeeded.

---

## 4. What is real versus what is scaffolding

**Real and working:**

- Pipeline CRUD with DAG validation at write time, returning the full error set
  with typed codes.
- An approval stage that blocks irreversible pipelines from running.
- Durable run ownership: claim, lease, heartbeat, cancellation, and restart
  recovery by lease expiry. Multiple orchestrator processes can run safely.
- Postgres as the work queue via `FOR UPDATE SKIP LOCKED`, at both run and step
  level.
- Bounded parallel execution of independent steps, with explicit sibling
  cancellation on failure.
- Retry with error classification and exponential backoff with jitter.
- Idempotent step execution: a retry after a dispatcher timeout replays rather
  than re-executing.
- Artifacts in durable shared storage with cleanup at run completion.
- Execution provenance on every run, with simulated and real counted separately
  in every metrics response.
- An authoritative event log, with a terminal event on every terminal path
  including panic and cancellation.
- Versioned, checksummed migrations.
- Two-stage draft validation against a catalog that is actually sent to the model.
- Degradation telemetry with per-reason counts.
- Worker: threaded, token-authenticated, typed wire contract, connector registry
  with per-connector capabilities and allowlists, credentials by reference only.

**Scaffolding, stubs, or absent — read this before trusting a green run:**

1. **There is still no NLP service.** `NLP_SERVICE_URL` points at `localhost:8091`
   and nothing implements it. Every `/interpret` call takes the
   `nlp_unavailable` fallback — the difference from before is that you can now
   *see* that in `/metrics`, instead of inferring it.
2. **`EXEC_MODE` still defaults to `local`, which executes nothing.** This is no
   longer dishonest — the run row carries `simulated = true`, it propagates into
   events and every metrics response, startup logs a warning, and an
   unrecognised `EXEC_MODE` now fails startup instead of silently downgrading.
   But a default-config green run still proves only that the scheduler walked a
   graph.
3. **Confidence is still self-reported by the model.** The threshold gates on a
   number the model emitted about itself. No log-probs, no self-consistency, no
   validation-derived score, and no evaluation showing any of them correlates
   with correctness. This is the single largest remaining gap in the NLP story.
4. **Prompt and model configuration do not exist as data.** When the NLP service
   is built, `{provider, model, sys_prompt, temperature}` should be a row with an
   ID recorded against each generated draft — otherwise prompt comparison is not
   measurable.
5. **The worker falls back to in-memory storage when `WORKER_DATABASE_URL` is
   unset.** It logs a loud warning, and in that mode it cannot be restarted or
   scaled and retries are not deduplicated. Convenience for local dev only.
6. **Allowlists are empty by default, and fail closed.** An unconfigured worker
   cannot reach any host, path, or DSN, and an unconfigured catalog denies every
   load destination. That is deliberate, and it means a fresh checkout needs
   configuration before a real pipeline runs.
7. **Still empty:** `ui/`, `tests/`, `.github/workflows/`. No Dockerfile, no
   compose file, no CI.
8. **No end-to-end test.** The Go suite is unit-level; the Python suite covers
   the runner, the wire contract, allowlists and replay, but nothing exercises
   interpret → create → approve → run against a live worker and database.
9. **No user or tenant model.** Auth is one shared static API key.

---

## 5. Environment and how to run it

### Orchestrator

| Variable | Default | Notes |
| --- | --- | --- |
| `PORT` | 8080 | HTTP port |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/pipeline?sslmode=disable` | migrations run on startup |
| `API_KEY` | *(empty)* | when empty, `/api` routes are unauthenticated and startup warns; **required** when `ENVIRONMENT` is staging/production |
| `ENVIRONMENT` | `development` | staging/production enforce `API_KEY` |
| `EXEC_MODE` | `local` | `local` = simulator, `worker` = real. Any other value **fails startup** |
| `WORKER_URL` / `WORKER_TIMEOUT_MS` | `http://localhost:8090` / 10000 | worker dispatch |
| `WORKER_TOKEN` | *(empty)* | sent as `X-Worker-Token`; must match the worker's |
| `RUNNER_OWNER` | `<hostname>-<pid>` | identity written to `claimed_by` |
| `RUN_LEASE_SECONDS` | 30 | claim lease; minimum 2 |
| `RUN_POLL_MS` | 1000 | wait after finding nothing claimable |
| `MAX_CONCURRENT_RUNS` | 4 | runs driven at once per process |
| `SCHEDULER_MAX_CONCURRENCY` | 4 | steps of one ready set at once |
| `RETRY_BACKOFF_BASE_MS` / `RETRY_BACKOFF_MAX_MS` | 200 / 10000 | transient-failure backoff bounds |
| `NLP_SERVICE_URL` / `NLP_TIMEOUT_MS` | `http://localhost:8091` / 8000 | no implementation yet |
| `NLP_MIN_CONFIDENCE` | 0.70 | below this → manual fallback |
| `CATALOG_CONNECTORS` | `file,http,postgres` | connector allowlist for generated drafts |
| `CATALOG_DESTINATIONS` | *(empty)* | load destination allowlist; **empty denies all** |
| `CATALOG_TRANSFORM_OPS` | `select,filter_eq,aggregate_sum` | allowed ops |
| `CATALOG_SCHEMA_PATH` / `CATALOG_SCHEMA_JSON` | *(empty)* | JSON array of `{name, columns}` describing tables |

`REDIS_URL` and `GRPC_PORT` were removed — they were declared, validated, and
never read.

### Worker

| Variable | Default | Notes |
| --- | --- | --- |
| `WORKER_HOST` / `WORKER_PORT` | `127.0.0.1` / 8090 | binds to loopback by default |
| `WORKER_TOKEN` | *(empty)* | required header; empty means unauthenticated and warns |
| `WORKER_DATABASE_URL` | falls back to `DATABASE_URL` | artifacts + ledger; unset → in-memory fallback |
| `WORKER_ALLOWED_HOSTS` | *(empty)* | http connector host allowlist |
| `WORKER_ALLOWED_SCHEMES` | `https` | http connector scheme allowlist |
| `WORKER_ALLOWED_PATHS` | *(empty)* | file connector path prefixes |
| `WORKER_ALLOWED_DSN_REFS` | *(empty)* | env var names the postgres connector may resolve |
| `WORKER_UNRESTRICTED_CONNECTORS` | `false` | disables allowlists; local dev only |
| `WORKER_MAX_THREADS` | 8 | |

### Commands

```bash
# orchestrator
cd orchestrator && go mod tidy && go run ./cmd/server
go test ./...

# worker (needed for EXEC_MODE=worker)
python -m venv .venv && source .venv/bin/activate
pip install -r executor/requirements.txt
PYTHONPATH="$PWD" python executor/worker/server.py

# python tests  (live Postgres tests skip unless PG_TEST_DSN is set)
PYTHONPATH="$PWD" python -m unittest discover -s executor/tests -p "test_*.py"
```

`examples/postgres_e2e.py` is a local Postgres extract → transform → load
walkthrough. It drives `StepRunner` directly rather than going through the
orchestrator, so it is a demonstration and not an end-to-end test — but it does
show the two things most worth seeing: credentials by `dsn_ref`, and a retry of a
completed load replaying instead of inserting twice.

---

## 6. Codebase navigation

This repo has a knowledge graph in `graphify-out/` (726 nodes, 1499 edges, 45
communities). Prefer it over grep:

```bash
graphify query "how does the run loop claim and hold a run"
graphify path "PipelineHandler.RunPipeline" "StepRunner.execute"
graphify explain "Dispatcher"
graphify update .          # after code changes, AST-only, no API cost
```

Hub nodes worth knowing: `PipelineStore`, `Scheduler`, `runner.Loop`,
`StepRunner`, `dag.Graph`, `catalog.Context`. `router.New` is the composition
root — every wiring decision, including the run loop, is made there.

---

## 7. What to do next, and why

The three changes that used to be Tier 1 — the fake-green default, artifacts in
process memory, and undriven runs — are done. What is left divides cleanly.

**Tier 1 — prove it works**

1. **One end-to-end test.** interpret → create → approve → run → assert rows
   landed, with `EXEC_MODE=worker` against a real worker and database. Nothing
   currently proves the whole path executes, and the execution model just
   changed substantially.
2. **CI and compose.** A workflow running `go test ./...` and the Python suite,
   and a compose file covering orchestrator + worker + Postgres. CI is what keeps
   the structural work from regressing; compose is what makes the e2e test cheap.

**Tier 2 — finish the promise in the name**

3. **Build the NLP service** (the missing `:8091`). It must accept the catalog
   context now in the request and return `{pipeline, confidence, warnings}`.
   Until it exists, "autonomous" is aspirational.
4. **Model and prompt config as data**, with the config ID recorded against each
   generated draft — the prerequisite for measuring whether a prompt change
   helped.
5. **Confidence calibration.** Self-reported confidence tracks fluency, not
   correctness. Build a small labelled eval set, then compare self-reported
   confidence against validation-derived scoring and self-consistency. This is
   the most interesting unsolved problem in the project.

**Tier 3 — scale and ergonomics**

6. Per-user/tenant auth and pipeline ownership.
7. Artifact storage that does not put large payloads in JSONB — object storage
   behind the same `ArtifactStore` interface.
8. Step-level timeouts, distinct from the worker HTTP timeout.
9. UI — most visible, and correctly last.

**Suggested order:** 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9.

---

## 8. Open questions for the owner

- Is the NLP service meant to live in this repo or as a separate deployable?
  That decision changes the compose and CI layout.
- Should `local` mode remain as a fast test harness now that it is honestly
  labelled, or be removed once the worker path has an e2e test?
- Multi-tenancy: real requirement, or is a single shared API key acceptable for
  the foreseeable roadmap?
- How large do artifacts get? JSONB is fine for thousands of rows and wrong for
  millions; the `ArtifactStore` interface was written so this can change without
  touching the runner.
- Is one orchestrator enough? The claim/lease model supports several, but that is
  currently untested beyond the query semantics.
