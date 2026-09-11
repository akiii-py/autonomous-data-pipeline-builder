# Project Understanding — Autonomous Data Pipeline Builder

Status snapshot: through Phase 6.5 (commit `997a8ed`). Written for anyone joining the project cold — what it is, why it exists, how it currently works, what is real versus stubbed, and what to do next.

---

## 1. What we are building and why

**Goal:** a user describes a data job in plain English ("pull sales rows from Postgres, aggregate by region, write to a warehouse table") and the platform turns that into a validated DAG pipeline, persists it, runs it with dependency ordering and retries, and reports what happened.

**Why it is structured as two services:**

- Correctness and control (validation, ordering, retries, state, audit) is orchestration work — it lives in Go, where a long-running server, typed models, and a real database fit naturally.
- Data work (connectors, transforms) is Python work — that is where the data ecosystem lives (psycopg, pandas-style ops later).
- Natural language is untrusted input, so it never becomes an executable pipeline directly. The NLP output is a *draft* that must clear a confidence threshold **and** structural validation before the system will treat it as usable. Otherwise the user gets a safe deterministic template and stays in control.

The design bet: **the LLM proposes, the DAG engine disposes.** No generated pipeline reaches execution without passing the same validation a hand-written pipeline passes.

---

## 2. System shape

```
   client
     │  POST /api/v1/interpret        (natural language → draft)
     │  POST /api/v1/pipelines        (draft → stored pipeline)
     │  POST /api/v1/pipelines/{id}/run
     ▼
┌─────────────────────────── Go orchestrator (:8080) ────────────────────────────┐
│ middleware: CORS → Recovery → Logger → APIKey                                   │
│ router.New wires every route + picks the dispatcher from EXEC_MODE              │
│                                                                                 │
│ handlers ── interpret.go ──► interpreter.HTTPClient ──► NLP service (:8091)     │
│          └─ pipeline.go / monitoring.go                                         │
│                    │                                                            │
│              scheduler.ExecuteRun                                               │
│                    │  builds dag.Graph, walks ready steps, retries, emits events │
│                    ├──► store.PipelineStore ──► PostgreSQL                      │
│                    └──► dispatcher.Dispatcher                                    │
│                            ├── LocalDispatcher  (EXEC_MODE=local, default)       │
│                            └── HTTPDispatcher   (EXEC_MODE=worker) ──┐           │
└──────────────────────────────────────────────────────────────────────┼──────────┘
                                                                       ▼
                                              ┌── Python executor worker (:8090) ──┐
                                              │ POST /execute → runner.execute_step │
                                              │  extract/transform/load dispatch    │
                                              │  connectors: file / http / postgres │
                                              │  transformations/ops.py             │
                                              │  artifacts.py (in-process dict)     │
                                              └─────────────────────────────────────┘
```

**Persistence (auto-migrated on startup, `internal/database/database.go`):**
`pipelines`, `pipeline_steps`, `pipeline_runs`, `step_runs`, `pipeline_run_events` — plus indexes on the run/event lookup paths.

---

## 3. How the main flows actually work

### 3.1 Natural language → draft (`POST /api/v1/interpret`)

`handlers/interpret.go` runs a strict gate chain. It falls back to a safe manual draft at every failure point rather than erroring out:

| Condition | Result |
| --- | --- |
| Interpreter not configured | `manual_fallback`, reason `interpreter_not_configured` |
| NLP call errored | `manual_fallback`, reason `nlp_unavailable` |
| Empty NLP response | `manual_fallback`, reason `invalid_nlp_response` |
| `confidence < NLP_MIN_CONFIDENCE` (default 0.70) | `manual_fallback`, reason `low_confidence` |
| Draft fails validation (name, step keys, step type in {extract, transform, load}, DAG builds without cycles/unknown deps) | `manual_fallback`, reason `invalid_pipeline` |
| All checks pass | `mode: auto` + normalized `pipeline_draft` |

The fallback draft is always the same deterministic extract → transform → load skeleton, named from a slug of the query. **The endpoint returns a draft only — it does not create the pipeline.** The client still has to POST it to `/api/v1/pipelines`.

### 3.2 Create → run → observe

1. `POST /api/v1/pipelines` — `PipelineStore.Create` validates the DAG before insert; a cycle or unknown dependency comes back as HTTP 400.
2. `POST /api/v1/pipelines/{id}/run` — creates a run row, emits a `run_queued` event, then launches `scheduler.ExecuteRun` in a **detached goroutine** and immediately returns 202 with the `run_id`.
3. `scheduler.ExecuteRun` — loads the pipeline, rebuilds the DAG from stored steps, marks the run `running`, then loops: compute ready steps (all dependencies completed), execute each, retry up to `config.retry_count` times, update `step_runs` status, emit an event at every transition. Any step exhausting retries fails the whole run.
4. Observability endpoints read that event/run history back: `/runs`, `/events`, `/metrics`, `/failure-breakdown`, plus global `/api/v1/metrics`. All paginated with `limit`/`offset` and filterable by status/level/event_type.

### 3.3 Step execution

`LocalDispatcher` sleeps 10ms and returns success. `HTTPDispatcher` POSTs `{run_id, step}` to the worker's `/execute`. In the worker, `runner.execute_step` branches on step type:

- **extract** — pick connector from `config.connector` (`file` | `http` | `postgres`), call `extract(config)`, store the result under `(run_id, step_key)`.
- **transform** — read the upstream artifact named by `config.input_from`, apply the op (`select`, `filter_eq`, `aggregate_sum`), store the result.
- **load** — read the upstream artifact, call the connector's `load(data, config)`.

Artifacts are how steps pass data. They live in `executor/worker/artifacts.py` — a module-level dict guarded by a lock.

---

## 4. What is real versus what is scaffolding

**Real and working:**
- Pipeline CRUD with DAG validation at write time; cycle and unknown-dependency rejection with tests.
- Dependency-ordered scheduling with per-step retry from `config.retry_count`.
- Full run/step state machine persisted to Postgres, with an event log behind every transition.
- Observability API surface with pagination, filters, metrics, and failure grouping.
- Confidence-gated NLP flow with deterministic fallback, covered by handler tests.
- API key middleware (`X-API-Key` or `Authorization: Bearer`), `/health` and CORS preflight exempt.
- Python worker with three connectors and three transform ops; unit tests for the runner flow and the Postgres connector.

**Scaffolding, stubs, or absent — read this before trusting a green run:**

1. **The default execution path executes nothing.** `EXEC_MODE` defaults to `local`, and `LocalDispatcher.ExecuteStep` is a 10ms sleep that always returns nil. Runs go green without touching data. Only `EXEC_MODE=worker` does real work.
2. **No NLP service exists in this repo.** `NLP_SERVICE_URL` points at `localhost:8091` and nothing implements it. Today every `/interpret` call takes the `nlp_unavailable` fallback path.
3. **Artifacts are process-local and never freed.** `artifacts.py` is an in-memory dict, so a worker restart loses in-flight run data, two worker replicas cannot share artifacts, and completed runs leak memory forever.
4. **Runs do not survive a restart.** Execution is a bare `go func()` with `context.Background()` — no queue, no lease, no cancellation, no timeout. Kill the orchestrator mid-run and the run is stuck in `running` with no one driving it.
5. **No step parallelism.** The scheduler computes the full ready set correctly, then executes it sequentially in one goroutine. Independent branches do not run concurrently.
6. **Retries have no backoff.** Failures retry immediately in a tight loop.
7. **Auth is one shared static key.** There is no user or tenant model, despite handler comments referring to "the authenticated user". The worker's `/execute` has no auth at all.
8. **Empty by design, still empty:** `ui/`, `docs/`, `tests/`, and `.github/` (no CI). No Dockerfile or compose file. Migrations are hand-rolled SQL constants with no version tracking.

---

## 5. Environment and how to run it

| Variable | Default | Notes |
| --- | --- | --- |
| `PORT` | 8080 | orchestrator HTTP port |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/pipeline?sslmode=disable` | migrations run on startup |
| `API_KEY` | *(empty)* | when empty, `/api` routes are unauthenticated |
| `EXEC_MODE` | `local` | `local` = no-op simulator, `worker` = real execution |
| `WORKER_URL` / `WORKER_TIMEOUT_MS` | `http://localhost:8090` / 10000 | worker dispatch |
| `NLP_SERVICE_URL` / `NLP_TIMEOUT_MS` | `http://localhost:8091` / 8000 | no implementation yet |
| `NLP_MIN_CONFIDENCE` | 0.70 | below this → manual fallback |
| `REDIS_URL`, `GRPC_PORT` | set, unused | reserved |

```bash
# orchestrator
cd orchestrator && go mod tidy && go run ./cmd/server
go test ./...

# worker (needed for EXEC_MODE=worker)
python -m venv .venv && source .venv/bin/activate
pip install -r executor/requirements.txt
PYTHONPATH="$PWD" python executor/worker/server.py

# python tests
PYTHONPATH="$PWD" python -m unittest discover -s executor/tests -p "test_*.py"
```

There is also `examples/phase4_postgres_e2e.py` for a local Postgres extract → transform → load walkthrough.

---

## 6. Codebase navigation

This repo has a knowledge graph in `graphify-out/` (306 nodes, 583 edges, 28 communities). Prefer it over grep — it is cheaper and gives structural context:

```bash
graphify query "how does the scheduler retry a failed step"
graphify path "PipelineHandler.RunPipeline" "execute_step"
graphify explain "Dispatcher"
graphify update .          # after code changes, AST-only, no API cost
```

Hub nodes worth knowing: `PipelineStore` (23 edges), `PipelineHandler` (19), `execute_step` (18), `router.New` (18), `dag.Graph` (15). `router.New` is the composition root — every wiring decision is made there.

---

## 7. What to do next, and why

Phases 7a (developer experience) and 7b (UI) are the stated milestones, but three of the gaps above are more urgent than either, because they make the system's own green signals untrustworthy.

**Tier 1 — make the system honest (do these first)**

1. **Fix the fake-green default.** Either make `LocalDispatcher` run the real step logic in-process, or make `local` mode loudly mark runs as simulated. Today a passing run proves nothing about the data path. *Cheapest high-value fix in the repo.*
2. **Move artifacts out of process memory.** Postgres table, object store, or filesystem keyed by `(run_id, step_key)`, with cleanup on run completion. Blocks: worker restarts, multiple workers, any real data volume.
3. **Make runs durable.** A run row plus a claim/lease so a restarted orchestrator can resume or fail-forward, instead of leaving rows stuck in `running`. Add run cancellation while you are in there.

**Tier 2 — complete the promise in the name**

4. **Build the NLP service** (the missing `:8091`). Until it exists, "autonomous" is aspirational — every interpret call falls back. It needs to return `{pipeline, confidence, warnings}` matching `internal/interpreter/client.go`.
5. **Close the interpret → create loop.** Optionally let `/interpret` persist a validated auto-mode draft (behind a flag or `dry_run: false`), so one natural-language request yields a runnable pipeline instead of two manual round-trips.

**Tier 3 — scale and ergonomics**

6. Parallel execution of independent ready steps, plus exponential backoff on retries.
7. Real auth: per-user/tenant keys, ownership on pipelines, and auth on the worker's `/execute`.
8. CI in `.github/workflows` running `go test ./...` and the Python suite; Dockerfiles and a compose file covering orchestrator + worker + Postgres.
9. Versioned migrations rather than hand-maintained SQL constants.
10. UI (Phase 7b) — it is the most visible work but depends on everything above being trustworthy first.

**Suggested order:** 1 → 2 → 3 → 8 → 4 → 5 → 6 → 7 → 9 → 10. Item 8 lands early because CI is what keeps items 1–3 from regressing.

---

## 8. Open questions for the owner

- Is the NLP service meant to live in this repo or as a separate deployable? That decision changes the compose/CI layout.
- Should `local` mode remain as a fast test harness, or be removed once the worker path is solid?
- Multi-tenancy: real requirement, or is a single shared API key acceptable for the foreseeable roadmap?
- Target scale — is single-orchestrator sufficient, or do we eventually need multiple orchestrators sharing a run queue? That answer determines how heavy item 3 needs to be.
