# Autonomous Data Pipeline Builder

Turn a plain-English data request into a validated, dependency-ordered DAG
pipeline — then run it with durable ownership, classified retries, and an audit
trail you can trust.

```
"pull sales rows from Postgres, aggregate by region, write to the warehouse"
                              │
                              ▼
        draft → validate → approve → run → observe
```

**The design bet: the LLM proposes, the DAG engine disposes.** No generated
pipeline reaches execution without passing the same validation a hand-written one
passes — plus a human approval step if it writes anywhere.

---

## Contents

- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Pipeline definitions](#pipeline-definitions)
- [API](#api)
- [Configuration](#configuration)
- [Execution guarantees](#execution-guarantees)
- [Development](#development)
- [Project layout](#project-layout)
- [Status and limitations](#status-and-limitations)

---

## Architecture

Two services, split along a deliberate line: **correctness and control in Go,
data work in Python.**

```
   client
     ▼
┌───────────────── Go orchestrator (:8080) ──────────────────┐
│  middleware: CORS → Recovery → Logger → APIKey             │
│  router.New — the single composition root                  │
│                                                            │
│  handlers ──► PipelineStore ──► PostgreSQL                 │
│      │          POST /run inserts an UNCLAIMED run         │
│      ▼                                                     │
│  runner.Loop — claims runs (SKIP LOCKED + lease)           │
│      ▼                                                     │
│  scheduler — ready set, bounded concurrency, retries       │
│      ▼                                                     │
│  Dispatcher ├── Local  (EXEC_MODE=local, simulated)        │
│             └── HTTP   (EXEC_MODE=worker) ──┐              │
└─────────────────────────────────────────────┼──────────────┘
                                              ▼
                      ┌── Python executor worker (:8090) ──┐
                      │  threaded, token-authenticated     │
                      │  connector registry + allowlists   │
                      │  artifacts + idempotency ledger    │
                      └────────────────────────────────────┘
```

The orchestrator never executes data work; the worker never makes scheduling
decisions. Everything durable — run ownership, queue position, artifacts —
lives in Postgres, so any process can be restarted without stranding work.

**Tables:** `pipelines`, `pipeline_steps`, `pipeline_runs`, `step_runs`,
`pipeline_run_events`, `run_artifacts`, `step_executions`, `degradation_events`,
`schema_migrations`.

---

## Quick start

### Prerequisites

- Go 1.26+
- PostgreSQL 9.6+ — the work queue needs `FOR UPDATE SKIP LOCKED` (9.5) and the
  metrics queries use aggregate `FILTER` (9.4)
- Python 3.10+

### 1. Start the orchestrator

```bash
createdb pipeline

cd orchestrator
go mod tidy
go run ./cmd/server
```

Migrations run automatically and are recorded in `schema_migrations`.

With no configuration you get a working API in **simulator mode** — runs walk the
graph without touching data. Every such run is recorded `simulated = true`, and
startup says so. That is the honest default, not a trap.

### 2. Start the worker (for real execution)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r executor/requirements.txt

export WORKER_TOKEN=dev-secret
export WORKER_DATABASE_URL="postgres://postgres:postgres@localhost:5432/pipeline?sslmode=disable"
export WORKER_ALLOWED_PATHS=/tmp/pipeline-data
export WORKER_ALLOWED_DSN_REFS=PG_SALES,PG_WAREHOUSE

PYTHONPATH="$PWD" python executor/worker/server.py
```

Then point the orchestrator at it:

```bash
export EXEC_MODE=worker
export WORKER_TOKEN=dev-secret
```

> **The worker fails closed.** It refuses to start without `WORKER_TOKEN`, and
> an unconfigured worker can reach no host, no path, and no database. Set
> `WORKER_ALLOWED_*` for the resources you actually intend to use. For local
> development only, `WORKER_INSECURE_DEV=1` permits an empty token and is also
> required to honour `WORKER_UNRESTRICTED_CONNECTORS=true`.

### 3. Create and run a pipeline

```bash
# create — returns 201 with status "pending_approval" because it has a load step
curl -sX POST localhost:8080/api/v1/pipelines \
  -H 'Content-Type: application/json' -d @pipeline.json

# approve — required before anything irreversible can run
curl -sX POST localhost:8080/api/v1/pipelines/$ID/approve \
  -H 'Content-Type: application/json' -d '{"approved_by":"you"}'

# run — returns 202 immediately; execution is picked up by the run loop
curl -sX POST localhost:8080/api/v1/pipelines/$ID/run

# watch
curl -s localhost:8080/api/v1/pipelines/$ID/status
curl -s "localhost:8080/api/v1/pipelines/$ID/events?limit=20"
```

---

## Pipeline definitions

A pipeline is a name plus a list of steps. Each step declares its dependencies,
and the DAG is validated before anything is stored.

```json
{
  "name": "sales-by-region",
  "description": "Aggregate sales and load into the warehouse",
  "steps": [
    {
      "key": "extract_sales",
      "name": "Extract Sales",
      "type": "extract",
      "config": {
        "connector": "postgres",
        "dsn_ref": "PG_SALES",
        "query": "SELECT region, amount FROM sales"
      },
      "depends_on": []
    },
    {
      "key": "aggregate_sales",
      "name": "Aggregate By Region",
      "type": "transform",
      "config": {
        "input_from": "extract_sales",
        "op": "aggregate_sum",
        "group_by": "region",
        "field": "amount"
      },
      "depends_on": ["extract_sales"]
    },
    {
      "key": "load_warehouse",
      "name": "Load Warehouse",
      "type": "load",
      "config": {
        "connector": "postgres",
        "dsn_ref": "PG_WAREHOUSE",
        "input_from": "aggregate_sales",
        "table": "warehouse.sales_by_region",
        "retry_count": 2
      },
      "depends_on": ["aggregate_sales"]
    }
  ]
}
```

### Step types

| Type | Reads | Writes | Notes |
| --- | --- | --- | --- |
| `extract` | a source connector | an artifact under its `key` | one `SELECT`/`WITH` statement; DML/DDL keywords rejected, and the query runs in a read-only transaction |
| `transform` | the artifact named by `input_from` | an artifact under its `key` | |
| `load` | the artifact named by `input_from` | a destination connector | triggers the approval requirement |

### Connectors

| Connector | Config | Idempotent load | Allowlist |
| --- | --- | --- | --- |
| `file` | `path`, `format` (`json` \| `csv`) | yes — whole-file overwrite | `WORKER_ALLOWED_PATHS` |
| `http` | `url` | no — a POST cannot be assumed safe to repeat | `WORKER_ALLOWED_HOSTS`, `WORKER_ALLOWED_SCHEMES` |
| `postgres` | `dsn_ref`, `query` / `table` | no — `INSERT` | `WORKER_ALLOWED_DSN_REFS` |

A connector whose load is not idempotent is **never retried**, independent of
what the failure looked like.

### Transform ops

`select` (`fields`) · `filter_eq` (`field`, `value`) · `aggregate_sum`
(`group_by`, `field`)

### Credentials

Never inline. `config.dsn` is **rejected**; use `dsn_ref`, which names an
environment variable the worker resolves and which must be allowlisted. Configs
are redacted on every read path.

### Retries

`config.retry_count` sets the per-step budget. It only applies to failures the
connector classified as **transient** — a timeout or dropped connection. A
permanent failure (malformed SQL, a value outside an allowlist) consumes no
budget, because it will fail identically forever. Backoff is exponential with
jitter.

---

## API

All `/api/v1` routes require `X-API-Key: <key>` or
`Authorization: Bearer <key>` when `API_KEY` is set. `/health` and CORS
preflight are always exempt.

### Pipelines

| Method | Path | Notes |
| --- | --- | --- |
| `GET` | `/api/v1/pipelines` | paginated: `limit`, `offset` |
| `POST` | `/api/v1/pipelines` | 400 returns **all** validation errors with `codes` |
| `GET` | `/api/v1/pipelines/{id}` | configs redacted |
| `DELETE` | `/api/v1/pipelines/{id}` | soft delete; run history survives |
| `POST` | `/api/v1/pipelines/{id}/approve` | body `{"approved_by": "..."}`; idempotent |

### Execution

| Method | Path | Notes |
| --- | --- | --- |
| `POST` | `/api/v1/pipelines/{id}/run` | 202 with `run_id`; **409** if approval is pending |
| `POST` | `/api/v1/pipelines/{id}/runs/{run_id}/cancel` | 202; observed on the holder's next heartbeat |
| `GET` | `/api/v1/pipelines/{id}/status` | optional `run_id`; includes provenance |

### Observability

| Method | Path | Query |
| --- | --- | --- |
| `GET` | `/api/v1/pipelines/{id}/runs` | `status`, `limit`, `offset` |
| `GET` | `/api/v1/pipelines/{id}/events` | `run_id`, `level`, `event_type`, `limit`, `offset` |
| `GET` | `/api/v1/pipelines/{id}/metrics` | simulated and real counted separately |
| `GET` | `/api/v1/pipelines/{id}/failure-breakdown` | `limit`, `offset` |
| `GET` | `/api/v1/metrics` | global, plus `degradations` by reason |

### Interpretation

| Method | Path | Notes |
| --- | --- | --- |
| `POST` | `/api/v1/interpret` | returns a **draft only** — it never creates or runs |

Returns `mode: "auto"` with a validated draft, or `mode: "manual_fallback"` with
a `fallback_reason` and a safe skeleton. Fallback reasons:
`interpreter_not_configured`, `nlp_unavailable`, `invalid_nlp_response`,
`low_confidence`, `invalid_pipeline`. Each one is also persisted and counted in
`/api/v1/metrics`.

### Health

`GET /health` — unauthenticated liveness probe.

---

## Configuration

### Orchestrator

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8080` | HTTP port |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/pipeline?sslmode=disable` | migrations run on startup |
| `API_KEY` | *(empty)* | when empty, `/api` is open and startup warns; **required** in staging/production |
| `ENVIRONMENT` | `development` | `staging`/`production` enforce `API_KEY` |
| `EXEC_MODE` | `local` | `local` = simulator, `worker` = real. Any other value **fails startup** |
| `WORKER_URL` | `http://localhost:8090` | worker base URL |
| `WORKER_TOKEN` | *(empty)* | sent as `X-Worker-Token` |
| `WORKER_TIMEOUT_MS` | `10000` | per-dispatch timeout |
| `RUNNER_OWNER` | `<hostname>-<pid>` | identity written to `claimed_by` |
| `RUN_LEASE_SECONDS` | `30` | claim lease; minimum 2 |
| `RUN_POLL_MS` | `1000` | poll interval when nothing is claimable |
| `MAX_CONCURRENT_RUNS` | `4` | runs driven per process |
| `SCHEDULER_MAX_CONCURRENCY` | `4` | steps of one ready set at once |
| `RETRY_BACKOFF_BASE_MS` | `200` | transient-retry backoff floor |
| `RETRY_BACKOFF_MAX_MS` | `10000` | backoff ceiling |
| `NLP_SERVICE_URL` | `http://localhost:8091` | not implemented yet |
| `NLP_TIMEOUT_MS` | `8000` | |
| `NLP_MIN_CONFIDENCE` | `0.70` | below this → manual fallback |
| `CATALOG_CONNECTORS` | `file,http,postgres` | connectors a generated draft may use |
| `CATALOG_DESTINATIONS` | *(empty)* | load destinations allowed — **empty denies all** |
| `CATALOG_TRANSFORM_OPS` | `select,filter_eq,aggregate_sum` | ops a generated draft may use |
| `CATALOG_SCHEMA_PATH` / `CATALOG_SCHEMA_JSON` | *(empty)* | JSON array of `{name, columns}` |

### Worker

| Variable | Default | Purpose |
| --- | --- | --- |
| `WORKER_HOST` | `127.0.0.1` | binds to loopback by default |
| `WORKER_PORT` | `8090` | |
| `WORKER_TOKEN` | *(empty)* | required header; the worker refuses to start without it unless `WORKER_INSECURE_DEV=1` |
| `WORKER_DATABASE_URL` | falls back to `DATABASE_URL` | artifacts + ledger; unset → in-memory fallback |
| `WORKER_ALLOWED_HOSTS` | *(empty)* | http connector hosts |
| `WORKER_ALLOWED_SCHEMES` | `https` | http connector schemes |
| `WORKER_ALLOWED_PATHS` | *(empty)* | file connector path prefixes |
| `WORKER_ALLOWED_DSN_REFS` | *(empty)* | env var names the postgres connector may resolve |
| `WORKER_UNRESTRICTED_CONNECTORS` | `false` | disables allowlists — requires `WORKER_INSECURE_DEV=1` |
| `WORKER_INSECURE_DEV` | `false` | explicit opt-in to run unauthenticated / unrestricted on a local machine |
| `WORKER_MAX_THREADS` | `8` | |

---

## Execution guarantees

**Runs survive restarts.** A run is owned by its database row, not by a
goroutine. A process claims it with `FOR UPDATE SKIP LOCKED`, holds it with a
heartbeat, and releases it on shutdown. If a process dies, the lease expires and
another picks the run up — no orphan sweep required, and several orchestrators
can run at once.

**Retrying a step never duplicates work.** The worker records completion against
`(run_id, step_key)` before returning. A retry after a dispatcher timeout replays
the stored result rather than re-running the load. The ledger keys on the logical
unit, not the attempt — keying on the attempt would never match, which is the
whole failure mode it exists to prevent.

**Nothing completes unrecorded.** Every status write shares a transaction with
the event describing it. A failed event write fails the transition. Every
terminal path emits a terminal event — completion, step failure, retry
exhaustion, panic, cancellation, lease loss.

**Independent steps run in parallel, and failure stops the set.** The ready set
is recomputed from completion state each pass and executed with bounded
concurrency. When one step fails its in-flight siblings are cancelled, marked
`cancelled`, and each records its own terminal event — a partially-applied ready
set is always visible in the log.

**A simulated run is never mistaken for a real one.** Runs record `exec_mode`,
`simulated`, and the worker they used, and every endpoint that reports outcomes
carries it through.

**A failure always says what failed.** Every worker reply — including a 400 for
a request that did not parse — carries `run_id` and `step_key`, pulled from the
raw body when the dataclass could not be built. The dispatcher treats a reply
that names a different execution as a permanent failure rather than a result.

---

## Development

```bash
# Go
cd orchestrator && go build ./... && go test ./...

# Python  (live Postgres tests skip unless PG_TEST_DSN is set)
PYTHONPATH="$PWD" python -m unittest discover -s executor/tests -p "test_*.py"
```

Both run in CI on every pull request (`.github/workflows/ci.yml`).

### Design rules

`CLAUDE.md` holds the binding system design rules — composition root, layering,
the authoritative event log, the idempotency contract, allowlist handling. Read
it before changing anything structural. Each rule names the decision it enforces
so the reason survives the next change.

### Schema changes

Append a numbered entry to `orchestrator/internal/database/migrations.go`. Never
edit an applied migration — checksums are verified at startup and a modified one
is a hard error.

### Codebase navigation

The repo ships a knowledge graph in `graphify-out/`:

```bash
graphify query "how does the run loop claim and hold a run"
graphify path "PipelineHandler.RunPipeline" "StepRunner.execute"
graphify update .    # after code changes
```

---

## Project layout

```
orchestrator/              Go — orchestration, validation, scheduling, state
  api/
    handlers/              HTTP handlers
    middleware/            CORS, recovery, logging, API key
    router/                composition root: builds the handler and the run loop
  cmd/server/              entrypoint
  internal/
    catalog/               what a generated pipeline may reference
    config/                the only place that reads the environment
    dag/                   graph, builders, structural validation
    database/              connection + versioned migrations
    dispatcher/            Dispatcher interface, local and HTTP implementations
    interpreter/           NLP service client
    models/                shared types: StepResult, StepConfig, wire models
    runner/                claim loop — owns run execution
    scheduler/             ready sets, concurrency, retries, terminal events
    store/                 persistence; transactional status+event writes
    validation/            structural and semantic draft validation

executor/                  Python — data work
  connectors/              file, http, postgres, and the registry that owns them
  transformations/         transform ops
  worker/                  HTTP server, runner, artifacts, ledger, wire contract
  tests/

docs/PROJECT_UNDERSTANDING.md   how the system works, in depth
AUDIT_FINDINGS.md               architecture audit against the design rules
SYSTEM_DESIGN_CHANGES.md        the structural changes and their rationale
CLAUDE.md                       binding design rules for contributors
```

---

## Status and limitations

Working: pipeline CRUD with full-set validation errors, the approval stage,
durable run ownership with cancellation and restart recovery, Postgres as the
work queue, bounded parallel execution with sibling cancellation, classified
retries with backoff, idempotent step execution, durable artifacts, execution
provenance end to end, an authoritative event log, versioned migrations,
two-stage draft validation, degradation telemetry, and a threaded
token-authenticated worker with per-connector allowlists.

Known gaps, in the order they matter:

1. **No NLP service.** `:8091` is unimplemented, so every `/interpret` call takes
   the `nlp_unavailable` fallback. It is visible in `/metrics` rather than
   silent, but "autonomous" remains aspirational.
2. **Confidence is self-reported.** The threshold gates on a number the model
   emits about itself, with no evidence it tracks correctness. No log-probs, no
   self-consistency, no eval set.
3. **No end-to-end test.** Both suites are unit-level. Nothing exercises
   interpret → create → approve → run against a live worker and database.
4. **No Docker, no compose.** CI exists (`.github/workflows/ci.yml` runs both
   suites) but nothing packages the three processes together.
5. **`EXEC_MODE` defaults to `local`,** which executes nothing. Honestly labelled
   now, but a default-config green run still only proves the scheduler walked a
   graph.
6. **No user or tenant model.** One shared static API key.
7. **No UI.** `ui/` is a placeholder.

See `docs/PROJECT_UNDERSTANDING.md` for the full picture and
`AUDIT_FINDINGS.md` for the audit these changes came from.

---

## License

Not yet specified.
