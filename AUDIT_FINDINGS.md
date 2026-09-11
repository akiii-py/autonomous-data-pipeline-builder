# Architecture Audit — Findings

Audit of the **Autonomous Data Pipeline Builder** against the design rules in `ARCHITECTURE_AUDIT.md` §3.

Scope: static read of every source file in `orchestrator/` and `executor/`, plus `docs/PROJECT_UNDERSTANDING.md`, `README.md`, `.github/`, `tests/`, `examples/`. Nothing was executed. Repository state: branch `main`, commit `997a8ed`. Date: 2026-09-11.

---

## 7.1 Summary

The Go orchestrator's structural design holds up — layering, composition root, DAG validation, and event pairing are genuinely as documented. Everything downstream of that is unsafe or untrue. The single most urgent finding is that the Python worker's `/execute` is completely unauthenticated and accepts a fully attacker-controlled `config`, which yields arbitrary SQL execution against any reachable database, arbitrary file read/write on the worker host, and outbound SSRF — one primitive, not three separate gaps. Close behind it: `EXEC_MODE` still defaults to a 10 ms no-op dispatcher with no `simulated` marker anywhere, so every green run and every metric in the system currently measures a simulator. There is no confirmation gate on `load` steps, no dedup on retry, no durability across restart, and no `recover()` in the run goroutine — a panicking step kills the whole orchestrator process. `PROJECT_UNDERSTANDING.md` is, unusually, accurate; `README.md` and several code comments are the things that mislead.

---

## 7.2 Rule compliance table

| Rule | Status | Evidence | Note |
|---|---|---|---|
| A1 | PASS | `orchestrator/api/router/router.go:20-38`, `internal/config/config.go:27-53` | Only `config.Load` reads `os.Getenv` in Go. Worker side differs — see F5. |
| A2 | PASS | `api/handlers/pipeline.go:3-14`, `api/handlers/monitoring.go:3-11` | No `database/sql` or `pq` import in `handlers/`; scheduler builds no HTTP responses. |
| A3 | PASS | `internal/scheduler/scheduler.go:14-20,73` | No type assertion, no `EXEC_MODE` read; `dispatcher.Dispatcher` used as interface only. |
| A4 | PASS | `api/router/router.go:31`, `internal/interpreter/client.go:63` | No new deployable appeared. The NLP service is referenced but does not exist in-repo. |
| B1 | PASS | `internal/store/pipeline_store.go:66-68`, `api/handlers/pipeline.go:54-57`, `internal/dag/dag_test.go:36,48` | Validates before insert; both cycle and unknown-dep return 400. 400 mapping is by `strings.Contains` on the error text — brittle, see S-2. |
| B2 | PASS | `api/handlers/interpret.go:145`, `internal/store/pipeline_store.go:66` | Both call sites validate. Not reported as redundancy. |
| B3 | PASS | `internal/scheduler/scheduler.go:50-51,121-140` | `readyStepKeys` recomputed every pass from the `completed` map. |
| B4 | FAIL | `internal/scheduler/scheduler.go:58-99` | Still sequential. Ready set computed correctly, then `for _, key := range ready` runs inline in one goroutine. No concurrency limit and no sibling-cancellation decision, because there is no concurrency. |
| B5 | PASS | `internal/scheduler/scheduler.go:83-89` | Retry exhaustion marks step failed, run failed, returns. No partial-success path. |
| C1 | FAIL | `api/handlers/pipeline.go:135` | `context.Background()` is the root of the entire run. Threading below it is correct — it reaches `http.NewRequestWithContext` at `internal/dispatcher/http.go:49` — but the root is never cancellable and carries no deadline. |
| C2 | FAIL | `internal/scheduler/scheduler.go:65-81` | No sleep of any kind between attempts. Tight loop. |
| C3 | FAIL | `internal/scheduler/scheduler.go:74-80`, `executor/worker/errors.py:2-4` | No classification before the retry decision. `WorkerExecutionError.transient` exists, is never set by any caller, and is never serialized — `server.py:35` sends only `str(exc)`. |
| C4 | FAIL | `executor/worker/runner.py:46-59`, `executor/connectors/postgres.py:39-52` | No dedup on `(run_id, step_key)`; `load` issues `INSERT` via `executemany`. See F-02. |
| C5 | FAIL | `api/handlers/pipeline.go:134-138`, `internal/database/database.go:30-45` | No startup sweep, no lease, no claim column. Bare `go func()`. |
| C6 | FAIL | `api/handlers/pipeline.go:134-138`, `api/middleware/middleware.go:32-42` | No `recover()` in the run goroutine. `Recovery` middleware covers only the request goroutine, which has already returned 202. |
| D1 | PARTIAL | `scheduler.go:38/41, 53/54, 66/71, 84/86, 91/96, 102/105`; `scheduler.go:115-118` | Every status write does have an adjacent emit. But `emitEvent` discards the error, so the pairing is not guaranteed at runtime. |
| D2 | FAIL | `scheduler.go:102-104`; `api/handlers/pipeline.go:134-138` | Normal completion ✓, step failure ✓, retry exhaustion ✓. Panic ✗ (process dies). Restart ✗ (no event, row stuck). Cancellation — no mechanism exists. And `scheduler.go:102-104` returns without any event if the final status write fails. |
| D3 | FAIL | `internal/config/config.go:45`, `internal/dispatcher/local.go:17-24`, `internal/database/database.go:79-86`, `internal/models/pipeline.go:44-51` | No `simulated` flag on the run row, in events, or in any metrics response. `EXEC_MODE` defaults to `local`. |
| D4 | FAIL | `api/handlers/interpret.go:108-126`, `internal/models/pipeline.go:105-116` | `fallback_reason` appears in the HTTP response body only. Not persisted, not counted, not an event. `/metrics` has no fallback fields. |
| D5 | PARTIAL | `api/handlers/monitoring.go:28-38,79-89,166-176`; `internal/store/pipeline_store.go:24-27` | `/runs`, `/events`, `/failure-breakdown` all paginated and filtered correctly. `GET /api/v1/pipelines` (`PipelineStore.List`) has no limit or offset — unbounded. |
| E1 | PASS | `api/handlers/interpret.go:39-99` | Returns a draft. No write to `pipelines`, no run trigger. |
| E2 | PASS | `api/handlers/interpret.go:53,64,68,75,87` | All five reasons return 200 with a fallback draft. No 5xx on any gate. |
| E3 | FAIL | `api/handlers/interpret.go:128-150` | Checks name, non-empty key, step type, DAG build. All four required semantic checks absent: no connector allowlist, no schema check (no schema is even sent — `interpreter.Request` at `client.go:22-27` has no schema field), no `input_from` cross-check, no destination allowlist. |
| E4 | FAIL | `api/handlers/interpret.go:39-99`, `api/handlers/pipeline.go:100-147` | No confirmation gate anywhere. A `load` step with an embedded DSN runs on the first `POST /run`. |
| E5 | NOT VERIFIABLE | — | No NLP service exists in the repo. No prompt config table, no config-ID column on any table, nothing to inspect. |
| E6 | FAIL | `internal/interpreter/client.go:59`, `api/handlers/interpret.go:74` | The only signal is `envelope.Confidence`, a float the remote service reports about itself. No log-probs, no self-consistency, no validation-derived score, no evaluation comparing any of them. The threshold is decorative. |
| E7 | NOT VERIFIABLE | — | No LLM call site exists in this repo to read. |
| E8 | FAIL | `internal/store/pipeline_store.go:99`, `internal/dispatcher/http.go:44`, `executor/connectors/postgres.py:16-19` | `config` is opaque `json.RawMessage` end to end. `cur.execute(query, params)` runs whatever string arrives. `params` are parameterized; the statement itself is not restricted to SELECT and no identifier is validated. |
| F1 | FAIL | `executor/worker/server.py:20-37` | No auth of any kind on `/execute`. |
| F2 | FAIL | `executor/connectors/http_api.py:7-29`, `executor/connectors/postgres.py:55-61` | No host or scheme allowlist; `urllib.request.urlopen` on a caller-supplied URL also accepts `file://` and `ftp://`. No DSN allowlist — `config["dsn"]` is taken verbatim. |
| F3 | FAIL | `internal/store/pipeline_store.go:99`, `api/handlers/pipeline.go:83`, `executor/connectors/postgres.py:57` | DSN is stored plaintext in `pipeline_steps.config` JSONB and echoed back by `GET /api/v1/pipelines/{id}`. |
| F4 | PARTIAL | `api/router/router.go:65-68`, `api/middleware/middleware.go:64,85-91` | Ordering is correct (CORS → Recovery → Logger → APIKey) and exemptions are exactly OPTIONS + `GET /health`. But `if key == ""` disables auth for every route, and `API_KEY` defaults to `""` (`config.go:43`). |
| F5 | PARTIAL | `internal/config/config.go:40-47`, `executor/connectors/postgres.py:60` | Go literals are all inside `config`, as the rule allows (including the `postgres:postgres` password default). `postgres.py:60` hardcodes `"postgresql:///postgres"` in connector code, outside any config layer. |
| G1 | FAIL | `executor/worker/artifacts.py:5-15` | Module-level dict. Only `put_artifact` and `get_artifact` exist — there is no delete, no cleanup, no TTL. |
| G2 | PASS | `internal/scheduler/scheduler.go:38,66,84,91,102,115` | Scheduler writes only to `pipeline_runs`, `step_runs`, `pipeline_run_events`. |
| G3 | PARTIAL | `internal/database/database.go:106-107`, `internal/store/pipeline_store.go:115` | No UPDATE or DELETE against the table in code. But `ON DELETE CASCADE` from `pipelines` means `DELETE /api/v1/pipelines/{id}` destroys that pipeline's entire audit trail. |
| G4 | FAIL | `internal/database/database.go:30-45` | A Go slice of three SQL string constants re-executed on every startup. No `schema_migrations` table, no version, no checksum, no down path. |
| G5 | PASS | `internal/store/pipeline_store.go:171-248` | Both status updaters are single-statement UPDATEs using `COALESCE`. No read-modify-write. Holds if B4 is fixed. |
| H1 | FAIL | `tests/.gitkeep`, `examples/phase4_postgres_e2e.py`, `orchestrator/api/handlers/handlers_test.go:10` | `tests/` is empty. The example script exercises connectors directly and never touches the orchestrator. There is no interpret → create → run test at any `EXEC_MODE`. |
| H2 | FAIL | `.github/workflows/` | The directory exists and contains zero files. |
| H3 | FAIL | repo root | No Dockerfile, no compose file anywhere in the tree. |
| H4 | FAIL | `internal/config/config.go:45`, `api/handlers/interpret.go:64` | Two silent-failure processes — see F-03 and F-12. |

**Tally:** 14 PASS · 20 FAIL · 6 PARTIAL · 2 NOT VERIFIABLE

---

## 7.3 Findings

### F-01 · HIGH · Unauthenticated `/execute` is an arbitrary-SQL, arbitrary-file and SSRF primitive

- **Rule:** F1, F2, E8
- **Evidence:** `executor/worker/server.py:20-37`, `executor/worker/runner.py:15-72`, `executor/connectors/postgres.py:16-19,55-61`, `executor/connectors/http_api.py:11-12,26-27`, `executor/connectors/file_io.py:13-23,32-47`
- **What:** `POST /execute` has no authentication and no source restriction. The caller supplies `step.type` and the entire `step.config`. Concretely, an attacker who can reach `:8090` controls:
  - `config.dsn` plus `config.query`, so `cur.execute()` runs any statement — not just SELECT — against any database the worker can route to;
  - `config.path`, so `file_io.extract` reads any file the worker user can read and `file_io.load` writes any path, creating parent directories via `mkdir(parents=True)`;
  - `config.url` on `urllib.urlopen`, giving GET and POST SSRF to internal addresses, with `file://` and `ftp://` as bonus schemes;
  - `run_id` and `input_from`, so artifacts belonging to other runs can be read back out of the worker's memory.
- **Impact:** Full read/write of any reachable database, file read/write on the worker host, and an internal-network request proxy — from one unauthenticated HTTP endpoint. This is a usable primitive, not a missing-control note.
- **Fix:** Require a shared secret header on `/execute` and bind the listener to loopback or an internal interface; then add a scheme/host allowlist to the http connector and a DSN allowlist (or DSN-alias indirection) to the postgres connector.
- **Effort:** M

### F-02 · HIGH · Step retry can double-execute a load

- **Rule:** C4
- **Evidence:** `internal/dispatcher/http.go:33-35,55-58`, `internal/scheduler/scheduler.go:73-81`, `executor/worker/runner.py:46-59`, `executor/connectors/postgres.py:39-52`
- **What:** The HTTP client has a fixed timeout (`WORKER_TIMEOUT_MS`, default 10s). A timeout returns an error indistinguishable from a failure, and the scheduler retries it. The worker deduplicates nothing on `(run_id, step_key)`, and `postgres.load` issues a plain `INSERT` via `executemany`. The symmetric problem also exists with `retry_count` unset: a slow-but-successful load produces a run recorded as failed while the data has, in fact, landed.
- **Impact:** Duplicate production rows on retry; falsely-failed runs without one.
- **Fix:** Have the worker record `(run_id, step_key)` completion durably and return the prior result on replay, or make load connectors upsert on a declared key.
- **Effort:** M

### F-03 · HIGH · Every run and every metric currently measures a simulator

- **Rule:** D3, H4
- **Evidence:** `internal/config/config.go:45`, `api/router/router.go:25-29`, `internal/dispatcher/local.go:17-24`, `internal/database/database.go:79-86`, `internal/models/pipeline.go:44-51`, `internal/store/pipeline_store.go:500-539`
- **What:** `EXEC_MODE` defaults to `"local"`; `LocalDispatcher.ExecuteStep` sleeps 10 ms and returns `nil`. Nothing on the run row, in any event, or in any metrics response distinguishes such a run from a real one. Separately, `router.go:25` uses `EqualFold` against `"worker"` with a silent `else` — so `EXEC_MODE=wroker`, or any typo, downgrades to the no-op simulator with no log line and no startup warning.
- **Impact:** A green run proves only that the scheduler walked a graph. Success rate, failure rate and average duration in `/metrics` are all simulator numbers, and the operator has no way to tell from the API which is which.
- **Fix:** Add a `simulated BOOLEAN` column on `pipeline_runs`, set it from the resolved dispatcher, propagate it into events and every metrics response, and fail startup loudly on an unrecognized `EXEC_MODE` value.
- **Effort:** M

### F-04 · HIGH · A panicking step kills the whole orchestrator process

- **Rule:** C6, D2
- **Evidence:** `api/handlers/pipeline.go:134-138`, `api/middleware/middleware.go:32-42`
- **What:** `ExecuteRun` runs in a detached `go func()` with no `recover()`. The `Recovery` middleware wraps only the request goroutine, which returned 202 before the run started. In Go an unrecovered panic in any goroutine terminates the process.
- **Impact:** One malformed step config does not fail one run — it takes down the orchestrator, every concurrently running run with it, and writes no terminal event for any of them.
- **Fix:** `defer recover()` inside the goroutine; on panic write a terminal step event and mark the run failed before returning.
- **Effort:** S

### F-05 · HIGH · An orchestrator restart leaves runs stuck in `running` forever

- **Rule:** C5, D2, C1
- **Evidence:** `api/handlers/pipeline.go:134-138`, `internal/database/database.go:79-86`, `cmd/server/main.go:56-67`
- **What:** Runs are in-process only. There is no queue, no lease, no claim column and no startup sweep. Graceful shutdown drains HTTP connections (`main.go:65`) but does not wait for, cancel, or record any in-flight run. No startup path reconciles rows left in `running`.
- **Impact:** Every kill, deploy or crash orphans in-flight runs permanently. They are counted as `RunsRunning` by `/metrics` forever, and excluded from average duration because `finished_at` stays NULL, so the number silently drifts.
- **Fix:** Add a claim/lease column with a heartbeat, and a startup sweep that fails runs whose lease has expired — emitting a terminal event for each.
- **Effort:** L

### F-06 · HIGH · Database credentials are stored in plaintext and served over the API

- **Rule:** F3
- **Evidence:** `internal/store/pipeline_store.go:99`, `api/handlers/pipeline.go:83`, `executor/connectors/postgres.py:57`
- **What:** The step config is persisted verbatim as JSONB, including `config.dsn`. `GET /api/v1/pipelines/{id}` encodes the full step list, `Config` included, so any caller holding the single shared API key can read every stored connection string back out in cleartext.
- **Impact:** Credentials at rest in plaintext and retrievable over HTTP. A leaked or shared API key becomes every database credential the system has ever been given.
- **Fix:** Store an env-var reference or secret-store handle instead of a DSN, resolve it worker-side, and redact credential-shaped config keys on read.
- **Effort:** M

### F-07 · HIGH · No human confirmation gate on load steps or DSN-bearing configs

- **Rule:** E4
- **Evidence:** `api/handlers/interpret.go:39-99`, `api/handlers/pipeline.go:100-147`, `internal/scheduler/scheduler.go:58-99`
- **What:** Nothing in the interpret → create → run path distinguishes a `load` step from any other. The confidence threshold is the only gate, and it sits on the interpret call — which does not execute anything. `POST /run` executes whatever was stored, irreversible steps included.
- **Impact:** The only irreversible operation in the system is gated by a number the model reported about itself, checked at the wrong stage.
- **Fix:** Require an explicit confirm token on `POST /run` when the pipeline contains a load step or any config carrying a DSN, independent of confidence.
- **Effort:** M

### F-08 · HIGH · A fully successful run can complete with nothing recording it

- **Rule:** D2, D1
- **Evidence:** `internal/scheduler/scheduler.go:102-104`, `internal/scheduler/scheduler.go:110-119`
- **What:** If the final `UpdatePipelineRunStatus(completed)` errors, `ExecuteRun` returns immediately — before the `run_completed` emit on line 105 — without marking the run failed and without emitting any terminal event. The run stays `running` although all its steps show `completed`. Compounding this, `emitEvent` swallows every error it gets (lines 115-118), so any individual event write can fail silently on any path.
- **Impact:** This is exactly the failure mode Section D2 warns about — the happy path works, and a second terminal path ends with no notification at all. All work done, nothing reports it.
- **Fix:** On that error, mark the run failed and emit a terminal event before returning; log `emitEvent` failures rather than discarding them.
- **Effort:** S

### F-09 · HIGH · Artifacts live in a process-local dict that is never freed

- **Rule:** G1
- **Evidence:** `executor/worker/artifacts.py:5-15`, `executor/worker/runner.py:29,43,58`
- **What:** A module-level dict behind a lock, with `put` and `get` and no delete. The three consequences the rule names all hold: a worker restart loses in-flight run data (a transform then fails with "missing upstream artifact"); two worker replicas cannot share artifacts, so any load balancing in front of `:8090` breaks runs non-deterministically; and completed runs are never evicted, so the dict grows for the life of the process.
- **Impact:** The worker cannot be restarted, cannot be scaled horizontally, and leaks the full payload of every run it has ever executed.
- **Fix:** Key artifacts by `(run_id, step_key)` in durable shared storage — filesystem, object store, or a table — with deletion on run completion.
- **Effort:** M

### F-10 · HIGH · An empty `API_KEY` silently disables authentication on every route

- **Rule:** F4
- **Evidence:** `api/middleware/middleware.go:64`, `internal/config/config.go:43`
- **What:** `if key == "" || isAuthExempt(r)` short-circuits to `next.ServeHTTP` for all traffic, and `API_KEY` defaults to `""`. The ordering and the exemption set are otherwise exactly as the rule requires.
- **Impact:** The default configuration is a fully open API, and it looks identical at runtime to a correctly configured one — no startup warning, no log line, no `/health` field. Documented in `PROJECT_UNDERSTANDING.md` §5, which lowers the surprise but not the exposure.
- **Fix:** Fail startup when `ENVIRONMENT` is not development and `API_KEY` is empty; log a prominent warning when it is empty at all.
- **Effort:** S

### F-11 · MEDIUM · Retries are a tight loop with no backoff and no error classification

- **Rule:** C2, C3
- **Evidence:** `internal/scheduler/scheduler.go:65-81`, `executor/worker/errors.py:2-4`, `executor/worker/server.py:34-37`
- **What:** The attempt loop has no sleep at all — three attempts complete in milliseconds. Nothing classifies the error first, so malformed SQL is retried exactly as eagerly as a connection reset. The worker already models this: `WorkerExecutionError` carries a `transient` flag, but no call site sets it and `server.py` serializes only `str(exc)`, so the flag never crosses the wire and is dead code.
- **Impact:** Retries cannot rescue a dependency that needs seconds to recover, and permanent failures burn their full retry budget, multiplying load and making failure reports noisier than the underlying fault.
- **Fix:** Classify worker responses as retryable or not — plumb the existing `transient` flag through the response body — and add exponential backoff with jitter around the retryable case only.
- **Effort:** M

### F-12 · MEDIUM · Interpret degrades silently and the reason is never counted

- **Rule:** D4, H4
- **Evidence:** `api/handlers/interpret.go:108-126`, `internal/models/pipeline.go:105-116`, `internal/config/config.go:47`
- **What:** All five fallback reasons return HTTP 200 with `fallback_reason` in the body only. Nothing is persisted, counted, or emitted as an event, and `/metrics` has no fallback fields. Because `NLP_SERVICE_URL` defaults to a service that does not exist, every interpret call today takes `nlp_unavailable` — and that is indistinguishable, from outside, from a healthy service returning a 0.68.
- **Impact:** The operational signal the rule exists to preserve is absent. A dead NLP service produces 200s indefinitely with no metric moving.
- **Fix:** Emit an event (or increment a labelled counter) on each fallback carrying the reason, and surface the breakdown in `/metrics`.
- **Effort:** S

### F-13 · MEDIUM · Draft validation is structural only, and the gap is not acknowledged

- **Rule:** E3, E8
- **Evidence:** `api/handlers/interpret.go:128-150`, `internal/interpreter/client.go:22-27`, `internal/store/pipeline_store.go:99`, `executor/connectors/postgres.py:16-19`
- **What:** `validateDraft` checks name, non-empty step keys, step type membership, and DAG buildability. All four required semantic checks are missing. Two details make this worse than a to-do: no schema is ever sent to the model (`interpreter.Request` has `query` and two free-text hints, nothing else), so the table/column check is not merely unimplemented but currently impossible; and `config.input_from` — which the worker relies on at `runner.py:33,47` — is never cross-checked against `depends_on` or against any real step key, so a draft can pass validation and then fail at run time with "missing upstream artifact".
- **Impact:** A draft can be perfectly acyclic, pass every gate, and write to the wrong table. The system's core bet — that the DAG engine disposes of what the LLM proposes — holds only for graph shape, not for meaning.
- **Fix:** Add connector and destination allowlists, validate `input_from` against declared step keys and `depends_on`, send the target schema in the interpret request, and constrain extract queries to SELECT.
- **Effort:** M

### F-14 · MEDIUM · Migrations are unversioned SQL constants re-run on every boot

- **Rule:** G4
- **Evidence:** `internal/database/database.go:30-45,47-120`
- **What:** Three Go string constants executed in slice order at every startup. No `schema_migrations` table, no applied-at record, no checksum, no down migration. Idempotence rests entirely on `IF NOT EXISTS` and `ADD COLUMN IF NOT EXISTS` holding in every future edit.
- **Impact:** There is no way to know what schema a given database is actually at, no safe way to write a migration that is not idempotent (a backfill, a type change), and no way to detect drift between environments.
- **Fix:** Adopt a versioned migration tool, or add a `schema_migrations` table and gate each statement on it.
- **Effort:** M

### F-15 · MEDIUM · No end-to-end test, no CI, no one-command local environment

- **Rule:** H1, H2, H3
- **Evidence:** `tests/.gitkeep`, `.github/workflows/` (empty), repo root, `orchestrator/api/handlers/handlers_test.go:10`, `examples/phase4_postgres_e2e.py`
- **What:** The Go suite is real but narrow — 26 unit tests, none of which exercise `PipelineHandler.CreatePipeline` or `RunPipeline` against a store. The Python suite is two tests, one of which needs a live Postgres and will error rather than skip without one (`test_postgres_connector.py:13`). The example script drives connectors directly and never touches the orchestrator. `.github/workflows` exists and is empty. There is no Dockerfile and no compose file.
- **Impact:** The rule's stated failure mode applies directly: there is no evidence anywhere that interpret → create → run has been executed end to end with `EXEC_MODE=worker`, and no automation that would notice if it broke.
- **Fix:** Add one e2e test against a compose-provided Postgres and worker, then wire `go test ./...` and unittest discovery into a workflow.
- **Effort:** L

### F-16 · MEDIUM · Independent steps still execute sequentially

- **Rule:** B4
- **Evidence:** `internal/scheduler/scheduler.go:50-99`
- **What:** Confirmed still true. `readyStepKeys` returns the full parallel-eligible set and the loop at line 58 executes it inline, one step at a time, in the calling goroutine. No bounded-concurrency limit exists because no concurrency exists, and there is consequently no decision recorded about sibling cancellation on failure.
- **Impact:** The DAG's parallelism information is computed and then discarded. Wall time is the sum of all steps regardless of graph shape.
- **Fix:** Bound a worker-group over the ready set with an explicit limit, and decide and document sibling-cancellation semantics before enabling it. Note that G5's single-statement updates already make the store side safe for this, but see S-8 — the worker is single-threaded and would serialize anyway.
- **Effort:** M

### F-17 · LOW · The event log is destructible through the pipeline delete cascade

- **Rule:** G3
- **Evidence:** `internal/database/database.go:106-107`, `internal/store/pipeline_store.go:114-124`, `api/router/router.go:47`
- **What:** No UPDATE or DELETE targets `pipeline_run_events` in application code, so the append-only property holds at that level. But the table's `pipeline_id` carries `ON DELETE CASCADE`, and `DELETE /api/v1/pipelines/{id}` is an exposed route — so deleting a pipeline erases its entire audit trail.
- **Impact:** The audit record of what ran is deletable by the same key that can run things, which is the wrong direction for an audit log.
- **Fix:** Retain events on pipeline delete (drop the cascade, soft-delete the pipeline), or copy them to an archive before the cascade fires.
- **Effort:** S

### F-18 · LOW · `GET /api/v1/pipelines` is unbounded

- **Rule:** D5
- **Evidence:** `internal/store/pipeline_store.go:24-27`, `api/handlers/pipeline.go:25-37`
- **What:** Every other list endpoint parses `limit` and `offset` through the shared helpers in `monitoring.go` and caps at `maxListLimit`. `ListPipelines` takes no parameters and issues a bare `SELECT ... ORDER BY created_at DESC`.
- **Impact:** Response size and query cost grow without bound as pipelines accumulate. Contained today only because the table is small.
- **Fix:** Apply the existing `parseLimit`/`parseOffset` helpers to this route.
- **Effort:** S

---

## 7.4 Documentation discrepancies

**`docs/PROJECT_UNDERSTANDING.md` — accurate.** Every claim checked against source held, including the ones it would have been convenient to overstate. Specifically verified correct: the middleware order in its §2 diagram; §3.1's five-row fallback table matching `interpret.go:53,64,68,75,87` exactly; §3.2's description of the detached goroutine and 202; §4's eight-item "scaffolding" list, all eight of which are still true; and §5's note that an empty `API_KEY` leaves `/api` unauthenticated. Two minor overclaims:

1. §4 "Real and working — ... an event log behind every transition." Structurally true, but `emitEvent` discards write errors (`scheduler.go:115-118`) and `scheduler.go:102-104` has a terminal path with no event at all. The code is the correction; see F-08.
2. §6 cites the graph as 306 nodes / 583 edges. Node count matches what `graphify query` reports today; the graph itself is dated 2026-09-05 and predates nothing material, but it is orientation, not evidence.

**`README.md` — misleads in three places. The code is correct in each case.**

3. **The only example payload in the README cannot run.** `README.md:201-229` shows `"config": {"source": "postgres"}`, `{"op": "aggregate"}`, `{"target": "s3"}`. The worker reads `config.connector`, not `config.source` (`runner.py:65`); `transform` requires `config.input_from`, which is absent, and fails at `runner.py:33-35`; `"aggregate"` is not a supported op — `ops.py:4-27` accepts only `select`, `filter_eq`, `aggregate_sum`; and there is no `s3` connector anywhere. Under `EXEC_MODE=worker` every one of these three steps fails. Under the default `EXEC_MODE=local` the whole pipeline goes green. A new user following the README gets a passing run from a payload that is wrong in four ways — the interaction of this with F-03 is what makes it worth listing.
4. **`README.md:114` describes `EXEC_MODE` as "default: local, options: local|worker" with no indication that `local` executes nothing,** and `README.md:32-36` lists Phase 4's "Local dispatcher and HTTP worker dispatcher mode" as delivered capability. `PROJECT_UNDERSTANDING.md` §4.1 states the truth plainly; the README does not.
5. **`README.md:65-89` lists `docs/`, `tests/` and `ui/` in the repository layout as though populated.** `tests/` and `ui/` contain only `.gitkeep`.

**Stale or wrong code comments** (the anti-pattern list calls these out directly):

6. `api/router/router.go:63` — "Wrap with middleware chain: logging → recovery → CORS". The actual chain is CORS → Recovery → Logger → APIKey, and APIKey is omitted entirely. Both `ARCHITECTURE_AUDIT.md` §2 and `PROJECT_UNDERSTANDING.md` §2 have it right; this comment is the only place in the repo that gets the order wrong.
7. `internal/dispatcher/dispatcher.go:10` — "In Phase 3 this is a local stub; Phase 4 will route to Python workers." Phase 4 shipped; `HTTPDispatcher` exists in the same package.
8. `api/handlers/pipeline.go:24` — "returns all pipelines for the authenticated user". There is no user model, no ownership column, and no per-user filtering. `PROJECT_UNDERSTANDING.md` §4.7 already flags this comment as misleading; it is still there.

**`ARCHITECTURE_AUDIT.md` §5 known-gap table — all eight gaps verified still true, none fixed.** Gap 8's ".github/ empty" is precise: the `workflows/` directory exists and contains zero files.

---

## 7.5 Structural observations

- **S-1 — `Dispatcher` throws away information the worker already produces.** `ExecuteStep` returns a bare `error` (`dispatcher.go:12`), and `executeResponse` (`http.go:25-28`) declares only `Status` and `Error` — while `server.py:33` sends `{"status": "ok", "result": {...}}` with row counts and load results inside. The missing concept is a `StepResult` value type carrying rows-processed, retryability, and a worker-side idempotency token. Its absence is the direct reason C3, C4 and D3 are each awkward to fix in isolation: three separate findings that share one root.
- **S-2 — `dag.ValidationError` is defined and never used at a boundary.** `validation.go:6-17` declares a typed error with a `Code` field, and the only consumer that needs it — `pipeline.go:54` deciding between 400 and 500 — instead does `strings.Contains(err.Error(), "invalid pipeline dag")`. The handler is coupled to the store's error-wrapping text; `errors.As` plus the existing type is what this was built for.
- **S-3 — `Validate` collects all errors; both builders discard all but the first.** `validation.go:19-41` accumulates into `errs`, then `builder.go:38` and `builder.go:73` both return `errs[0]`. The caller who needs the full list is a user fixing a rejected draft, who now has to resubmit once per problem. This is on the anti-pattern list verbatim.
- **S-4 — Two sources of truth for the confidence default.** `config.go:49` defaults `NLP_MIN_CONFIDENCE` to 0.70, and `interpret.go:18` defines `defaultNLPMinConfidence = 0.70` as a separate fallback used when the injected value is out of range. They agree today by coincidence of editing.
- **S-5 — `interpretEnvelope` is speculative flexibility for a service that does not exist.** `client.go:53-61` accepts three different response shapes (`pipeline`, `pipeline_draft`, or flat `name`/`description`/`steps`). Nothing has ever sent any of them. Tolerant parsing chosen before the contract exists tends to make the contract permanently ambiguous. Note also that `client.go:107-109` is unreachable: the branch at line 99 always assigns a non-nil pointer, so `pipeline` cannot be nil by the time it is checked again.
- **S-6 — `executor/worker/models.py` is dead.** `StepPayload`, `ExecuteRequest` and `ExecuteResponse` are defined as dataclasses and imported by nothing; `server.py` and `runner.py` pass raw dicts and use `.get()` throughout. The typed contract exists and is unused, which is why `runner.execute_step` re-validates `step.key` by hand at line 20.
- **S-7 — `connectors/base.py` is an abstraction nothing implements.** The `Connector` class is never subclassed; all three connectors are module-level function pairs assembled into a dict by `runner._connector` (`runner.py:64-72`). That function is also where a connector allowlist would naturally live (see F-13) — a registry is the concept both the base class and the allowlist are reaching for.
- **S-8 — The worker is single-threaded, which caps B4 before it starts.** `server.py:58` uses `HTTPServer`, not `ThreadingHTTPServer`, so `/execute` handles exactly one step at a time process-wide. Parallelising the scheduler (F-16) would produce concurrent dispatches that the worker then serialises, converting a design win into queueing latency plus `WORKER_TIMEOUT_MS` risk. These two changes have to land together.
- **S-9 — The scheduler's `queued` map is vestigial.** `scheduler.go:49,60,124` maintain it, but because execution is synchronous within the ready loop, a step is never observably queued-but-not-completed. It is correct structure anticipating F-16 rather than dead code, and worth keeping — noted so it is not mistaken for the latter.
- **S-10 — `RedisURL` and `GRPCPort` are loaded, validated, and never read.** `config.go:13-14,33-36,41-42`. `getEnvInt("GRPC_PORT", …)` is the only config read whose parse error aborts startup (`config.go:33-36`), for a value nothing consumes. `README.md:110-111` and `PROJECT_UNDERSTANDING.md` §5 both correctly describe them as reserved.
- **S-11 — `http_api.load` returns a status code it never checks.** `http_api.py:27-29`. `urlopen` raises on 4xx/5xx so the common cases are covered by exception, but a 3xx or an unusual 2xx is reported as success with the code buried in the result dict that S-1 discards anyway.

---

## 7.6 What I could not verify

- **E5 and E7 — prompt configuration and structured-output mode.** There is no NLP service in this repository: no provider SDK in `go.mod` or `executor/requirements.txt`, no prompt string anywhere, no `prompt_config` table in the migrations, and no config-ID column on `pipelines` or `pipeline_steps` to attribute a draft to. `NLP_SERVICE_URL` points at `localhost:8091` and nothing binds it. I looked for all of these and found none; I cannot report on a call site that does not exist. If the service lives in another repository, these two rules need to be audited there.
- **Nothing was executed.** No Postgres instance was started, `go test ./...` was not run, the worker was not launched, and no HTTP request was made to any endpoint. Every finding above is from static reading. In particular I did not empirically confirm F-04's process-death claim or F-02's duplicate-row outcome — both are read off the code paths and Go/psycopg semantics, not observed.
- **Test suite status unknown.** I enumerated 26 Go test functions and 2 Python ones by name but did not run either suite, so I cannot say whether they currently pass.
- **Dependency vulnerabilities not audited.** `go.sum`, `lib/pq v1.11.2`, `google/uuid v1.6.0` and `executor/requirements.txt` were not checked against any advisory database.
- **No deployment configuration exists to inspect.** There is no Dockerfile, compose file, CI workflow, k8s manifest, or `.env` example in the tree, so I cannot say what `EXEC_MODE`, `API_KEY`, or `WORKER_URL` are actually set to in any real environment. F-03 and F-10 describe the defaults, which is all the repository reveals.
- **Live schema not verified.** Findings about table shape (F-03's missing `simulated` column, F-05's missing lease column, F-17's cascade) are read from `database.go`'s migration constants. Because those constants are unversioned and re-run on every boot (F-14), a long-lived database may have drifted from them, and there is no way to detect that from the repository.
- **`graphify-out/` was used for orientation only.** The graph is dated 2026-09-05 while the source files carry March timestamps. Every citation in this report was re-read from source rather than taken from the graph.
- **Out of scope, not examined:** `.claude/`, `.vscode/`, `.venv/`, and `graphify-out/` contents beyond the query used for orientation.
