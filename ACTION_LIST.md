# Autonomous Data Pipeline Builder — Things to Address

Consolidated from the project doc, the Raptor tips report, and our discussion.
28 items across 5 tiers, plus scope decisions to make before starting.

Legend: **[S]** small (hours) · **[M]** medium (1–2 days) · **[L]** large (a week+)

---

## Tier 1 — Make the system honest

These matter most because until they're fixed, a green run proves nothing and
your own test signals can't be trusted.

### 1. Fix the fake-green default **[S]**
`EXEC_MODE` defaults to `local`, and `LocalDispatcher.ExecuteStep` sleeps 10ms
and returns nil. Runs go green without touching data.

*My recommendation, differing from the doc:* the doc offers two options —
reimplement step logic in Go, or mark local runs as simulated. Take the second.
Reimplementing connectors in Go gives you two executors that will silently drift
apart. Instead add a `simulated` boolean on the run row and propagate it into
events and every metrics endpoint, so it's impossible to look at a green run
without seeing that nothing ran.

### 2. Move artifacts out of process memory **[M]**
`artifacts.py` is a module-level dict. Consequences: worker restart loses
in-flight data, two workers can't share artifacts, nothing is ever freed.

Postgres table or filesystem keyed by `(run_id, step_key)`, with cleanup on run
completion. Blocks items 3, 16, and 18 — do it before them.

### 3. Make runs durable **[S] student version / [L] full version**
Execution is a bare `go func()` with `context.Background()`. Kill the
orchestrator mid-run and the row stays `running` forever with nobody driving it.

- **Student version (do this):** on startup, find runs in `running` that nobody
  is driving and mark them `failed` with reason `orchestrator_restart`. Honest,
  cheap, easy to defend.
- **Full version (probably skip):** claim/lease with heartbeat and expiry so a
  second orchestrator can reclaim. Real distributed systems work.

### 4. Thread a real `context.Context` **[S–M]**
Highest leverage single change in the repo. Currently `context.Background()`
everywhere, which is why there's no cancellation, no step timeout, and no
graceful shutdown. Thread a real context from run start → scheduler →
dispatcher → worker HTTP call. Fixes three listed gaps at once.

### 5. Idempotency / dedup on step execution **[M]**
Not in the original doc — comes from the Raptor report's `tips_job_done:{job_id}`
dedup key.

A dispatcher timeout does **not** mean the worker didn't do the work. It may
have completed the extract, written the artifact, and just responded slowly.
Retrying re-runs it. For a `load` step that's duplicate rows in production.

Fix: idempotency key on `(run_id, step_key)` checked by the worker before
executing, or make load connectors upsert rather than insert. Note this is a
contract the orchestrator must document and connector authors must honor.

### 6. Terminal event on every code path **[S]**
Directly from Raptor's "Plan D" incident: they removed a status channel and the
async completion path was left with zero notification, so successful runs stored
data that the UI never showed.

Audit whether `pipeline_run_events` gets a terminal event on *all* paths:
normal completion, step failure, panic in the goroutine, step timeout,
cancellation, orchestrator restart. The last one currently does not.

---

## Tier 2 — The NLP service (the missing `:8091`)

### 7. Build the service **[S]**
One process, **synchronous**, no queue, no callback. `NLP_TIMEOUT_MS` is already
8000 and an LLM call is 2–10s — don't buy async machinery for a request that
completes in four seconds. Returns `{pipeline, confidence, warnings}` matching
`internal/interpreter/client.go`.

Borrow Raptor's `task_type` dispatch as a **handler map inside this one
service**, not as a separate routing tier, so a future "explain this pipeline"
task is a dict entry rather than a new deployable.

### 8. Model + prompt config in DB rows **[S]**
From Raptor's `lg_service_model_map`. Store `{provider, model, sys_prompt,
temperature, max_tokens}` as a row with an ID.

*Why this matters more for you than it did for them:* record which config
produced which draft, and prompt-variant comparison against your eval set
becomes a measurable experiment instead of "I tuned it and it felt better."

Their gotcha to avoid: they cache this table in Redis and any edit silently
serves stale config until a restart. If you cache, add a version column.

### 9. Ground the prompt properly **[S]**
Single biggest lever on accuracy. The prompt must carry:
- available connectors (`file`, `http`, `postgres`)
- available transform ops (`select`, `filter_eq`, `aggregate_sum`)
- schema of any targetable tables
- a few worked query → pipeline JSON examples

Use JSON-schema-constrained output (provider schema/tool mode), not "please
reply in JSON."

Apply Raptor's prompt lessons: mechanical instructions beat percentage targets;
replace "don't hallucinate" with an executable check ("every table and column
you reference must appear in the provided schema").

### 10. Confidence calibration experiment **[M–L] — the differentiator**
The entire gate chain rests on `confidence < 0.70 → fallback`, but self-reported
LLM confidence tracks fluency, not correctness. As designed the threshold is
close to decorative.

Implement and compare four methods:
1. Self-reported (baseline)
2. Token log-probabilities
3. Self-consistency — sample 5×, measure agreement (costs 5×, often strongest)
4. Validation-derived — score from how many structural + semantic checks pass

My guess: #4 wins, which is itself an interesting finding.

### 11. Build the eval set **[M]**
50–100 pairs of (English query, correct pipeline JSON). Include easy
single-source, multi-step, and deliberately ambiguous/unanswerable cases.

Measure **structural match** (step types, dependency edges) separately from
**config correctness** — they'll differ a lot, and reporting one number hides
the real result. For item 10, report precision/recall of the gating decision:
how often does the gate correctly block a bad pipeline vs. wrongly block a good
one.

### 12. Semantic validation, not just structural **[M]**
"The LLM proposes, the DAG engine disposes" — but the DAG engine only disposes
on *shape*. A draft can be perfectly acyclic and completely wrong: wrong table,
dropped filter, writing to production.

Add: connector allowlist, referenced tables/columns exist in provided schema,
`input_from` points at a real upstream step, destination allowlist. **And
require explicit human confirmation before any `load` step or any config
containing a DSN executes, regardless of confidence score.**

### 13. Telemetry on fallback reasons **[S]**
Every gate failure currently produces `manual_fallback`. Once :8091 exists you
won't be able to distinguish "NLP was down" from "NLP answered but scored 0.68."
Add a counter per reason *before* building the service, or you ship it blind.

### 14. In-band context passthrough, including error paths **[S]**
The best single idea in the Raptor report. They killed a Redis context stash and
threaded context through the payload instead — and critically spread it into
**both error return paths**, because a failed call still has to say *which* task
failed.

Your version: the worker's error response must carry `run_id` and `step_key`.
Otherwise the orchestrator knows something broke but not what.

### 15. Close the interpret → create loop **[S]**
Optionally let `/interpret` persist a validated auto-mode draft behind a flag,
so one NL request yields a runnable pipeline instead of two round-trips. Gate it
behind item 12's human confirmation for load steps.

---

## Tier 3 — Execution correctness and scale

### 16. Parallel execution of the ready set **[M]**
The scheduler computes the ready set correctly, then runs it sequentially.
`errgroup.WithContext` + `SetLimit(n)`. The graph work is already done — the
information about what's safe to parallelize is sitting right there.

Decision to make deliberately: on one step's failure, cancel in-flight siblings
or let them finish? Cancelling is faster; finishing means their artifacts exist
for a future resume feature. Write down which and why.

### 17. Exponential backoff + failure classification **[S]**
Retries currently fire immediately in a tight loop — three failures in 30ms
against a briefly-overloaded database, when waiting 2s would have worked.

Add exponential backoff with **jitter** (prevents thundering herd when many
tasks fail at once). Also classify: network timeout is retryable, malformed SQL
is not and will fail identically forever.

### 18. Postgres as the work queue — *instead of* Celery **[M]**
You asked where Celery fits. Mostly it doesn't: Celery is Python, your scheduler
is Go, and having Go enqueue Celery tasks means either a fragile compat library
or a Python HTTP shim — which is literally what Raptor's broker is, and you've
read what it cost them.

Use what you already have:
```sql
SELECT * FROM step_runs WHERE status = 'queued'
ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1;
```
`FOR UPDATE SKIP LOCKED` lets multiple workers claim different rows atomically
without blocking. Add `claimed_by` + `lease_expires_at` and you have leases,
durable queuing, multiple workers, and cancellation with **zero new
infrastructure**.

Also better in a viva: "I implemented a lease-based work queue using FOR UPDATE
SKIP LOCKED" says more than "I used Celery."

### 19. Optional: Celery Beat for housekeeping only **[S]**
If you want Celery in the project, contain it to Beat-scheduled jobs that live
entirely in the Python worker and require no Go interop:
- artifact cleanup sweeper (item 2)
- zombie-run sweeper (item 3)

Keeps Celery on your CV without inheriting Raptor's most common failure mode: a
worker process nobody started, and jobs that return 200 OK then sit pending
forever.

---

## Tier 4 — Security

### 20. Worker `/execute` is unauthenticated and takes arbitrary URLs/DSNs **[S] — rank this first**
Higher priority than the doc's item 7. The worker has no auth and its connectors
include `http` and `postgres`. Anyone who can reach `:8090` can make it connect
to an arbitrary host or database. That's SSRF plus arbitrary outbound DB
connections from inside your network — not merely "missing auth."

Minimum: shared secret on `/execute`, plus a host/scheme allowlist for the http
connector and a DSN allowlist for postgres.

### 21. Secrets management **[M]**
Not in the doc's open questions and it should be. Step configs are persisted in
`pipeline_steps`, and a Postgres connector config needs a connection string. Are
those sitting in plaintext in the database? Decide deliberately — env-var
references, a secrets table with encryption at rest, or an external store.

### 22. Injection risk in generated queries **[S]**
An LLM-generated `query` field goes straight to a database. Parameterize where
possible; at minimum restrict generated SQL to SELECT and validate against the
known schema.

### 23. Real auth and tenancy **[L] — probably skip**
Per-user keys, ownership on pipelines. Handler comments already reference "the
authenticated user" that doesn't exist. Honest scope call: out of scope for a
student project, mention as future work.

---

## Tier 5 — Engineering hygiene

### 24. CI **[S]**
`.github/workflows` running `go test ./...` and the Python suite. Lands early
because CI is what stops items 1–6 from regressing.

### 25. Docker + compose **[S]**
Orchestrator + worker + Postgres (+ NLP service). Also makes items 16 and 18
testable with multiple workers.

### 26. Versioned migrations **[S]**
Hand-rolled SQL constants with no version tracking. Use golang-migrate or goose.

### 27. Integration / end-to-end tests **[M]**
Raptor's flagged risk: "the entire path had never been run end-to-end in one
real test." Don't repeat it. At least one test that goes interpret → create →
run → assert data landed, with `EXEC_MODE=worker`.

### 28. UI **[L] — last**
Most visible, depends on everything above being trustworthy first.

---

## Scope decisions to make before you start

1. **NLP service in this repo or separate?** Changes compose/CI layout. My
   suggestion: same repo, separate process.
2. **Keep `local` mode?** As a fast test harness with item 1's simulated flag,
   yes. As a default, no.
3. **Multi-tenancy — real requirement?** Suggest no, document as future work.
4. **Scale target** — single orchestrator or several sharing a queue? Determines
   how heavy item 3 and 18 need to be.
5. **Where do credentials live?** (item 21)
6. **Cancel siblings on step failure?** (item 16)
7. **Sibling question worth mentioning in the report:** transform pushdown —
   emitting SQL for the destination to run instead of pulling data through the
   worker. That's the ELT pattern, and naming it shows you understand why the
   industry moved.

---

## Suggested order

**Phase A (make it honest):** 1 → 24 → 6 → 4 → 2 → 3
**Phase B (security floor):** 20 → 21
**Phase C (the actual feature):** 13 → 8 → 7 → 9 → 12 → 11 → 10
**Phase D (scale):** 17 → 16 → 5 → 18
**Phase E (polish):** 25 → 26 → 27 → 15 → 28

Rationale for the ordering: CI lands second so nothing after it regresses.
Item 13 lands before 7 so you have fallback telemetry the moment the NLP service
goes live. Item 11 (eval set) before item 10 (calibration) because you can't
measure calibration without ground truth.

**If you have limited time,** the defensible minimum project is: Phase A + item
20 + Phase C. That gives you an honest system, a security floor, a working NLP
service, and a measured finding about confidence gating — which is the part that
elevates this above "I called an LLM API."
