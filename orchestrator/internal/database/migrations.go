package database

// Migration is one versioned schema change (D-11).
//
// Rules for editing this file:
//   - Append only. Never edit a migration that has been applied anywhere; the
//     checksum comparison will reject it.
//   - Versions are contiguous and start at 1.
//   - IF NOT EXISTS is a convenience, not the idempotence mechanism. The
//     schema_migrations table is what stops a migration running twice, which is
//     what makes non-idempotent migrations (backfills, type changes) possible.
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// migrations is the ordered schema history.
//
// Versions 1-3 reproduce the schema that was previously applied on every boot as
// unversioned constants. They are unchanged so that an existing database records
// them as already-applied without altering anything.
var migrations = []Migration{
	{Version: 1, Name: "create_pipelines", SQL: migrationCreatePipelines},
	{Version: 2, Name: "runs_and_step_runs", SQL: migrationPhaseTwo},
	{Version: 3, Name: "run_events", SQL: migrationPhaseFive},
	{Version: 4, Name: "execution_ownership_and_provenance", SQL: migrationExecutionModel},
}

const migrationCreatePipelines = `
CREATE TABLE IF NOT EXISTS pipelines (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'draft',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_steps (
    id          TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    type        TEXT NOT NULL,
    config      JSONB NOT NULL DEFAULT '{}',
    depends_on  TEXT[] NOT NULL DEFAULT '{}',
    step_order  INT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_steps_pipeline_id ON pipeline_steps(pipeline_id);
`

const migrationPhaseTwo = `
ALTER TABLE pipeline_steps
ADD COLUMN IF NOT EXISTS step_key TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_steps_pipeline_step_key
ON pipeline_steps(pipeline_id, step_key)
WHERE step_key <> '';

CREATE TABLE IF NOT EXISTS pipeline_runs (
	id          TEXT PRIMARY KEY,
	pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
	status      TEXT NOT NULL DEFAULT 'pending',
	started_at  TIMESTAMPTZ,
	finished_at TIMESTAMPTZ,
	created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS step_runs (
	id              TEXT PRIMARY KEY,
	pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
	step_id         TEXT NOT NULL REFERENCES pipeline_steps(id) ON DELETE CASCADE,
	status          TEXT NOT NULL DEFAULT 'pending',
	started_at      TIMESTAMPTZ,
	finished_at     TIMESTAMPTZ,
	error           TEXT NOT NULL DEFAULT '',
	created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_id ON pipeline_runs(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_step_runs_run_id ON step_runs(pipeline_run_id);
`

const migrationPhaseFive = `
CREATE TABLE IF NOT EXISTS pipeline_run_events (
	id          TEXT PRIMARY KEY,
	pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
	run_id      TEXT REFERENCES pipeline_runs(id) ON DELETE CASCADE,
	step_id     TEXT REFERENCES pipeline_steps(id) ON DELETE SET NULL,
	step_key    TEXT NOT NULL DEFAULT '',
	level       TEXT NOT NULL,
	event_type  TEXT NOT NULL,
	message     TEXT NOT NULL,
	metadata    JSONB NOT NULL DEFAULT '{}',
	created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_run_events_pipeline_id ON pipeline_run_events(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_run_events_run_id ON pipeline_run_events(run_id);
CREATE INDEX IF NOT EXISTS idx_run_events_created_at ON pipeline_run_events(created_at DESC);
`

// migrationExecutionModel carries the structural changes: run ownership (D-07),
// the Postgres work queue (D-08), execution provenance (D-09), durable artifacts
// (D-06), the worker's idempotency ledger (D-03), the approval stage (D-12),
// history that outlives definitions (D-10), and degradation events (D-18).
const migrationExecutionModel = `
-- D-12: the approval stage. requires_approval is computed at create time from
-- the step set; approved_at is the transition a human performs.
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS requires_approval BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ;
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS approved_by TEXT NOT NULL DEFAULT '';

-- D-10: pipelines are soft-deleted so that run history and events outlive the
-- definition they describe.
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_pipelines_not_deleted ON pipelines(created_at DESC) WHERE deleted_at IS NULL;

-- D-09: how the run executed. Existing rows default to simulated = TRUE because
-- they predate provenance and were almost certainly run by the no-op
-- dispatcher; claiming otherwise would be the exact dishonesty D-09 removes.
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS exec_mode TEXT NOT NULL DEFAULT 'local';
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS simulated BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS worker_url TEXT NOT NULL DEFAULT '';

-- D-07: the run row owns the run. A process holds it by claim and heartbeat.
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS claimed_by TEXT NOT NULL DEFAULT '';
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ;
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN NOT NULL DEFAULT FALSE;

-- D-08: the claim query orders by created_at over unclaimed or expired rows.
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_claimable
ON pipeline_runs(created_at)
WHERE status IN ('pending', 'running');

-- D-03: execution identity and what the attempt actually did.
ALTER TABLE step_runs ADD COLUMN IF NOT EXISTS attempt INT NOT NULL DEFAULT 0;
ALTER TABLE step_runs ADD COLUMN IF NOT EXISTS idempotency_key TEXT NOT NULL DEFAULT '';
ALTER TABLE step_runs ADD COLUMN IF NOT EXISTS error_class TEXT NOT NULL DEFAULT '';
ALTER TABLE step_runs ADD COLUMN IF NOT EXISTS rows_processed BIGINT NOT NULL DEFAULT 0;
ALTER TABLE step_runs ADD COLUMN IF NOT EXISTS simulated BOOLEAN NOT NULL DEFAULT FALSE;

-- D-08: step claim ordering within a run.
CREATE INDEX IF NOT EXISTS idx_step_runs_queue ON step_runs(pipeline_run_id, status);

-- D-10: detach history from the definition lifecycle. An audit log destroyable
-- by the same key that can run things is not an audit log.
ALTER TABLE pipeline_run_events DROP CONSTRAINT IF EXISTS pipeline_run_events_pipeline_id_fkey;
ALTER TABLE pipeline_run_events DROP CONSTRAINT IF EXISTS pipeline_run_events_run_id_fkey;
ALTER TABLE pipeline_run_events DROP CONSTRAINT IF EXISTS pipeline_run_events_step_id_fkey;
ALTER TABLE pipeline_runs DROP CONSTRAINT IF EXISTS pipeline_runs_pipeline_id_fkey;
ALTER TABLE step_runs DROP CONSTRAINT IF EXISTS step_runs_step_id_fkey;

-- D-06: artifacts keyed by (run_id, step_key) in storage that outlives the
-- worker process and is visible to more than one worker.
CREATE TABLE IF NOT EXISTS run_artifacts (
	run_id      TEXT NOT NULL,
	step_key    TEXT NOT NULL,
	payload     JSONB NOT NULL,
	byte_size   BIGINT NOT NULL DEFAULT 0,
	created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	PRIMARY KEY (run_id, step_key)
);

CREATE INDEX IF NOT EXISTS idx_run_artifacts_run_id ON run_artifacts(run_id);

-- D-03: the worker's durable record of what it has already done. A replayed
-- attempt returns the stored result instead of re-executing.
--
-- The primary key is the logical unit of work, (run_id, step_key) — deliberately
-- NOT the per-attempt idempotency key. A dispatcher timeout produces a retry
-- with a new attempt number; if the ledger were keyed per attempt it would never
-- match, and the retry would re-execute the load that had in fact succeeded.
-- attempt and idempotency_key are recorded for traceability only.
CREATE TABLE IF NOT EXISTS step_executions (
	run_id          TEXT NOT NULL,
	step_key        TEXT NOT NULL,
	idempotency_key TEXT NOT NULL,
	attempt         INT NOT NULL DEFAULT 0,
	result          JSONB NOT NULL,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	PRIMARY KEY (run_id, step_key)
);

-- D-18: degradation is a system event, not a field in a reply.
CREATE TABLE IF NOT EXISTS degradation_events (
	id          TEXT PRIMARY KEY,
	component   TEXT NOT NULL,
	reason      TEXT NOT NULL,
	detail      TEXT NOT NULL DEFAULT '',
	metadata    JSONB NOT NULL DEFAULT '{}',
	created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_degradation_component_reason ON degradation_events(component, reason);
CREATE INDEX IF NOT EXISTS idx_degradation_created_at ON degradation_events(created_at DESC);
`
