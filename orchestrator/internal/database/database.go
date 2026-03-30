package database

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

func Connect(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	log.Println("connected to PostgreSQL")
	return db, nil
}

func RunMigrations(db *sql.DB) error {
	migrations := []string{
		migrationCreatePipelines,
		migrationPhaseTwo,
		migrationPhaseFive,
	}

	for i, m := range migrations {
		if _, err := db.Exec(m); err != nil {
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
	}

	log.Println("database migrations applied")
	return nil
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
