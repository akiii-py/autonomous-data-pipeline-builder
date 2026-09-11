package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/akshat/pipeline-orchestrator/internal/models"
	"github.com/google/uuid"
)

// EventInput is one audit-log entry. The event log is authoritative (rule 5.3):
// every status transition writes its event in the same transaction as the status
// itself, so a failed emit fails the transition rather than silently leaving the
// log incomplete.
type EventInput struct {
	PipelineID string
	RunID      string
	StepID     string
	StepKey    string
	Level      string
	EventType  string
	Message    string
	Metadata   map[string]interface{}
}

// StepTransition is everything a step status change records. It is populated
// from a models.StepResult so that no field the worker reported is dropped at
// the boundary (D-01).
type StepTransition struct {
	StepID        string
	Status        string
	Error         string
	ErrorClass    models.ErrorClass
	Attempt       int
	RowsProcessed int64
	Simulated     bool
}

// NewStepTransition builds a transition from a dispatcher result.
func NewStepTransition(stepID, status string, res *models.StepResult) StepTransition {
	t := StepTransition{StepID: stepID, Status: status}
	if res == nil {
		return t
	}
	t.Error = res.ErrorMessage
	t.ErrorClass = res.ErrorClass
	t.Attempt = res.Attempt
	t.RowsProcessed = res.RowsProcessed
	t.Simulated = res.Simulated
	return t
}

// CreateRun creates the run and its step rows. The run starts `pending` and
// unclaimed; it is not owned by the process that created it (D-07).
func (s *PipelineStore) CreateRun(ctx context.Context, pipelineID string) (*models.PipelineRun, error) {
	pipeline, err := s.GetByID(ctx, pipelineID)
	if err != nil {
		return nil, err
	}
	if pipeline == nil {
		return nil, ErrPipelineNotFound
	}
	if !pipeline.Runnable() {
		return nil, ErrNotApproved
	}
	if len(pipeline.Steps) == 0 {
		return nil, fmt.Errorf("pipeline has no steps")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	runID := uuid.New().String()
	var run models.PipelineRun
	err = tx.QueryRowContext(ctx,
		`INSERT INTO pipeline_runs (id, pipeline_id, status)
		 VALUES ($1, $2, $3)
		 RETURNING id, pipeline_id, status, started_at, finished_at, created_at,
		           exec_mode, simulated, worker_url, claimed_by, claim_expires_at,
		           heartbeat_at, cancel_requested`,
		runID, pipelineID, models.RunStatusPending,
	).Scan(&run.ID, &run.PipelineID, &run.Status, &run.StartedAt, &run.FinishedAt, &run.CreatedAt,
		&run.ExecMode, &run.Simulated, &run.WorkerURL, &run.ClaimedBy, &run.ClaimExpiresAt,
		&run.HeartbeatAt, &run.CancelRequested)
	if err != nil {
		return nil, fmt.Errorf("insert pipeline run: %w", err)
	}

	for _, st := range pipeline.Steps {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO step_runs (id, pipeline_run_id, step_id, status)
			 VALUES ($1, $2, $3, $4)`,
			uuid.New().String(), runID, st.ID, models.RunStatusPending,
		); err != nil {
			return nil, fmt.Errorf("insert step run: %w", err)
		}
	}

	if err := createEventTx(ctx, tx, EventInput{
		PipelineID: pipelineID,
		RunID:      runID,
		Level:      "info",
		EventType:  "run_queued",
		Message:    "pipeline run queued",
		Metadata:   map[string]interface{}{"source": "api"},
	}); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit run tx: %w", err)
	}

	return &run, nil
}

// ClaimRun atomically takes ownership of one runnable run (D-07, D-08).
//
// SKIP LOCKED is what makes this the whole queue: several orchestrator processes
// can run this concurrently and each takes a different row without blocking. A
// run whose claim has expired — because the process holding it died — becomes
// claimable again, which is how restart recovery happens without a separate
// sweep.
func (s *PipelineStore) ClaimRun(ctx context.Context, owner string, lease time.Duration, prov models.Provenance) (*models.PipelineRun, error) {
	now := time.Now().UTC()
	expires := now.Add(lease)

	var run models.PipelineRun
	err := s.db.QueryRowContext(ctx, `
		UPDATE pipeline_runs SET
			status = $1,
			claimed_by = $2,
			claim_expires_at = $3,
			heartbeat_at = $4,
			started_at = COALESCE(started_at, $4),
			exec_mode = $5,
			simulated = $6,
			worker_url = $7
		WHERE id = (
			SELECT id FROM pipeline_runs
			WHERE status IN ($8, $1)
			  AND (claimed_by = '' OR claim_expires_at IS NULL OR claim_expires_at < $4)
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, pipeline_id, status, started_at, finished_at, created_at,
		          exec_mode, simulated, worker_url, claimed_by, claim_expires_at,
		          heartbeat_at, cancel_requested`,
		models.RunStatusRunning, owner, expires, now,
		prov.ExecMode, prov.Simulated, prov.Target,
		models.RunStatusPending,
	).Scan(&run.ID, &run.PipelineID, &run.Status, &run.StartedAt, &run.FinishedAt, &run.CreatedAt,
		&run.ExecMode, &run.Simulated, &run.WorkerURL, &run.ClaimedBy, &run.ClaimExpiresAt,
		&run.HeartbeatAt, &run.CancelRequested)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim run: %w", err)
	}
	return &run, nil
}

// Heartbeat extends the claim. It returns false when the claim has been lost —
// another process reclaimed an expired lease — which tells the holder to stop.
func (s *PipelineStore) Heartbeat(ctx context.Context, runID, owner string, lease time.Duration) (bool, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx,
		`UPDATE pipeline_runs
		 SET heartbeat_at = $1, claim_expires_at = $2
		 WHERE id = $3 AND claimed_by = $4`,
		now, now.Add(lease), runID, owner)
	if err != nil {
		return false, fmt.Errorf("heartbeat: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ReleaseClaim drops ownership without changing the run's status, so another
// process can pick the run up. Used on graceful shutdown.
func (s *PipelineStore) ReleaseClaim(ctx context.Context, runID, owner string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE pipeline_runs SET claimed_by = '', claim_expires_at = NULL
		 WHERE id = $1 AND claimed_by = $2`, runID, owner)
	if err != nil {
		return fmt.Errorf("release claim: %w", err)
	}
	return nil
}

// RequestCancel flags a run for cancellation. The holder observes the flag on
// its next heartbeat and cancels its context (rule 4.2).
func (s *PipelineStore) RequestCancel(ctx context.Context, pipelineID, runID string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE pipeline_runs SET cancel_requested = TRUE
		 WHERE id = $1 AND pipeline_id = $2 AND status IN ($3, $4)`,
		runID, pipelineID, models.RunStatusPending, models.RunStatusRunning)
	if err != nil {
		return fmt.Errorf("request cancel: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("run is not cancellable")
	}
	return nil
}

// CancelRequested reports whether cancellation has been asked for.
func (s *PipelineStore) CancelRequested(ctx context.Context, runID string) (bool, error) {
	var flag bool
	if err := s.db.QueryRowContext(ctx,
		`SELECT cancel_requested FROM pipeline_runs WHERE id = $1`, runID).Scan(&flag); err != nil {
		return false, err
	}
	return flag, nil
}

// EnqueueSteps moves a ready set into the queue and emits one event per step, in
// a single transaction (rule 5.3).
func (s *PipelineStore) EnqueueSteps(ctx context.Context, runID string, stepIDs []string, events []EventInput) error {
	if len(stepIDs) == 0 {
		return nil
	}

	return s.withTx(ctx, func(tx *sql.Tx) error {
		for _, stepID := range stepIDs {
			if _, err := tx.ExecContext(ctx,
				`UPDATE step_runs SET status = $1 WHERE pipeline_run_id = $2 AND step_id = $3`,
				models.RunStatusQueued, runID, stepID); err != nil {
				return fmt.Errorf("enqueue step: %w", err)
			}
		}
		for _, ev := range events {
			if err := createEventTx(ctx, tx, ev); err != nil {
				return err
			}
		}
		return nil
	})
}

// ClaimedStep is a step taken off the queue.
type ClaimedStep struct {
	StepID         string
	Attempt        int
	IdempotencyKey string
}

// ClaimQueuedStep takes one queued step for this run with SKIP LOCKED, so
// several executor goroutines — or several processes — never take the same one
// (D-08). Attempt is incremented here, which is what makes the idempotency key
// unique per attempt (D-03).
func (s *PipelineStore) ClaimQueuedStep(ctx context.Context, runID string) (*ClaimedStep, error) {
	now := time.Now().UTC()

	var claimed ClaimedStep
	err := s.db.QueryRowContext(ctx, `
		UPDATE step_runs SET
			status = $1,
			attempt = attempt + 1,
			started_at = COALESCE(started_at, $2)
		WHERE id = (
			SELECT id FROM step_runs
			WHERE pipeline_run_id = $3 AND status = $4
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING step_id, attempt`,
		models.RunStatusRunning, now, runID, models.RunStatusQueued,
	).Scan(&claimed.StepID, &claimed.Attempt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim queued step: %w", err)
	}

	claimed.IdempotencyKey = IdempotencyKey(runID, claimed.StepID, claimed.Attempt)

	if _, err := s.db.ExecContext(ctx,
		`UPDATE step_runs SET idempotency_key = $1 WHERE pipeline_run_id = $2 AND step_id = $3`,
		claimed.IdempotencyKey, runID, claimed.StepID); err != nil {
		return nil, fmt.Errorf("record idempotency key: %w", err)
	}

	return &claimed, nil
}

// RequeueStep puts a step back on the queue for another attempt.
func (s *PipelineStore) RequeueStep(ctx context.Context, runID, stepID string, ev EventInput) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`UPDATE step_runs SET status = $1 WHERE pipeline_run_id = $2 AND step_id = $3`,
			models.RunStatusQueued, runID, stepID); err != nil {
			return fmt.Errorf("requeue step: %w", err)
		}
		return createEventTx(ctx, tx, ev)
	})
}

// IdempotencyKey is execution identity: one key per (run, step, attempt) (D-03).
func IdempotencyKey(runID, stepID string, attempt int) string {
	return fmt.Sprintf("%s:%s:%d", runID, stepID, attempt)
}

// TransitionRun writes a run status and its event atomically (rule 5.3).
func (s *PipelineStore) TransitionRun(ctx context.Context, runID, status string, ev EventInput) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if err := updateRunStatusTx(ctx, tx, runID, status); err != nil {
			return err
		}
		return createEventTx(ctx, tx, ev)
	})
}

// TransitionStep writes a step status and its event atomically (rule 5.3).
func (s *PipelineStore) TransitionStep(ctx context.Context, runID string, t StepTransition, ev EventInput) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if err := updateStepStatusTx(ctx, tx, runID, t); err != nil {
			return err
		}
		return createEventTx(ctx, tx, ev)
	})
}

// FinishRun writes the run's terminal status, cancels any step still in flight,
// deletes the run's artifacts, and emits the terminal event — all in one
// transaction, so there is no ordering in which work completes and nothing
// records it (rule 5.4).
func (s *PipelineStore) FinishRun(ctx context.Context, runID, status string, ev EventInput, siblingEvents []EventInput) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`UPDATE step_runs
			 SET status = $1, finished_at = COALESCE(finished_at, $2)
			 WHERE pipeline_run_id = $3 AND status IN ($4, $5, $6)`,
			models.RunStatusCancelled, time.Now().UTC(), runID,
			models.RunStatusPending, models.RunStatusQueued, models.RunStatusRunning,
		); err != nil {
			return fmt.Errorf("cancel in-flight steps: %w", err)
		}

		if err := updateRunStatusTx(ctx, tx, runID, status); err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx, `DELETE FROM run_artifacts WHERE run_id = $1`, runID); err != nil {
			return fmt.Errorf("delete run artifacts: %w", err)
		}

		for _, sev := range siblingEvents {
			if err := createEventTx(ctx, tx, sev); err != nil {
				return err
			}
		}
		return createEventTx(ctx, tx, ev)
	})
}

// EmitEvent writes a standalone informational event — progress notes that are
// not themselves status transitions. It returns its error rather than
// discarding it: the log is authoritative (rule 5.3). Status changes never use
// this; they go through TransitionRun/TransitionStep/FinishRun so the status and
// its event share one transaction.
func (s *PipelineStore) EmitEvent(ctx context.Context, ev EventInput) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		return createEventTx(ctx, tx, ev)
	})
}

// RecordDegradation writes a degradation event. Degradation is a system event,
// not a field in a reply (D-18).
func (s *PipelineStore) RecordDegradation(ctx context.Context, component, reason, detail string, metadata map[string]interface{}) error {
	meta := json.RawMessage(`{}`)
	if metadata != nil {
		b, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("marshal degradation metadata: %w", err)
		}
		meta = b
	}

	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO degradation_events (id, component, reason, detail, metadata)
		 VALUES ($1, $2, $3, $4, $5)`,
		uuid.New().String(), component, reason, detail, meta,
	); err != nil {
		return fmt.Errorf("insert degradation event: %w", err)
	}
	return nil
}

func (s *PipelineStore) degradationCounts(ctx context.Context) ([]models.DegradationCount, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT component, reason, COUNT(*)
		 FROM degradation_events
		 GROUP BY component, reason
		 ORDER BY COUNT(*) DESC, component, reason`)
	if err != nil {
		return nil, fmt.Errorf("count degradations: %w", err)
	}
	defer rows.Close()

	out := make([]models.DegradationCount, 0)
	for rows.Next() {
		var d models.DegradationCount
		if err := rows.Scan(&d.Component, &d.Reason, &d.Count); err != nil {
			return nil, fmt.Errorf("scan degradation: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *PipelineStore) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func updateRunStatusTx(ctx context.Context, tx *sql.Tx, runID, status string) error {
	now := time.Now().UTC()

	switch status {
	case models.RunStatusRunning:
		_, err := tx.ExecContext(ctx,
			`UPDATE pipeline_runs SET status = $1, started_at = COALESCE(started_at, $2) WHERE id = $3`,
			status, now, runID)
		if err != nil {
			return fmt.Errorf("update run running: %w", err)
		}
	case models.RunStatusCompleted, models.RunStatusFailed, models.RunStatusCancelled:
		_, err := tx.ExecContext(ctx,
			`UPDATE pipeline_runs
			 SET status = $1,
			     started_at = COALESCE(started_at, $2),
			     finished_at = $2,
			     claimed_by = '',
			     claim_expires_at = NULL
			 WHERE id = $3`,
			status, now, runID)
		if err != nil {
			return fmt.Errorf("update run terminal: %w", err)
		}
	default:
		_, err := tx.ExecContext(ctx, `UPDATE pipeline_runs SET status = $1 WHERE id = $2`, status, runID)
		if err != nil {
			return fmt.Errorf("update run status: %w", err)
		}
	}
	return nil
}

func updateStepStatusTx(ctx context.Context, tx *sql.Tx, runID string, t StepTransition) error {
	now := time.Now().UTC()

	switch t.Status {
	case models.RunStatusRunning, models.RunStatusQueued:
		_, err := tx.ExecContext(ctx,
			`UPDATE step_runs
			 SET status = $1, started_at = COALESCE(started_at, $2)
			 WHERE pipeline_run_id = $3 AND step_id = $4`,
			t.Status, now, runID, t.StepID)
		if err != nil {
			return fmt.Errorf("update step running: %w", err)
		}
	default:
		_, err := tx.ExecContext(ctx,
			`UPDATE step_runs
			 SET status = $1,
			     error = $2,
			     error_class = $3,
			     attempt = GREATEST(attempt, $4),
			     rows_processed = $5,
			     simulated = $6,
			     started_at = COALESCE(started_at, $7),
			     finished_at = $7
			 WHERE pipeline_run_id = $8 AND step_id = $9`,
			t.Status, t.Error, string(t.ErrorClass), t.Attempt, t.RowsProcessed, t.Simulated,
			now, runID, t.StepID)
		if err != nil {
			return fmt.Errorf("update step terminal: %w", err)
		}
	}
	return nil
}

func createEventTx(ctx context.Context, tx *sql.Tx, ev EventInput) error {
	meta := json.RawMessage(`{}`)
	if ev.Metadata != nil {
		b, err := json.Marshal(ev.Metadata)
		if err != nil {
			return fmt.Errorf("marshal event metadata: %w", err)
		}
		meta = b
	}

	_, err := tx.ExecContext(ctx,
		`INSERT INTO pipeline_run_events
		 (id, pipeline_id, run_id, step_id, step_key, level, event_type, message, metadata)
		 VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, $6, $7, $8, $9)`,
		uuid.New().String(), ev.PipelineID, ev.RunID, ev.StepID, ev.StepKey,
		ev.Level, ev.EventType, ev.Message, meta,
	)
	if err != nil {
		return fmt.Errorf("insert run event: %w", err)
	}
	return nil
}
