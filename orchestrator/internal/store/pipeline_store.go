package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/akshat/pipeline-orchestrator/internal/dag"
	"github.com/akshat/pipeline-orchestrator/internal/models"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// ErrPipelineNotFound is returned by operations addressing a pipeline that does
// not exist or has been soft-deleted. Callers match on it with errors.Is rather
// than on message text (rule 5.1).
var ErrPipelineNotFound = errors.New("pipeline not found")

// ErrNotApproved is returned when a run is requested for a pipeline that has
// not cleared the approval stage (D-12).
var ErrNotApproved = errors.New("pipeline requires approval before it can run")

type PipelineStore struct {
	db *sql.DB
}

func NewPipelineStore(db *sql.DB) *PipelineStore {
	return &PipelineStore{db: db}
}

// DB exposes the handle for the execution store's transactional helpers. It is
// not for ad-hoc queries from other layers (rule 6.2).
func (s *PipelineStore) DB() *sql.DB { return s.db }

func (s *PipelineStore) List(ctx context.Context, limit, offset int) ([]models.Pipeline, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, description, status, requires_approval, approved_at, approved_by,
		        created_at, updated_at
		 FROM pipelines
		 WHERE deleted_at IS NULL
		 ORDER BY created_at DESC
		 LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	defer rows.Close()

	pipelines := make([]models.Pipeline, 0)
	for rows.Next() {
		var p models.Pipeline
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.Status,
			&p.RequiresApproval, &p.ApprovedAt, &p.ApprovedBy, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan pipeline: %w", err)
		}
		pipelines = append(pipelines, p)
	}
	return pipelines, rows.Err()
}

// GetByID returns a pipeline with its steps. Step configs are redacted, so no
// read path can leak a credential that reached the database (rule 6.4).
func (s *PipelineStore) GetByID(ctx context.Context, id string) (*models.Pipeline, error) {
	return s.getByID(ctx, id, true)
}

// GetForExecution returns a pipeline with unredacted step configs, for the
// scheduler to dispatch. It is never reachable from a handler.
func (s *PipelineStore) GetForExecution(ctx context.Context, id string) (*models.Pipeline, error) {
	return s.getByID(ctx, id, false)
}

func (s *PipelineStore) getByID(ctx context.Context, id string, redact bool) (*models.Pipeline, error) {
	var p models.Pipeline
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, description, status, requires_approval, approved_at, approved_by,
		        deleted_at, created_at, updated_at
		 FROM pipelines WHERE id = $1 AND deleted_at IS NULL`, id).
		Scan(&p.ID, &p.Name, &p.Description, &p.Status, &p.RequiresApproval,
			&p.ApprovedAt, &p.ApprovedBy, &p.DeletedAt, &p.CreatedAt, &p.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get pipeline: %w", err)
	}

	steps, err := s.getSteps(ctx, id, redact)
	if err != nil {
		return nil, err
	}
	p.Steps = steps
	return &p, nil
}

// Create validates the DAG before insert and computes the approval requirement
// from the step set (D-12). Validation errors are returned as the typed
// dag.ValidationErrors so the handler can choose its status code without
// matching on text (rule 5.1).
func (s *PipelineStore) Create(ctx context.Context, req models.CreatePipelineRequest) (*models.Pipeline, error) {
	if _, err := dag.BuildFromCreateSteps(req.Steps); err != nil {
		return nil, err
	}

	requiresApproval := models.RequiresApproval(req.Steps)
	status := models.StatusApproved
	if requiresApproval {
		status = models.StatusPendingApproval
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	pipelineID := uuid.New().String()
	var p models.Pipeline
	err = tx.QueryRowContext(ctx,
		`INSERT INTO pipelines (id, name, description, status, requires_approval)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id, name, description, status, requires_approval, approved_at, approved_by,
		           created_at, updated_at`,
		pipelineID, req.Name, req.Description, status, requiresApproval).
		Scan(&p.ID, &p.Name, &p.Description, &p.Status, &p.RequiresApproval,
			&p.ApprovedAt, &p.ApprovedBy, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert pipeline: %w", err)
	}

	for i, sr := range req.Steps {
		stepID := uuid.New().String()
		cfg := sr.Config
		if cfg == nil {
			cfg = json.RawMessage(`{}`)
		}
		var step models.Step
		err = tx.QueryRowContext(ctx,
			`INSERT INTO pipeline_steps (id, pipeline_id, step_key, name, type, config, depends_on, step_order)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			 RETURNING id, pipeline_id, step_key, name, type, config, depends_on, step_order, created_at`,
			stepID, pipelineID, sr.Key, sr.Name, sr.Type, cfg, pq.Array(sr.DependsOn), i+1).
			Scan(&step.ID, &step.PipelineID, &step.Key, &step.Name, &step.Type, &step.Config,
				pq.Array(&step.DependsOn), &step.StepOrder, &step.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("insert step %d: %w", i+1, err)
		}
		step.Config = models.RedactConfig(step.Config)
		p.Steps = append(p.Steps, step)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return &p, nil
}

// Approve performs the lifecycle transition that makes an irreversible pipeline
// runnable (D-12). It is idempotent: approving an approved pipeline is a no-op.
func (s *PipelineStore) Approve(ctx context.Context, pipelineID, approvedBy string) (*models.Pipeline, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx,
		`UPDATE pipelines
		 SET status = $1,
		     approved_at = COALESCE(approved_at, $2),
		     approved_by = CASE WHEN approved_at IS NULL THEN $3 ELSE approved_by END,
		     updated_at = $2
		 WHERE id = $4 AND deleted_at IS NULL`,
		models.StatusApproved, now, approvedBy, pipelineID,
	)
	if err != nil {
		return nil, fmt.Errorf("approve pipeline: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, ErrPipelineNotFound
	}

	return s.GetByID(ctx, pipelineID)
}

// Delete soft-deletes the definition. Run history and events are deliberately
// untouched — they outlive the pipeline they describe (D-10).
func (s *PipelineStore) Delete(ctx context.Context, id string) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx,
		`UPDATE pipelines SET deleted_at = $1, updated_at = $1 WHERE id = $2 AND deleted_at IS NULL`,
		now, id)
	if err != nil {
		return fmt.Errorf("delete pipeline: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrPipelineNotFound
	}
	return nil
}

func (s *PipelineStore) GetLatestRunStatus(ctx context.Context, pipelineID string) (*models.PipelineStatusResponse, error) {
	var runID string
	err := s.db.QueryRowContext(ctx,
		`SELECT id FROM pipeline_runs WHERE pipeline_id = $1 ORDER BY created_at DESC LIMIT 1`,
		pipelineID,
	).Scan(&runID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("latest run: %w", err)
	}
	return s.GetRunStatusByID(ctx, pipelineID, runID)
}

func (s *PipelineStore) GetRunStatusByID(ctx context.Context, pipelineID, runID string) (*models.PipelineStatusResponse, error) {
	run, err := s.getRun(ctx, pipelineID, runID)
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, nil
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT sr.step_id, ps.step_key, ps.name, sr.status, sr.error, sr.error_class,
		        sr.attempt, sr.rows_processed, sr.simulated, sr.started_at, sr.finished_at
		 FROM step_runs sr
		 JOIN pipeline_steps ps ON ps.id = sr.step_id
		 WHERE sr.pipeline_run_id = $1
		 ORDER BY ps.step_order`,
		run.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("list step runs: %w", err)
	}
	defer rows.Close()

	steps := make([]models.StepRunStatus, 0)
	for rows.Next() {
		var srs models.StepRunStatus
		if err := rows.Scan(&srs.StepID, &srs.StepKey, &srs.StepName, &srs.Status,
			&srs.Error, &srs.ErrorClass, &srs.Attempt, &srs.RowsProcessed, &srs.Simulated,
			&srs.StartedAt, &srs.FinishedAt); err != nil {
			return nil, fmt.Errorf("scan step run: %w", err)
		}
		steps = append(steps, srs)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &models.PipelineStatusResponse{
		PipelineID: pipelineID,
		Run:        run,
		Steps:      steps,
	}, nil
}

func (s *PipelineStore) getRun(ctx context.Context, pipelineID, runID string) (*models.PipelineRun, error) {
	var run models.PipelineRun
	err := s.db.QueryRowContext(ctx,
		`SELECT id, pipeline_id, status, started_at, finished_at, created_at,
		        exec_mode, simulated, worker_url, claimed_by, claim_expires_at,
		        heartbeat_at, cancel_requested
		 FROM pipeline_runs
		 WHERE id = $1 AND pipeline_id = $2`,
		runID, pipelineID,
	).Scan(&run.ID, &run.PipelineID, &run.Status, &run.StartedAt, &run.FinishedAt, &run.CreatedAt,
		&run.ExecMode, &run.Simulated, &run.WorkerURL, &run.ClaimedBy, &run.ClaimExpiresAt,
		&run.HeartbeatAt, &run.CancelRequested)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("run by id: %w", err)
	}
	return &run, nil
}

// ListRuns returns run history. Provenance travels with every row (D-09).
func (s *PipelineStore) ListRuns(ctx context.Context, pipelineID, status string, limit, offset int) ([]models.PipelineRunHistoryItem, error) {
	query := `
	SELECT
		pr.id,
		pr.pipeline_id,
		pr.status,
		pr.started_at,
		pr.finished_at,
		pr.created_at,
		pr.exec_mode,
		pr.simulated,
		COALESCE(COUNT(sr.step_id), 0) AS total_steps,
		COALESCE(SUM(CASE WHEN sr.status = 'completed' THEN 1 ELSE 0 END), 0) AS completed_steps,
		COALESCE(SUM(CASE WHEN sr.status = 'failed' THEN 1 ELSE 0 END), 0) AS failed_steps,
		COALESCE(SUM(sr.rows_processed), 0) AS rows_processed
	FROM pipeline_runs pr
	LEFT JOIN step_runs sr ON sr.pipeline_run_id = pr.id
	WHERE pr.pipeline_id = $1`

	args := []interface{}{pipelineID}
	if status != "" {
		query += ` AND pr.status = $2 GROUP BY pr.id ORDER BY pr.created_at DESC LIMIT $3 OFFSET $4`
		args = append(args, status, limit, offset)
	} else {
		query += ` GROUP BY pr.id ORDER BY pr.created_at DESC LIMIT $2 OFFSET $3`
		args = append(args, limit, offset)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()

	items := make([]models.PipelineRunHistoryItem, 0)
	for rows.Next() {
		var item models.PipelineRunHistoryItem
		if err := rows.Scan(
			&item.ID, &item.PipelineID, &item.Status, &item.StartedAt, &item.FinishedAt,
			&item.CreatedAt, &item.ExecMode, &item.Simulated,
			&item.TotalSteps, &item.CompletedSteps, &item.FailedSteps, &item.RowsProcessed,
		); err != nil {
			return nil, fmt.Errorf("scan run history: %w", err)
		}
		items = append(items, item)
	}

	return items, rows.Err()
}

func (s *PipelineStore) ListRunEvents(ctx context.Context, pipelineID, runID, level, eventType string, limit, offset int) ([]models.RunEvent, error) {
	query := `
	SELECT id, pipeline_id, COALESCE(run_id, ''), COALESCE(step_id, ''), step_key, level, event_type, message, metadata, created_at
	FROM pipeline_run_events
	WHERE pipeline_id = $1`

	args := []interface{}{pipelineID}
	argPos := 2
	if runID != "" {
		query += fmt.Sprintf(` AND run_id = $%d`, argPos)
		args = append(args, runID)
		argPos++
	}
	if level != "" {
		query += fmt.Sprintf(` AND level = $%d`, argPos)
		args = append(args, level)
		argPos++
	}
	if eventType != "" {
		query += fmt.Sprintf(` AND event_type = $%d`, argPos)
		args = append(args, eventType)
		argPos++
	}

	query += fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, argPos, argPos+1)
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list run events: %w", err)
	}
	defer rows.Close()

	items := make([]models.RunEvent, 0)
	for rows.Next() {
		var item models.RunEvent
		if err := rows.Scan(&item.ID, &item.PipelineID, &item.RunID, &item.StepID, &item.StepKey,
			&item.Level, &item.EventType, &item.Message, &item.Metadata, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan run event: %w", err)
		}
		items = append(items, item)
	}

	return items, rows.Err()
}

// GetMetrics reports global aggregates. Simulated and real runs are counted
// separately, because an aggregate that mixes them measures a simulator without
// saying so (D-09). Degradation reasons are counted here too (D-18).
func (s *PipelineStore) GetMetrics(ctx context.Context) (*models.MetricsResponse, error) {
	metrics := &models.MetricsResponse{
		GeneratedAtEpoch: time.Now().UTC().Unix(),
		Degradations:     make([]models.DegradationCount, 0),
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipelines WHERE deleted_at IS NULL`).Scan(&metrics.PipelinesTotal); err != nil {
		return nil, fmt.Errorf("count pipelines: %w", err)
	}

	err := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = $1),
			COUNT(*) FILTER (WHERE status = $2),
			COUNT(*) FILTER (WHERE status = $3),
			COUNT(*) FILTER (WHERE status = $4),
			COUNT(*) FILTER (WHERE simulated),
			COUNT(*) FILTER (WHERE NOT simulated)
		FROM pipeline_runs`,
		models.RunStatusRunning, models.RunStatusCompleted, models.RunStatusFailed, models.RunStatusCancelled,
	).Scan(&metrics.RunsTotal, &metrics.RunsRunning, &metrics.RunsCompleted,
		&metrics.RunsFailed, &metrics.RunsCancelled, &metrics.RunsSimulated, &metrics.RunsReal)
	if err != nil {
		return nil, fmt.Errorf("count runs: %w", err)
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM step_runs WHERE status = $1`, models.RunStatusFailed,
	).Scan(&metrics.StepRunsFailed); err != nil {
		return nil, fmt.Errorf("count failed step runs: %w", err)
	}

	if metrics.RunsTotal > 0 {
		metrics.RunsSuccessRate = float64(metrics.RunsCompleted) / float64(metrics.RunsTotal)
		metrics.RunsFailureRate = float64(metrics.RunsFailed) / float64(metrics.RunsTotal)
	}

	var avg sql.NullFloat64
	if err := s.db.QueryRowContext(ctx,
		`SELECT AVG(EXTRACT(EPOCH FROM (finished_at - started_at)))
		 FROM pipeline_runs
		 WHERE started_at IS NOT NULL AND finished_at IS NOT NULL`,
	).Scan(&avg); err != nil {
		return nil, fmt.Errorf("avg run duration: %w", err)
	}
	if avg.Valid {
		metrics.AvgRunDurationS = avg.Float64
	}

	degradations, err := s.degradationCounts(ctx)
	if err != nil {
		return nil, err
	}
	metrics.Degradations = degradations

	return metrics, nil
}

func (s *PipelineStore) GetPipelineMetrics(ctx context.Context, pipelineID string) (*models.PipelineMetricsResponse, error) {
	metrics := &models.PipelineMetricsResponse{
		PipelineID:       pipelineID,
		TopFailedSteps:   make([]models.StepFailureMetric, 0),
		GeneratedAtEpoch: time.Now().UTC().Unix(),
	}

	err := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = $2),
			COUNT(*) FILTER (WHERE status = $3),
			COUNT(*) FILTER (WHERE status = $4),
			COUNT(*) FILTER (WHERE status = $5),
			COUNT(*) FILTER (WHERE simulated),
			COUNT(*) FILTER (WHERE NOT simulated)
		FROM pipeline_runs WHERE pipeline_id = $1`,
		pipelineID,
		models.RunStatusRunning, models.RunStatusCompleted, models.RunStatusFailed, models.RunStatusCancelled,
	).Scan(&metrics.RunsTotal, &metrics.RunsRunning, &metrics.RunsCompleted,
		&metrics.RunsFailed, &metrics.RunsCancelled, &metrics.RunsSimulated, &metrics.RunsReal)
	if err != nil {
		return nil, fmt.Errorf("count pipeline runs: %w", err)
	}

	if metrics.RunsTotal > 0 {
		metrics.RunsSuccessRate = float64(metrics.RunsCompleted) / float64(metrics.RunsTotal)
		metrics.RunsFailureRate = float64(metrics.RunsFailed) / float64(metrics.RunsTotal)
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*)
		 FROM step_runs sr
		 JOIN pipeline_runs pr ON pr.id = sr.pipeline_run_id
		 WHERE pr.pipeline_id = $1 AND sr.status = $2`,
		pipelineID, models.RunStatusFailed,
	).Scan(&metrics.StepRunsFailed); err != nil {
		return nil, fmt.Errorf("count pipeline failed step runs: %w", err)
	}

	var avg sql.NullFloat64
	if err := s.db.QueryRowContext(ctx,
		`SELECT AVG(EXTRACT(EPOCH FROM (finished_at - started_at)))
		 FROM pipeline_runs
		 WHERE pipeline_id = $1 AND started_at IS NOT NULL AND finished_at IS NOT NULL`,
		pipelineID,
	).Scan(&avg); err != nil {
		return nil, fmt.Errorf("avg pipeline run duration: %w", err)
	}
	if avg.Valid {
		metrics.AvgRunDurationS = avg.Float64
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT ps.step_key, ps.name, COUNT(*) AS failures
		 FROM step_runs sr
		 JOIN pipeline_runs pr ON pr.id = sr.pipeline_run_id
		 JOIN pipeline_steps ps ON ps.id = sr.step_id
		 WHERE pr.pipeline_id = $1 AND sr.status = $2
		 GROUP BY ps.step_key, ps.name
		 ORDER BY failures DESC, ps.step_key
		 LIMIT 10`,
		pipelineID, models.RunStatusFailed,
	)
	if err != nil {
		return nil, fmt.Errorf("list pipeline failed steps: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item models.StepFailureMetric
		if err := rows.Scan(&item.StepKey, &item.StepName, &item.Failures); err != nil {
			return nil, fmt.Errorf("scan pipeline failed step: %w", err)
		}
		metrics.TopFailedSteps = append(metrics.TopFailedSteps, item)
	}

	return metrics, rows.Err()
}

// GetPipelineFailureBreakdown groups failures by step, message and
// classification. Simulated failures are distinguished from real ones (D-09).
func (s *PipelineStore) GetPipelineFailureBreakdown(ctx context.Context, pipelineID string, limit, offset int) ([]models.StepFailureBreakdownItem, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT
			ps.step_key,
			ps.name,
			COALESCE(NULLIF(sr.error, ''), '') AS error_message,
			sr.error_class,
			sr.simulated,
			COUNT(*) AS failures,
			MAX(COALESCE(sr.finished_at, sr.created_at)) AS last_failed_at
		 FROM step_runs sr
		 JOIN pipeline_runs pr ON pr.id = sr.pipeline_run_id
		 JOIN pipeline_steps ps ON ps.id = sr.step_id
		 WHERE pr.pipeline_id = $1 AND sr.status = $2
		 GROUP BY ps.step_key, ps.name, sr.error, sr.error_class, sr.simulated
		 ORDER BY failures DESC, last_failed_at DESC
		 LIMIT $3 OFFSET $4`,
		pipelineID, models.RunStatusFailed, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("list pipeline failure breakdown: %w", err)
	}
	defer rows.Close()

	items := make([]models.StepFailureBreakdownItem, 0)
	for rows.Next() {
		var item models.StepFailureBreakdownItem
		if err := rows.Scan(&item.StepKey, &item.StepName, &item.ErrorMessage, &item.ErrorClass,
			&item.Simulated, &item.Failures, &item.LastFailedAt); err != nil {
			return nil, fmt.Errorf("scan pipeline failure breakdown: %w", err)
		}
		items = append(items, item)
	}

	return items, rows.Err()
}

func (s *PipelineStore) getSteps(ctx context.Context, pipelineID string, redact bool) ([]models.Step, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, pipeline_id, step_key, name, type, config, depends_on, step_order, created_at
		 FROM pipeline_steps WHERE pipeline_id = $1 ORDER BY step_order`, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("list steps: %w", err)
	}
	defer rows.Close()

	steps := make([]models.Step, 0)
	for rows.Next() {
		var st models.Step
		if err := rows.Scan(&st.ID, &st.PipelineID, &st.Key, &st.Name, &st.Type, &st.Config,
			pq.Array(&st.DependsOn), &st.StepOrder, &st.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan step: %w", err)
		}
		if redact {
			st.Config = models.RedactConfig(st.Config)
		}
		steps = append(steps, st)
	}
	return steps, rows.Err()
}
