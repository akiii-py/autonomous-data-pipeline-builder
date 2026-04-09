package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/akshat/pipeline-orchestrator/internal/dag"
	"github.com/akshat/pipeline-orchestrator/internal/models"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type PipelineStore struct {
	db *sql.DB
}

func NewPipelineStore(db *sql.DB) *PipelineStore {
	return &PipelineStore{db: db}
}

func (s *PipelineStore) List(ctx context.Context) ([]models.Pipeline, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, description, status, created_at, updated_at
		 FROM pipelines ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	defer rows.Close()

	var pipelines []models.Pipeline
	for rows.Next() {
		var p models.Pipeline
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.Status, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan pipeline: %w", err)
		}
		pipelines = append(pipelines, p)
	}
	return pipelines, rows.Err()
}

func (s *PipelineStore) GetByID(ctx context.Context, id string) (*models.Pipeline, error) {
	var p models.Pipeline
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, description, status, created_at, updated_at
		 FROM pipelines WHERE id = $1`, id).
		Scan(&p.ID, &p.Name, &p.Description, &p.Status, &p.CreatedAt, &p.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get pipeline: %w", err)
	}

	steps, err := s.getSteps(ctx, id)
	if err != nil {
		return nil, err
	}
	p.Steps = steps
	return &p, nil
}

func (s *PipelineStore) Create(ctx context.Context, req models.CreatePipelineRequest) (*models.Pipeline, error) {
	if _, err := dag.BuildFromCreateSteps(req.Steps); err != nil {
		return nil, fmt.Errorf("invalid pipeline dag: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	pipelineID := uuid.New().String()
	var p models.Pipeline
	err = tx.QueryRowContext(ctx,
		`INSERT INTO pipelines (id, name, description, status)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id, name, description, status, created_at, updated_at`,
		pipelineID, req.Name, req.Description, models.StatusDraft).
		Scan(&p.ID, &p.Name, &p.Description, &p.Status, &p.CreatedAt, &p.UpdatedAt)
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
		p.Steps = append(p.Steps, step)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return &p, nil
}

func (s *PipelineStore) Delete(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM pipelines WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete pipeline: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("pipeline not found")
	}
	return nil
}

func (s *PipelineStore) CreateRun(ctx context.Context, pipelineID string) (*models.PipelineRun, error) {
	steps, err := s.getSteps(ctx, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("load steps: %w", err)
	}
	if len(steps) == 0 {
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
		 RETURNING id, pipeline_id, status, started_at, finished_at, created_at`,
		runID, pipelineID, models.RunStatusPending,
	).Scan(&run.ID, &run.PipelineID, &run.Status, &run.StartedAt, &run.FinishedAt, &run.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert pipeline run: %w", err)
	}

	for _, st := range steps {
		stepRunID := uuid.New().String()
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO step_runs (id, pipeline_run_id, step_id, status)
			 VALUES ($1, $2, $3, $4)`,
			stepRunID, runID, st.ID, models.RunStatusPending,
		); err != nil {
			return nil, fmt.Errorf("insert step run: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit run tx: %w", err)
	}

	return &run, nil
}

func (s *PipelineStore) UpdatePipelineRunStatus(ctx context.Context, runID, status string) error {
	now := time.Now().UTC()
	switch status {
	case models.RunStatusRunning:
		_, err := s.db.ExecContext(ctx,
			`UPDATE pipeline_runs
			 SET status = $1, started_at = COALESCE(started_at, $2)
			 WHERE id = $3`,
			status, now, runID,
		)
		if err != nil {
			return fmt.Errorf("update run running: %w", err)
		}
		return nil
	case models.RunStatusCompleted, models.RunStatusFailed:
		_, err := s.db.ExecContext(ctx,
			`UPDATE pipeline_runs
			 SET status = $1,
			     started_at = COALESCE(started_at, $2),
			     finished_at = $2
			 WHERE id = $3`,
			status, now, runID,
		)
		if err != nil {
			return fmt.Errorf("update run terminal: %w", err)
		}
		return nil
	default:
		_, err := s.db.ExecContext(ctx,
			`UPDATE pipeline_runs SET status = $1 WHERE id = $2`,
			status, runID,
		)
		if err != nil {
			return fmt.Errorf("update run status: %w", err)
		}
		return nil
	}
}

func (s *PipelineStore) UpdateStepRunStatus(ctx context.Context, runID, stepID, status, errText string) error {
	now := time.Now().UTC()
	switch status {
	case models.RunStatusRunning:
		_, err := s.db.ExecContext(ctx,
			`UPDATE step_runs
			 SET status = $1, started_at = COALESCE(started_at, $2)
			 WHERE pipeline_run_id = $3 AND step_id = $4`,
			status, now, runID, stepID,
		)
		if err != nil {
			return fmt.Errorf("update step running: %w", err)
		}
		return nil
	case models.RunStatusCompleted, models.RunStatusFailed, models.RunStatusSkipped:
		_, err := s.db.ExecContext(ctx,
			`UPDATE step_runs
			 SET status = $1,
			     error = $2,
			     started_at = COALESCE(started_at, $3),
			     finished_at = $3
			 WHERE pipeline_run_id = $4 AND step_id = $5`,
			status, errText, now, runID, stepID,
		)
		if err != nil {
			return fmt.Errorf("update step terminal: %w", err)
		}
		return nil
	default:
		_, err := s.db.ExecContext(ctx,
			`UPDATE step_runs SET status = $1, error = $2 WHERE pipeline_run_id = $3 AND step_id = $4`,
			status, errText, runID, stepID,
		)
		if err != nil {
			return fmt.Errorf("update step status: %w", err)
		}
		return nil
	}
}

func (s *PipelineStore) GetLatestRunStatus(ctx context.Context, pipelineID string) (*models.PipelineStatusResponse, error) {
	var run models.PipelineRun
	err := s.db.QueryRowContext(ctx,
		`SELECT id, pipeline_id, status, started_at, finished_at, created_at
		 FROM pipeline_runs
		 WHERE pipeline_id = $1
		 ORDER BY created_at DESC
		 LIMIT 1`,
		pipelineID,
	).Scan(&run.ID, &run.PipelineID, &run.Status, &run.StartedAt, &run.FinishedAt, &run.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("latest run: %w", err)
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT sr.step_id, ps.step_key, ps.name, sr.status, sr.error, sr.started_at, sr.finished_at
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
		if err := rows.Scan(&srs.StepID, &srs.StepKey, &srs.StepName, &srs.Status, &srs.Error, &srs.StartedAt, &srs.FinishedAt); err != nil {
			return nil, fmt.Errorf("scan step run: %w", err)
		}
		steps = append(steps, srs)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &models.PipelineStatusResponse{
		PipelineID: pipelineID,
		Run:        &run,
		Steps:      steps,
	}, nil
}

func (s *PipelineStore) GetRunStatusByID(ctx context.Context, pipelineID, runID string) (*models.PipelineStatusResponse, error) {
	var run models.PipelineRun
	err := s.db.QueryRowContext(ctx,
		`SELECT id, pipeline_id, status, started_at, finished_at, created_at
		 FROM pipeline_runs
		 WHERE id = $1 AND pipeline_id = $2`,
		runID, pipelineID,
	).Scan(&run.ID, &run.PipelineID, &run.Status, &run.StartedAt, &run.FinishedAt, &run.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("run by id: %w", err)
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT sr.step_id, ps.step_key, ps.name, sr.status, sr.error, sr.started_at, sr.finished_at
		 FROM step_runs sr
		 JOIN pipeline_steps ps ON ps.id = sr.step_id
		 WHERE sr.pipeline_run_id = $1
		 ORDER BY ps.step_order`,
		run.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("list step runs by id: %w", err)
	}
	defer rows.Close()

	steps := make([]models.StepRunStatus, 0)
	for rows.Next() {
		var srs models.StepRunStatus
		if err := rows.Scan(&srs.StepID, &srs.StepKey, &srs.StepName, &srs.Status, &srs.Error, &srs.StartedAt, &srs.FinishedAt); err != nil {
			return nil, fmt.Errorf("scan step run by id: %w", err)
		}
		steps = append(steps, srs)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &models.PipelineStatusResponse{
		PipelineID: pipelineID,
		Run:        &run,
		Steps:      steps,
	}, nil
}

func (s *PipelineStore) ListRuns(ctx context.Context, pipelineID, status string, limit, offset int) ([]models.PipelineRunHistoryItem, error) {
	query := `
	SELECT
		pr.id,
		pr.pipeline_id,
		pr.status,
		pr.started_at,
		pr.finished_at,
		pr.created_at,
		COALESCE(COUNT(sr.step_id), 0) AS total_steps,
		COALESCE(SUM(CASE WHEN sr.status = 'completed' THEN 1 ELSE 0 END), 0) AS completed_steps,
		COALESCE(SUM(CASE WHEN sr.status = 'failed' THEN 1 ELSE 0 END), 0) AS failed_steps
	FROM pipeline_runs pr
	LEFT JOIN step_runs sr ON sr.pipeline_run_id = pr.id
	WHERE pr.pipeline_id = $1`

	args := []interface{}{pipelineID}
	if status != "" {
		query += ` AND pr.status = $2`
		args = append(args, status)
		query += ` GROUP BY pr.id ORDER BY pr.created_at DESC LIMIT $3 OFFSET $4`
		args = append(args, limit, offset)
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
			&item.ID,
			&item.PipelineID,
			&item.Status,
			&item.StartedAt,
			&item.FinishedAt,
			&item.CreatedAt,
			&item.TotalSteps,
			&item.CompletedSteps,
			&item.FailedSteps,
		); err != nil {
			return nil, fmt.Errorf("scan run history: %w", err)
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

func (s *PipelineStore) CreateRunEvent(
	ctx context.Context,
	pipelineID, runID, stepID, stepKey, level, eventType, message string,
	metadata map[string]interface{},
) error {
	meta := json.RawMessage(`{}`)
	if metadata != nil {
		b, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("marshal event metadata: %w", err)
		}
		meta = b
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO pipeline_run_events
		 (id, pipeline_id, run_id, step_id, step_key, level, event_type, message, metadata)
		 VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, $6, $7, $8, $9)`,
		uuid.New().String(),
		pipelineID,
		runID,
		stepID,
		stepKey,
		level,
		eventType,
		message,
		meta,
	)
	if err != nil {
		return fmt.Errorf("insert run event: %w", err)
	}

	return nil
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
		if err := rows.Scan(
			&item.ID,
			&item.PipelineID,
			&item.RunID,
			&item.StepID,
			&item.StepKey,
			&item.Level,
			&item.EventType,
			&item.Message,
			&item.Metadata,
			&item.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan run event: %w", err)
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

func (s *PipelineStore) GetMetrics(ctx context.Context) (*models.MetricsResponse, error) {
	metrics := &models.MetricsResponse{GeneratedAtEpoch: time.Now().UTC().Unix()}

	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pipelines`).Scan(&metrics.PipelinesTotal); err != nil {
		return nil, fmt.Errorf("count pipelines: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pipeline_runs`).Scan(&metrics.RunsTotal); err != nil {
		return nil, fmt.Errorf("count runs: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pipeline_runs WHERE status = $1`, models.RunStatusRunning).Scan(&metrics.RunsRunning); err != nil {
		return nil, fmt.Errorf("count running runs: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pipeline_runs WHERE status = $1`, models.RunStatusCompleted).Scan(&metrics.RunsCompleted); err != nil {
		return nil, fmt.Errorf("count completed runs: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pipeline_runs WHERE status = $1`, models.RunStatusFailed).Scan(&metrics.RunsFailed); err != nil {
		return nil, fmt.Errorf("count failed runs: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM step_runs WHERE status = $1`, models.RunStatusFailed).Scan(&metrics.StepRunsFailed); err != nil {
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

	return metrics, nil
}

func (s *PipelineStore) GetPipelineMetrics(ctx context.Context, pipelineID string) (*models.PipelineMetricsResponse, error) {
	metrics := &models.PipelineMetricsResponse{
		PipelineID:       pipelineID,
		TopFailedSteps:   make([]models.StepFailureMetric, 0),
		GeneratedAtEpoch: time.Now().UTC().Unix(),
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_runs WHERE pipeline_id = $1`, pipelineID,
	).Scan(&metrics.RunsTotal); err != nil {
		return nil, fmt.Errorf("count pipeline runs: %w", err)
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_runs WHERE pipeline_id = $1 AND status = $2`, pipelineID, models.RunStatusRunning,
	).Scan(&metrics.RunsRunning); err != nil {
		return nil, fmt.Errorf("count pipeline running runs: %w", err)
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_runs WHERE pipeline_id = $1 AND status = $2`, pipelineID, models.RunStatusCompleted,
	).Scan(&metrics.RunsCompleted); err != nil {
		return nil, fmt.Errorf("count pipeline completed runs: %w", err)
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_runs WHERE pipeline_id = $1 AND status = $2`, pipelineID, models.RunStatusFailed,
	).Scan(&metrics.RunsFailed); err != nil {
		return nil, fmt.Errorf("count pipeline failed runs: %w", err)
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
		pipelineID,
		models.RunStatusFailed,
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
		pipelineID,
		models.RunStatusFailed,
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pipeline failed steps: %w", err)
	}

	return metrics, nil
}

func (s *PipelineStore) GetPipelineFailureBreakdown(ctx context.Context, pipelineID string, limit, offset int) ([]models.StepFailureBreakdownItem, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT
			ps.step_key,
			ps.name,
			COALESCE(NULLIF(sr.error, ''), '') AS error_message,
			COUNT(*) AS failures,
			MAX(COALESCE(sr.finished_at, sr.created_at)) AS last_failed_at
		 FROM step_runs sr
		 JOIN pipeline_runs pr ON pr.id = sr.pipeline_run_id
		 JOIN pipeline_steps ps ON ps.id = sr.step_id
		 WHERE pr.pipeline_id = $1 AND sr.status = $2
		 GROUP BY ps.step_key, ps.name, sr.error
		 ORDER BY failures DESC, last_failed_at DESC
		 LIMIT $3 OFFSET $4`,
		pipelineID,
		models.RunStatusFailed,
		limit,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("list pipeline failure breakdown: %w", err)
	}
	defer rows.Close()

	items := make([]models.StepFailureBreakdownItem, 0)
	for rows.Next() {
		var item models.StepFailureBreakdownItem
		if err := rows.Scan(&item.StepKey, &item.StepName, &item.ErrorMessage, &item.Failures, &item.LastFailedAt); err != nil {
			return nil, fmt.Errorf("scan pipeline failure breakdown: %w", err)
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pipeline failure breakdown: %w", err)
	}

	return items, nil
}

func (s *PipelineStore) getSteps(ctx context.Context, pipelineID string) ([]models.Step, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, pipeline_id, step_key, name, type, config, depends_on, step_order, created_at
		 FROM pipeline_steps WHERE pipeline_id = $1 ORDER BY step_order`, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("list steps: %w", err)
	}
	defer rows.Close()

	var steps []models.Step
	for rows.Next() {
		var st models.Step
		if err := rows.Scan(&st.ID, &st.PipelineID, &st.Key, &st.Name, &st.Type, &st.Config,
			pq.Array(&st.DependsOn), &st.StepOrder, &st.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan step: %w", err)
		}
		steps = append(steps, st)
	}
	return steps, rows.Err()
}
