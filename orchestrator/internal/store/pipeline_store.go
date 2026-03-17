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
