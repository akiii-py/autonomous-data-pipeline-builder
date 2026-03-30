package scheduler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/akshat/pipeline-orchestrator/internal/dag"
	"github.com/akshat/pipeline-orchestrator/internal/dispatcher"
	"github.com/akshat/pipeline-orchestrator/internal/models"
	"github.com/akshat/pipeline-orchestrator/internal/store"
)

type Scheduler struct {
	store      *store.PipelineStore
	dispatcher dispatcher.Dispatcher
}

func New(s *store.PipelineStore, d dispatcher.Dispatcher) *Scheduler {
	return &Scheduler{store: s, dispatcher: d}
}

// ExecuteRun resolves DAG dependencies and executes ready steps in valid order.
func (s *Scheduler) ExecuteRun(ctx context.Context, pipelineID, runID string) error {
	pipeline, err := s.store.GetByID(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("load pipeline: %w", err)
	}
	if pipeline == nil {
		return fmt.Errorf("pipeline not found")
	}

	g, err := dag.BuildFromStoredSteps(pipeline.Steps)
	if err != nil {
		return fmt.Errorf("build dag: %w", err)
	}

	if err := s.store.UpdatePipelineRunStatus(ctx, runID, models.RunStatusRunning); err != nil {
		return fmt.Errorf("set run running: %w", err)
	}
	s.emitEvent(ctx, pipelineID, runID, "", "", "info", "run_started", "pipeline run started", nil)

	stepsByKey := make(map[string]models.Step, len(pipeline.Steps))
	for _, st := range pipeline.Steps {
		stepsByKey[st.Key] = st
	}

	completed := make(map[string]bool, len(stepsByKey))
	queued := make(map[string]bool, len(stepsByKey))
	for len(completed) < len(stepsByKey) {
		ready := readyStepKeys(g, completed, queued)
		if len(ready) == 0 {
			_ = s.store.UpdatePipelineRunStatus(ctx, runID, models.RunStatusFailed)
			s.emitEvent(ctx, pipelineID, runID, "", "", "error", "run_failed", "no schedulable steps remaining", nil)
			return fmt.Errorf("no schedulable steps remaining")
		}

		for _, key := range ready {
			step := stepsByKey[key]
			queued[key] = true
			s.emitEvent(ctx, pipelineID, runID, step.ID, step.Key, "info", "step_queued", "step is ready for execution", map[string]interface{}{"step_type": step.Type})

			retries := maxRetriesForStep(step)
			var execErr error
			for attempt := 0; attempt <= retries; attempt++ {
				if err := s.store.UpdateStepRunStatus(ctx, runID, step.ID, models.RunStatusRunning, ""); err != nil {
					_ = s.store.UpdatePipelineRunStatus(ctx, runID, models.RunStatusFailed)
					s.emitEvent(ctx, pipelineID, runID, step.ID, step.Key, "error", "run_failed", "failed to set step running", map[string]interface{}{"error": err.Error()})
					return fmt.Errorf("set step running: %w", err)
				}
				s.emitEvent(ctx, pipelineID, runID, step.ID, step.Key, "info", "step_running", "step execution started", map[string]interface{}{"attempt": attempt + 1, "max_attempts": retries + 1})

				execErr = s.dispatcher.ExecuteStep(ctx, runID, step)
				if execErr == nil {
					break
				}

				if attempt < retries {
					s.emitEvent(ctx, pipelineID, runID, step.ID, step.Key, "warn", "step_retry", "step execution failed, retrying", map[string]interface{}{"attempt": attempt + 1, "max_attempts": retries + 1, "error": execErr.Error()})
				}
			}

			if execErr != nil {
				_ = s.store.UpdateStepRunStatus(ctx, runID, step.ID, models.RunStatusFailed, execErr.Error())
				_ = s.store.UpdatePipelineRunStatus(ctx, runID, models.RunStatusFailed)
				s.emitEvent(ctx, pipelineID, runID, step.ID, step.Key, "error", "step_failed", "step execution failed", map[string]interface{}{"error": execErr.Error()})
				s.emitEvent(ctx, pipelineID, runID, "", "", "error", "run_failed", "pipeline run failed", map[string]interface{}{"failed_step_key": step.Key})
				return fmt.Errorf("execute step %s: %w", step.Key, execErr)
			}

			if err := s.store.UpdateStepRunStatus(ctx, runID, step.ID, models.RunStatusCompleted, ""); err != nil {
				_ = s.store.UpdatePipelineRunStatus(ctx, runID, models.RunStatusFailed)
				s.emitEvent(ctx, pipelineID, runID, step.ID, step.Key, "error", "run_failed", "failed to set step completed", map[string]interface{}{"error": err.Error()})
				return fmt.Errorf("set step completed: %w", err)
			}
			s.emitEvent(ctx, pipelineID, runID, step.ID, step.Key, "info", "step_completed", "step execution completed", nil)

			completed[key] = true
		}
	}

	if err := s.store.UpdatePipelineRunStatus(ctx, runID, models.RunStatusCompleted); err != nil {
		return fmt.Errorf("set run completed: %w", err)
	}
	s.emitEvent(ctx, pipelineID, runID, "", "", "info", "run_completed", "pipeline run completed", nil)

	return nil
}

func (s *Scheduler) emitEvent(
	ctx context.Context,
	pipelineID, runID, stepID, stepKey, level, eventType, message string,
	metadata map[string]interface{},
) {
	if err := s.store.CreateRunEvent(ctx, pipelineID, runID, stepID, stepKey, level, eventType, message, metadata); err != nil {
		// Event emission must not block scheduling progress.
		_ = err
	}
}

func readyStepKeys(g *dag.Graph, completed, queued map[string]bool) []string {
	ready := make([]string, 0)
	for _, n := range g.Nodes() {
		if completed[n.ID] || queued[n.ID] {
			continue
		}
		deps := g.DependsOn(n.ID)
		ok := true
		for _, dep := range deps {
			if !completed[dep] {
				ok = false
				break
			}
		}
		if ok {
			ready = append(ready, n.ID)
		}
	}
	return ready
}

func maxRetriesForStep(step models.Step) int {
	if len(step.Config) == 0 {
		return 0
	}

	var cfg map[string]interface{}
	if err := json.Unmarshal(step.Config, &cfg); err != nil {
		return 0
	}

	raw, ok := cfg["retry_count"]
	if !ok {
		return 0
	}

	switch v := raw.(type) {
	case float64:
		if v < 0 {
			return 0
		}
		return int(v)
	default:
		return 0
	}
}
