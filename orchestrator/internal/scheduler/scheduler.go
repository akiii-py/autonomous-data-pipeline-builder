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
			return fmt.Errorf("no schedulable steps remaining")
		}

		for _, key := range ready {
			step := stepsByKey[key]
			queued[key] = true

			retries := maxRetriesForStep(step)
			var execErr error
			for attempt := 0; attempt <= retries; attempt++ {
				if err := s.store.UpdateStepRunStatus(ctx, runID, step.ID, models.RunStatusRunning, ""); err != nil {
					_ = s.store.UpdatePipelineRunStatus(ctx, runID, models.RunStatusFailed)
					return fmt.Errorf("set step running: %w", err)
				}

				execErr = s.dispatcher.ExecuteStep(ctx, runID, step)
				if execErr == nil {
					break
				}
			}

			if execErr != nil {
				_ = s.store.UpdateStepRunStatus(ctx, runID, step.ID, models.RunStatusFailed, execErr.Error())
				_ = s.store.UpdatePipelineRunStatus(ctx, runID, models.RunStatusFailed)
				return fmt.Errorf("execute step %s: %w", step.Key, execErr)
			}

			if err := s.store.UpdateStepRunStatus(ctx, runID, step.ID, models.RunStatusCompleted, ""); err != nil {
				_ = s.store.UpdatePipelineRunStatus(ctx, runID, models.RunStatusFailed)
				return fmt.Errorf("set step completed: %w", err)
			}

			completed[key] = true
		}
	}

	if err := s.store.UpdatePipelineRunStatus(ctx, runID, models.RunStatusCompleted); err != nil {
		return fmt.Errorf("set run completed: %w", err)
	}

	return nil
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
