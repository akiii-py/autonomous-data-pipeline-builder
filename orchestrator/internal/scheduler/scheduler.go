package scheduler

import (
	"context"
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

			if err := s.store.UpdateStepRunStatus(ctx, runID, step.ID, models.RunStatusRunning, ""); err != nil {
				_ = s.store.UpdatePipelineRunStatus(ctx, runID, models.RunStatusFailed)
				return fmt.Errorf("set step running: %w", err)
			}

			err := s.dispatcher.ExecuteStep(ctx, runID, step)
			if err != nil {
				_ = s.store.UpdateStepRunStatus(ctx, runID, step.ID, models.RunStatusFailed, err.Error())
				_ = s.store.UpdatePipelineRunStatus(ctx, runID, models.RunStatusFailed)
				return fmt.Errorf("execute step %s: %w", step.Key, err)
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
