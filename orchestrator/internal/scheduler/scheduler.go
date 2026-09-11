package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/akshat/pipeline-orchestrator/internal/dag"
	"github.com/akshat/pipeline-orchestrator/internal/dispatcher"
	"github.com/akshat/pipeline-orchestrator/internal/models"
	"github.com/akshat/pipeline-orchestrator/internal/store"
)

// Options configures scheduling behaviour. Every value originates in
// internal/config — the scheduler reads no environment itself (rule 6.1).
type Options struct {
	// MaxConcurrency bounds how many steps of one ready set execute at once
	// (rule 4.3). Parallelism across steps belongs here because the scheduler is
	// the only component that knows the DAG (rule 4.1).
	MaxConcurrency int
	// BackoffBase and BackoffMax bound exponential retry backoff. Jitter is
	// applied so a ready set that fails together does not retry in lockstep.
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

func (o Options) withDefaults() Options {
	if o.MaxConcurrency < 1 {
		o.MaxConcurrency = 1
	}
	if o.BackoffBase <= 0 {
		o.BackoffBase = 200 * time.Millisecond
	}
	if o.BackoffMax <= 0 {
		o.BackoffMax = 10 * time.Second
	}
	return o
}

type Scheduler struct {
	store      *store.PipelineStore
	dispatcher dispatcher.Dispatcher
	opts       Options
}

func New(s *store.PipelineStore, d dispatcher.Dispatcher, opts Options) *Scheduler {
	return &Scheduler{store: s, dispatcher: d, opts: opts.withDefaults()}
}

// Provenance reports how this scheduler's dispatcher executes, so the caller can
// record it on the run at claim time (D-09).
func (s *Scheduler) Provenance() models.Provenance { return s.dispatcher.Provenance() }

// stepOutcome is one finished attempt, reported back from an executor goroutine.
type stepOutcome struct {
	key    string
	stepID string
	result *models.StepResult
	err    error
}

// ExecuteRun drives a claimed run to a terminal state.
//
// The context is the run's lifetime: it comes from the claim loop, is cancelled
// when the lease is lost or cancellation is requested, and reaches the worker
// HTTP call (rule 6.5). Every exit from this function passes through finish(),
// which writes a terminal status and a terminal event in one transaction — there
// is no path on which work completes and nothing records it (rule 5.4).
func (s *Scheduler) ExecuteRun(ctx context.Context, run *models.PipelineRun) (err error) {
	pipelineID := run.PipelineID
	runID := run.ID

	// A panic in a step must fail that run, not take the process down (audit
	// F-04). The recovered value becomes a terminal event like any other.
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("panic during run: %v", rec)
			s.finish(context.WithoutCancel(ctx), pipelineID, runID, models.RunStatusFailed,
				"run_failed", "pipeline run panicked", map[string]interface{}{"panic": fmt.Sprint(rec)}, nil)
		}
	}()

	pipeline, err := s.store.GetForExecution(ctx, pipelineID)
	if err != nil {
		s.finish(ctx, pipelineID, runID, models.RunStatusFailed, "run_failed",
			"failed to load pipeline", map[string]interface{}{"error": err.Error()}, nil)
		return fmt.Errorf("load pipeline: %w", err)
	}
	if pipeline == nil {
		s.finish(ctx, pipelineID, runID, models.RunStatusFailed, "run_failed",
			"pipeline not found", nil, nil)
		return fmt.Errorf("pipeline not found")
	}

	g, err := dag.BuildFromStoredSteps(pipeline.Steps)
	if err != nil {
		s.finish(ctx, pipelineID, runID, models.RunStatusFailed, "run_failed",
			"stored pipeline is not a valid dag", map[string]interface{}{"error": err.Error()}, nil)
		return fmt.Errorf("build dag: %w", err)
	}

	stepsByKey := make(map[string]models.Step, len(pipeline.Steps))
	for _, st := range pipeline.Steps {
		stepsByKey[st.Key] = st
	}

	if err := s.store.TransitionRun(ctx, runID, models.RunStatusRunning, store.EventInput{
		PipelineID: pipelineID,
		RunID:      runID,
		Level:      "info",
		EventType:  "run_started",
		Message:    "pipeline run started",
		Metadata: map[string]interface{}{
			"exec_mode":       run.ExecMode,
			"simulated":       run.Simulated,
			"max_concurrency": s.opts.MaxConcurrency,
		},
	}); err != nil {
		return fmt.Errorf("set run running: %w", err)
	}

	completed := make(map[string]bool, len(stepsByKey))

	for len(completed) < len(stepsByKey) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			s.finish(context.WithoutCancel(ctx), pipelineID, runID, models.RunStatusCancelled,
				"run_cancelled", "pipeline run cancelled", map[string]interface{}{"reason": ctxErr.Error()}, nil)
			return ctxErr
		}

		ready := readyStepKeys(g, completed)
		if len(ready) == 0 {
			s.finish(ctx, pipelineID, runID, models.RunStatusFailed, "run_failed",
				"no schedulable steps remaining", nil, nil)
			return fmt.Errorf("no schedulable steps remaining")
		}

		failure, cancelled, err := s.runReadySet(ctx, pipeline.ID, run, ready, stepsByKey, completed)
		if err != nil {
			s.finish(context.WithoutCancel(ctx), pipelineID, runID, models.RunStatusFailed,
				"run_failed", "scheduling error", map[string]interface{}{"error": err.Error()}, nil)
			return err
		}
		if cancelled {
			s.finish(context.WithoutCancel(ctx), pipelineID, runID, models.RunStatusCancelled,
				"run_cancelled", "pipeline run cancelled", nil, nil)
			return context.Canceled
		}
		if failure != nil {
			s.finish(context.WithoutCancel(ctx), pipelineID, runID, models.RunStatusFailed,
				"run_failed", "pipeline run failed",
				map[string]interface{}{
					"failed_step_key": failure.key,
					"error":           failure.message,
					"error_class":     failure.class,
				}, nil)
			return fmt.Errorf("execute step %s: %s", failure.key, failure.message)
		}
	}

	s.finish(ctx, pipelineID, runID, models.RunStatusCompleted, "run_completed",
		"pipeline run completed", map[string]interface{}{"simulated": run.Simulated}, nil)
	return nil
}

type runFailure struct {
	key     string
	message string
	class   string
}

// runReadySet enqueues one ready set and executes it with bounded concurrency.
//
// Sibling cancellation (rule 4.2): the first step to fail cancels the set's
// context, so in-flight siblings stop rather than completing work for a run that
// is already doomed. Each cancelled sibling still reaches a terminal state and
// still emits an event — a partially-applied ready set must be visible.
func (s *Scheduler) runReadySet(
	ctx context.Context,
	pipelineID string,
	run *models.PipelineRun,
	ready []string,
	stepsByKey map[string]models.Step,
	completed map[string]bool,
) (*runFailure, bool, error) {
	setCtx, cancelSet := context.WithCancel(ctx)
	defer cancelSet()

	stepIDs := make([]string, 0, len(ready))
	events := make([]store.EventInput, 0, len(ready))
	for _, key := range ready {
		step := stepsByKey[key]
		stepIDs = append(stepIDs, step.ID)
		events = append(events, store.EventInput{
			PipelineID: pipelineID,
			RunID:      run.ID,
			StepID:     step.ID,
			StepKey:    step.Key,
			Level:      "info",
			EventType:  "step_queued",
			Message:    "step is ready for execution",
			Metadata:   map[string]interface{}{"step_type": step.Type},
		})
	}

	if err := s.store.EnqueueSteps(ctx, run.ID, stepIDs, events); err != nil {
		return nil, false, err
	}

	byStepID := make(map[string]models.Step, len(ready))
	for _, key := range ready {
		byStepID[stepsByKey[key].ID] = stepsByKey[key]
	}

	workers := s.opts.MaxConcurrency
	if workers > len(ready) {
		workers = len(ready)
	}

	outcomes := make(chan stepOutcome, len(ready))
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// Claiming from the queue rather than iterating a slice is what
				// keeps this safe when more than one process drives the same run
				// (D-08).
				claimed, err := s.store.ClaimQueuedStep(ctx, run.ID)
				if err != nil {
					outcomes <- stepOutcome{err: err}
					return
				}
				if claimed == nil {
					return
				}

				step, ok := byStepID[claimed.StepID]
				if !ok {
					outcomes <- stepOutcome{err: fmt.Errorf("claimed unknown step %s", claimed.StepID)}
					return
				}

				result, execErr := s.executeStep(setCtx, pipelineID, run, step, claimed.Attempt)
				outcomes <- stepOutcome{key: step.Key, stepID: step.ID, result: result, err: execErr}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outcomes)
	}()

	var failure *runFailure
	var schedErr error

	for out := range outcomes {
		if out.err != nil {
			if schedErr == nil {
				schedErr = out.err
			}
			cancelSet()
			continue
		}
		if out.result.Failed() {
			if failure == nil {
				failure = &runFailure{
					key:     out.key,
					message: out.result.ErrorMessage,
					class:   string(out.result.ErrorClass),
				}
				// First failure cancels the rest of the set.
				cancelSet()
			}
			continue
		}
		completed[out.key] = true
	}

	if schedErr != nil {
		return nil, false, schedErr
	}
	if failure != nil {
		return failure, false, nil
	}
	if ctx.Err() != nil {
		return nil, true, nil
	}
	return nil, false, nil
}

// executeStep runs one step, retrying only failures the connector classified as
// transient (D-02). A permanent failure — malformed SQL, a rejected request —
// consumes no retry budget, because it will fail identically forever.
func (s *Scheduler) executeStep(
	ctx context.Context,
	pipelineID string,
	run *models.PipelineRun,
	step models.Step,
	firstAttempt int,
) (*models.StepResult, error) {
	cfg, err := models.ParseStepConfig(step.Config)
	if err != nil {
		return nil, fmt.Errorf("parse step config for %s: %w", step.Key, err)
	}
	maxRetries := cfg.MaxRetries()

	var result *models.StepResult
	attempt := firstAttempt

	for {
		if err := s.emit(ctx, store.EventInput{
			PipelineID: pipelineID,
			RunID:      run.ID,
			StepID:     step.ID,
			StepKey:    step.Key,
			Level:      "info",
			EventType:  "step_running",
			Message:    "step execution started",
			Metadata: map[string]interface{}{
				"attempt":      attempt,
				"max_attempts": firstAttempt + maxRetries,
			},
		}); err != nil {
			return nil, err
		}

		req := models.StepRequest{
			RunID:          run.ID,
			PipelineID:     pipelineID,
			Step:           step,
			Attempt:        attempt,
			IdempotencyKey: store.IdempotencyKey(run.ID, step.ID, attempt),
		}

		result, err = s.dispatcher.ExecuteStep(ctx, req)
		if err != nil {
			// The attempt could not be made at all. That is a configuration
			// fault, not a transient one.
			result = &models.StepResult{
				RunID:          req.RunID,
				StepKey:        step.Key,
				IdempotencyKey: req.IdempotencyKey,
				Attempt:        attempt,
				ErrorClass:     models.ErrorClassPermanent,
				ErrorMessage:   err.Error(),
			}
		}

		if !result.Failed() {
			break
		}

		retriesUsed := attempt - firstAttempt
		if !result.Retryable() || retriesUsed >= maxRetries || ctx.Err() != nil {
			break
		}

		delay := s.backoff(retriesUsed)
		if emitErr := s.emit(ctx, store.EventInput{
			PipelineID: pipelineID,
			RunID:      run.ID,
			StepID:     step.ID,
			StepKey:    step.Key,
			Level:      "warn",
			EventType:  "step_retry",
			Message:    "step execution failed, retrying",
			Metadata: map[string]interface{}{
				"attempt":     attempt,
				"error":       result.ErrorMessage,
				"error_class": string(result.ErrorClass),
				"backoff_ms":  delay.Milliseconds(),
			},
		}); emitErr != nil {
			return nil, emitErr
		}

		select {
		case <-ctx.Done():
			result.ErrorClass = models.ErrorClassPermanent
			result.ErrorMessage = fmt.Sprintf("cancelled during backoff: %v", ctx.Err())
			return s.recordTerminal(ctx, pipelineID, run, step, result)
		case <-time.After(delay):
		}

		attempt++
	}

	return s.recordTerminal(ctx, pipelineID, run, step, result)
}

// recordTerminal writes the step's terminal status and event together. It uses a
// cancellation-free context so that a cancelled run still records what happened
// to the step that was in flight (rule 5.4).
func (s *Scheduler) recordTerminal(
	ctx context.Context,
	pipelineID string,
	run *models.PipelineRun,
	step models.Step,
	result *models.StepResult,
) (*models.StepResult, error) {
	writeCtx := context.WithoutCancel(ctx)

	status := models.RunStatusCompleted
	level := "info"
	eventType := "step_completed"
	message := "step execution completed"

	if result.Failed() {
		status = models.RunStatusFailed
		level = "error"
		eventType = "step_failed"
		message = "step execution failed"

		if errors.Is(ctx.Err(), context.Canceled) {
			status = models.RunStatusCancelled
			eventType = "step_cancelled"
			message = "step cancelled because a sibling step failed"
			level = "warn"
		}
	}

	metadata := map[string]interface{}{
		"attempt":        result.Attempt,
		"rows_processed": result.RowsProcessed,
		"simulated":      result.Simulated,
		"replayed":       result.Replayed,
	}
	if result.Failed() {
		metadata["error"] = result.ErrorMessage
		metadata["error_class"] = string(result.ErrorClass)
	}

	err := s.store.TransitionStep(writeCtx, run.ID,
		store.NewStepTransition(step.ID, status, result),
		store.EventInput{
			PipelineID: pipelineID,
			RunID:      run.ID,
			StepID:     step.ID,
			StepKey:    step.Key,
			Level:      level,
			EventType:  eventType,
			Message:    message,
			Metadata:   metadata,
		})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// finish writes the run's terminal status and terminal event in one
// transaction, cancelling any step still in flight and releasing the run's
// artifacts (D-06 cleanup, rule 5.4).
func (s *Scheduler) finish(
	ctx context.Context,
	pipelineID, runID, status, eventType, message string,
	metadata map[string]interface{},
	siblingEvents []store.EventInput,
) {
	level := "info"
	if status != models.RunStatusCompleted {
		level = "error"
	}

	err := s.store.FinishRun(ctx, runID, status, store.EventInput{
		PipelineID: pipelineID,
		RunID:      runID,
		Level:      level,
		EventType:  eventType,
		Message:    message,
		Metadata:   metadata,
	}, siblingEvents)
	if err != nil {
		// The event log is authoritative, so a failed terminal write is a real
		// problem and must be visible rather than discarded (rule 5.3). The run
		// keeps its claim and is reclaimed by the lease sweep.
		logTerminalWriteFailure(runID, status, err)
	}
}

// emit writes a progress event that is not itself a status transition.
// Transitions never use this — they go through the store's transactional
// methods so status and event share one write (rule 5.3).
func (s *Scheduler) emit(ctx context.Context, ev store.EventInput) error {
	return s.store.EmitEvent(context.WithoutCancel(ctx), ev)
}

// logTerminalWriteFailure reports a terminal transition that could not be
// persisted. Because the event log is authoritative, this is the one case where
// the system knows something happened and could not record it — it must be loud.
// The run keeps its claim, so the lease sweep will reclaim and re-drive it.
func logTerminalWriteFailure(runID, status string, err error) {
	log.Printf("CRITICAL: failed to write terminal state for run=%s status=%s: %v", runID, status, err)
}

// backoff is exponential with jitter, so a ready set that fails together does
// not retry in lockstep. The jittered value stays within [d/2, d] so that
// BackoffMax is a real ceiling rather than an average.
func (s *Scheduler) backoff(retriesUsed int) time.Duration {
	d := s.opts.BackoffBase << retriesUsed
	if d > s.opts.BackoffMax || d <= 0 {
		d = s.opts.BackoffMax
	}

	half := int64(d) / 2
	if half <= 0 {
		return d
	}
	return time.Duration(half + rand.Int63n(half+1))
}

// readyStepKeys recomputes the ready set from completion state each pass. A step
// is ready when every dependency has completed.
func readyStepKeys(g *dag.Graph, completed map[string]bool) []string {
	ready := make([]string, 0)
	for _, n := range g.Nodes() {
		if completed[n.ID] {
			continue
		}
		ok := true
		for _, dep := range g.DependsOn(n.ID) {
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
