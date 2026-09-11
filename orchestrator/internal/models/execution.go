package models

import "encoding/json"

// ErrorClass classifies a step failure so the retry decision has an input other
// than "an error occurred" (D-02). It is produced by the connector, carried over
// the wire in StepResult, and consumed by the scheduler.
type ErrorClass string

const (
	ErrorClassNone      ErrorClass = ""
	ErrorClassTransient ErrorClass = "transient"
	ErrorClassPermanent ErrorClass = "permanent"
)

// ExecMode names how a run was executed. Recorded on the run row (D-09).
const (
	ExecModeLocal  = "local"
	ExecModeWorker = "worker"
)

// StepRequest is the unit of work handed to a Dispatcher. IdempotencyKey and
// Attempt together establish execution identity (D-03) — the orchestrator mints
// the key, the worker records completion against it.
type StepRequest struct {
	RunID          string `json:"run_id"`
	PipelineID     string `json:"pipeline_id"`
	Step           Step   `json:"step"`
	Attempt        int    `json:"attempt"`
	IdempotencyKey string `json:"idempotency_key"`
}

// StepResult is the single value a step execution returns (D-01). Anything the
// orchestrator needs to know about an execution belongs here — never in a side
// channel.
type StepResult struct {
	RunID          string          `json:"run_id"`
	StepKey        string          `json:"step_key"`
	IdempotencyKey string          `json:"idempotency_key"`
	Attempt        int             `json:"attempt"`
	RowsProcessed  int64           `json:"rows_processed"`
	Simulated      bool            `json:"simulated"`
	Replayed       bool            `json:"replayed"`
	ErrorClass     ErrorClass      `json:"error_class,omitempty"`
	ErrorMessage   string          `json:"error_message,omitempty"`
	Detail         json.RawMessage `json:"detail,omitempty"`
}

// Failed reports whether the worker considered this execution a failure.
func (r *StepResult) Failed() bool {
	return r != nil && r.ErrorMessage != ""
}

// Retryable reports whether the scheduler may attempt this step again. A result
// with no classification is treated as permanent: retrying an unclassified
// failure is what the old tight loop did, and it is the behaviour D-02 exists to
// remove.
func (r *StepResult) Retryable() bool {
	return r != nil && r.ErrorClass == ErrorClassTransient
}

// Provenance describes how a dispatcher executes steps, so the run row can
// record what actually ran it (D-09).
type Provenance struct {
	ExecMode  string `json:"exec_mode"`
	Target    string `json:"target,omitempty"`
	Simulated bool   `json:"simulated"`
}
