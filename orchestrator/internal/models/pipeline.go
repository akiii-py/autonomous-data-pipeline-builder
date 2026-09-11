package models

import (
	"encoding/json"
	"time"
)

type Pipeline struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Status      string    `json:"status"`
	Steps       []Step    `json:"steps,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	// Approval stage (D-12). A pipeline containing an irreversible operation is
	// not runnable until a human moves it through POST /approve.
	RequiresApproval bool       `json:"requires_approval"`
	ApprovedAt       *time.Time `json:"approved_at,omitempty"`
	ApprovedBy       string     `json:"approved_by,omitempty"`
	DeletedAt        *time.Time `json:"deleted_at,omitempty"`
}

// Runnable reports whether this pipeline has cleared the approval stage.
func (p *Pipeline) Runnable() bool {
	if p == nil || p.DeletedAt != nil {
		return false
	}
	return !p.RequiresApproval || p.ApprovedAt != nil
}

type Step struct {
	ID         string          `json:"id"`
	PipelineID string          `json:"pipeline_id"`
	Key        string          `json:"key"`
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Config     json.RawMessage `json:"config"`
	DependsOn  []string        `json:"depends_on"`
	StepOrder  int             `json:"step_order"`
	CreatedAt  time.Time       `json:"created_at"`
}

type CreatePipelineRequest struct {
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Steps       []CreateStepRequest `json:"steps"`
}

type CreateStepRequest struct {
	Key       string          `json:"key"`
	Name      string          `json:"name"`
	Type      string          `json:"type"`
	Config    json.RawMessage `json:"config"`
	DependsOn []string        `json:"depends_on"`
}

type PipelineRun struct {
	ID         string     `json:"id"`
	PipelineID string     `json:"pipeline_id"`
	Status     string     `json:"status"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`

	// Execution provenance (D-09). A run is a record of something that
	// happened; omitting how it happened makes every aggregate over it
	// ambiguous.
	ExecMode  string `json:"exec_mode"`
	Simulated bool   `json:"simulated"`
	WorkerURL string `json:"worker_url,omitempty"`

	// Ownership (D-07). The row owns the run; a process holds it by claim.
	ClaimedBy       string     `json:"claimed_by,omitempty"`
	ClaimExpiresAt  *time.Time `json:"claim_expires_at,omitempty"`
	HeartbeatAt     *time.Time `json:"heartbeat_at,omitempty"`
	CancelRequested bool       `json:"cancel_requested"`
}

type StepRun struct {
	ID            string     `json:"id"`
	PipelineRunID string     `json:"pipeline_run_id"`
	StepID        string     `json:"step_id"`
	StepKey       string     `json:"step_key"`
	Status        string     `json:"status"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	Error         string     `json:"error,omitempty"`
	ErrorClass    string     `json:"error_class,omitempty"`
	Attempt       int        `json:"attempt"`
	RowsProcessed int64      `json:"rows_processed"`
	Simulated     bool       `json:"simulated"`
	CreatedAt     time.Time  `json:"created_at"`
}

type StepRunStatus struct {
	StepID        string     `json:"step_id"`
	StepKey       string     `json:"step_key"`
	StepName      string     `json:"step_name"`
	Status        string     `json:"status"`
	Error         string     `json:"error,omitempty"`
	ErrorClass    string     `json:"error_class,omitempty"`
	Attempt       int        `json:"attempt"`
	RowsProcessed int64      `json:"rows_processed"`
	Simulated     bool       `json:"simulated"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
}

type PipelineStatusResponse struct {
	PipelineID string          `json:"pipeline_id"`
	Run        *PipelineRun    `json:"run,omitempty"`
	Steps      []StepRunStatus `json:"steps,omitempty"`
}

type PipelineRunHistoryItem struct {
	ID             string     `json:"id"`
	PipelineID     string     `json:"pipeline_id"`
	Status         string     `json:"status"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	ExecMode       string     `json:"exec_mode"`
	Simulated      bool       `json:"simulated"`
	TotalSteps     int        `json:"total_steps"`
	CompletedSteps int        `json:"completed_steps"`
	FailedSteps    int        `json:"failed_steps"`
	RowsProcessed  int64      `json:"rows_processed"`
}

type RunEvent struct {
	ID         string          `json:"id"`
	PipelineID string          `json:"pipeline_id"`
	RunID      string          `json:"run_id,omitempty"`
	StepID     string          `json:"step_id,omitempty"`
	StepKey    string          `json:"step_key,omitempty"`
	Level      string          `json:"level"`
	EventType  string          `json:"event_type"`
	Message    string          `json:"message"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

// DegradationCount is one reason bucket in the degradation breakdown (D-18).
type DegradationCount struct {
	Component string `json:"component"`
	Reason    string `json:"reason"`
	Count     int64  `json:"count"`
}

type MetricsResponse struct {
	PipelinesTotal   int64              `json:"pipelines_total"`
	RunsTotal        int64              `json:"runs_total"`
	RunsRunning      int64              `json:"runs_running"`
	RunsCompleted    int64              `json:"runs_completed"`
	RunsFailed       int64              `json:"runs_failed"`
	RunsCancelled    int64              `json:"runs_cancelled"`
	RunsSimulated    int64              `json:"runs_simulated"`
	RunsReal         int64              `json:"runs_real"`
	RunsSuccessRate  float64            `json:"runs_success_rate"`
	RunsFailureRate  float64            `json:"runs_failure_rate"`
	StepRunsFailed   int64              `json:"step_runs_failed"`
	AvgRunDurationS  float64            `json:"avg_run_duration_s"`
	Degradations     []DegradationCount `json:"degradations"`
	GeneratedAtEpoch int64              `json:"generated_at_epoch"`
}

type Pagination struct {
	Limit    int `json:"limit"`
	Offset   int `json:"offset"`
	Returned int `json:"returned"`
}

type StepFailureMetric struct {
	StepKey  string `json:"step_key"`
	StepName string `json:"step_name"`
	Failures int64  `json:"failures"`
}

type PipelineMetricsResponse struct {
	PipelineID       string              `json:"pipeline_id"`
	RunsTotal        int64               `json:"runs_total"`
	RunsRunning      int64               `json:"runs_running"`
	RunsCompleted    int64               `json:"runs_completed"`
	RunsFailed       int64               `json:"runs_failed"`
	RunsCancelled    int64               `json:"runs_cancelled"`
	RunsSimulated    int64               `json:"runs_simulated"`
	RunsReal         int64               `json:"runs_real"`
	RunsSuccessRate  float64             `json:"runs_success_rate"`
	RunsFailureRate  float64             `json:"runs_failure_rate"`
	StepRunsFailed   int64               `json:"step_runs_failed"`
	AvgRunDurationS  float64             `json:"avg_run_duration_s"`
	TopFailedSteps   []StepFailureMetric `json:"top_failed_steps"`
	GeneratedAtEpoch int64               `json:"generated_at_epoch"`
}

type StepFailureBreakdownItem struct {
	StepKey      string     `json:"step_key"`
	StepName     string     `json:"step_name"`
	ErrorMessage string     `json:"error_message,omitempty"`
	ErrorClass   string     `json:"error_class,omitempty"`
	Simulated    bool       `json:"simulated"`
	Failures     int64      `json:"failures"`
	LastFailedAt *time.Time `json:"last_failed_at,omitempty"`
}

// Pipeline lifecycle (D-12).
const (
	StatusDraft           = "draft"
	StatusPendingApproval = "pending_approval"
	StatusApproved        = "approved"
	StatusActive          = "active"
	StatusPaused          = "paused"
	StatusFailed          = "failed"
)

// Run and step lifecycle. RunStatusQueued is the state a step sits in while it
// waits to be claimed from the Postgres work queue (D-08); RunStatusCancelled is
// the terminal state of a sibling cancelled because a peer failed (rule 4.2).
const (
	RunStatusPending   = "pending"
	RunStatusQueued    = "queued"
	RunStatusRunning   = "running"
	RunStatusCompleted = "completed"
	RunStatusFailed    = "failed"
	RunStatusCancelled = "cancelled"
	RunStatusSkipped   = "skipped"
)

const (
	StepTypeExtract   = "extract"
	StepTypeTransform = "transform"
	StepTypeLoad      = "load"
)

// Degradation components and reasons (D-18).
const (
	DegradationComponentInterpret = "interpret"

	DegradationInterpreterNotConfigured = "interpreter_not_configured"
	DegradationNLPUnavailable           = "nlp_unavailable"
	DegradationInvalidNLPResponse       = "invalid_nlp_response"
	DegradationLowConfidence            = "low_confidence"
	DegradationInvalidPipeline          = "invalid_pipeline"
)
