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
}

type StepRun struct {
	ID            string     `json:"id"`
	PipelineRunID string     `json:"pipeline_run_id"`
	StepID        string     `json:"step_id"`
	Status        string     `json:"status"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	Error         string     `json:"error,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
}

type StepRunStatus struct {
	StepID     string     `json:"step_id"`
	StepKey    string     `json:"step_key"`
	StepName   string     `json:"step_name"`
	Status     string     `json:"status"`
	Error      string     `json:"error,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
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
	TotalSteps     int        `json:"total_steps"`
	CompletedSteps int        `json:"completed_steps"`
	FailedSteps    int        `json:"failed_steps"`
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

type MetricsResponse struct {
	PipelinesTotal   int64   `json:"pipelines_total"`
	RunsTotal        int64   `json:"runs_total"`
	RunsRunning      int64   `json:"runs_running"`
	RunsCompleted    int64   `json:"runs_completed"`
	RunsFailed       int64   `json:"runs_failed"`
	StepRunsFailed   int64   `json:"step_runs_failed"`
	AvgRunDurationS  float64 `json:"avg_run_duration_s"`
	GeneratedAtEpoch int64   `json:"generated_at_epoch"`
}

const (
	StatusDraft  = "draft"
	StatusActive = "active"
	StatusPaused = "paused"
	StatusFailed = "failed"
)

const (
	RunStatusPending   = "pending"
	RunStatusRunning   = "running"
	RunStatusCompleted = "completed"
	RunStatusFailed    = "failed"
	RunStatusSkipped   = "skipped"
)

const (
	StepTypeExtract   = "extract"
	StepTypeTransform = "transform"
	StepTypeLoad      = "load"
)
