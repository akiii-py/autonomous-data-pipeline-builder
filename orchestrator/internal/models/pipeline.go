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
