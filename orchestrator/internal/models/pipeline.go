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
	Name      string          `json:"name"`
	Type      string          `json:"type"`
	Config    json.RawMessage `json:"config"`
	DependsOn []string        `json:"depends_on"`
}

const (
	StatusDraft  = "draft"
	StatusActive = "active"
	StatusPaused = "paused"
	StatusFailed = "failed"
)

const (
	StepTypeExtract   = "extract"
	StepTypeTransform = "transform"
	StepTypeLoad      = "load"
)
