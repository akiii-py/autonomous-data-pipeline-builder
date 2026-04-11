package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/akshat/pipeline-orchestrator/internal/dag"
	"github.com/akshat/pipeline-orchestrator/internal/interpreter"
	"github.com/akshat/pipeline-orchestrator/internal/models"
)

const (
	interpretModeAuto           = "auto"
	interpretModeManualFallback = "manual_fallback"
	defaultNLPMinConfidence     = 0.70
)

type interpretRequest struct {
	Query      string `json:"query"`
	SourceHint string `json:"source_hint,omitempty"`
	TargetHint string `json:"target_hint,omitempty"`
	DryRun     bool   `json:"dry_run"`
}

type interpretResponse struct {
	Mode           string                       `json:"mode"`
	Query          string                       `json:"query"`
	Confidence     float64                      `json:"confidence,omitempty"`
	PipelineDraft  models.CreatePipelineRequest `json:"pipeline_draft"`
	Warnings       []string                     `json:"warnings,omitempty"`
	Errors         []string                     `json:"errors,omitempty"`
	FallbackReason string                       `json:"fallback_reason,omitempty"`
}

// InterpretRequest converts natural language input into a validated pipeline draft.
func (h *PipelineHandler) InterpretRequest(w http.ResponseWriter, r *http.Request) {
	var req interpretRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	query := strings.TrimSpace(req.Query)
	if query == "" {
		http.Error(w, `{"error":"query is required"}`, http.StatusBadRequest)
		return
	}

	if h.Interpreter == nil {
		h.respondManualFallback(w, query, 0, "interpreter_not_configured", nil, []string{"nlp interpreter is not configured"})
		return
	}

	result, err := h.Interpreter.Interpret(r.Context(), interpreter.Request{
		Query:      query,
		SourceHint: strings.TrimSpace(req.SourceHint),
		TargetHint: strings.TrimSpace(req.TargetHint),
		DryRun:     req.DryRun,
	})
	if err != nil {
		h.respondManualFallback(w, query, 0, "nlp_unavailable", []string{err.Error()}, []string{"nlp service unavailable, returning manual draft"})
		return
	}
	if result == nil {
		h.respondManualFallback(w, query, 0, "invalid_nlp_response", []string{"empty nlp response"}, nil)
		return
	}

	draft := normalizeDraft(result.Pipeline, query)
	minConfidence := h.minConfidence()
	if result.Confidence < minConfidence {
		h.respondManualFallback(
			w,
			query,
			result.Confidence,
			"low_confidence",
			nil,
			append(result.Warnings, fmt.Sprintf("confidence %.2f below threshold %.2f", result.Confidence, minConfidence)),
		)
		return
	}

	if err := validateDraft(draft); err != nil {
		h.respondManualFallback(w, query, result.Confidence, "invalid_pipeline", []string{err.Error()}, append(result.Warnings, "nlp draft failed validation"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(interpretResponse{
		Mode:          interpretModeAuto,
		Query:         query,
		Confidence:    result.Confidence,
		PipelineDraft: draft,
		Warnings:      result.Warnings,
	})
}

func (h *PipelineHandler) minConfidence() float64 {
	if h.NLPMinConfidence <= 0 || h.NLPMinConfidence > 1 {
		return defaultNLPMinConfidence
	}
	return h.NLPMinConfidence
}

func (h *PipelineHandler) respondManualFallback(
	w http.ResponseWriter,
	query string,
	confidence float64,
	reason string,
	errors []string,
	warnings []string,
) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(interpretResponse{
		Mode:           interpretModeManualFallback,
		Query:          query,
		Confidence:     confidence,
		PipelineDraft:  fallbackDraft(query),
		Warnings:       warnings,
		Errors:         errors,
		FallbackReason: reason,
	})
}

func validateDraft(draft models.CreatePipelineRequest) error {
	if strings.TrimSpace(draft.Name) == "" {
		return fmt.Errorf("pipeline name is required")
	}
	if len(draft.Steps) == 0 {
		return fmt.Errorf("at least one step is required")
	}

	for i, step := range draft.Steps {
		if strings.TrimSpace(step.Key) == "" {
			return fmt.Errorf("step at index %d is missing key", i)
		}
		if !isSupportedStepType(step.Type) {
			return fmt.Errorf("unsupported step type %q for step %q", step.Type, step.Key)
		}
	}

	if _, err := dag.BuildFromCreateSteps(draft.Steps); err != nil {
		return err
	}

	return nil
}

func isSupportedStepType(stepType string) bool {
	switch strings.TrimSpace(stepType) {
	case models.StepTypeExtract, models.StepTypeTransform, models.StepTypeLoad:
		return true
	default:
		return false
	}
}

func normalizeDraft(draft models.CreatePipelineRequest, query string) models.CreatePipelineRequest {
	draft.Name = strings.TrimSpace(draft.Name)
	if draft.Name == "" {
		draft.Name = slugFromQuery(query)
	}

	draft.Description = strings.TrimSpace(draft.Description)
	if draft.Description == "" {
		draft.Description = query
	}

	for i := range draft.Steps {
		draft.Steps[i].Key = strings.TrimSpace(draft.Steps[i].Key)
		draft.Steps[i].Name = strings.TrimSpace(draft.Steps[i].Name)
		draft.Steps[i].Type = strings.TrimSpace(draft.Steps[i].Type)
		if len(draft.Steps[i].Config) == 0 {
			draft.Steps[i].Config = json.RawMessage(`{}`)
		}
	}

	return draft
}

func fallbackDraft(query string) models.CreatePipelineRequest {
	return models.CreatePipelineRequest{
		Name:        slugFromQuery(query),
		Description: query,
		Steps: []models.CreateStepRequest{
			{
				Key:       "extract_source",
				Name:      "Extract Source",
				Type:      models.StepTypeExtract,
				Config:    json.RawMessage(`{}`),
				DependsOn: []string{},
			},
			{
				Key:       "transform_data",
				Name:      "Transform Data",
				Type:      models.StepTypeTransform,
				Config:    json.RawMessage(`{"input_from":"extract_source"}`),
				DependsOn: []string{"extract_source"},
			},
			{
				Key:       "load_output",
				Name:      "Load Output",
				Type:      models.StepTypeLoad,
				Config:    json.RawMessage(`{"input_from":"transform_data"}`),
				DependsOn: []string{"transform_data"},
			},
		},
	}
}

func slugFromQuery(query string) string {
	trimmed := strings.TrimSpace(strings.ToLower(query))
	if trimmed == "" {
		return "generated-pipeline"
	}

	clean := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(trimmed, "-")
	clean = strings.Trim(clean, "-")
	if clean == "" {
		return "generated-pipeline"
	}
	if len(clean) > 50 {
		clean = strings.Trim(clean[:50], "-")
	}
	if clean == "" {
		return "generated-pipeline"
	}

	return clean
}
