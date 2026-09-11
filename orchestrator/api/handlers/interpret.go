package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"

	"github.com/akshat/pipeline-orchestrator/internal/catalog"
	"github.com/akshat/pipeline-orchestrator/internal/interpreter"
	"github.com/akshat/pipeline-orchestrator/internal/models"
	"github.com/akshat/pipeline-orchestrator/internal/validation"
)

const (
	interpretModeAuto           = "auto"
	interpretModeManualFallback = "manual_fallback"
)

type interpretRequest struct {
	Query      string `json:"query"`
	SourceHint string `json:"source_hint,omitempty"`
	TargetHint string `json:"target_hint,omitempty"`
	DryRun     bool   `json:"dry_run"`
}

type interpretResponse struct {
	Mode             string                       `json:"mode"`
	Query            string                       `json:"query"`
	Confidence       float64                      `json:"confidence,omitempty"`
	PipelineDraft    models.CreatePipelineRequest `json:"pipeline_draft"`
	RequiresApproval bool                         `json:"requires_approval"`
	Warnings         []string                     `json:"warnings,omitempty"`
	Errors           []string                     `json:"errors,omitempty"`
	FallbackReason   string                       `json:"fallback_reason,omitempty"`
}

// InterpretRequest converts natural language into a validated pipeline draft.
//
// The endpoint degrades rather than erroring, which is deliberate. What is new
// is that each degradation is recorded as a system event with its reason (D-18),
// so "the NLP service is dead" and "the model scored 0.68" are no longer the
// same observation from outside.
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
		h.degrade(w, r, query, 0, models.DegradationInterpreterNotConfigured,
			nil, []string{"nlp interpreter is not configured"})
		return
	}

	cat, err := h.catalogContext(r.Context())
	if err != nil {
		h.degrade(w, r, query, 0, models.DegradationInterpreterNotConfigured,
			[]string{err.Error()}, []string{"catalog unavailable, returning manual draft"})
		return
	}

	// The catalog goes out with the request and comes back in as the thing the
	// reply is validated against (rule 3.3).
	result, err := h.Interpreter.Interpret(r.Context(), interpreter.Request{
		Query:      query,
		SourceHint: strings.TrimSpace(req.SourceHint),
		TargetHint: strings.TrimSpace(req.TargetHint),
		DryRun:     req.DryRun,
		Catalog:    cat,
	})
	if err != nil {
		h.degrade(w, r, query, 0, models.DegradationNLPUnavailable,
			[]string{err.Error()}, []string{"nlp service unavailable, returning manual draft"})
		return
	}
	if result == nil {
		h.degrade(w, r, query, 0, models.DegradationInvalidNLPResponse,
			[]string{"empty nlp response"}, nil)
		return
	}

	draft := normalizeDraft(result.Pipeline, query)

	minConfidence := h.minConfidence()
	if result.Confidence < minConfidence {
		h.degrade(w, r, query, result.Confidence, models.DegradationLowConfidence, nil,
			append(result.Warnings, fmt.Sprintf("confidence %.2f below threshold %.2f", result.Confidence, minConfidence)))
		return
	}

	// Two stages, different inputs (D-13). Structural needs only the draft;
	// semantic needs the catalog it was generated against. Both return the full
	// error set, not the first problem (rule 5.2).
	problems := validation.Structural(draft)
	if len(problems) == 0 {
		problems = append(problems, validation.Semantic(draft, cat)...)
	}
	if len(problems) > 0 {
		h.degrade(w, r, query, result.Confidence, models.DegradationInvalidPipeline,
			problems.Messages(), append(result.Warnings, "nlp draft failed validation"))
		return
	}

	writeJSON(w, http.StatusOK, interpretResponse{
		Mode:             interpretModeAuto,
		Query:            query,
		Confidence:       result.Confidence,
		PipelineDraft:    draft,
		RequiresApproval: models.RequiresApproval(draft.Steps),
		Warnings:         result.Warnings,
	})
}

func (h *PipelineHandler) catalogContext(ctx context.Context) (*catalog.Context, error) {
	if h.Catalog == nil {
		return nil, fmt.Errorf("catalog provider is not configured")
	}
	return h.Catalog.Current(ctx)
}

func (h *PipelineHandler) minConfidence() float64 {
	if h.NLPMinConfidence <= 0 || h.NLPMinConfidence > 1 {
		return DefaultNLPMinConfidence
	}
	return h.NLPMinConfidence
}

// degrade records the degradation and returns the deterministic fallback draft.
// The reason is persisted before the reply is written, so a degradation that the
// client never reports is still counted (D-18).
func (h *PipelineHandler) degrade(
	w http.ResponseWriter,
	r *http.Request,
	query string,
	confidence float64,
	reason string,
	errs []string,
	warnings []string,
) {
	metadata := map[string]interface{}{"confidence": confidence}
	if len(errs) > 0 {
		metadata["errors"] = errs
	}

	detail := ""
	if len(errs) > 0 {
		detail = strings.Join(errs, "; ")
	}

	if h.Store != nil {
		if err := h.Store.RecordDegradation(
			r.Context(), models.DegradationComponentInterpret, reason, detail, metadata,
		); err != nil {
			log.Printf("record interpret degradation (%s): %v", reason, err)
		}
	}

	draft := fallbackDraft(query)
	writeJSON(w, http.StatusOK, interpretResponse{
		Mode:             interpretModeManualFallback,
		Query:            query,
		Confidence:       confidence,
		PipelineDraft:    draft,
		RequiresApproval: models.RequiresApproval(draft.Steps),
		Warnings:         warnings,
		Errors:           errs,
		FallbackReason:   reason,
	})
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

// fallbackDraft is the deterministic skeleton returned on every degradation. It
// declares input_from consistently with depends_on so that it satisfies the
// semantic stage's linkage check rather than only the structural one.
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

var slugCleaner = regexp.MustCompile(`[^a-z0-9]+`)

func slugFromQuery(query string) string {
	trimmed := strings.TrimSpace(strings.ToLower(query))
	if trimmed == "" {
		return "generated-pipeline"
	}

	clean := slugCleaner.ReplaceAllString(trimmed, "-")
	clean = strings.Trim(clean, "-")
	if len(clean) > 50 {
		clean = strings.Trim(clean[:50], "-")
	}
	if clean == "" {
		return "generated-pipeline"
	}

	return clean
}
