package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/akshat/pipeline-orchestrator/internal/interpreter"
	"github.com/akshat/pipeline-orchestrator/internal/models"
)

type stubInterpreter struct {
	result *interpreter.Result
	err    error
}

func (s stubInterpreter) Interpret(ctx context.Context, req interpreter.Request) (*interpreter.Result, error) {
	return s.result, s.err
}

func TestInterpretRequestInvalidBody(t *testing.T) {
	h := &PipelineHandler{}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/interpret", strings.NewReader("{"))
	rec := httptest.NewRecorder()

	h.InterpretRequest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestInterpretRequestAutoMode(t *testing.T) {
	h := &PipelineHandler{
		Interpreter: stubInterpreter{
			result: &interpreter.Result{
				Confidence: 0.92,
				Pipeline: models.CreatePipelineRequest{
					Name:        "sales-pipeline",
					Description: "Generated from prompt",
					Steps: []models.CreateStepRequest{
						{
							Key:       "extract_sales",
							Name:      "Extract Sales",
							Type:      models.StepTypeExtract,
							Config:    json.RawMessage(`{}`),
							DependsOn: []string{},
						},
					},
				},
			},
		},
		NLPMinConfidence: 0.7,
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/interpret", strings.NewReader(`{"query":"build a sales pipeline"}`))
	rec := httptest.NewRecorder()

	h.InterpretRequest(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp interpretResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Mode != interpretModeAuto {
		t.Fatalf("expected mode %q, got %q", interpretModeAuto, resp.Mode)
	}
	if resp.PipelineDraft.Name != "sales-pipeline" {
		t.Fatalf("unexpected pipeline name: %s", resp.PipelineDraft.Name)
	}
}

func TestInterpretRequestManualFallbackOnServiceError(t *testing.T) {
	h := &PipelineHandler{
		Interpreter:      stubInterpreter{err: errors.New("dial tcp timeout")},
		NLPMinConfidence: 0.7,
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/interpret", strings.NewReader(`{"query":"build a sales pipeline"}`))
	rec := httptest.NewRecorder()

	h.InterpretRequest(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp interpretResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Mode != interpretModeManualFallback {
		t.Fatalf("expected fallback mode, got %q", resp.Mode)
	}
	if resp.FallbackReason != "nlp_unavailable" {
		t.Fatalf("unexpected fallback reason: %s", resp.FallbackReason)
	}
	if len(resp.PipelineDraft.Steps) == 0 {
		t.Fatalf("expected fallback draft steps")
	}
}

func TestInterpretRequestManualFallbackOnLowConfidence(t *testing.T) {
	h := &PipelineHandler{
		Interpreter: stubInterpreter{
			result: &interpreter.Result{
				Confidence: 0.2,
				Pipeline: models.CreatePipelineRequest{
					Name: "draft",
					Steps: []models.CreateStepRequest{
						{
							Key:       "extract",
							Name:      "Extract",
							Type:      models.StepTypeExtract,
							Config:    json.RawMessage(`{}`),
							DependsOn: []string{},
						},
					},
				},
			},
		},
		NLPMinConfidence: 0.7,
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/interpret", strings.NewReader(`{"query":"build a sales pipeline"}`))
	rec := httptest.NewRecorder()

	h.InterpretRequest(rec, req)

	var resp interpretResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.FallbackReason != "low_confidence" {
		t.Fatalf("expected low_confidence fallback, got %s", resp.FallbackReason)
	}
}

func TestInterpretRequestManualFallbackOnInvalidPipeline(t *testing.T) {
	h := &PipelineHandler{
		Interpreter: stubInterpreter{
			result: &interpreter.Result{
				Confidence: 0.95,
				Pipeline: models.CreatePipelineRequest{
					Name: "draft",
					Steps: []models.CreateStepRequest{
						{
							Key:       "step1",
							Name:      "Unsupported",
							Type:      "custom",
							Config:    json.RawMessage(`{}`),
							DependsOn: []string{},
						},
					},
				},
			},
		},
		NLPMinConfidence: 0.7,
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/interpret", strings.NewReader(`{"query":"build a sales pipeline"}`))
	rec := httptest.NewRecorder()

	h.InterpretRequest(rec, req)

	var resp interpretResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.FallbackReason != "invalid_pipeline" {
		t.Fatalf("expected invalid_pipeline fallback, got %s", resp.FallbackReason)
	}
}
