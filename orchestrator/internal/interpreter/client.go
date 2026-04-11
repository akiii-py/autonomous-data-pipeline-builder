package interpreter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/akshat/pipeline-orchestrator/internal/models"
)

// Service converts natural language user requests into pipeline drafts.
type Service interface {
	Interpret(ctx context.Context, req Request) (*Result, error)
}

// Request is the payload sent to an NLP interpretation service.
type Request struct {
	Query      string `json:"query"`
	SourceHint string `json:"source_hint,omitempty"`
	TargetHint string `json:"target_hint,omitempty"`
	DryRun     bool   `json:"dry_run"`
}

// Result is a normalized NLP interpretation response.
type Result struct {
	Pipeline   models.CreatePipelineRequest
	Confidence float64
	Warnings   []string
}

// HTTPClient calls a remote NLP interpretation service over HTTP.
type HTTPClient struct {
	baseURL string
	client  *http.Client
}

func NewHTTPClient(baseURL string, timeout time.Duration) *HTTPClient {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	return &HTTPClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: timeout},
	}
}

type interpretEnvelope struct {
	Pipeline      *models.CreatePipelineRequest `json:"pipeline"`
	PipelineDraft *models.CreatePipelineRequest `json:"pipeline_draft"`
	Name          string                        `json:"name"`
	Description   string                        `json:"description"`
	Steps         []models.CreateStepRequest    `json:"steps"`
	Confidence    float64                       `json:"confidence"`
	Warnings      []string                      `json:"warnings"`
}

func (c *HTTPClient) Interpret(ctx context.Context, req Request) (*Result, error) {
	if c.baseURL == "" {
		return nil, fmt.Errorf("nlp service base url is empty")
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal nlp request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/interpret", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build nlp request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("call nlp service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("nlp service returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var envelope interpretEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode nlp response: %w", err)
	}

	pipeline := envelope.Pipeline
	if pipeline == nil {
		pipeline = envelope.PipelineDraft
	}
	if pipeline == nil {
		pipeline = &models.CreatePipelineRequest{
			Name:        envelope.Name,
			Description: envelope.Description,
			Steps:       envelope.Steps,
		}
	}

	if pipeline == nil {
		return nil, fmt.Errorf("nlp response missing pipeline draft")
	}

	return &Result{
		Pipeline:   *pipeline,
		Confidence: envelope.Confidence,
		Warnings:   append([]string(nil), envelope.Warnings...),
	}, nil
}
