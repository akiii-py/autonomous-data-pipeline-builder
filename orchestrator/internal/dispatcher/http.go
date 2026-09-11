package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/akshat/pipeline-orchestrator/internal/models"
)

// HTTPDispatcher routes steps to the Python executor worker.
type HTTPDispatcher struct {
	baseURL   string
	authToken string
	client    *http.Client
}

// executeResponse is the worker's reply. It carries the full StepResult rather
// than a status string, so nothing the worker knows is discarded at the boundary
// (D-01).
type executeResponse struct {
	Status string             `json:"status"`
	Error  string             `json:"error,omitempty"`
	Result *models.StepResult `json:"result,omitempty"`
}

func NewHTTPDispatcher(baseURL, authToken string, timeout time.Duration) *HTTPDispatcher {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &HTTPDispatcher{
		baseURL:   strings.TrimRight(baseURL, "/"),
		authToken: strings.TrimSpace(authToken),
		client:    &http.Client{Timeout: timeout},
	}
}

func (d *HTTPDispatcher) Provenance() models.Provenance {
	return models.Provenance{
		ExecMode:  models.ExecModeWorker,
		Target:    d.baseURL,
		Simulated: false,
	}
}

func (d *HTTPDispatcher) ExecuteStep(ctx context.Context, req models.StepRequest) (*models.StepResult, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("worker base URL is empty")
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal execute request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, d.baseURL+"/execute", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create execute request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if d.authToken != "" {
		httpReq.Header.Set("X-Worker-Token", d.authToken)
	}

	resp, err := d.client.Do(httpReq)
	if err != nil {
		// A transport failure is the case retries exist for — but a cancelled or
		// expired context is not, because the caller has already decided to stop.
		class := models.ErrorClassTransient
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			class = models.ErrorClassPermanent
		}
		return d.failure(req, class, fmt.Sprintf("worker call failed: %v", err)), nil
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		return d.failure(req, models.ErrorClassTransient, fmt.Sprintf("read worker response: %v", readErr)), nil
	}

	var out executeResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return d.failure(req, models.ErrorClassPermanent,
			fmt.Sprintf("decode worker response (status %d): %v", resp.StatusCode, err)), nil
	}

	// 5xx is the worker itself failing, which is worth another attempt. 4xx is a
	// rejected request and will be rejected identically forever.
	if resp.StatusCode >= 500 {
		return d.failure(req, models.ErrorClassTransient, workerError(out, resp.StatusCode)), nil
	}
	if resp.StatusCode >= 400 {
		return d.failure(req, models.ErrorClassPermanent, workerError(out, resp.StatusCode)), nil
	}

	if out.Result == nil {
		return d.failure(req, models.ErrorClassPermanent, "worker returned no result"), nil
	}

	result := out.Result
	// Trust the orchestrator's own identity fields over whatever came back.
	result.RunID = req.RunID
	result.StepKey = req.Step.Key
	result.IdempotencyKey = req.IdempotencyKey
	result.Attempt = req.Attempt

	if out.Error != "" && result.ErrorMessage == "" {
		result.ErrorMessage = out.Error
	}
	if result.Failed() && result.ErrorClass == models.ErrorClassNone {
		result.ErrorClass = models.ErrorClassPermanent
	}

	return result, nil
}

func (d *HTTPDispatcher) failure(req models.StepRequest, class models.ErrorClass, msg string) *models.StepResult {
	return &models.StepResult{
		RunID:          req.RunID,
		StepKey:        req.Step.Key,
		IdempotencyKey: req.IdempotencyKey,
		Attempt:        req.Attempt,
		ErrorClass:     class,
		ErrorMessage:   msg,
	}
}

func workerError(out executeResponse, status int) string {
	if out.Error != "" {
		return fmt.Sprintf("worker error (status %d): %s", status, out.Error)
	}
	if out.Result != nil && out.Result.ErrorMessage != "" {
		return fmt.Sprintf("worker error (status %d): %s", status, out.Result.ErrorMessage)
	}
	return fmt.Sprintf("worker returned status %d", status)
}
