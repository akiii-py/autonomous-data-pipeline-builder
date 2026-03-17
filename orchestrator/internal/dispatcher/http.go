package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/akshat/pipeline-orchestrator/internal/models"
)

type HTTPDispatcher struct {
	baseURL string
	client  *http.Client
}

type executeRequest struct {
	RunID string      `json:"run_id"`
	Step  models.Step `json:"step"`
}

type executeResponse struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func NewHTTPDispatcher(baseURL string, timeout time.Duration) *HTTPDispatcher {
	return &HTTPDispatcher{
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

func (d *HTTPDispatcher) ExecuteStep(ctx context.Context, runID string, step models.Step) error {
	if d.baseURL == "" {
		return fmt.Errorf("worker base URL is empty")
	}

	payload, err := json.Marshal(executeRequest{RunID: runID, Step: step})
	if err != nil {
		return fmt.Errorf("marshal execute request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.baseURL+"/execute", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create execute request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("worker call failed: %w", err)
	}
	defer resp.Body.Close()

	var out executeResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)

	if resp.StatusCode >= 300 {
		if out.Error != "" {
			return fmt.Errorf("worker error: %s", out.Error)
		}
		return fmt.Errorf("worker returned status %d", resp.StatusCode)
	}

	if strings.ToLower(out.Status) != "ok" && out.Error != "" {
		return fmt.Errorf("worker execution failed: %s", out.Error)
	}

	return nil
}
