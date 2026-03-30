package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/akshat/pipeline-orchestrator/internal/models"
)

const (
	defaultListLimit = 50
	maxListLimit     = 200
)

// PipelineRuns returns recent runs for a pipeline, with optional status filtering.
func (h *PipelineHandler) PipelineRuns(w http.ResponseWriter, r *http.Request) {
	pipelineID := r.PathValue("id")

	pipeline, err := h.Store.GetByID(r.Context(), pipelineID)
	if err != nil || pipeline == nil {
		http.Error(w, `{"error":"pipeline not found"}`, http.StatusNotFound)
		return
	}

	limit, err := parseLimit(r.URL.Query().Get("limit"), defaultListLimit, maxListLimit)
	if err != nil {
		http.Error(w, `{"error":"invalid limit"}`, http.StatusBadRequest)
		return
	}

	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status != "" && !isValidRunStatus(status) {
		http.Error(w, `{"error":"invalid status filter"}`, http.StatusBadRequest)
		return
	}

	runs, err := h.Store.ListRuns(r.Context(), pipelineID, status, limit)
	if err != nil {
		http.Error(w, `{"error":"failed to fetch run history"}`, http.StatusInternalServerError)
		return
	}

	if runs == nil {
		runs = []models.PipelineRunHistoryItem{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"pipeline_id": pipelineID,
		"runs":        runs,
	})
}

// PipelineRunEvents returns recent event logs for a pipeline and optional run ID.
func (h *PipelineHandler) PipelineRunEvents(w http.ResponseWriter, r *http.Request) {
	pipelineID := r.PathValue("id")

	pipeline, err := h.Store.GetByID(r.Context(), pipelineID)
	if err != nil || pipeline == nil {
		http.Error(w, `{"error":"pipeline not found"}`, http.StatusNotFound)
		return
	}

	limit, err := parseLimit(r.URL.Query().Get("limit"), defaultListLimit, maxListLimit)
	if err != nil {
		http.Error(w, `{"error":"invalid limit"}`, http.StatusBadRequest)
		return
	}

	runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
	events, err := h.Store.ListRunEvents(r.Context(), pipelineID, runID, limit)
	if err != nil {
		http.Error(w, `{"error":"failed to fetch run events"}`, http.StatusInternalServerError)
		return
	}

	if events == nil {
		events = []models.RunEvent{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"pipeline_id": pipelineID,
		"run_id":      runID,
		"events":      events,
	})
}

// Metrics returns aggregate observability metrics for the orchestrator.
func (h *PipelineHandler) Metrics(w http.ResponseWriter, r *http.Request) {
	metrics, err := h.Store.GetMetrics(r.Context())
	if err != nil {
		http.Error(w, `{"error":"failed to fetch metrics"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func parseLimit(raw string, fallback, max int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, errors.New("limit must be positive")
	}
	if n > max {
		return max, nil
	}
	return n, nil
}

func isValidRunStatus(s string) bool {
	switch s {
	case models.RunStatusPending, models.RunStatusRunning, models.RunStatusCompleted, models.RunStatusFailed, models.RunStatusSkipped:
		return true
	default:
		return false
	}
}
