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

	offset, err := parseOffset(r.URL.Query().Get("offset"))
	if err != nil {
		http.Error(w, `{"error":"invalid offset"}`, http.StatusBadRequest)
		return
	}

	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status != "" && !isValidRunStatus(status) {
		http.Error(w, `{"error":"invalid status filter"}`, http.StatusBadRequest)
		return
	}

	runs, err := h.Store.ListRuns(r.Context(), pipelineID, status, limit, offset)
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
		"status":      status,
		"pagination": models.Pagination{
			Limit:    limit,
			Offset:   offset,
			Returned: len(runs),
		},
		"runs": runs,
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

	offset, err := parseOffset(r.URL.Query().Get("offset"))
	if err != nil {
		http.Error(w, `{"error":"invalid offset"}`, http.StatusBadRequest)
		return
	}

	runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
	level := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("level")))
	if level != "" && !isValidEventLevel(level) {
		http.Error(w, `{"error":"invalid level filter"}`, http.StatusBadRequest)
		return
	}

	eventType := strings.TrimSpace(r.URL.Query().Get("event_type"))
	events, err := h.Store.ListRunEvents(r.Context(), pipelineID, runID, level, eventType, limit, offset)
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
		"level":       level,
		"event_type":  eventType,
		"pagination": models.Pagination{
			Limit:    limit,
			Offset:   offset,
			Returned: len(events),
		},
		"events": events,
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

// PipelineMetrics returns aggregate observability metrics for a specific pipeline.
func (h *PipelineHandler) PipelineMetrics(w http.ResponseWriter, r *http.Request) {
	pipelineID := r.PathValue("id")

	pipeline, err := h.Store.GetByID(r.Context(), pipelineID)
	if err != nil || pipeline == nil {
		http.Error(w, `{"error":"pipeline not found"}`, http.StatusNotFound)
		return
	}

	metrics, err := h.Store.GetPipelineMetrics(r.Context(), pipelineID)
	if err != nil {
		http.Error(w, `{"error":"failed to fetch pipeline metrics"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// PipelineFailureBreakdown returns grouped failure insights for a specific pipeline.
func (h *PipelineHandler) PipelineFailureBreakdown(w http.ResponseWriter, r *http.Request) {
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

	offset, err := parseOffset(r.URL.Query().Get("offset"))
	if err != nil {
		http.Error(w, `{"error":"invalid offset"}`, http.StatusBadRequest)
		return
	}

	items, err := h.Store.GetPipelineFailureBreakdown(r.Context(), pipelineID, limit, offset)
	if err != nil {
		http.Error(w, `{"error":"failed to fetch failure breakdown"}`, http.StatusInternalServerError)
		return
	}
	if items == nil {
		items = []models.StepFailureBreakdownItem{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"pipeline_id": pipelineID,
		"pagination": models.Pagination{
			Limit:    limit,
			Offset:   offset,
			Returned: len(items),
		},
		"failures": items,
	})
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

func parseOffset(raw string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, errors.New("offset must be non-negative")
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

func isValidEventLevel(s string) bool {
	switch s {
	case "info", "warn", "error":
		return true
	default:
		return false
	}
}
