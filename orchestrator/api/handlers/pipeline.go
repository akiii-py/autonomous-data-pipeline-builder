package handlers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/akshat/pipeline-orchestrator/internal/models"
	"github.com/akshat/pipeline-orchestrator/internal/scheduler"
	"github.com/akshat/pipeline-orchestrator/internal/store"
)

// PipelineHandler holds dependencies for pipeline endpoints.
type PipelineHandler struct {
	Store     *store.PipelineStore
	Scheduler *scheduler.Scheduler
}

// ListPipelines returns all pipelines for the authenticated user.
func (h *PipelineHandler) ListPipelines(w http.ResponseWriter, r *http.Request) {
	pipelines, err := h.Store.List(r.Context())
	if err != nil {
		log.Printf("list pipelines: %v", err)
		http.Error(w, `{"error":"failed to list pipelines"}`, http.StatusInternalServerError)
		return
	}
	if pipelines == nil {
		pipelines = []models.Pipeline{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pipelines)
}

// CreatePipeline accepts a pipeline definition and stores it in the database.
func (h *PipelineHandler) CreatePipeline(w http.ResponseWriter, r *http.Request) {
	var req models.CreatePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, `{"error":"name is required"}`, http.StatusBadRequest)
		return
	}

	pipeline, err := h.Store.Create(r.Context(), req)
	if err != nil {
		log.Printf("create pipeline: %v", err)
		if strings.Contains(err.Error(), "invalid pipeline dag") {
			http.Error(w, `{"error":"invalid pipeline dependency graph"}`, http.StatusBadRequest)
			return
		}
		http.Error(w, `{"error":"failed to create pipeline"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(pipeline)
}

// GetPipeline returns a single pipeline by ID, including its steps.
func (h *PipelineHandler) GetPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	pipeline, err := h.Store.GetByID(r.Context(), id)
	if err != nil {
		log.Printf("get pipeline: %v", err)
		http.Error(w, `{"error":"failed to get pipeline"}`, http.StatusInternalServerError)
		return
	}
	if pipeline == nil {
		http.Error(w, `{"error":"pipeline not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pipeline)
}

// DeletePipeline removes a pipeline and cascades to its steps.
func (h *PipelineHandler) DeletePipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if err := h.Store.Delete(r.Context(), id); err != nil {
		log.Printf("delete pipeline: %v", err)
		http.Error(w, `{"error":"pipeline not found"}`, http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// RunPipeline creates a run and schedules DAG execution.
func (h *PipelineHandler) RunPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	pipeline, err := h.Store.GetByID(r.Context(), id)
	if err != nil || pipeline == nil {
		http.Error(w, `{"error":"pipeline not found"}`, http.StatusNotFound)
		return
	}
	if h.Scheduler == nil {
		http.Error(w, `{"error":"scheduler not configured"}`, http.StatusInternalServerError)
		return
	}

	run, err := h.Store.CreateRun(r.Context(), id)
	if err != nil {
		log.Printf("create run: %v", err)
		http.Error(w, `{"error":"failed to create run"}`, http.StatusInternalServerError)
		return
	}

	go func(pipelineID, runID string) {
		if err := h.Scheduler.ExecuteRun(context.Background(), pipelineID, runID); err != nil {
			log.Printf("execute run failed (pipeline=%s run=%s): %v", pipelineID, runID, err)
		}
	}(id, run.ID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "pipeline run queued",
		"id":      id,
		"run_id":  run.ID,
	})
}

// PipelineStatus returns the latest run status and step-level state.
func (h *PipelineHandler) PipelineStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	pipeline, err := h.Store.GetByID(r.Context(), id)
	if err != nil || pipeline == nil {
		http.Error(w, `{"error":"pipeline not found"}`, http.StatusNotFound)
		return
	}

	status, err := h.Store.GetLatestRunStatus(r.Context(), id)
	if err != nil {
		log.Printf("pipeline status: %v", err)
		http.Error(w, `{"error":"failed to fetch pipeline status"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if status == nil {
		json.NewEncoder(w).Encode(models.PipelineStatusResponse{PipelineID: id})
		return
	}
	json.NewEncoder(w).Encode(status)
}
