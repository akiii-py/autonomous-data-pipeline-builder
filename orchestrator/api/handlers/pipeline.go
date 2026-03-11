package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/akshat/pipeline-orchestrator/internal/models"
	"github.com/akshat/pipeline-orchestrator/internal/store"
)

// PipelineHandler holds dependencies for pipeline endpoints.
type PipelineHandler struct {
	Store *store.PipelineStore
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

// RunPipeline triggers execution of a pipeline.
// Stub — requires scheduler from Phase 3.
func (h *PipelineHandler) RunPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	pipeline, err := h.Store.GetByID(r.Context(), id)
	if err != nil || pipeline == nil {
		http.Error(w, `{"error":"pipeline not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "pipeline run queued — scheduler not implemented yet (Phase 3)",
		"id":      id,
	})
}

// PipelineStatus returns the current execution state of a pipeline.
// Stub — requires execution tracking from Phase 3.
func (h *PipelineHandler) PipelineStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	pipeline, err := h.Store.GetByID(r.Context(), id)
	if err != nil || pipeline == nil {
		http.Error(w, `{"error":"pipeline not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": pipeline.Status,
		"id":     id,
	})
}
