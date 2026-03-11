package handlers

import (
	"encoding/json"
	"net/http"
)

// ListPipelines returns all pipelines for the authenticated user.
// In later phases this will query PostgreSQL via the pipeline service.
func ListPipelines(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "list pipelines — not implemented yet"})
}

// CreatePipeline accepts a pipeline definition (JSON or from NLP output)
// and stores it as a DAG in the database.
func CreatePipeline(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"message": "create pipeline — not implemented yet"})
}

// GetPipeline returns a single pipeline by ID, including its DAG structure.
func GetPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "get pipeline", "id": id})
}

// DeletePipeline removes a pipeline and cancels any running executions.
func DeletePipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "delete pipeline", "id": id})
}

// RunPipeline triggers execution of a pipeline — the scheduler picks it up
// and dispatches tasks to Python workers.
func RunPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"message": "pipeline run queued", "id": id})
}

// PipelineStatus returns the current execution state of a pipeline.
func PipelineStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "pipeline status", "id": id})
}
