package handlers

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/akshat/pipeline-orchestrator/internal/catalog"
	"github.com/akshat/pipeline-orchestrator/internal/dag"
	"github.com/akshat/pipeline-orchestrator/internal/interpreter"
	"github.com/akshat/pipeline-orchestrator/internal/models"
	"github.com/akshat/pipeline-orchestrator/internal/store"
)

// DefaultNLPMinConfidence is the fallback threshold used when the injected value
// is out of range.
const DefaultNLPMinConfidence = 0.70

// PipelineHandler holds dependencies for pipeline endpoints. Everything here is
// injected by router.New (rule 6.1).
type PipelineHandler struct {
	Store            *store.PipelineStore
	Interpreter      interpreter.Service
	Catalog          catalog.Provider
	NLPMinConfidence float64
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// ListPipelines returns stored pipelines, paginated like every other list
// endpoint.
func (h *PipelineHandler) ListPipelines(w http.ResponseWriter, r *http.Request) {
	limit, err := parseLimit(r.URL.Query().Get("limit"), defaultListLimit, maxListLimit)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid limit")
		return
	}
	offset, err := parseOffset(r.URL.Query().Get("offset"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid offset")
		return
	}

	pipelines, err := h.Store.List(r.Context(), limit, offset)
	if err != nil {
		log.Printf("list pipelines: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to list pipelines")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"pagination": models.Pagination{Limit: limit, Offset: offset, Returned: len(pipelines)},
		"pipelines":  pipelines,
	})
}

// CreatePipeline stores a pipeline definition. A pipeline containing an
// irreversible operation is created pending approval and cannot run until a
// human moves it (D-12).
func (h *PipelineHandler) CreatePipeline(w http.ResponseWriter, r *http.Request) {
	var req models.CreatePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	pipeline, err := h.Store.Create(r.Context(), req)
	if err != nil {
		// Typed, not text-matched (rule 5.1). The whole error set reaches the
		// caller so a fix takes one round trip, not one per problem (rule 5.2).
		var verrs dag.ValidationErrors
		if errors.As(err, &verrs) {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"error":  "invalid pipeline dependency graph",
				"codes":  verrs.Codes(),
				"errors": verrs.Messages(),
			})
			return
		}
		log.Printf("create pipeline: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to create pipeline")
		return
	}

	writeJSON(w, http.StatusCreated, pipeline)
}

func (h *PipelineHandler) GetPipeline(w http.ResponseWriter, r *http.Request) {
	pipeline, err := h.Store.GetByID(r.Context(), r.PathValue("id"))
	if err != nil {
		log.Printf("get pipeline: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to get pipeline")
		return
	}
	if pipeline == nil {
		writeError(w, http.StatusNotFound, "pipeline not found")
		return
	}

	writeJSON(w, http.StatusOK, pipeline)
}

// ApprovePipeline is the lifecycle transition that makes an irreversible
// pipeline runnable (D-12). Confirmation is a state change on the pipeline, not
// a parameter on the run call.
func (h *PipelineHandler) ApprovePipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var body struct {
		ApprovedBy string `json:"approved_by"`
	}
	// An empty body is acceptable; approved_by is recorded when supplied.
	_ = json.NewDecoder(r.Body).Decode(&body)

	pipeline, err := h.Store.Approve(r.Context(), id, body.ApprovedBy)
	if err != nil {
		if errors.Is(err, store.ErrPipelineNotFound) {
			writeError(w, http.StatusNotFound, "pipeline not found")
			return
		}
		log.Printf("approve pipeline: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to approve pipeline")
		return
	}

	writeJSON(w, http.StatusOK, pipeline)
}

// DeletePipeline soft-deletes the definition. Run history and events survive
// (D-10).
func (h *PipelineHandler) DeletePipeline(w http.ResponseWriter, r *http.Request) {
	if err := h.Store.Delete(r.Context(), r.PathValue("id")); err != nil {
		if errors.Is(err, store.ErrPipelineNotFound) {
			writeError(w, http.StatusNotFound, "pipeline not found")
			return
		}
		log.Printf("delete pipeline: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to delete pipeline")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// RunPipeline enqueues a run. It does not execute one: the run row is created
// unclaimed, and whichever process claims it owns it (D-07, D-08). Nothing here
// spawns a goroutine that is the sole owner of durable work.
func (h *PipelineHandler) RunPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	run, err := h.Store.CreateRun(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrPipelineNotFound):
			writeError(w, http.StatusNotFound, "pipeline not found")
		case errors.Is(err, store.ErrNotApproved):
			// 409: the pipeline exists and is well-formed, but the lifecycle
			// stage that authorises irreversible work has not happened (D-12).
			writeJSON(w, http.StatusConflict, map[string]interface{}{
				"error":       "pipeline requires approval before it can run",
				"approve_url": "/api/v1/pipelines/" + id + "/approve",
			})
		default:
			log.Printf("create run: %v", err)
			writeError(w, http.StatusInternalServerError, "failed to create run")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"message": "pipeline run queued",
		"id":      id,
		"run_id":  run.ID,
	})
}

// CancelRun flags a run for cancellation. The process holding the claim observes
// the flag on its next heartbeat and cancels the run's context; in-flight steps
// stop and each records a terminal event (rule 4.2).
func (h *PipelineHandler) CancelRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	runID := r.PathValue("run_id")

	if err := h.Store.RequestCancel(r.Context(), id, runID); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"message": "cancellation requested",
		"run_id":  runID,
	})
}

// PipelineStatus returns the latest run status and step-level state, including
// execution provenance (D-09).
func (h *PipelineHandler) PipelineStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	pipeline, err := h.Store.GetByID(r.Context(), id)
	if err != nil || pipeline == nil {
		writeError(w, http.StatusNotFound, "pipeline not found")
		return
	}

	runID := r.URL.Query().Get("run_id")
	var status *models.PipelineStatusResponse
	if runID != "" {
		status, err = h.Store.GetRunStatusByID(r.Context(), id, runID)
	} else {
		status, err = h.Store.GetLatestRunStatus(r.Context(), id)
	}
	if err != nil {
		log.Printf("pipeline status: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to fetch pipeline status")
		return
	}

	if status == nil {
		writeJSON(w, http.StatusOK, models.PipelineStatusResponse{PipelineID: id})
		return
	}
	writeJSON(w, http.StatusOK, status)
}
