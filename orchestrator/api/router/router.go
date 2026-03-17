package router

import (
	"database/sql"
	"net/http"

	"github.com/akshat/pipeline-orchestrator/api/handlers"
	"github.com/akshat/pipeline-orchestrator/api/middleware"
	"github.com/akshat/pipeline-orchestrator/internal/dispatcher"
	"github.com/akshat/pipeline-orchestrator/internal/scheduler"
	"github.com/akshat/pipeline-orchestrator/internal/store"
)

// New creates the main HTTP router with all routes registered.
// This is the single entry point for all API traffic.
func New(db *sql.DB) http.Handler {
	mux := http.NewServeMux()
	ps := store.NewPipelineStore(db)

	ph := &handlers.PipelineHandler{
		Store:     ps,
		Scheduler: scheduler.New(ps, dispatcher.NewLocalDispatcher()),
	}

	// Health check — used by Docker/K8s to verify the service is alive
	mux.HandleFunc("GET /health", handlers.Health)

	// Pipeline CRUD endpoints
	mux.HandleFunc("GET /api/v1/pipelines", ph.ListPipelines)
	mux.HandleFunc("POST /api/v1/pipelines", ph.CreatePipeline)
	mux.HandleFunc("GET /api/v1/pipelines/{id}", ph.GetPipeline)
	mux.HandleFunc("DELETE /api/v1/pipelines/{id}", ph.DeletePipeline)

	// Pipeline execution
	mux.HandleFunc("POST /api/v1/pipelines/{id}/run", ph.RunPipeline)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/status", ph.PipelineStatus)

	// NLP request interpretation — user sends natural language, gets a pipeline spec
	mux.HandleFunc("POST /api/v1/interpret", handlers.InterpretRequest)

	// Wrap with middleware chain: logging → recovery → CORS
	var handler http.Handler = mux
	handler = middleware.Logger(handler)
	handler = middleware.Recovery(handler)
	handler = middleware.CORS(handler)

	return handler
}
