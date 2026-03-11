package router

import (
	"net/http"

	"github.com/akshat/pipeline-orchestrator/api/handlers"
	"github.com/akshat/pipeline-orchestrator/api/middleware"
	"github.com/akshat/pipeline-orchestrator/internal/config"
)

// New creates the main HTTP router with all routes registered.
// This is the single entry point for all API traffic.
func New(cfg *config.Config) http.Handler {
	mux := http.NewServeMux()

	// Health check — used by Docker/K8s to verify the service is alive
	mux.HandleFunc("GET /health", handlers.Health)

	// Pipeline CRUD endpoints
	mux.HandleFunc("GET /api/v1/pipelines", handlers.ListPipelines)
	mux.HandleFunc("POST /api/v1/pipelines", handlers.CreatePipeline)
	mux.HandleFunc("GET /api/v1/pipelines/{id}", handlers.GetPipeline)
	mux.HandleFunc("DELETE /api/v1/pipelines/{id}", handlers.DeletePipeline)

	// Pipeline execution
	mux.HandleFunc("POST /api/v1/pipelines/{id}/run", handlers.RunPipeline)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/status", handlers.PipelineStatus)

	// NLP request interpretation — user sends natural language, gets a pipeline spec
	mux.HandleFunc("POST /api/v1/interpret", handlers.InterpretRequest)

	// Wrap with middleware chain: logging → recovery → CORS
	var handler http.Handler = mux
	handler = middleware.Logger(handler)
	handler = middleware.Recovery(handler)
	handler = middleware.CORS(handler)

	return handler
}
