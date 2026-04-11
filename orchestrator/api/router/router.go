package router

import (
	"database/sql"
	"net/http"
	"strings"
	"time"

	"github.com/akshat/pipeline-orchestrator/api/handlers"
	"github.com/akshat/pipeline-orchestrator/api/middleware"
	"github.com/akshat/pipeline-orchestrator/internal/config"
	"github.com/akshat/pipeline-orchestrator/internal/dispatcher"
	"github.com/akshat/pipeline-orchestrator/internal/interpreter"
	"github.com/akshat/pipeline-orchestrator/internal/scheduler"
	"github.com/akshat/pipeline-orchestrator/internal/store"
)

// New creates the main HTTP router with all routes registered.
// This is the single entry point for all API traffic.
func New(db *sql.DB, cfg *config.Config) http.Handler {
	mux := http.NewServeMux()
	ps := store.NewPipelineStore(db)

	var d dispatcher.Dispatcher
	if strings.EqualFold(cfg.ExecMode, "worker") {
		d = dispatcher.NewHTTPDispatcher(cfg.WorkerURL, time.Duration(cfg.WorkerTimeoutMS)*time.Millisecond)
	} else {
		d = dispatcher.NewLocalDispatcher()
	}

	interp := interpreter.NewHTTPClient(cfg.NLPServiceURL, time.Duration(cfg.NLPTimeoutMS)*time.Millisecond)

	ph := &handlers.PipelineHandler{
		Store:            ps,
		Scheduler:        scheduler.New(ps, d),
		Interpreter:      interp,
		NLPMinConfidence: cfg.NLPMinConfidence,
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
	mux.HandleFunc("GET /api/v1/pipelines/{id}/runs", ph.PipelineRuns)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/events", ph.PipelineRunEvents)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/metrics", ph.PipelineMetrics)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/failure-breakdown", ph.PipelineFailureBreakdown)

	// Monitoring and metrics
	mux.HandleFunc("GET /api/v1/metrics", ph.Metrics)

	// NLP request interpretation — user sends natural language, gets a pipeline spec
	mux.HandleFunc("POST /api/v1/interpret", ph.InterpretRequest)

	// Wrap with middleware chain: logging → recovery → CORS
	var handler http.Handler = mux
	handler = middleware.Logger(handler)
	handler = middleware.Recovery(handler)
	handler = middleware.CORS(handler)

	return handler
}
