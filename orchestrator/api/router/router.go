package router

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/akshat/pipeline-orchestrator/api/handlers"
	"github.com/akshat/pipeline-orchestrator/api/middleware"
	"github.com/akshat/pipeline-orchestrator/internal/catalog"
	"github.com/akshat/pipeline-orchestrator/internal/config"
	"github.com/akshat/pipeline-orchestrator/internal/dispatcher"
	"github.com/akshat/pipeline-orchestrator/internal/interpreter"
	"github.com/akshat/pipeline-orchestrator/internal/runner"
	"github.com/akshat/pipeline-orchestrator/internal/scheduler"
	"github.com/akshat/pipeline-orchestrator/internal/store"
)

// App is everything New wires together: the HTTP handler and the background run
// loop that owns execution. Both come out of the single composition root
// (rule 6.1); main starts them.
type App struct {
	Handler http.Handler
	Runner  *runner.Loop
}

// New builds the application. Every dependency decision is made here.
func New(db *sql.DB, cfg *config.Config) (*App, error) {
	ps := store.NewPipelineStore(db)

	var d dispatcher.Dispatcher
	if cfg.IsWorkerMode() {
		d = dispatcher.NewHTTPDispatcher(cfg.WorkerURL, cfg.WorkerToken, cfg.WorkerTimeout())
	} else {
		d = dispatcher.NewLocalDispatcher()
	}

	cat, err := catalog.Load(catalog.Options{
		Connectors:   cfg.CatalogConnectors,
		Destinations: cfg.CatalogDestinations,
		TransformOps: cfg.CatalogTransformOps,
		SchemaPath:   cfg.CatalogSchemaPath,
		SchemaInline: cfg.CatalogSchemaInline,
	})
	if err != nil {
		return nil, fmt.Errorf("load catalog: %w", err)
	}

	sched := scheduler.New(ps, d, scheduler.Options{
		MaxConcurrency: cfg.MaxStepConcurrency,
		BackoffBase:    cfg.RetryBackoffBase(),
		BackoffMax:     cfg.RetryBackoffMax(),
	})

	runLoop := runner.New(ps, sched, runner.Options{
		Owner:             cfg.RunnerOwner,
		Lease:             cfg.RunLease(),
		PollInterval:      cfg.RunPollInterval(),
		MaxConcurrentRuns: cfg.MaxConcurrentRuns,
	})

	ph := &handlers.PipelineHandler{
		Store:            ps,
		Interpreter:      interpreter.NewHTTPClient(cfg.NLPServiceURL, cfg.NLPTimeout()),
		Catalog:          catalog.NewStatic(cat),
		NLPMinConfidence: cfg.NLPMinConfidence,
	}

	mux := http.NewServeMux()

	// Health check — used by Docker/K8s to verify the service is alive
	mux.HandleFunc("GET /health", handlers.Health)

	// Pipeline CRUD
	mux.HandleFunc("GET /api/v1/pipelines", ph.ListPipelines)
	mux.HandleFunc("POST /api/v1/pipelines", ph.CreatePipeline)
	mux.HandleFunc("GET /api/v1/pipelines/{id}", ph.GetPipeline)
	mux.HandleFunc("DELETE /api/v1/pipelines/{id}", ph.DeletePipeline)

	// Approval stage: the transition that makes an irreversible pipeline
	// runnable (D-12)
	mux.HandleFunc("POST /api/v1/pipelines/{id}/approve", ph.ApprovePipeline)

	// Execution
	mux.HandleFunc("POST /api/v1/pipelines/{id}/run", ph.RunPipeline)
	mux.HandleFunc("POST /api/v1/pipelines/{id}/runs/{run_id}/cancel", ph.CancelRun)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/status", ph.PipelineStatus)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/runs", ph.PipelineRuns)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/events", ph.PipelineRunEvents)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/metrics", ph.PipelineMetrics)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/failure-breakdown", ph.PipelineFailureBreakdown)

	// Monitoring and metrics
	mux.HandleFunc("GET /api/v1/metrics", ph.Metrics)

	// NLP request interpretation
	mux.HandleFunc("POST /api/v1/interpret", ph.InterpretRequest)

	// Request order is CORS → Recovery → Logger → APIKey: the outermost wrapper
	// runs first, so the chain is written here in reverse.
	var handler http.Handler = mux
	handler = middleware.APIKey(handler, cfg.APIKey)
	handler = middleware.Logger(handler)
	handler = middleware.Recovery(handler)
	handler = middleware.CORS(handler)

	return &App{Handler: handler, Runner: runLoop}, nil
}
