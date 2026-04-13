# Autonomous Data Pipeline Builder

An intelligent platform that turns natural-language data requests into executable DAG-based data pipelines.

## Overview

This project provides:

- A Go-based orchestrator API for pipeline lifecycle management.
- DAG validation and dependency-aware scheduling.
- Local and worker-based execution dispatch.
- A Python execution runtime with connectors and transformations.
- Observability APIs for run history, event timelines, and metrics.

## Current Implementation Status

Implemented through Phase 6.5:

- Phase 1: Core orchestrator foundation
  - HTTP server, middleware, config loading
  - PostgreSQL connection and migrations
  - Pipeline CRUD API and persistence
- Phase 2: DAG engine and validation
  - DAG graph/build utilities
  - Dependency validation and cycle checks
  - Stable step-key mapping
- Phase 3: Scheduler and run lifecycle
  - Pipeline run + step run state transitions
  - Dependency-aware execution ordering
  - Retry support for failed steps
  - Dispatcher abstraction
- Phase 4: Worker execution plane
  - Local dispatcher and HTTP worker dispatcher mode
  - Python worker runtime
  - Connectors (including PostgreSQL) and transformation operations
  - Local Postgres E2E example flow
- Phase 5: Observability baseline
  - Run history endpoint
  - Run events endpoint
  - Aggregate metrics endpoint
  - Scheduler event emission into DB event log
- Phase 6: NLP interpretation and safe fallback
  - HTTP NLP client integration for natural-language pipeline draft generation
  - Confidence-gated auto mode
  - DAG and step-type validation before accepting generated drafts
  - Deterministic manual fallback draft when NLP is unavailable/low-confidence/invalid
- Phase 6.5: API security baseline
  - API key middleware for /api routes
  - Auth via X-API-Key or Authorization: Bearer <key>
  - Health endpoint and CORS preflight remain public

## Architecture

- Orchestrator (Go)
  - API layer: routing, handlers, middleware
  - Core: DAG, scheduler, dispatcher, store
  - Persistence: PostgreSQL tables for pipelines, runs, step runs, events
- Executor (Python)
  - Worker server for step execution
  - Connector modules for data sources/sinks
  - Transformation operations
- UI (scaffolded)
  - Reserved for pipeline builder and monitoring dashboard

## Repository Layout

- orchestrator/
  - api/
    - handlers/
    - middleware/
    - router/
  - cmd/server/
  - internal/
    - config/
    - dag/
    - database/
    - dispatcher/
    - interpreter/
    - models/
    - scheduler/
    - store/
- executor/
  - connectors/
  - transformations/
  - worker/
- docs/
- examples/
- tests/
- ui/

## Tech Stack

- Go 1.26.1 (orchestrator)
- PostgreSQL (metadata, run tracking, event logs)
- Python 3.x (worker runtime)
- psycopg 3.x (PostgreSQL connector in worker)

## Quick Start

### Prerequisites

- Go 1.26+
- PostgreSQL
- Python 3.10+ (for executor components)

### Environment Variables (Orchestrator)

- PORT (default: 8080)
- DATABASE_URL (default: postgres://postgres:postgres@localhost:5432/pipeline?sslmode=disable)
- REDIS_URL (reserved for future use)
- GRPC_PORT (reserved for future NLP/worker integrations)
- API_KEY (optional; when set, protects /api endpoints)
- WORKER_URL (default: http://localhost:8090)
- EXEC_MODE (default: local, options: local|worker)
- WORKER_TIMEOUT_MS (default: 10000)
- NLP_SERVICE_URL (default: http://localhost:8091)
- NLP_TIMEOUT_MS (default: 8000)
- NLP_MIN_CONFIDENCE (default: 0.70)
- LOG_LEVEL (default: info)
- ENVIRONMENT (default: development)

### Run Orchestrator

From orchestrator/:

```bash
go mod tidy
go run ./cmd/server
```

Migrations run automatically on startup.

### Run Go Tests

From orchestrator/:

```bash
go test ./...
```

### Run Python Executor Tests

From repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r executor/requirements.txt
PYTHONPATH="$PWD" python -m unittest discover -s executor/tests -p "test_*.py"
```

## API Endpoints

### Health

- GET /health

### Security (Phase 6.5)

- If API_KEY is configured, all /api/v1 endpoints require authentication.
- Send either:
  - Header: X-API-Key: <your-key>
  - Header: Authorization: Bearer <your-key>
- /health remains unauthenticated for liveness checks.

### Pipeline Management

- GET /api/v1/pipelines
- POST /api/v1/pipelines
- GET /api/v1/pipelines/{id}
- DELETE /api/v1/pipelines/{id}

### Execution

- POST /api/v1/pipelines/{id}/run
- GET /api/v1/pipelines/{id}/status
  - Optional query: run_id

### Observability (Phase 5)

- GET /api/v1/pipelines/{id}/runs
  - Optional query: status, limit, offset
- GET /api/v1/pipelines/{id}/events
  - Optional query: run_id, level, event_type, limit, offset
- GET /api/v1/pipelines/{id}/metrics
  - Returns pipeline-scoped run metrics, success/failure rates, and top failed steps
- GET /api/v1/pipelines/{id}/failure-breakdown
  - Returns grouped step-level failure insights (step, error, frequency, last occurrence)
- GET /api/v1/metrics
  - Returns global orchestrator metrics and success/failure rates

### NLP Interpretation (Phase 6)

- POST /api/v1/interpret
  - Accepts natural language query and returns either:
    - mode=auto with validated pipeline_draft
    - mode=manual_fallback with fallback_reason and safe draft template

## Example Pipeline Payload

```json
{
  "name": "sales-pipeline",
  "description": "Extract, transform, and load sales data",
  "steps": [
    {
      "key": "extract_sales",
      "name": "Extract Sales",
      "type": "extract",
      "config": {"source": "postgres"},
      "depends_on": []
    },
    {
      "key": "transform_sales",
      "name": "Transform Sales",
      "type": "transform",
      "config": {"op": "aggregate"},
      "depends_on": ["extract_sales"]
    },
    {
      "key": "load_warehouse",
      "name": "Load Warehouse",
      "type": "load",
      "config": {"target": "s3"},
      "depends_on": ["transform_sales"]
    }
  ]
}
```

## Next Milestone

Planned in the next phase:

- Phase 7a: developer experience and API ergonomics
- Phase 7b: UI/dashboard implementation

## License

License not added yet.
