# Autonomous Data Pipeline Builder

An intelligent platform that turns natural-language data requests into executable data pipelines.

## What This Project Does

The Autonomous Data Pipeline Builder is designed to:

- Interpret user data workflow requests
- Generate pipeline workflows as DAGs (Directed Acyclic Graphs)
- Orchestrate execution across worker nodes
- Monitor execution status and recover from failures

## Current Status

Implemented so far:

- Phase 1 (Core Orchestrator Foundation)
  - Go HTTP server with graceful shutdown
  - Config loading from environment variables
  - Health and pipeline API endpoints
  - Middleware (logging, recovery, CORS)
  - PostgreSQL connection and initial schema migrations
  - Pipeline CRUD persistence layer

- Phase 2 (DAG Engine and Workflow Management)
  - DAG graph model and topological ordering
  - Dependency validation (including cycle detection)
  - DAG construction from pipeline step definitions
  - Step keys for stable dependency mapping
  - Run-tracking schema models and migration scaffolding

## Architecture Overview

High-level components:

- Orchestrator (Go)
  - Accepts API requests
  - Validates and stores pipeline definitions
  - Builds and validates DAGs
  - Will schedule and dispatch tasks in upcoming phases

- Executor (Python, scaffolded)
  - Connectors for external sources (APIs, DBs, files)
  - Transformations for data processing
  - Worker runtime for task execution

- UI (scaffolded)
  - Dashboard and pipeline visualization

## Repository Structure

- orchestrator/
  - cmd/server: service entrypoint
  - api: handlers, router, middleware
  - internal/config: configuration loading
  - internal/database: DB connection and migrations
  - internal/models: pipeline and run models
  - internal/store: persistence layer
  - internal/dag: DAG graph, builder, validation

- executor/
  - connectors/
  - transformations/
  - worker/

- ui/
- tests/
- docs/
- examples/

## Tech Stack

- Go 1.26.x (orchestrator)
- PostgreSQL (metadata and run tracking)
- Python (planned worker/executor runtime)

## Quick Start (Orchestrator)

### Prerequisites

- Go 1.26+
- PostgreSQL

### Environment Variables

Defaults are provided in code, but you can override:

- PORT (default: 8080)
- DATABASE_URL (default points to local postgres)
- REDIS_URL (reserved for upcoming phases)
- GRPC_PORT (reserved for worker communication)
- LOG_LEVEL
- ENVIRONMENT

### Run Locally

1. Start PostgreSQL
2. From orchestrator directory:

```bash
go mod tidy
go run ./cmd/server
```

On startup, migrations run automatically.

## API Endpoints

- GET /health
- GET /api/v1/pipelines
- POST /api/v1/pipelines
- GET /api/v1/pipelines/{id}
- DELETE /api/v1/pipelines/{id}
- POST /api/v1/pipelines/{id}/run
- GET /api/v1/pipelines/{id}/status
- POST /api/v1/interpret

Notes:

- Run and status endpoints are placeholders for scheduler/execution integration.
- Pipeline creation now validates dependency graphs before insertion.

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
      "config": { "source": "postgres" },
      "depends_on": []
    },
    {
      "key": "transform_sales",
      "name": "Transform Sales",
      "type": "transform",
      "config": { "op": "aggregate" },
      "depends_on": ["extract_sales"]
    },
    {
      "key": "load_warehouse",
      "name": "Load Warehouse",
      "type": "load",
      "config": { "target": "s3" },
      "depends_on": ["transform_sales"]
    }
  ]
}
```

## Roadmap

- Phase 3: Scheduler and dispatcher
- Phase 4: Python worker execution engine
- Phase 5: Monitoring and reliability features
- Phase 6: NLP interpretation service integration
- Phase 7: UI/dashboard
- Phase 8: Production hardening and scaling

## Contributing

Contributions are welcome. For now, focus areas are:

- scheduler/dispatcher implementation
- execution state transitions
- integration tests
- connector and worker runtime scaffolding

## License

License not added yet.
