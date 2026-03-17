package dispatcher

import (
	"context"

	"github.com/akshat/pipeline-orchestrator/internal/models"
)

// Dispatcher executes one step for a given run.
// In Phase 3 this is a local stub; Phase 4 will route to Python workers.
type Dispatcher interface {
	ExecuteStep(ctx context.Context, runID string, step models.Step) error
}
