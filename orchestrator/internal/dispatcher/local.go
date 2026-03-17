package dispatcher

import (
	"context"
	"time"

	"github.com/akshat/pipeline-orchestrator/internal/models"
)

// LocalDispatcher is a temporary in-process executor used before worker integration.
type LocalDispatcher struct{}

func NewLocalDispatcher() *LocalDispatcher {
	return &LocalDispatcher{}
}

func (d *LocalDispatcher) ExecuteStep(ctx context.Context, runID string, step models.Step) error {
	// Simulate small execution time and support cancellation.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Millisecond):
		return nil
	}
}
