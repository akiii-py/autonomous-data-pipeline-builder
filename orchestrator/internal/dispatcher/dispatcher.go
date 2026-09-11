package dispatcher

import (
	"context"

	"github.com/akshat/pipeline-orchestrator/internal/models"
)

// Dispatcher executes one step for a given run.
//
// ExecuteStep returns a StepResult describing what happened — rows processed,
// error classification, whether the worker replayed a prior execution, whether
// the work was simulated (D-01). A non-nil error means the attempt could not be
// made at all (a malformed request, no configured target); an attempt that was
// made and failed is reported through the result, so the scheduler's retry
// decision has a classification to read (D-02).
type Dispatcher interface {
	ExecuteStep(ctx context.Context, req models.StepRequest) (*models.StepResult, error)

	// Provenance describes how this dispatcher executes, so the run row can
	// record what actually ran it (D-09). The scheduler reads this instead of
	// type-asserting the implementation (rule 6.3).
	Provenance() models.Provenance
}
