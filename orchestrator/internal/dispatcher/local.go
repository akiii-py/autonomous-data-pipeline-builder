package dispatcher

import (
	"context"
	"time"

	"github.com/akshat/pipeline-orchestrator/internal/models"
)

// LocalDispatcher simulates execution in-process. It touches no data, and says
// so: every result it returns is marked Simulated, and its Provenance reports
// the same, so no run executed this way can be mistaken for a real one (D-09).
type LocalDispatcher struct {
	delay time.Duration
}

func NewLocalDispatcher() *LocalDispatcher {
	return &LocalDispatcher{delay: 10 * time.Millisecond}
}

func (d *LocalDispatcher) Provenance() models.Provenance {
	return models.Provenance{
		ExecMode:  models.ExecModeLocal,
		Simulated: true,
	}
}

func (d *LocalDispatcher) ExecuteStep(ctx context.Context, req models.StepRequest) (*models.StepResult, error) {
	select {
	case <-ctx.Done():
		return &models.StepResult{
			RunID:          req.RunID,
			StepKey:        req.Step.Key,
			IdempotencyKey: req.IdempotencyKey,
			Attempt:        req.Attempt,
			Simulated:      true,
			ErrorClass:     models.ErrorClassPermanent,
			ErrorMessage:   ctx.Err().Error(),
		}, nil
	case <-time.After(d.delay):
		return &models.StepResult{
			RunID:          req.RunID,
			StepKey:        req.Step.Key,
			IdempotencyKey: req.IdempotencyKey,
			Attempt:        req.Attempt,
			Simulated:      true,
		}, nil
	}
}
