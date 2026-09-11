// Package runner owns the process side of run execution.
//
// The run row owns the run; this loop is a process that claims runs and
// heartbeats to hold them (D-07). Nothing here is the sole owner of durable
// work: if this process dies, the claims it held expire and another process
// picks the runs up, which is why restart recovery needs no separate sweep.
package runner

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/akshat/pipeline-orchestrator/internal/models"
	"github.com/akshat/pipeline-orchestrator/internal/scheduler"
	"github.com/akshat/pipeline-orchestrator/internal/store"
)

// Options configures the claim loop. Values come from internal/config.
type Options struct {
	// Owner identifies this process in claimed_by.
	Owner string
	// Lease is how long a claim is held before it becomes reclaimable.
	Lease time.Duration
	// Heartbeat is how often the claim is extended. Must be well under Lease.
	Heartbeat time.Duration
	// PollInterval is how long to wait after finding no claimable run.
	PollInterval time.Duration
	// MaxConcurrentRuns bounds how many runs this process drives at once.
	MaxConcurrentRuns int
}

func (o Options) withDefaults() Options {
	if o.Owner == "" {
		o.Owner = "orchestrator"
	}
	if o.Lease <= 0 {
		o.Lease = 30 * time.Second
	}
	if o.Heartbeat <= 0 || o.Heartbeat >= o.Lease {
		o.Heartbeat = o.Lease / 3
	}
	if o.PollInterval <= 0 {
		o.PollInterval = time.Second
	}
	if o.MaxConcurrentRuns < 1 {
		o.MaxConcurrentRuns = 1
	}
	return o
}

type Loop struct {
	store     *store.PipelineStore
	scheduler *scheduler.Scheduler
	opts      Options

	wg     sync.WaitGroup
	slots  chan struct{}
	active sync.Map // runID -> struct{}
}

func New(s *store.PipelineStore, sch *scheduler.Scheduler, opts Options) *Loop {
	opts = opts.withDefaults()
	return &Loop{
		store:     s,
		scheduler: sch,
		opts:      opts,
		slots:     make(chan struct{}, opts.MaxConcurrentRuns),
	}
}

// Start runs the claim loop until ctx is cancelled, then waits for in-flight
// runs to stop and releases their claims so another process can take them.
func (l *Loop) Start(ctx context.Context) {
	log.Printf("run loop started (owner=%s lease=%s max_concurrent=%d)",
		l.opts.Owner, l.opts.Lease, l.opts.MaxConcurrentRuns)

	prov := l.scheduler.Provenance()

	for {
		select {
		case <-ctx.Done():
			l.drain()
			return
		case l.slots <- struct{}{}:
		}

		run, err := l.store.ClaimRun(ctx, l.opts.Owner, l.opts.Lease, prov)
		if err != nil {
			<-l.slots
			if ctx.Err() != nil {
				l.drain()
				return
			}
			log.Printf("claim run: %v", err)
			l.sleep(ctx, l.opts.PollInterval)
			continue
		}
		if run == nil {
			<-l.slots
			if !l.sleep(ctx, l.opts.PollInterval) {
				l.drain()
				return
			}
			continue
		}

		l.wg.Add(1)
		go func(run *models.PipelineRun) {
			defer l.wg.Done()
			defer func() { <-l.slots }()
			l.drive(ctx, run)
		}(run)
	}
}

// drive executes one claimed run, holding the claim with a heartbeat and
// cancelling the run's context if the claim is lost or cancellation is
// requested.
func (l *Loop) drive(parent context.Context, run *models.PipelineRun) {
	l.active.Store(run.ID, struct{}{})
	defer l.active.Delete(run.ID)

	// The run's context is deliberately not derived from the HTTP request that
	// created it — that request returned long ago. It is derived from the
	// process lifetime and cancelled by lease loss or an explicit cancel
	// request (rule 6.5).
	runCtx, cancel := context.WithCancel(parent)
	defer cancel()

	done := make(chan struct{})
	go l.holdClaim(runCtx, run.ID, cancel, done)

	defer func() {
		if rec := recover(); rec != nil {
			// The scheduler recovers panics itself and records them; this is the
			// last line of defence so one run can never take the process down.
			log.Printf("recovered panic driving run=%s: %v", run.ID, rec)
		}
	}()

	err := l.scheduler.ExecuteRun(runCtx, run)
	close(done)

	if err != nil {
		log.Printf("run finished with error (pipeline=%s run=%s): %v", run.PipelineID, run.ID, err)
	}
}

// holdClaim extends the lease until the run finishes. Losing the claim — or an
// operator requesting cancellation — cancels the run's context.
func (l *Loop) holdClaim(ctx context.Context, runID string, cancel context.CancelFunc, done <-chan struct{}) {
	ticker := time.NewTicker(l.opts.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			held, err := l.store.Heartbeat(ctx, runID, l.opts.Owner, l.opts.Lease)
			if err != nil {
				log.Printf("heartbeat failed for run=%s: %v", runID, err)
				continue
			}
			if !held {
				log.Printf("lost claim on run=%s, stopping", runID)
				cancel()
				return
			}

			cancelled, err := l.store.CancelRequested(ctx, runID)
			if err != nil {
				continue
			}
			if cancelled {
				log.Printf("cancellation requested for run=%s", runID)
				cancel()
				return
			}
		}
	}
}

// drain waits for in-flight runs and releases their claims, so a restart hands
// work back rather than stranding it until the lease expires.
func (l *Loop) drain() {
	l.wg.Wait()

	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	l.active.Range(func(key, _ any) bool {
		runID, _ := key.(string)
		if err := l.store.ReleaseClaim(releaseCtx, runID, l.opts.Owner); err != nil {
			log.Printf("release claim for run=%s: %v", runID, err)
		}
		return true
	})

	log.Println("run loop stopped")
}

// sleep waits for d or until ctx is done. It reports false if ctx ended.
func (l *Loop) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
