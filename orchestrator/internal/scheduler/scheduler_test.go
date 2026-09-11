package scheduler

import (
	"testing"
	"time"

	"github.com/akshat/pipeline-orchestrator/internal/dag"
	"github.com/akshat/pipeline-orchestrator/internal/models"
)

func TestReadyStepKeys(t *testing.T) {
	g := dag.NewGraph()
	g.AddNode(&dag.Node{ID: "extract_a"})
	g.AddNode(&dag.Node{ID: "extract_b"})
	g.AddNode(&dag.Node{ID: "join"})
	g.AddNode(&dag.Node{ID: "load"})

	g.AddDependency("join", "extract_a")
	g.AddDependency("join", "extract_b")
	g.AddDependency("load", "join")

	completed := map[string]bool{}

	// Both roots are ready together: the ready set carries the DAG's
	// parallelism information, and the scheduler is expected to use it.
	ready := readyStepKeys(g, completed)
	if len(ready) != 2 {
		t.Fatalf("expected 2 ready steps at start, got %d (%v)", len(ready), ready)
	}

	completed["extract_a"] = true
	ready = readyStepKeys(g, completed)
	if len(ready) != 1 || ready[0] != "extract_b" {
		t.Fatalf("expected only extract_b ready, got %v", ready)
	}

	completed["extract_b"] = true
	ready = readyStepKeys(g, completed)
	if len(ready) != 1 || ready[0] != "join" {
		t.Fatalf("expected join ready after dependencies complete, got %v", ready)
	}
}

func TestMaxRetriesFromStepConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		want    int
		wantErr bool
	}{
		{name: "no config", config: "", want: 0},
		{name: "valid retry_count", config: `{"retry_count": 2}`, want: 2},
		{name: "negative retry_count", config: `{"retry_count": -1}`, want: 0},
		{name: "absent retry_count", config: `{"connector":"file"}`, want: 0},
		{name: "wrong type", config: `{"retry_count": "x"}`, want: 0, wantErr: true},
		{name: "invalid json", config: `{"retry_count":`, want: 0, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := models.ParseStepConfig([]byte(tc.config))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected a parse error for %q", tc.config)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse config: %v", err)
			}
			if got := cfg.MaxRetries(); got != tc.want {
				t.Fatalf("expected %d retries, got %d", tc.want, got)
			}
		})
	}
}

// Backoff must grow and must stay inside the configured ceiling, so a ready set
// that fails together does not retry in lockstep.
func TestBackoffGrowsAndIsCapped(t *testing.T) {
	s := New(nil, nil, Options{BackoffBase: 100 * time.Millisecond, BackoffMax: time.Second})

	first := s.backoff(0)
	if first < 50*time.Millisecond || first > 100*time.Millisecond {
		t.Fatalf("first backoff outside jitter window: %v", first)
	}

	for i := 0; i < 10; i++ {
		if d := s.backoff(i); d > time.Second {
			t.Fatalf("backoff exceeded max at retry %d: %v", i, d)
		}
	}
}

func TestOptionsDefaults(t *testing.T) {
	opts := Options{}.withDefaults()
	if opts.MaxConcurrency < 1 {
		t.Fatalf("concurrency must be at least 1, got %d", opts.MaxConcurrency)
	}
	if opts.BackoffBase <= 0 || opts.BackoffMax <= 0 {
		t.Fatalf("backoff bounds must be positive, got %v/%v", opts.BackoffBase, opts.BackoffMax)
	}
}
