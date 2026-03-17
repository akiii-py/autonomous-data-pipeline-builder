package scheduler

import (
	"testing"

	"github.com/akshat/pipeline-orchestrator/internal/dag"
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
	queued := map[string]bool{}

	ready := readyStepKeys(g, completed, queued)
	if len(ready) != 2 {
		t.Fatalf("expected 2 ready steps at start, got %d", len(ready))
	}

	completed["extract_a"] = true
	queued["extract_b"] = true
	ready = readyStepKeys(g, completed, queued)
	if len(ready) != 0 {
		t.Fatalf("expected no ready steps until both dependencies done, got %d", len(ready))
	}

	completed["extract_b"] = true
	ready = readyStepKeys(g, completed, map[string]bool{})
	if len(ready) != 1 || ready[0] != "join" {
		t.Fatalf("expected join ready after dependencies complete, got %#v", ready)
	}
}
