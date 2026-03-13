package dag

import (
	"testing"

	"github.com/akshat/pipeline-orchestrator/internal/models"
)

func TestBuildFromCreateStepsValid(t *testing.T) {
	steps := []models.CreateStepRequest{
		{Key: "extract_sales", Name: "Extract Sales", Type: models.StepTypeExtract},
		{Key: "extract_inventory", Name: "Extract Inventory", Type: models.StepTypeExtract},
		{Key: "join_data", Name: "Join", Type: models.StepTypeTransform, DependsOn: []string{"extract_sales", "extract_inventory"}},
		{Key: "load_s3", Name: "Load", Type: models.StepTypeLoad, DependsOn: []string{"join_data"}},
	}

	g, err := BuildFromCreateSteps(steps)
	if err != nil {
		t.Fatalf("expected valid DAG, got error: %v", err)
	}

	roots := g.Roots()
	if len(roots) != 2 {
		t.Fatalf("expected 2 roots, got %d", len(roots))
	}

	order, ok := g.TopologicalSort()
	if !ok {
		t.Fatal("expected topological sort to succeed")
	}
	if len(order) != 4 {
		t.Fatalf("expected 4 nodes in order, got %d", len(order))
	}
}

func TestBuildFromCreateStepsRejectsCycles(t *testing.T) {
	steps := []models.CreateStepRequest{
		{Key: "a", Name: "A", Type: models.StepTypeExtract, DependsOn: []string{"b"}},
		{Key: "b", Name: "B", Type: models.StepTypeTransform, DependsOn: []string{"a"}},
	}

	_, err := BuildFromCreateSteps(steps)
	if err == nil {
		t.Fatal("expected cycle validation error, got nil")
	}
}

func TestBuildFromCreateStepsRejectsUnknownDependencies(t *testing.T) {
	steps := []models.CreateStepRequest{
		{Key: "a", Name: "A", Type: models.StepTypeExtract, DependsOn: []string{"missing"}},
	}

	_, err := BuildFromCreateSteps(steps)
	if err == nil {
		t.Fatal("expected unknown dependency error, got nil")
	}
}
