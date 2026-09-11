package dag

import (
	"fmt"

	"github.com/akshat/pipeline-orchestrator/internal/models"
)

// stepShape is the minimal view of a step both builders need, so the two entry
// points share one implementation instead of duplicating the collection logic.
type stepShape struct {
	Key       string
	Name      string
	Type      string
	Config    []byte
	DependsOn []string
}

func BuildFromCreateSteps(steps []models.CreateStepRequest) (*Graph, error) {
	shapes := make([]stepShape, 0, len(steps))
	for _, s := range steps {
		shapes = append(shapes, stepShape{
			Key:       s.Key,
			Name:      s.Name,
			Type:      s.Type,
			Config:    s.Config,
			DependsOn: s.DependsOn,
		})
	}
	return build(shapes, "step")
}

func BuildFromStoredSteps(steps []models.Step) (*Graph, error) {
	shapes := make([]stepShape, 0, len(steps))
	for _, s := range steps {
		shapes = append(shapes, stepShape{
			Key:       s.Key,
			Name:      s.Name,
			Type:      s.Type,
			Config:    s.Config,
			DependsOn: s.DependsOn,
		})
	}
	return build(shapes, "stored step")
}

// build collects every structural problem rather than returning the first, so a
// caller fixing a rejected draft sees the whole set at once (D-16).
func build(steps []stepShape, noun string) (*Graph, error) {
	g := NewGraph()
	errs := make(ValidationErrors, 0)

	for i, s := range steps {
		if s.Key == "" {
			errs = append(errs, ValidationError{
				Code:    "missing_key",
				Message: fmt.Sprintf("%s at index %d is missing key", noun, i),
			})
			continue
		}
		if _, exists := g.Node(s.Key); exists {
			errs = append(errs, ValidationError{
				Code:    "duplicate_key",
				StepKey: s.Key,
				Message: "duplicate step key",
			})
			continue
		}

		g.AddNode(&Node{
			ID:     s.Key,
			Name:   s.Name,
			Type:   s.Type,
			Config: s.Config,
		})
	}

	for _, s := range steps {
		if s.Key == "" {
			continue
		}
		for _, dep := range s.DependsOn {
			if _, ok := g.Node(dep); !ok {
				errs = append(errs, ValidationError{
					Code:    "unknown_dependency",
					StepKey: s.Key,
					Message: fmt.Sprintf("depends on unknown key %s", dep),
				})
				continue
			}
			g.AddDependency(s.Key, dep)
		}
	}

	errs = append(errs, Validate(g)...)

	if len(errs) > 0 {
		return nil, errs
	}
	return g, nil
}
