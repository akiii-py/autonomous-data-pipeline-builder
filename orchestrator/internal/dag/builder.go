package dag

import (
	"fmt"

	"github.com/akshat/pipeline-orchestrator/internal/models"
)

func BuildFromCreateSteps(steps []models.CreateStepRequest) (*Graph, error) {
	g := NewGraph()

	for i, s := range steps {
		if s.Key == "" {
			return nil, fmt.Errorf("step at index %d is missing key", i)
		}
		if _, exists := g.Node(s.Key); exists {
			return nil, fmt.Errorf("duplicate step key: %s", s.Key)
		}

		g.AddNode(&Node{
			ID:     s.Key,
			Name:   s.Name,
			Type:   s.Type,
			Config: s.Config,
		})
	}

	for _, s := range steps {
		for _, dep := range s.DependsOn {
			if _, ok := g.Node(dep); !ok {
				return nil, fmt.Errorf("step %s depends on unknown key %s", s.Key, dep)
			}
			g.AddDependency(s.Key, dep)
		}
	}

	if errs := Validate(g); len(errs) > 0 {
		return nil, errs[0]
	}

	return g, nil
}

func BuildFromStoredSteps(steps []models.Step) (*Graph, error) {
	g := NewGraph()

	for i, s := range steps {
		if s.Key == "" {
			return nil, fmt.Errorf("stored step at index %d has empty key", i)
		}
		if _, exists := g.Node(s.Key); exists {
			return nil, fmt.Errorf("duplicate stored step key: %s", s.Key)
		}

		g.AddNode(&Node{
			ID:     s.Key,
			Name:   s.Name,
			Type:   s.Type,
			Config: s.Config,
		})
	}

	for _, s := range steps {
		for _, dep := range s.DependsOn {
			if _, ok := g.Node(dep); !ok {
				return nil, fmt.Errorf("stored step %s depends on unknown key %s", s.Key, dep)
			}
			g.AddDependency(s.Key, dep)
		}
	}

	if errs := Validate(g); len(errs) > 0 {
		return nil, errs[0]
	}

	return g, nil
}
