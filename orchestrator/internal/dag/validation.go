package dag

import "fmt"

// ValidationError represents a DAG validation failure.
type ValidationError struct {
	Code    string
	StepKey string
	Message string
}

func (e ValidationError) Error() string {
	if e.StepKey == "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s (%s): %s", e.Code, e.StepKey, e.Message)
}

func Validate(g *Graph) []error {
	errs := make([]error, 0)

	for _, n := range g.Nodes() {
		for _, dep := range g.DependsOn(n.ID) {
			if dep == n.ID {
				errs = append(errs, ValidationError{
					Code:    "self_dependency",
					StepKey: n.ID,
					Message: "step cannot depend on itself",
				})
			}
		}
	}

	if _, ok := g.TopologicalSort(); !ok {
		errs = append(errs, ValidationError{
			Code:    "cycle",
			Message: "dependency cycle detected",
		})
	}

	return errs
}
