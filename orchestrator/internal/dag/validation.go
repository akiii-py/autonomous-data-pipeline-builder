package dag

import (
	"fmt"
	"strings"
)

// ValidationError represents a single DAG validation failure. Callers match on
// it with errors.As rather than on message text (D-16).
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

// ValidationErrors is the full set of problems found in one pass. Builders
// return this rather than the first error, because the caller is a user fixing a
// rejected draft and resubmitting once per problem is the wrong loop (D-16).
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return "invalid pipeline dag"
	}

	parts := make([]string, 0, len(e))
	for _, ve := range e {
		parts = append(parts, ve.Error())
	}
	return strings.Join(parts, "; ")
}

// Codes returns the distinct failure codes, for event metadata and API replies.
func (e ValidationErrors) Codes() []string {
	seen := make(map[string]bool, len(e))
	codes := make([]string, 0, len(e))
	for _, ve := range e {
		if seen[ve.Code] {
			continue
		}
		seen[ve.Code] = true
		codes = append(codes, ve.Code)
	}
	return codes
}

// Messages returns every failure as a string, for surfacing the whole set to the
// client in one response.
func (e ValidationErrors) Messages() []string {
	out := make([]string, 0, len(e))
	for _, ve := range e {
		out = append(out, ve.Error())
	}
	return out
}

// Validate collects every structural problem in the graph. It does not stop at
// the first.
func Validate(g *Graph) ValidationErrors {
	errs := make(ValidationErrors, 0)

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
