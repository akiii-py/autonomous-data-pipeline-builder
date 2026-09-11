// Package validation splits draft checking into two stages with different
// inputs (D-13).
//
// Structural needs only the draft: shape, keys, types, acyclicity. Semantic
// needs the draft plus the catalog context it was generated against. They are
// separate call sites so the semantic stage can grow without disturbing the
// structural one.
package validation

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/akshat/pipeline-orchestrator/internal/catalog"
	"github.com/akshat/pipeline-orchestrator/internal/dag"
	"github.com/akshat/pipeline-orchestrator/internal/models"
)

// selectOnly matches a statement whose first keyword is SELECT or WITH. Extract
// steps are restricted to reads (rule 1.2 of the trust boundary, audit E8).
var selectOnly = regexp.MustCompile(`(?is)^\s*(?:select|with)\b`)

// statementSeparator catches a second statement stacked onto a query.
var statementSeparator = regexp.MustCompile(`;\s*\S`)

// Structural validates everything that can be checked from the draft alone.
func Structural(draft models.CreatePipelineRequest) dag.ValidationErrors {
	errs := make(dag.ValidationErrors, 0)

	if strings.TrimSpace(draft.Name) == "" {
		errs = append(errs, dag.ValidationError{
			Code:    "missing_name",
			Message: "pipeline name is required",
		})
	}
	if len(draft.Steps) == 0 {
		errs = append(errs, dag.ValidationError{
			Code:    "no_steps",
			Message: "at least one step is required",
		})
	}

	for i, step := range draft.Steps {
		key := strings.TrimSpace(step.Key)
		if key == "" {
			errs = append(errs, dag.ValidationError{
				Code:    "missing_key",
				Message: fmt.Sprintf("step at index %d is missing key", i),
			})
			continue
		}
		if !isSupportedStepType(step.Type) {
			errs = append(errs, dag.ValidationError{
				Code:    "unsupported_step_type",
				StepKey: key,
				Message: fmt.Sprintf("unsupported step type %q", strings.TrimSpace(step.Type)),
			})
		}
		if _, err := models.ParseStepConfig(step.Config); err != nil {
			errs = append(errs, dag.ValidationError{
				Code:    "unparseable_config",
				StepKey: key,
				Message: err.Error(),
			})
		}
	}

	if _, err := dag.BuildFromCreateSteps(draft.Steps); err != nil {
		var built dag.ValidationErrors
		if asValidationErrors(err, &built) {
			errs = append(errs, built...)
		} else {
			errs = append(errs, dag.ValidationError{Code: "invalid_dag", Message: err.Error()})
		}
	}

	return errs
}

// Semantic validates meaning against the context the draft was generated from.
// A draft can be perfectly acyclic and still write to the wrong table; this is
// the stage that catches that.
func Semantic(draft models.CreatePipelineRequest, cat *catalog.Context) dag.ValidationErrors {
	errs := make(dag.ValidationErrors, 0)

	stepTypes := make(map[string]string, len(draft.Steps))
	declaredDeps := make(map[string]map[string]bool, len(draft.Steps))
	for _, step := range draft.Steps {
		key := strings.TrimSpace(step.Key)
		if key == "" {
			continue
		}
		stepTypes[key] = strings.TrimSpace(step.Type)
		deps := make(map[string]bool, len(step.DependsOn))
		for _, d := range step.DependsOn {
			deps[strings.TrimSpace(d)] = true
		}
		declaredDeps[key] = deps
	}

	for _, step := range draft.Steps {
		key := strings.TrimSpace(step.Key)
		if key == "" {
			continue
		}
		stepType := strings.TrimSpace(step.Type)

		cfg, err := models.ParseStepConfig(step.Config)
		if err != nil {
			// Structural already reported this; do not double-report.
			continue
		}

		if cfg.DSN != "" {
			errs = append(errs, dag.ValidationError{
				Code:    "plaintext_credential",
				StepKey: key,
				Message: "config.dsn is a plaintext credential; use dsn_ref naming an environment variable",
			})
		}

		if stepType == models.StepTypeExtract || stepType == models.StepTypeLoad {
			if !cat.HasConnector(cfg.ConnectorName()) {
				errs = append(errs, dag.ValidationError{
					Code:    "connector_not_allowed",
					StepKey: key,
					Message: fmt.Sprintf("connector %q is not in the allowlist %v", cfg.ConnectorName(), cat.Connectors),
				})
			}
		}

		switch stepType {
		case models.StepTypeExtract:
			errs = append(errs, validateExtract(key, cfg, cat)...)
		case models.StepTypeTransform:
			errs = append(errs, validateTransform(key, cfg, cat)...)
		case models.StepTypeLoad:
			errs = append(errs, validateLoad(key, cfg, cat)...)
		}

		// input_from must name a real upstream step and must be declared as a
		// dependency — otherwise the scheduler can run this step before the
		// artifact it reads exists.
		if cfg.InputFrom != "" {
			src := strings.TrimSpace(cfg.InputFrom)
			if _, ok := stepTypes[src]; !ok {
				errs = append(errs, dag.ValidationError{
					Code:    "unknown_input_from",
					StepKey: key,
					Message: fmt.Sprintf("input_from %q does not name a step in this pipeline", src),
				})
			} else if !declaredDeps[key][src] {
				errs = append(errs, dag.ValidationError{
					Code:    "input_from_not_declared",
					StepKey: key,
					Message: fmt.Sprintf("input_from %q is not listed in depends_on", src),
				})
			}
		}
	}

	return errs
}

func validateExtract(key string, cfg models.StepConfig, cat *catalog.Context) dag.ValidationErrors {
	errs := make(dag.ValidationErrors, 0)

	if cfg.Query != "" {
		if !selectOnly.MatchString(cfg.Query) {
			errs = append(errs, dag.ValidationError{
				Code:    "non_select_extract",
				StepKey: key,
				Message: "extract queries are restricted to SELECT",
			})
		}
		if statementSeparator.MatchString(cfg.Query) {
			errs = append(errs, dag.ValidationError{
				Code:    "stacked_statement",
				StepKey: key,
				Message: "extract query contains more than one statement",
			})
		}
	}

	if cfg.Table != "" && cat.DescribesSchema() && !cat.HasTable(cfg.Table) {
		errs = append(errs, dag.ValidationError{
			Code:    "unknown_table",
			StepKey: key,
			Message: fmt.Sprintf("table %q is not in the described schema", cfg.Table),
		})
	}

	return errs
}

func validateTransform(key string, cfg models.StepConfig, cat *catalog.Context) dag.ValidationErrors {
	errs := make(dag.ValidationErrors, 0)

	if cfg.InputFrom == "" {
		errs = append(errs, dag.ValidationError{
			Code:    "missing_input_from",
			StepKey: key,
			Message: "transform requires config.input_from",
		})
	}
	if !cat.HasTransformOp(cfg.Op) {
		errs = append(errs, dag.ValidationError{
			Code:    "unsupported_transform_op",
			StepKey: key,
			Message: fmt.Sprintf("transform op %q is not one of %v", cfg.Op, cat.TransformOps),
		})
	}

	return errs
}

func validateLoad(key string, cfg models.StepConfig, cat *catalog.Context) dag.ValidationErrors {
	errs := make(dag.ValidationErrors, 0)

	if cfg.InputFrom == "" {
		errs = append(errs, dag.ValidationError{
			Code:    "missing_input_from",
			StepKey: key,
			Message: "load requires config.input_from",
		})
	}

	dest := cfg.Destination()
	if dest == "" {
		errs = append(errs, dag.ValidationError{
			Code:    "missing_destination",
			StepKey: key,
			Message: "load requires a destination (table, url, or path)",
		})
	} else if !cat.HasDestination(dest) {
		errs = append(errs, dag.ValidationError{
			Code:    "destination_not_allowed",
			StepKey: key,
			Message: fmt.Sprintf("destination %q is not in the allowlist", dest),
		})
	}

	if cfg.Table != "" && cat.DescribesSchema() && !cat.HasTable(cfg.Table) {
		errs = append(errs, dag.ValidationError{
			Code:    "unknown_table",
			StepKey: key,
			Message: fmt.Sprintf("table %q is not in the described schema", cfg.Table),
		})
	}

	return errs
}

func isSupportedStepType(stepType string) bool {
	switch strings.TrimSpace(stepType) {
	case models.StepTypeExtract, models.StepTypeTransform, models.StepTypeLoad:
		return true
	default:
		return false
	}
}

// asValidationErrors is errors.As specialised to the concrete slice type, which
// errors.As cannot target directly because ValidationErrors is not a pointer
// type.
func asValidationErrors(err error, out *dag.ValidationErrors) bool {
	if ve, ok := err.(dag.ValidationErrors); ok {
		*out = ve
		return true
	}
	return false
}
