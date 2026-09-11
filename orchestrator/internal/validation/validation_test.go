package validation

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/akshat/pipeline-orchestrator/internal/catalog"
	"github.com/akshat/pipeline-orchestrator/internal/dag"
	"github.com/akshat/pipeline-orchestrator/internal/models"
)

func testCatalog(t *testing.T) *catalog.Context {
	t.Helper()
	c, err := catalog.Load(catalog.Options{
		Connectors:   []string{"file", "postgres"},
		Destinations: []string{"warehouse.sales"},
		TransformOps: []string{"select", "aggregate_sum"},
		SchemaInline: `[{"name":"warehouse.sales","columns":[{"name":"region"},{"name":"amount"}]}]`,
	})
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	return c
}

func step(key, stepType, config string, deps ...string) models.CreateStepRequest {
	if deps == nil {
		deps = []string{}
	}
	return models.CreateStepRequest{
		Key:       key,
		Name:      key,
		Type:      stepType,
		Config:    json.RawMessage(config),
		DependsOn: deps,
	}
}

func codes(errs dag.ValidationErrors) map[string]bool {
	out := make(map[string]bool, len(errs))
	for _, e := range errs {
		out[e.Code] = true
	}
	return out
}

func TestStructuralReportsEveryProblemAtOnce(t *testing.T) {
	draft := models.CreatePipelineRequest{
		Name: "",
		Steps: []models.CreateStepRequest{
			step("a", "nonsense", `{}`),
			step("b", models.StepTypeTransform, `{}`, "missing"),
		},
	}

	errs := Structural(draft)
	got := codes(errs)

	// A user fixing a rejected draft should not have to resubmit once per
	// problem (rule 5.2).
	for _, want := range []string{"missing_name", "unsupported_step_type", "unknown_dependency"} {
		if !got[want] {
			t.Errorf("expected code %q in %v", want, errs.Codes())
		}
	}
}

func TestStructuralAcceptsAValidDraft(t *testing.T) {
	draft := models.CreatePipelineRequest{
		Name: "sales",
		Steps: []models.CreateStepRequest{
			step("extract_sales", models.StepTypeExtract, `{"connector":"file","path":"/data/in.json"}`),
			step("agg", models.StepTypeTransform, `{"input_from":"extract_sales","op":"select"}`, "extract_sales"),
		},
	}

	if errs := Structural(draft); len(errs) > 0 {
		t.Fatalf("expected no structural errors, got %v", errs.Messages())
	}
}

func TestSemanticRejectsConnectorOutsideAllowlist(t *testing.T) {
	draft := models.CreatePipelineRequest{
		Name:  "x",
		Steps: []models.CreateStepRequest{step("e", models.StepTypeExtract, `{"connector":"http","url":"https://x/y"}`)},
	}

	if !codes(Semantic(draft, testCatalog(t)))["connector_not_allowed"] {
		t.Fatal("expected connector_not_allowed")
	}
}

func TestSemanticRejectsDestinationOutsideAllowlist(t *testing.T) {
	draft := models.CreatePipelineRequest{
		Name: "x",
		Steps: []models.CreateStepRequest{
			step("e", models.StepTypeExtract, `{"connector":"file","path":"/in"}`),
			step("l", models.StepTypeLoad, `{"connector":"postgres","input_from":"e","table":"prod.customers"}`, "e"),
		},
	}

	got := codes(Semantic(draft, testCatalog(t)))
	if !got["destination_not_allowed"] {
		t.Fatal("expected destination_not_allowed")
	}
	// The table is also absent from the described schema, and both are worth
	// reporting.
	if !got["unknown_table"] {
		t.Fatal("expected unknown_table")
	}
}

// A draft can be perfectly acyclic and still be wired wrongly: this is the case
// structural validation cannot see (D-13).
func TestSemanticRejectsInputFromNotDeclaredAsDependency(t *testing.T) {
	draft := models.CreatePipelineRequest{
		Name: "x",
		Steps: []models.CreateStepRequest{
			step("e", models.StepTypeExtract, `{"connector":"file","path":"/in"}`),
			step("t", models.StepTypeTransform, `{"input_from":"e","op":"select"}`),
		},
	}

	if errs := Structural(draft); len(errs) > 0 {
		t.Fatalf("draft should be structurally valid, got %v", errs.Messages())
	}
	if !codes(Semantic(draft, testCatalog(t)))["input_from_not_declared"] {
		t.Fatal("expected input_from_not_declared")
	}
}

func TestSemanticRejectsUnknownInputFrom(t *testing.T) {
	draft := models.CreatePipelineRequest{
		Name: "x",
		Steps: []models.CreateStepRequest{
			step("t", models.StepTypeTransform, `{"input_from":"ghost","op":"select"}`),
		},
	}

	if !codes(Semantic(draft, testCatalog(t)))["unknown_input_from"] {
		t.Fatal("expected unknown_input_from")
	}
}

func TestSemanticRestrictsExtractToSelect(t *testing.T) {
	cases := map[string]bool{
		`{"connector":"postgres","query":"SELECT 1"}`:                               false,
		`{"connector":"postgres","query":"  with x as (select 1) select * from x"}`: false,
		`{"connector":"postgres","query":"DELETE FROM users"}`:                      true,
		`{"connector":"postgres","query":"SELECT 1; DROP TABLE users"}`:             true,
	}

	for config, wantProblem := range cases {
		draft := models.CreatePipelineRequest{
			Name:  "x",
			Steps: []models.CreateStepRequest{step("e", models.StepTypeExtract, config)},
		}
		got := codes(Semantic(draft, testCatalog(t)))
		problem := got["non_select_extract"] || got["stacked_statement"]
		if problem != wantProblem {
			t.Errorf("config %s: expected problem=%v, got %v", config, wantProblem, problem)
		}
	}
}

func TestSemanticRejectsPlaintextCredentials(t *testing.T) {
	draft := models.CreatePipelineRequest{
		Name:  "x",
		Steps: []models.CreateStepRequest{step("e", models.StepTypeExtract, `{"connector":"postgres","dsn":"postgresql://u:p@h/db","query":"SELECT 1"}`)},
	}

	if !codes(Semantic(draft, testCatalog(t)))["plaintext_credential"] {
		t.Fatal("expected plaintext_credential")
	}
}

// An empty destination allowlist must deny, not permit: an unconfigured system
// should not be able to write anywhere.
func TestEmptyDestinationAllowlistFailsClosed(t *testing.T) {
	c, err := catalog.Load(catalog.Options{Connectors: []string{"file"}, TransformOps: []string{"select"}})
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}

	draft := models.CreatePipelineRequest{
		Name: "x",
		Steps: []models.CreateStepRequest{
			step("e", models.StepTypeExtract, `{"connector":"file","path":"/in"}`),
			step("l", models.StepTypeLoad, `{"connector":"file","input_from":"e","path":"/out"}`, "e"),
		},
	}

	if !codes(Semantic(draft, c))["destination_not_allowed"] {
		t.Fatal("an empty destination allowlist must deny every destination")
	}
}

func TestRequiresApprovalDetectsIrreversibleWork(t *testing.T) {
	tests := []struct {
		name  string
		steps []models.CreateStepRequest
		want  bool
	}{
		{
			name:  "read only",
			steps: []models.CreateStepRequest{step("e", models.StepTypeExtract, `{"connector":"file","path":"/in"}`)},
			want:  false,
		},
		{
			name:  "load step",
			steps: []models.CreateStepRequest{step("l", models.StepTypeLoad, `{"connector":"file","path":"/out"}`)},
			want:  true,
		},
		{
			name:  "credential bearing extract",
			steps: []models.CreateStepRequest{step("e", models.StepTypeExtract, `{"connector":"postgres","dsn_ref":"PG"}`)},
			want:  true,
		},
		{
			name:  "unparseable config is not assumed safe",
			steps: []models.CreateStepRequest{step("e", models.StepTypeExtract, `{`)},
			want:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := models.RequiresApproval(tc.steps); got != tc.want {
				t.Fatalf("expected RequiresApproval=%v, got %v", tc.want, got)
			}
		})
	}
}

func TestRedactConfigStripsCredentials(t *testing.T) {
	out := string(models.RedactConfig(json.RawMessage(`{"table":"t","dsn":"postgresql://u:secret@h/db"}`)))

	if strings.Contains(out, "secret") {
		t.Fatalf("credential survived redaction: %s", out)
	}
	if !strings.Contains(out, `"table":"t"`) {
		t.Fatalf("redaction dropped non-credential fields: %s", out)
	}
}
