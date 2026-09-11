package models

import (
	"encoding/json"
	"strings"
)

// StepConfig is the typed view of a step's JSON config. It exists so that retry
// policy, approval detection, semantic validation and redaction all read the
// same shape instead of each unmarshalling into map[string]interface{} and
// picking out keys by hand.
type StepConfig struct {
	Connector  string   `json:"connector,omitempty"`
	InputFrom  string   `json:"input_from,omitempty"`
	Op         string   `json:"op,omitempty"`
	Query      string   `json:"query,omitempty"`
	Table      string   `json:"table,omitempty"`
	Path       string   `json:"path,omitempty"`
	URL        string   `json:"url,omitempty"`
	Format     string   `json:"format,omitempty"`
	Field      string   `json:"field,omitempty"`
	GroupBy    string   `json:"group_by,omitempty"`
	Fields     []string `json:"fields,omitempty"`
	RetryCount *int     `json:"retry_count,omitempty"`

	// DSNRef names an environment variable the worker resolves. This is the
	// supported way to reach a database (rule 6.4).
	DSNRef string `json:"dsn_ref,omitempty"`
	// DSN is a plaintext connection string. Accepted by the parser only so that
	// validation can reject it and redaction can strip it — never persist it.
	DSN string `json:"dsn,omitempty"`
}

// ParseStepConfig decodes a step config. An empty config is valid and yields a
// zero value.
func ParseStepConfig(raw json.RawMessage) (StepConfig, error) {
	var cfg StepConfig
	if len(raw) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// ConnectorName returns the effective connector, applying the worker's default.
func (c StepConfig) ConnectorName() string {
	name := strings.TrimSpace(c.Connector)
	if name == "" {
		return "file"
	}
	return name
}

// MaxRetries is the per-step retry budget. Absent or negative means none.
func (c StepConfig) MaxRetries() int {
	if c.RetryCount == nil || *c.RetryCount < 0 {
		return 0
	}
	return *c.RetryCount
}

// CarriesCredentials reports whether this config reaches a system that needs
// credentials, which is one of the two triggers for the approval stage (D-12).
func (c StepConfig) CarriesCredentials() bool {
	return strings.TrimSpace(c.DSN) != "" || strings.TrimSpace(c.DSNRef) != ""
}

// Destination returns the sink this config writes to, for allowlist checks.
func (c StepConfig) Destination() string {
	switch {
	case strings.TrimSpace(c.Table) != "":
		return strings.TrimSpace(c.Table)
	case strings.TrimSpace(c.URL) != "":
		return strings.TrimSpace(c.URL)
	case strings.TrimSpace(c.Path) != "":
		return strings.TrimSpace(c.Path)
	default:
		return ""
	}
}

// RequiresApproval reports whether a pipeline contains an irreversible operation
// and therefore may not run until a human approves it (D-12).
func RequiresApproval(steps []CreateStepRequest) bool {
	for _, s := range steps {
		if strings.TrimSpace(s.Type) == StepTypeLoad {
			return true
		}
		cfg, err := ParseStepConfig(s.Config)
		if err != nil {
			// An unparseable config is treated as requiring approval: we cannot
			// prove it is safe.
			return true
		}
		if cfg.CarriesCredentials() {
			return true
		}
	}
	return false
}

// StoredStepsRequireApproval is the same check against persisted steps.
func StoredStepsRequireApproval(steps []Step) bool {
	reqs := make([]CreateStepRequest, 0, len(steps))
	for _, s := range steps {
		reqs = append(reqs, CreateStepRequest{Type: s.Type, Config: s.Config})
	}
	return RequiresApproval(reqs)
}

// RedactConfig strips credential-shaped fields from a config before it leaves
// the process on a read path (rule 6.4).
func RedactConfig(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}

	var generic map[string]json.RawMessage
	if err := json.Unmarshal(raw, &generic); err != nil {
		return raw
	}

	redacted := false
	for _, key := range []string{"dsn", "password", "secret", "token", "api_key"} {
		if _, ok := generic[key]; ok {
			generic[key] = json.RawMessage(`"[redacted]"`)
			redacted = true
		}
	}
	if !redacted {
		return raw
	}

	out, err := json.Marshal(generic)
	if err != nil {
		return raw
	}
	return out
}
