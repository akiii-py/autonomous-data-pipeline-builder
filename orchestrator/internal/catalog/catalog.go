// Package catalog holds the context a generated pipeline is validated against:
// the schema the model was told about, the connectors it may use, and the
// destinations it may write to.
//
// It exists because "referenced tables must exist" is not implementable until
// the system tells the model what tables exist and keeps the same description
// around to check the answer (D-14). The one object is both what is sent and
// what validates the reply (rule 3.3).
package catalog

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
)

// Column is one column in the described schema.
type Column struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
}

// Table is one table the model is allowed to know about.
type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns,omitempty"`
}

// Context is the full description sent to the interpreter and used to validate
// what comes back.
type Context struct {
	Tables       []Table  `json:"tables"`
	Connectors   []string `json:"connectors"`
	Destinations []string `json:"destinations"`
	TransformOps []string `json:"transform_ops"`
}

// Provider supplies the current catalog. It is an interface so the catalog can
// later be read from the database without changing any call site.
type Provider interface {
	Current(ctx context.Context) (*Context, error)
}

// Static is a Provider backed by a fixed Context.
type Static struct{ ctx *Context }

func NewStatic(c *Context) *Static { return &Static{ctx: c} }

func (s *Static) Current(context.Context) (*Context, error) { return s.ctx, nil }

// Options configures Load. Every field comes from internal/config — this
// package never reads the environment itself (rule 6.1), except for the
// explicitly passed schema file path.
type Options struct {
	Connectors   []string
	Destinations []string
	TransformOps []string
	SchemaPath   string
	SchemaInline string
}

// Load builds a Context. A schema is optional; connectors and destinations are
// not — an empty destination allowlist means no generated pipeline may write
// anywhere, which is the correct fail-closed default.
func Load(opts Options) (*Context, error) {
	c := &Context{
		Tables:       make([]Table, 0),
		Connectors:   normalizeList(opts.Connectors),
		Destinations: normalizeList(opts.Destinations),
		TransformOps: normalizeList(opts.TransformOps),
	}

	raw := strings.TrimSpace(opts.SchemaInline)
	if raw == "" && strings.TrimSpace(opts.SchemaPath) != "" {
		b, err := os.ReadFile(opts.SchemaPath)
		if err != nil {
			return nil, err
		}
		raw = string(b)
	}
	if raw == "" {
		return c, nil
	}

	var tables []Table
	if err := json.Unmarshal([]byte(raw), &tables); err != nil {
		return nil, err
	}
	c.Tables = tables
	return c, nil
}

func normalizeList(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// HasConnector reports whether a connector is allowed.
func (c *Context) HasConnector(name string) bool {
	return c != nil && contains(c.Connectors, strings.TrimSpace(name))
}

// HasTransformOp reports whether a transform op is allowed.
func (c *Context) HasTransformOp(op string) bool {
	return c != nil && contains(c.TransformOps, strings.TrimSpace(op))
}

// HasDestination reports whether a sink is allowed. An empty allowlist denies
// everything — fail closed.
func (c *Context) HasDestination(dest string) bool {
	return c != nil && contains(c.Destinations, strings.TrimSpace(dest))
}

// HasTable reports whether the described schema contains a table.
func (c *Context) HasTable(name string) bool {
	_, ok := c.table(name)
	return ok
}

// HasColumn reports whether a table in the described schema has a column.
func (c *Context) HasColumn(table, column string) bool {
	t, ok := c.table(table)
	if !ok {
		return false
	}
	for _, col := range t.Columns {
		if strings.EqualFold(col.Name, strings.TrimSpace(column)) {
			return true
		}
	}
	return false
}

// DescribesSchema reports whether any table was supplied. When false, table and
// column checks are skipped rather than failing everything — there is nothing to
// check against, and that is reported as a warning by the validator.
func (c *Context) DescribesSchema() bool {
	return c != nil && len(c.Tables) > 0
}

func (c *Context) table(name string) (Table, bool) {
	if c == nil {
		return Table{}, false
	}
	name = strings.TrimSpace(name)
	for _, t := range c.Tables {
		if strings.EqualFold(t.Name, name) {
			return t, true
		}
	}
	return Table{}, false
}

func contains(list []string, v string) bool {
	if v == "" {
		return false
	}
	for _, item := range list {
		if strings.EqualFold(item, v) {
			return true
		}
	}
	return false
}
