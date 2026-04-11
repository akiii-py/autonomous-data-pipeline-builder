package interpreter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPClientInterpretSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/interpret" {
			t.Fatalf("expected /interpret, got %s", r.URL.Path)
		}

		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Query == "" {
			t.Fatalf("expected query to be forwarded")
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"confidence": 0.95,
			"warnings":   []string{"minor ambiguity"},
			"pipeline": map[string]interface{}{
				"name":        "generated-sales-pipeline",
				"description": "draft",
				"steps": []map[string]interface{}{
					{
						"key":        "extract_sales",
						"name":       "Extract Sales",
						"type":       "extract",
						"config":     map[string]interface{}{},
						"depends_on": []string{},
					},
				},
			},
		})
	}))
	defer srv.Close()

	client := NewHTTPClient(srv.URL, 2*time.Second)
	res, err := client.Interpret(context.Background(), Request{Query: "build pipeline"})
	if err != nil {
		t.Fatalf("interpret: %v", err)
	}
	if res == nil {
		t.Fatalf("expected result, got nil")
	}
	if res.Confidence < 0.9 {
		t.Fatalf("unexpected confidence: %f", res.Confidence)
	}
	if res.Pipeline.Name != "generated-sales-pipeline" {
		t.Fatalf("unexpected pipeline name: %s", res.Pipeline.Name)
	}
	if len(res.Pipeline.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(res.Pipeline.Steps))
	}
}

func TestHTTPClientInterpretNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client := NewHTTPClient(srv.URL, 2*time.Second)
	_, err := client.Interpret(context.Background(), Request{Query: "build pipeline"})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
