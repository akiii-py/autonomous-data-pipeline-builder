package dispatcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/akshat/pipeline-orchestrator/internal/models"
)

func TestHTTPDispatcherExecuteStepSuccess(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/execute" {
			t.Fatalf("expected /execute, got %s", r.URL.Path)
		}
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
	}))
	defer ts.Close()

	d := NewHTTPDispatcher(ts.URL, 2*time.Second)
	err := d.ExecuteStep(context.Background(), "run-1", models.Step{ID: "s1", Key: "extract", Type: models.StepTypeExtract})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestHTTPDispatcherExecuteStepFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "error", "error": "bad step"})
	}))
	defer ts.Close()

	d := NewHTTPDispatcher(ts.URL, 2*time.Second)
	err := d.ExecuteStep(context.Background(), "run-1", models.Step{ID: "s1", Key: "extract", Type: models.StepTypeExtract})
	if err == nil {
		t.Fatal("expected non-nil error")
	}
}
