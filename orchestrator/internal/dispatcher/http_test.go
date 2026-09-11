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

func sampleRequest() models.StepRequest {
	return models.StepRequest{
		RunID:          "run-1",
		PipelineID:     "pipe-1",
		Step:           models.Step{ID: "s1", Key: "extract", Type: models.StepTypeExtract},
		Attempt:        1,
		IdempotencyKey: "run-1:s1:1",
	}
}

func TestHTTPDispatcherCarriesResultAndIdentity(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/execute" {
			t.Errorf("expected /execute, got %s", r.URL.Path)
		}
		if got := r.Header.Get("X-Worker-Token"); got != "secret" {
			t.Errorf("expected worker token to be sent, got %q", got)
		}

		var body models.StepRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if body.IdempotencyKey != "run-1:s1:1" {
			t.Errorf("idempotency key not sent, got %q", body.IdempotencyKey)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "ok",
			"result": map[string]interface{}{"rows_processed": 42, "replayed": true},
		})
	}))
	defer ts.Close()

	d := NewHTTPDispatcher(ts.URL, "secret", 2*time.Second)

	res, err := d.ExecuteStep(context.Background(), sampleRequest())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if res.Failed() {
		t.Fatalf("expected success, got %q", res.ErrorMessage)
	}
	// The worker already knew these; discarding them at the boundary is what
	// D-01 removes.
	if res.RowsProcessed != 42 {
		t.Fatalf("expected rows_processed to survive the boundary, got %d", res.RowsProcessed)
	}
	if !res.Replayed {
		t.Fatal("expected replayed flag to survive the boundary")
	}
	if res.IdempotencyKey != "run-1:s1:1" || res.StepKey != "extract" {
		t.Fatalf("identity fields not preserved: %+v", res)
	}
}

// A 4xx will be rejected identically forever; a 5xx may not be. The scheduler
// must be able to tell them apart without reading the message (D-02).
func TestHTTPDispatcherClassifiesByStatus(t *testing.T) {
	tests := []struct {
		name   string
		status int
		want   models.ErrorClass
	}{
		{name: "client error is permanent", status: http.StatusBadRequest, want: models.ErrorClassPermanent},
		{name: "server error is transient", status: http.StatusInternalServerError, want: models.ErrorClassTransient},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"status": "error", "error": "bad step",
				})
			}))
			defer ts.Close()

			d := NewHTTPDispatcher(ts.URL, "", 2*time.Second)
			res, err := d.ExecuteStep(context.Background(), sampleRequest())
			if err != nil {
				t.Fatalf("transport-level error should be reported in the result, got %v", err)
			}
			if !res.Failed() {
				t.Fatal("expected a failed result")
			}
			if res.ErrorClass != tc.want {
				t.Fatalf("expected class %q, got %q", tc.want, res.ErrorClass)
			}
			if res.Retryable() != (tc.want == models.ErrorClassTransient) {
				t.Fatalf("Retryable() disagrees with class %q", res.ErrorClass)
			}
		})
	}
}

func TestHTTPDispatcherUnreachableWorkerIsTransient(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := ts.URL
	ts.Close() // nothing is listening now

	d := NewHTTPDispatcher(url, "", 500*time.Millisecond)
	res, err := d.ExecuteStep(context.Background(), sampleRequest())
	if err != nil {
		t.Fatalf("expected the failure in the result, got error %v", err)
	}
	if !res.Retryable() {
		t.Fatalf("an unreachable worker should be retryable, got class %q", res.ErrorClass)
	}
}

// A cancelled context means the caller has already decided to stop, so it must
// not be treated as a transient fault worth retrying.
func TestHTTPDispatcherCancelledContextIsPermanent(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	d := NewHTTPDispatcher(ts.URL, "", 2*time.Second)
	res, err := d.ExecuteStep(ctx, sampleRequest())
	if err != nil {
		t.Fatalf("expected the failure in the result, got error %v", err)
	}
	if res.Retryable() {
		t.Fatal("a cancelled context must not be retryable")
	}
}

func TestLocalDispatcherMarksResultsSimulated(t *testing.T) {
	d := NewLocalDispatcher()

	if prov := d.Provenance(); !prov.Simulated || prov.ExecMode != models.ExecModeLocal {
		t.Fatalf("local dispatcher must report itself as simulated, got %+v", prov)
	}

	res, err := d.ExecuteStep(context.Background(), sampleRequest())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Simulated {
		t.Fatal("a run that touched no data must be marked simulated")
	}
}
