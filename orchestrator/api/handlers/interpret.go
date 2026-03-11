package handlers

import (
	"encoding/json"
	"net/http"
)

// InterpretRequest receives natural language from the user like:
// "Pull sales data from PostgreSQL, clean nulls, aggregate by region, push to S3"
// and forwards it to the Python NLP service which converts it into a pipeline DAG spec.
func InterpretRequest(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error": "invalid request body"}`, http.StatusBadRequest)
		return
	}
	if req.Query == "" {
		http.Error(w, `{"error": "query is required"}`, http.StatusBadRequest)
		return
	}

	// TODO: Forward to Python NLP service via gRPC in Phase 6
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"message": "interpret request — NLP service not connected yet",
		"query":   req.Query,
	})
}
