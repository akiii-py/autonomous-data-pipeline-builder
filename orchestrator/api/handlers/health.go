package handlers

import (
	"encoding/json"
	"net/http"
)

// Health is a simple liveness probe endpoint.
// Docker/Kubernetes pings this to know if the service is running.
func Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "healthy",
		"service": "pipeline-orchestrator",
	})
}
