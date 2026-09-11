package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for the orchestrator service. This is the only
// place in the Go code that reads the environment (rule 6.1).
type Config struct {
	Port        int    // HTTP server port
	DatabaseURL string // PostgreSQL connection string
	APIKey      string // API key protecting /api routes
	Environment string // development, staging, production
	LogLevel    string // debug, info, warn, error

	// Execution plane
	ExecMode        string // local or worker
	WorkerURL       string // worker service base URL
	WorkerToken     string // shared secret sent to the worker
	WorkerTimeoutMS int    // worker call timeout

	// Run ownership and the Postgres work queue (D-07, D-08)
	RunnerOwner        string
	RunLeaseSeconds    int
	RunPollMS          int
	MaxConcurrentRuns  int
	MaxStepConcurrency int
	RetryBackoffBaseMS int
	RetryBackoffMaxMS  int

	// NLP interpretation
	NLPServiceURL    string
	NLPTimeoutMS     int
	NLPMinConfidence float64

	// Catalog: what a generated pipeline is allowed to reference (D-14)
	CatalogConnectors   []string
	CatalogDestinations []string
	CatalogTransformOps []string
	CatalogSchemaPath   string
	CatalogSchemaInline string
}

// Load reads config from environment variables with sensible defaults.
func Load() (*Config, error) {
	port, err := getEnvInt("PORT", 8080)
	if err != nil {
		return nil, fmt.Errorf("invalid PORT: %w", err)
	}

	cfg := &Config{
		Port:        port,
		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/pipeline?sslmode=disable"),
		APIKey:      getEnv("API_KEY", ""),
		Environment: getEnv("ENVIRONMENT", "development"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),

		ExecMode:        getEnv("EXEC_MODE", "local"),
		WorkerURL:       getEnv("WORKER_URL", "http://localhost:8090"),
		WorkerToken:     getEnv("WORKER_TOKEN", ""),
		WorkerTimeoutMS: mustGetEnvInt("WORKER_TIMEOUT_MS", 10000),

		RunnerOwner:        getEnv("RUNNER_OWNER", defaultOwner()),
		RunLeaseSeconds:    mustGetEnvInt("RUN_LEASE_SECONDS", 30),
		RunPollMS:          mustGetEnvInt("RUN_POLL_MS", 1000),
		MaxConcurrentRuns:  mustGetEnvInt("MAX_CONCURRENT_RUNS", 4),
		MaxStepConcurrency: mustGetEnvInt("SCHEDULER_MAX_CONCURRENCY", 4),
		RetryBackoffBaseMS: mustGetEnvInt("RETRY_BACKOFF_BASE_MS", 200),
		RetryBackoffMaxMS:  mustGetEnvInt("RETRY_BACKOFF_MAX_MS", 10000),

		NLPServiceURL:    getEnv("NLP_SERVICE_URL", "http://localhost:8091"),
		NLPTimeoutMS:     mustGetEnvInt("NLP_TIMEOUT_MS", 8000),
		NLPMinConfidence: mustGetEnvFloat("NLP_MIN_CONFIDENCE", 0.70),

		CatalogConnectors:   getEnvList("CATALOG_CONNECTORS", []string{"file", "http", "postgres"}),
		CatalogDestinations: getEnvList("CATALOG_DESTINATIONS", nil),
		CatalogTransformOps: getEnvList("CATALOG_TRANSFORM_OPS", []string{"select", "filter_eq", "aggregate_sum"}),
		CatalogSchemaPath:   getEnv("CATALOG_SCHEMA_PATH", ""),
		CatalogSchemaInline: getEnv("CATALOG_SCHEMA_JSON", ""),
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// validate rejects configurations that would run but behave dishonestly. An
// unrecognised EXEC_MODE used to fall through to the no-op simulator silently;
// it is now a startup failure (D-09).
func (c *Config) validate() error {
	switch strings.ToLower(strings.TrimSpace(c.ExecMode)) {
	case "local", "worker":
	default:
		return fmt.Errorf("invalid EXEC_MODE %q: must be \"local\" or \"worker\"", c.ExecMode)
	}

	if c.IsProduction() && strings.TrimSpace(c.APIKey) == "" {
		return fmt.Errorf("API_KEY is required when ENVIRONMENT=%s", c.Environment)
	}

	if c.RunLeaseSeconds < 2 {
		return fmt.Errorf("RUN_LEASE_SECONDS must be at least 2, got %d", c.RunLeaseSeconds)
	}

	return nil
}

func (c *Config) IsProduction() bool {
	env := strings.ToLower(strings.TrimSpace(c.Environment))
	return env == "production" || env == "staging"
}

// IsWorkerMode reports whether real execution is configured. Only router.New
// consumes this — no other component reads EXEC_MODE (rule 6.3).
func (c *Config) IsWorkerMode() bool {
	return strings.EqualFold(strings.TrimSpace(c.ExecMode), "worker")
}

func (c *Config) RunLease() time.Duration {
	return time.Duration(c.RunLeaseSeconds) * time.Second
}

func (c *Config) RunPollInterval() time.Duration {
	return time.Duration(c.RunPollMS) * time.Millisecond
}

func (c *Config) WorkerTimeout() time.Duration {
	return time.Duration(c.WorkerTimeoutMS) * time.Millisecond
}

func (c *Config) NLPTimeout() time.Duration {
	return time.Duration(c.NLPTimeoutMS) * time.Millisecond
}

func (c *Config) RetryBackoffBase() time.Duration {
	return time.Duration(c.RetryBackoffBaseMS) * time.Millisecond
}

func (c *Config) RetryBackoffMax() time.Duration {
	return time.Duration(c.RetryBackoffMaxMS) * time.Millisecond
}

func defaultOwner() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "orchestrator"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvList(key string, fallback []string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func getEnvInt(key string, fallback int) (int, error) {
	val := os.Getenv(key)
	if val == "" {
		return fallback, nil
	}
	return strconv.Atoi(val)
}

func mustGetEnvInt(key string, fallback int) int {
	v, err := getEnvInt(key, fallback)
	if err != nil {
		return fallback
	}
	return v
}

func getEnvFloat(key string, fallback float64) (float64, error) {
	val := os.Getenv(key)
	if val == "" {
		return fallback, nil
	}
	return strconv.ParseFloat(val, 64)
}

func mustGetEnvFloat(key string, fallback float64) float64 {
	v, err := getEnvFloat(key, fallback)
	if err != nil {
		return fallback
	}
	return v
}
