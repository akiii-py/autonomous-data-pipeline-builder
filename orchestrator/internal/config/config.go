package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all configuration for the orchestrator service.
type Config struct {
	Port            int    // HTTP server port
	DatabaseURL     string // PostgreSQL connection string
	RedisURL        string // Redis connection string for caching/queues
	GRPCPort        int    // Port for gRPC communication with Python workers
	WorkerURL       string // Worker service base URL (Phase 4)
	ExecMode        string // local or worker
	WorkerTimeoutMS int    // Worker call timeout in milliseconds
	LogLevel        string // debug, info, warn, error
	Environment     string // development, staging, production
}

// Load reads config from environment variables with sensible defaults.
func Load() (*Config, error) {
	port, err := getEnvInt("PORT", 8080)
	if err != nil {
		return nil, fmt.Errorf("invalid PORT: %w", err)
	}

	grpcPort, err := getEnvInt("GRPC_PORT", 50051)
	if err != nil {
		return nil, fmt.Errorf("invalid GRPC_PORT: %w", err)
	}

	return &Config{
		Port:            port,
		DatabaseURL:     getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/pipeline?sslmode=disable"),
		RedisURL:        getEnv("REDIS_URL", "redis://localhost:6379/0"),
		GRPCPort:        grpcPort,
		WorkerURL:       getEnv("WORKER_URL", "http://localhost:8090"),
		ExecMode:        getEnv("EXEC_MODE", "local"),
		WorkerTimeoutMS: mustGetEnvInt("WORKER_TIMEOUT_MS", 10000),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		Environment:     getEnv("ENVIRONMENT", "development"),
	}, nil
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
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
