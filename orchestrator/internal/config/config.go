package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all configuration for the orchestrator service.
type Config struct {
	Port        int    // HTTP server port
	DatabaseURL string // PostgreSQL connection string
	RedisURL    string // Redis connection string for caching/queues
	GRPCPort    int    // Port for gRPC communication with Python workers
	LogLevel    string // debug, info, warn, error
	Environment string // development, staging, production
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
		Port:        port,
		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/pipeline?sslmode=disable"),
		RedisURL:    getEnv("REDIS_URL", "redis://localhost:6379/0"),
		GRPCPort:    grpcPort,
		LogLevel:    getEnv("LOG_LEVEL", "info"),
		Environment: getEnv("ENVIRONMENT", "development"),
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
