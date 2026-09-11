package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/akshat/pipeline-orchestrator/api/router"
	"github.com/akshat/pipeline-orchestrator/internal/config"
	"github.com/akshat/pipeline-orchestrator/internal/database"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	if cfg.APIKey == "" {
		log.Println("WARNING: API_KEY is empty — all /api routes are unauthenticated")
	}
	if !cfg.IsWorkerMode() {
		log.Println("WARNING: EXEC_MODE=local — runs are simulated and touch no data; " +
			"they are recorded with simulated=true")
	}

	db, err := database.Connect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := database.RunMigrations(db); err != nil {
		log.Fatalf("failed to run migrations: %v", err)
	}

	app, err := router.New(db, cfg)
	if err != nil {
		log.Fatalf("failed to build application: %v", err)
	}

	// The run loop's context is the process lifetime. Runs it claims are driven
	// under this context, so shutdown cancels them and their claims are released
	// for another process to take (D-07).
	runCtx, stopRuns := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		app.Runner.Start(runCtx)
	}()

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      app.Handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("orchestrator listening on :%d", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down orchestrator...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	// Stop claiming, let in-flight runs unwind, and hand their claims back.
	stopRuns()
	wg.Wait()

	log.Println("orchestrator stopped")
}
