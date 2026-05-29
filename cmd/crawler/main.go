package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"golang.org/x/time/rate"

	"sui-crawler/internal/api"
	"sui-crawler/internal/client"
	"sui-crawler/internal/config"
	"sui-crawler/internal/models"
	"sui-crawler/internal/orchestrator"
	"sui-crawler/internal/repository"
	"sui-crawler/internal/storage"
	"sui-crawler/internal/worker"
)

// @title           SUI Crawler API
// @version         1.0
// @description     API for managing SUI checkpoint crawling jobs
// @host            localhost:8080
// @BasePath        /
func main() {
	// Load .env file if it exists (ignore error if file not found)
	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	printConfig(cfg)

	// Setup context with graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Connect to MongoDB
	mongoClient, err := storage.NewMongoClient(ctx, cfg.MongoURI)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer func() {
		disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer disconnectCancel()
		if err := mongoClient.Disconnect(disconnectCtx); err != nil {
			log.Printf("Error disconnecting from MongoDB: %v", err)
		}
	}()
	log.Println("Connected to MongoDB")

	db := mongoClient.Database(cfg.MongoDB)
	jobRepo := repository.NewJobRepository(db)

	// Create a shared SuiClient for the API (no rate limiter; read-only, low-frequency calls).
	apiSuiClient, err := client.NewSuiClient(ctx, cfg.SuiRPCURL, nil)
	if err != nil {
		log.Printf("Warning: failed to create API SuiClient: %v", err)
		apiSuiClient = nil
	} else {
		apiSuiClient.SetRPCTimeout(cfg.SuiRPCTimeout)
		apiSuiClient.SetJSONRPCURL(cfg.SuiJSONRPCURL)
		defer apiSuiClient.Close()
	}

	// Start API server
	router := api.NewRouter(jobRepo, apiSuiClient)
	apiAddr := fmt.Sprintf(":%s", cfg.APIPort)
	srv := &http.Server{
		Addr:    apiAddr,
		Handler: router,
	}

	go func() {
		log.Printf("API server starting on %s", apiAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("API server failed: %v", err)
		}
	}()

	var limiter *rate.Limiter
	if cfg.SuiRateLimitRPS > 0 {
		burst := int(cfg.SuiRateLimitRPS)
		if burst < 1 {
			burst = 1
		}
		limiter = rate.NewLimiter(rate.Limit(cfg.SuiRateLimitRPS), burst)
	}

	workerCfg := worker.WorkerConfig{
		SuiRPCURL:   cfg.SuiRPCURL,
		SuiRateRPS:  cfg.SuiRateLimitRPS,
		RPCTimeout:  cfg.SuiRPCTimeout,
		CHAddr:      cfg.ClickHouseAddr,
		CHDatabase:  cfg.ClickHouseDatabase,
		CHUsername:  cfg.ClickHouseUsername,
		CHPassword:  cfg.ClickHousePassword,
		RateLimiter: limiter,
	}

	const workerID = "crawler-worker"
	assignCh := make(chan models.JobAssignment)
	reportCh := make(chan models.JobReport, 100)
	crawlerWorker := worker.NewWorker(workerID, assignCh, reportCh, workerCfg)
	go crawlerWorker.Run(ctx)

	orch := orchestrator.NewOrchestrator(
		jobRepo,
		reportCh,
		assignCh,
		workerID,
	)

	go func() {
		if err := orch.Run(ctx); err != nil && err != context.Canceled {
			log.Printf("Orchestrator error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sig := <-sigCh
	log.Printf("Received signal %v, shutting down gracefully...", sig)
	cancel()

	// Shutdown API server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("API server shutdown error: %v", err)
	}

	log.Println("Shutdown complete")
}

func printConfig(cfg *config.Config) {
	fmt.Println("=== SUI Crawler Configuration ===")
	fmt.Printf("  MongoDB URI: %s\n", cfg.MongoURI)
	fmt.Printf("  MongoDB DB:  %s\n", cfg.MongoDB)
	fmt.Printf("  Workers:     %d\n", 1)
	fmt.Printf("  API Port:    %s\n", cfg.APIPort)
	fmt.Printf("  RPC URL:     %s\n", cfg.SuiRPCURL)
	fmt.Printf("  RPC Timeout: %s\n", cfg.SuiRPCTimeout)
	fmt.Printf("  Rate Limit:  %.1f RPS\n", cfg.SuiRateLimitRPS)
	fmt.Printf("  Output Mode: ClickHouse\n")
	fmt.Printf("  CH Address:  %s\n", cfg.ClickHouseAddr)
	fmt.Printf("  CH Database: %s\n", cfg.ClickHouseDatabase)
	fmt.Println("=================================")
}
