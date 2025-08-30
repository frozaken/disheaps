package main

import (
	"fmt"
	"log"
	"time"

	"github.com/disheap/disheap/disheap-api/pkg/client"
	"github.com/disheap/disheap/disheap-api/pkg/config"
	"github.com/disheap/disheap/disheap-api/pkg/server"
)

const (
	appName    = "disheap-api"
	appVersion = "1.0.0"
)

func main() {
	// Print banner
	printBanner()

	// Load configuration
	cfg, err := loadConfiguration()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Configuration validation failed: %v", err)
	}

	// Initialize logger based on configuration
	initLogger(cfg)

	// Create engine client
	engineClient, err := createEngineClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create engine client: %v", err)
	}

	// Create HTTP server
	srv, err := createServer(cfg, engineClient)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Register shutdown handlers
	RegisterClientShutdown(engineClient, "engine")
	RegisterServerShutdown(srv, "HTTP")

	// Print startup information
	printStartupInfo(cfg)

	// Start server in a goroutine
	serverErrChan := make(chan error, 1)
	go func() {
		log.Printf("Starting %s server on %s:%d", appName, cfg.Server.Host, cfg.Server.Port)
		if err := srv.Start(); err != nil {
			serverErrChan <- fmt.Errorf("server error: %w", err)
		}
	}()

	// Wait for either server error or shutdown signal
	go func() {
		if err := <-serverErrChan; err != nil {
			log.Printf("Server failed: %v", err)
			globalShutdownManager.ForceShutdown(30 * time.Second)
		}
	}()

	// Start signal handler (blocks until signal received)
	StartSignalHandler(30 * time.Second)
}

// printBanner prints the application banner
func printBanner() {
	fmt.Printf(`
╔═══════════════════════════════════════╗
║            DISHEAP API                ║
║     Priority Messaging Gateway        ║
║                                       ║
║  Version: %-10s               ║
╚═══════════════════════════════════════╝

`, appVersion)
}

// loadConfiguration loads and validates the application configuration
func loadConfiguration() (*config.Config, error) {
	// Try to load from environment variables first
	cfg, err := config.GetConfigFromEnvironmentWithDefaults()
	if err != nil {
		return nil, fmt.Errorf("failed to load config from environment: %w", err)
	}

	// TODO: Add support for loading from config file
	// For now, we only support environment variables

	// Apply any missing defaults
	cfg.ApplyDefaults()

	return cfg, nil
}

// initLogger initializes the application logger based on configuration
func initLogger(cfg *config.Config) {
	// TODO: Implement structured logging with configurable format
	// For now, we use the standard Go logger

	logPrefix := fmt.Sprintf("[%s] ", appName)
	log.SetPrefix(logPrefix)

	if cfg.IsDevelopment() {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	} else {
		log.SetFlags(log.LstdFlags)
	}

	log.Printf("Logger initialized (level=%s, format=%s)",
		cfg.Logging.Level, cfg.Logging.Format)
}

// createEngineClient creates and initializes the engine client
func createEngineClient(cfg *config.Config) (client.EngineClient, error) {
	clientConfig := cfg.ToClientConfig()

	log.Printf("Connecting to engine at: %v", clientConfig.Endpoints)

	engineClient, err := client.New(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create engine client: %w", err)
	}

	// Test connection
	if !engineClient.IsHealthy() {
		log.Println("Warning: Engine client is not healthy at startup")
		// Don't fail here - the client will retry connections
	} else {
		log.Println("Engine client connected successfully")
	}

	return engineClient, nil
}

// createServer creates and configures the HTTP server
func createServer(cfg *config.Config, engineClient client.EngineClient) (*server.Server, error) {
	serverConfig := cfg.ToServerConfig()

	srv, err := server.New(serverConfig, engineClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create server: %w", err)
	}

	log.Printf("Server configured:")
	log.Printf("  - Address: %s:%d", cfg.Server.Host, cfg.Server.Port)
	log.Printf("  - TLS: %t", cfg.Server.TLSCertFile != "" && cfg.Server.TLSKeyFile != "")
	log.Printf("  - CORS: %t", cfg.Server.CORS.Enabled)
	log.Printf("  - Rate Limiting: %t", cfg.Server.RateLimit.Enabled)
	if cfg.Server.RateLimit.Enabled {
		log.Printf("    - Requests per minute: %d", cfg.Server.RateLimit.RequestsPerMinute)
		log.Printf("    - Burst size: %d", cfg.Server.RateLimit.BurstSize)
	}
	log.Printf("  - Metrics: %t", cfg.Observability.MetricsEnabled)
	if cfg.Observability.MetricsEnabled {
		log.Printf("    - Metrics path: %s", cfg.Observability.MetricsPath)
	}

	return srv, nil
}

// printStartupInfo prints useful startup information
func printStartupInfo(cfg *config.Config) {
	log.Println("=== Startup Information ===")
	log.Printf("Mode: %s", func() string {
		if cfg.IsDevelopment() {
			return "Development"
		}
		return "Production"
	}())
	log.Printf("Engine endpoints: %v", cfg.Engine.Endpoints)
	log.Printf("Storage: %s (%s)", cfg.Storage.Type, cfg.Storage.ConnectionString)
	log.Printf("JWT expiry: %v", cfg.Auth.JWTExpiry)
	log.Printf("Log level: %s", cfg.Logging.Level)

	if cfg.IsDevelopment() {
		log.Println("")
		log.Println("=== Development Mode Tips ===")
		log.Println("- API documentation: http://localhost:8080/docs (when implemented)")
		log.Println("- Health check: http://localhost:8080/health")
		log.Println("- Metrics: http://localhost:8080/metrics")
		log.Println("- Set DISHEAP_LOG_LEVEL=debug for verbose logging")
	}
	log.Println("==============================")
}
