package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/disheap/disheap/disheap-api/pkg/auth"
	"github.com/disheap/disheap/disheap-api/pkg/client"
	"github.com/disheap/disheap/disheap-api/pkg/handlers"
	"github.com/gin-gonic/gin"
)

// Config holds server configuration
type Config struct {
	// Server address and port
	Host string
	Port int

	// Timeout configurations
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration

	// TLS configuration
	TLSCertFile string
	TLSKeyFile  string

	// CORS configuration
	CORS CORSConfig

	// Rate limiting
	RateLimit RateLimitConfig
}

// CORSConfig holds CORS configuration
type CORSConfig struct {
	Enabled          bool
	AllowedOrigins   []string
	AllowedMethods   []string
	AllowedHeaders   []string
	ExposeHeaders    []string
	AllowCredentials bool
	MaxAge           time.Duration
}

// RateLimitConfig holds rate limiting configuration
type RateLimitConfig struct {
	Enabled           bool
	RequestsPerMinute int
	BurstSize         int
}

// DefaultConfig returns default server configuration
func DefaultConfig() Config {
	return Config{
		Host:         "0.0.0.0",
		Port:         8080,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
		CORS: CORSConfig{
			Enabled:          true,
			AllowedOrigins:   []string{"*"},
			AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
			AllowedHeaders:   []string{"*"},
			ExposeHeaders:    []string{"X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"},
			AllowCredentials: false,
			MaxAge:           12 * time.Hour,
		},
		RateLimit: RateLimitConfig{
			Enabled:           true,
			RequestsPerMinute: 1000,
			BurstSize:         100,
		},
	}
}

// Server represents the HTTP API server
type Server struct {
	config       Config
	httpServer   *http.Server
	engineClient client.EngineClient
	authService  *auth.AuthService
	handlers     *handlers.Handlers
	router       *gin.Engine
}

// New creates a new server instance
func New(config Config, engineClient client.EngineClient) (*Server, error) {
	// Configure Gin mode
	gin.SetMode(gin.ReleaseMode)

	// Create router with recovery middleware
	router := gin.New()

	// Add global middleware
	router.Use(gin.Recovery())
	router.Use(Logger())

	if config.CORS.Enabled {
		router.Use(CORS(config.CORS))
	}

	if config.RateLimit.Enabled {
		router.Use(RateLimit(config.RateLimit))
	}

	router.Use(ErrorHandler())

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		if engineClient.IsHealthy() {
			c.JSON(http.StatusOK, gin.H{
				"status":    "healthy",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
			})
		} else {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status":    "unhealthy",
				"reason":    "engine unavailable",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
			})
		}
	})

	// Readiness check endpoint
	router.GET("/ready", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "ready",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	})

	// Create HTTP server
	address := fmt.Sprintf("%s:%d", config.Host, config.Port)
	httpServer := &http.Server{
		Addr:         address,
		Handler:      router,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		IdleTimeout:  config.IdleTimeout,
	}

	// Create auth service with mock stores for development
	authService := auth.NewMockAuthService()

	// Create handlers
	handlersInstance := handlers.NewHandlers(engineClient, authService)

	server := &Server{
		config:       config,
		httpServer:   httpServer,
		engineClient: engineClient,
		authService:  authService,
		handlers:     handlersInstance,
		router:       router,
	}

	// Setup API routes
	server.setupRoutes()

	return server, nil
}

// setupRoutes configures all API routes
func (s *Server) setupRoutes() {
	// API v1 routes group
	v1 := s.router.Group("/v1")

	// TODO: Add authentication middleware here when implemented
	// v1.Use(AuthMiddleware())

	// Register all handlers
	s.handlers.RegisterRoutes(v1)
}

// Start starts the HTTP server
func (s *Server) Start() error {
	fmt.Printf("Starting server on %s:%d\n", s.config.Host, s.config.Port)

	// Check for TLS configuration
	if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
		fmt.Println("Starting server with TLS")
		return s.httpServer.ListenAndServeTLS(s.config.TLSCertFile, s.config.TLSKeyFile)
	}

	fmt.Println("Starting server without TLS")
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	fmt.Println("Shutting down server...")

	// Close engine client connection
	if s.engineClient != nil {
		if err := s.engineClient.Close(); err != nil {
			fmt.Printf("Error closing engine client: %v\n", err)
		}
	}

	// Shutdown HTTP server
	return s.httpServer.Shutdown(ctx)
}

// GetRouter returns the Gin router instance (for testing)
func (s *Server) GetRouter() *gin.Engine {
	return s.router
}
