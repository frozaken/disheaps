package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"bytes"

	"github.com/disheap/disheap/disheap-engine/pkg/coordinator"
	"github.com/disheap/disheap/disheap-engine/pkg/grpc"
	disheap_raft "github.com/disheap/disheap/disheap-engine/pkg/raft"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	DefaultPort        = 9090
	DefaultRaftPort    = 8300
	DefaultStoragePath = "./data"
	DefaultLogLevel    = "info"
)

// Config holds all configuration for the disheap engine
type Config struct {
	// Server configuration
	Port int `json:"port"`

	// Storage configuration
	StoragePath string `json:"storage_path"`

	// Raft configuration
	NodeID           string   `json:"node_id"`
	RaftPort         int      `json:"raft_port"`
	RaftDir          string   `json:"raft_dir"`
	Bootstrap        bool     `json:"bootstrap"`
	JoinAddresses    []string `json:"join_addresses"`
	EnableSingleNode bool     `json:"enable_single_node"`

	// Logging configuration
	LogLevel string `json:"log_level"`
	LogJSON  bool   `json:"log_json"`

	// Feature flags
	EnableReflection  bool `json:"enable_reflection"`
	EnableHealthCheck bool `json:"enable_health_check"`
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	// Get hostname for default node ID
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "disheap-node"
	}

	return &Config{
		Port:              DefaultPort,
		StoragePath:       DefaultStoragePath,
		NodeID:            hostname,
		RaftPort:          DefaultRaftPort,
		RaftDir:           filepath.Join(DefaultStoragePath, "raft"),
		Bootstrap:         false,
		JoinAddresses:     []string{},
		EnableSingleNode:  false,
		LogLevel:          DefaultLogLevel,
		LogJSON:           false,
		EnableReflection:  true,
		EnableHealthCheck: true,
	}
}

// parseFlags parses command line flags and returns configuration
func parseFlags() *Config {
	config := DefaultConfig()

	// Server flags
	flag.IntVar(&config.Port, "port", DefaultPort, "gRPC server port")
	flag.StringVar(&config.StoragePath, "storage-path", DefaultStoragePath, "Path to storage directory")

	// Raft flags
	flag.StringVar(&config.NodeID, "node-id", config.NodeID, "Raft node ID")
	flag.IntVar(&config.RaftPort, "raft-port", DefaultRaftPort, "Raft transport port")
	flag.StringVar(&config.RaftDir, "raft-dir", config.RaftDir, "Raft data directory")
	flag.BoolVar(&config.Bootstrap, "bootstrap", false, "Bootstrap a new cluster")
	flag.BoolVar(&config.EnableSingleNode, "single-node", false, "Run as single node cluster")

	// Logging flags
	flag.StringVar(&config.LogLevel, "log-level", DefaultLogLevel, "Log level (debug, info, warn, error)")
	flag.BoolVar(&config.LogJSON, "log-json", false, "Output logs in JSON format")

	// Feature flags
	flag.BoolVar(&config.EnableReflection, "enable-reflection", true, "Enable gRPC reflection")
	flag.BoolVar(&config.EnableHealthCheck, "enable-health", true, "Enable health checks")

	flag.Parse()

	// Parse join addresses from environment variable
	if joinEnv := os.Getenv("RAFT_JOIN_ADDRESSES"); joinEnv != "" {
		config.JoinAddresses = strings.Split(joinEnv, ",")
		for i, addr := range config.JoinAddresses {
			config.JoinAddresses[i] = strings.TrimSpace(addr)
		}
	}

	// Override config from environment variables
	if nodeID := os.Getenv("NODE_ID"); nodeID != "" {
		config.NodeID = nodeID
	}
	if bootstrap := os.Getenv("BOOTSTRAP"); bootstrap == "true" {
		config.Bootstrap = true
	}
	if singleNode := os.Getenv("SINGLE_NODE"); singleNode == "true" {
		config.EnableSingleNode = true
	}

	return config
}

// setupLogger creates a zap logger based on configuration
func setupLogger(config *Config) (*zap.Logger, error) {
	// Parse log level
	level, err := zapcore.ParseLevel(config.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %s: %w", config.LogLevel, err)
	}

	// Create logger config
	var zapConfig zap.Config
	if config.LogJSON {
		zapConfig = zap.NewProductionConfig()
	} else {
		zapConfig = zap.NewDevelopmentConfig()
		zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	zapConfig.Level = zap.NewAtomicLevelAt(level)

	// Build logger
	logger, err := zapConfig.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	return logger, nil
}

// ensureStorageDir ensures the storage directory exists
func ensureStorageDir(path string) error {
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create storage directory %s: %w", path, err)
	}
	return nil
}

func main() {
	// Parse configuration
	config := parseFlags()

	// Setup logger
	logger, err := setupLogger(config)
	if err != nil {
		log.Fatalf("Failed to setup logger: %v", err)
	}
	defer logger.Sync()

	logger.Info("Starting disheap-engine",
		zap.String("version", "dev"),
		zap.Int("port", config.Port),
		zap.String("storage_path", config.StoragePath),
		zap.String("node_id", config.NodeID),
		zap.Int("raft_port", config.RaftPort),
		zap.Bool("bootstrap", config.Bootstrap),
		zap.Bool("single_node", config.EnableSingleNode),
		zap.Strings("join_addresses", config.JoinAddresses),
		zap.String("log_level", config.LogLevel))

	// Ensure storage and raft directories exist
	if err := ensureStorageDir(config.StoragePath); err != nil {
		logger.Fatal("Failed to setup storage directory", zap.Error(err))
	}
	if err := ensureStorageDir(config.RaftDir); err != nil {
		logger.Fatal("Failed to setup raft directory", zap.Error(err))
	}

	// Create context that gets cancelled on shutdown signals
	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	// Initialize storage
	storageConfig := storage.DefaultConfig()
	storageConfig.Path = filepath.Join(config.StoragePath, "badger")
	storageConfig.SyncWrites = true

	badgerStorage, err := storage.NewBadgerStorage(storageConfig, logger.Named("storage"))
	if err != nil {
		logger.Fatal("Failed to create storage", zap.Error(err))
	}

	// Open storage
	if err := badgerStorage.Open(ctx); err != nil {
		logger.Fatal("Failed to open storage", zap.Error(err))
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := badgerStorage.Close(ctx); err != nil {
			logger.Error("Failed to close storage", zap.Error(err))
		}
	}()

	// Initialize Raft node - CRITICAL: separate bind vs advertise addresses
	hostname, err := os.Hostname()
	if err != nil {
		logger.Fatal("Failed to get hostname", zap.Error(err))
	}

	// Bind address (where we listen)
	raftBindAddr := os.Getenv("RAFT_BIND")
	if raftBindAddr == "" {
		raftBindAddr = fmt.Sprintf("%s:%d", hostname, config.RaftPort)
	}

	// Advertise address (what peers should dial) - stable/routable name
	raftAdvertiseAddr := os.Getenv("RAFT_ADVERTISE")
	if raftAdvertiseAddr == "" {
		raftAdvertiseAddr = fmt.Sprintf("%s:%d", hostname, config.RaftPort)
	}

	logger.Info("Raft network configuration",
		zap.String("bind_addr", raftBindAddr),
		zap.String("advertise_addr", raftAdvertiseAddr))

	nodeConfig := disheap_raft.DefaultNodeConfigWithAddrs(config.NodeID, raftBindAddr, raftAdvertiseAddr, config.RaftDir)
	nodeConfig.EnableSingleNode = config.EnableSingleNode || config.Bootstrap
	// Do not pass join addresses to the Node; the auto-join worker will handle dynamic join
	// nodeConfig.JoinAddresses = config.JoinAddresses

	raftNode, err := disheap_raft.NewNode(nodeConfig, badgerStorage, logger.Named("raft"))
	if err != nil {
		logger.Fatal("Failed to create raft node", zap.Error(err))
	}

	// Start Raft node
	if err := raftNode.Start(ctx); err != nil {
		logger.Fatal("Failed to start raft node", zap.Error(err))
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := raftNode.Stop(ctx); err != nil {
			logger.Error("Failed to stop raft node", zap.Error(err))
		}
	}()

	// Wait for leadership if bootstrapping
	if nodeConfig.EnableSingleNode || config.Bootstrap {
		logger.Info("Waiting for leadership...")
		if err := raftNode.WaitForLeader(30 * time.Second); err != nil {
			logger.Fatal("Failed to establish leadership", zap.Error(err))
		}
		logger.Info("Leadership established")
	}

	// Create multi-partition coordinator
	coordinatorConfig := coordinator.DefaultCoordinatorConfig()
	router := coordinator.NewRendezvousRouter(logger.Named("router"))
	multiCoordinator, err := coordinator.NewMultiPartitionCoordinator(coordinatorConfig, router, badgerStorage, logger.Named("coordinator"))
	if err != nil {
		logger.Fatal("Failed to create coordinator", zap.Error(err))
	}

	if err := multiCoordinator.Start(ctx); err != nil {
		logger.Fatal("Failed to start coordinator", zap.Error(err))
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := multiCoordinator.Stop(ctx); err != nil {
			logger.Error("Failed to stop coordinator", zap.Error(err))
		}
	}()

	// Create gRPC service
	serviceDeps := grpc.ServiceDependencies{
		Storage:  badgerStorage,
		RaftNode: raftNode,
		Logger:   logger.Named("service"),
	}

	service := grpc.NewDisheapService(serviceDeps)

	// Create gRPC server
	// Use default config and override specific values
	serverConfig := grpc.DefaultServerConfig()
	serverConfig.Port = config.Port
	serverConfig.EnableReflection = config.EnableReflection
	serverConfig.EnableHealthCheck = config.EnableHealthCheck

	server := grpc.NewServer(serverConfig, service, logger.Named("server"))

	// Start gRPC server
	if err := server.Start(ctx); err != nil {
		logger.Fatal("Failed to start gRPC server", zap.Error(err))
	}

	// Start HTTP server for health and join endpoints
	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: setupHTTPHandlers(raftNode, logger),
	}

	go func() {
		logger.Info("Starting HTTP server", zap.String("addr", httpServer.Addr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failed", zap.Error(err))
		}
	}()

	// Graceful shutdown for HTTP server
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("Failed to shutdown HTTP server", zap.Error(err))
		}
	}()

	logger.Info("disheap-engine started successfully",
		zap.String("grpc_address", server.Address()),
		zap.String("http_address", httpServer.Addr))

	// Auto-join worker for followers (Option 2)
	if !config.Bootstrap && len(config.JoinAddresses) > 0 {
		seeds := make([]string, len(config.JoinAddresses))
		copy(seeds, config.JoinAddresses)
		myID := config.NodeID
		myAddr := raftAdvertiseAddr

		go func() {
			logger.Info("starting auto-join worker",
				zap.Strings("seeds", seeds),
				zap.String("node_id", myID),
				zap.String("address", myAddr))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				for _, seed := range seeds {
					seedHost := seed
					if idx := strings.Index(seed, ":"); idx != -1 {
						seedHost = seed[:idx]
					}
					// Wait for seed to be healthy and leader
					healthURL := fmt.Sprintf("http://%s:%d/health", seedHost, 8080)
					resp, err := http.Get(healthURL)
					if err != nil {
						logger.Debug("seed health check failed", zap.String("url", healthURL), zap.Error(err))
						time.Sleep(2 * time.Second)
						continue
					}
					var body map[string]any
					_ = json.NewDecoder(resp.Body).Decode(&body)
					resp.Body.Close()
					if status, ok := body["healthy"].(bool); !ok || !status {
						logger.Debug("seed not healthy yet", zap.String("url", healthURL))
						time.Sleep(2 * time.Second)
						continue
					}
					if isLeader, ok := body["is_leader"].(bool); !ok || !isLeader {
						logger.Debug("seed is not leader yet", zap.String("url", healthURL))
						time.Sleep(2 * time.Second)
						continue
					}

					// Attempt join
					joinURL := fmt.Sprintf("http://%s:%d/join", seedHost, 8080)
					payload := map[string]string{"node_id": myID, "address": myAddr}
					buf := new(bytes.Buffer)
					_ = json.NewEncoder(buf).Encode(payload)
					req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL, buf)
					if err != nil {
						logger.Warn("failed to create join request", zap.Error(err))
						time.Sleep(2 * time.Second)
						continue
					}
					req.Header.Set("Content-Type", "application/json")
					resp, err = http.DefaultClient.Do(req)
					if err != nil {
						logger.Info("join request failed", zap.String("url", joinURL), zap.Error(err))
						time.Sleep(3 * time.Second)
						continue
					}
					if resp.Body != nil {
						resp.Body.Close()
					}
					if resp.StatusCode == http.StatusOK {
						logger.Info("auto-join successful", zap.String("seed", seedHost))
						return
					}
					logger.Info("auto-join rejected by leader", zap.Int("status", resp.StatusCode))
					// try next seed
				}
				time.Sleep(3 * time.Second)
			}
		}()
	}

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("Shutdown signal received, stopping server...")

	// Stop server gracefully
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Stop(shutdownCtx); err != nil {
		logger.Error("Failed to stop server gracefully", zap.Error(err))
	}

	logger.Info("disheap-engine stopped")
}

// JoinRequest represents a cluster join request
type JoinRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// setupHTTPHandlers creates HTTP handlers for health and join endpoints
func setupHTTPHandlers(raftNode *disheap_raft.Node, logger *zap.Logger) http.Handler {
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Check if node is running and has a cluster configuration
		isLeader := raftNode.IsLeader()
		leader := raftNode.GetLeader()
		isHealthy := isLeader || leader != ""

		status := map[string]interface{}{
			"status":    "ok",
			"is_leader": isLeader,
			"healthy":   isHealthy,
			"leader":    string(leader),
		}

		if !isHealthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		json.NewEncoder(w).Encode(status)
	})

	// Raft status endpoint for cluster monitoring
	mux.HandleFunc("/raft/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		stats := raftNode.GetStats()
		config, err := raftNode.GetClusterManager().GetConfiguration()

		status := map[string]interface{}{
			"node_id":        stats.NodeID,
			"state":          stats.State.String(),
			"is_leader":      stats.IsLeader,
			"leader":         string(stats.LeaderID),
			"term":           stats.Term,
			"last_log_index": stats.LastLogIndex,
			"last_log_term":  stats.LastLogTerm,
			"commit_index":   stats.CommitIndex,
			"applied_index":  stats.AppliedIndex,
			"cluster_size":   stats.ClusterSize,
			"voters":         stats.VotersCount,
		}

		if err == nil {
			var servers []map[string]interface{}
			for _, server := range config.Servers {
				servers = append(servers, map[string]interface{}{
					"id":       string(server.ID),
					"address":  string(server.Address),
					"suffrage": server.Suffrage.String(),
				})
			}
			status["servers"] = servers
		}

		json.NewEncoder(w).Encode(status)
	})

	// Join endpoint for cluster membership
	mux.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		// Only leader can accept join requests
		if !raftNode.IsLeader() {
			leader := raftNode.GetLeader()
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error":  "not leader",
				"leader": string(leader),
			})
			return
		}

		var req JoinRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "invalid request: " + err.Error(),
			})
			return
		}

		if req.NodeID == "" || req.Address == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "node_id and address are required",
			})
			return
		}

		logger.Info("Received join request",
			zap.String("node_id", req.NodeID),
			zap.String("address", req.Address))

		// Add the voter to the cluster via ClusterManager
		err := raftNode.GetClusterManager().AddVoter(req.NodeID, req.Address, 10*time.Second)
		if err != nil {
			logger.Error("Failed to add voter", zap.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "failed to add voter: " + err.Error(),
			})
			return
		}

		logger.Info("Successfully added voter to cluster",
			zap.String("node_id", req.NodeID),
			zap.String("address", req.Address))

		json.NewEncoder(w).Encode(map[string]string{
			"status":  "success",
			"message": "node joined cluster",
		})
	})

	return mux
}
