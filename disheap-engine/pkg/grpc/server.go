package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	disheapv1 "github.com/disheap/disheap"
	"go.uber.org/zap"
)

// ServerConfig holds configuration for the gRPC server
type ServerConfig struct {
	Port                  int           `json:"port"`
	MaxRecvMessageSize    int           `json:"max_recv_message_size"`
	MaxSendMessageSize    int           `json:"max_send_message_size"`
	MaxConcurrentStreams  uint32        `json:"max_concurrent_streams"`
	KeepaliveTime         time.Duration `json:"keepalive_time"`
	KeepaliveTimeout      time.Duration `json:"keepalive_timeout"`
	KeepaliveMinTime      time.Duration `json:"keepalive_min_time"`
	MaxConnectionIdle     time.Duration `json:"max_connection_idle"`
	MaxConnectionAge      time.Duration `json:"max_connection_age"`
	MaxConnectionAgeGrace time.Duration `json:"max_connection_age_grace"`
	EnableReflection      bool          `json:"enable_reflection"`
	EnableHealthCheck     bool          `json:"enable_health_check"`
}

// DefaultServerConfig returns sensible defaults for gRPC server
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Port:                  9090,
		MaxRecvMessageSize:    4 << 20, // 4MB
		MaxSendMessageSize:    4 << 20, // 4MB
		MaxConcurrentStreams:  1000,
		KeepaliveTime:         60 * time.Second,
		KeepaliveTimeout:      20 * time.Second,
		KeepaliveMinTime:      5 * time.Second,
		MaxConnectionIdle:     15 * time.Minute,
		MaxConnectionAge:      30 * time.Minute,
		MaxConnectionAgeGrace: 5 * time.Second,
		EnableReflection:      true,
		EnableHealthCheck:     true,
	}
}

// Server represents the Disheap gRPC server
type Server struct {
	config *ServerConfig
	logger *zap.Logger

	// gRPC components
	grpcServer   *grpc.Server
	healthServer *health.Server
	listener     net.Listener

	// Service implementation
	service *DisheapService

	// Lifecycle
	mu      sync.RWMutex
	running bool
	done    chan struct{}
}

// NewServer creates a new Disheap gRPC server
func NewServer(config *ServerConfig, service *DisheapService, logger *zap.Logger) *Server {
	if config == nil {
		config = DefaultServerConfig()
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	return &Server{
		config:  config,
		service: service,
		logger:  logger,
		done:    make(chan struct{}),
	}
}

// Start starts the gRPC server
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil
	}

	// Create listener - explicitly bind to IPv4
	listener, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", s.config.Port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.config.Port, err)
	}
	s.listener = listener
	// Configure gRPC server options - simplified for debugging
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(s.config.MaxRecvMessageSize),
		grpc.MaxSendMsgSize(s.config.MaxSendMessageSize),
		// Add unary interceptor for debugging
		grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			s.logger.Info("gRPC unary call received",
				zap.String("component", "grpc_server"),
				zap.String("method", info.FullMethod))
			resp, err := handler(ctx, req)
			if err != nil {
				s.logger.Error("gRPC unary call failed",
					zap.String("component", "grpc_server"),
					zap.String("method", info.FullMethod),
					zap.Error(err))
			} else {
				s.logger.Info("gRPC unary call completed",
					zap.String("component", "grpc_server"),
					zap.String("method", info.FullMethod))
			}
			return resp, err
		}),
		// Temporarily remove keepalive and connection timeout options
		// grpc.MaxConcurrentStreams(s.config.MaxConcurrentStreams),
		// grpc.KeepaliveParams(keepalive.ServerParameters{
		// 	Time:    s.config.KeepaliveTime,
		// 	Timeout: s.config.KeepaliveTimeout,
		// }),
		// grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		// 	MinTime:             s.config.KeepaliveMinTime,
		// 	PermitWithoutStream: true,
		// }),
		// grpc.ConnectionTimeout(s.config.MaxConnectionAge),
	}

	// Create gRPC server
	s.grpcServer = grpc.NewServer(opts...)

	// Register Disheap service
	disheapv1.RegisterDisheapServer(s.grpcServer, s.service)

	// Register health service if enabled
	if s.config.EnableHealthCheck {
		s.healthServer = health.NewServer()
		grpc_health_v1.RegisterHealthServer(s.grpcServer, s.healthServer)
		s.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
		s.healthServer.SetServingStatus("disheap.v1.Disheap", grpc_health_v1.HealthCheckResponse_SERVING)
	}

	// Register reflection service if enabled
	if s.config.EnableReflection {
		reflection.Register(s.grpcServer)
	}

	s.running = true

	s.logger.Info("Starting gRPC server",
		zap.String("component", "grpc_server"),
		zap.Int("port", s.config.Port),
		zap.String("address", listener.Addr().String()))

	// Start serving in background
	go func() {
		defer close(s.done)
		s.logger.Info("gRPC server starting to serve",
			zap.String("component", "grpc_server"),
			zap.String("address", s.listener.Addr().String()))
		if err := s.grpcServer.Serve(s.listener); err != nil {
			s.logger.Error("gRPC server error",
				zap.String("component", "grpc_server"),
				zap.Error(err))
		}
		s.logger.Info("gRPC server stopped serving",
			zap.String("component", "grpc_server"))
	}()

	return nil
}

// Stop stops the gRPC server gracefully
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	s.logger.Info("Stopping gRPC server",
		zap.String("component", "grpc_server"))

	// Set health status to NOT_SERVING
	if s.healthServer != nil {
		s.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		s.healthServer.SetServingStatus("disheap.v1.Disheap", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	}

	// Attempt graceful shutdown
	stopped := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(stopped)
	}()

	// Wait for graceful shutdown or context timeout
	select {
	case <-stopped:
		s.logger.Info("gRPC server stopped gracefully",
			zap.String("component", "grpc_server"))
	case <-ctx.Done():
		s.logger.Warn("gRPC server graceful shutdown timeout, forcing stop",
			zap.String("component", "grpc_server"))
		s.grpcServer.Stop()
	}

	// Close listener
	if s.listener != nil {
		s.listener.Close()
	}

	s.running = false

	// Wait for serve goroutine to finish
	<-s.done

	return nil
}

// Address returns the server's listen address
func (s *Server) Address() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.listener == nil {
		return ""
	}

	return s.listener.Addr().String()
}

// IsRunning returns true if the server is running
func (s *Server) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.running
}

// HealthCheck performs a health check of the server
func (s *Server) HealthCheck(ctx context.Context) error {
	if !s.IsRunning() {
		return fmt.Errorf("gRPC server is not running")
	}

	// Perform service-level health checks
	if err := s.service.HealthCheck(ctx); err != nil {
		return fmt.Errorf("service health check failed: %w", err)
	}

	return nil
}
