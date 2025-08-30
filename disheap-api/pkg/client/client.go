package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// EngineClient defines the interface for interacting with the disheap engine
type EngineClient interface {
	// Admin operations
	MakeHeap(ctx context.Context, req *disheapv1.MakeHeapReq) (*disheapv1.MakeHeapResp, error)
	DeleteHeap(ctx context.Context, req *disheapv1.DeleteHeapReq) error
	UpdateHeapConfig(ctx context.Context, req *disheapv1.UpdateHeapConfigReq) error
	ListHeaps(ctx context.Context, req *disheapv1.ListHeapsReq) (*disheapv1.ListHeapsResp, error)
	Stats(ctx context.Context, req *disheapv1.StatsReq) (*disheapv1.StatsResp, error)
	Purge(ctx context.Context, req *disheapv1.PurgeReq) error
	MoveToDLQ(ctx context.Context, req *disheapv1.MoveToDlqReq) error

	// Data operations
	Enqueue(ctx context.Context, req *disheapv1.EnqueueReq) (*disheapv1.EnqueueResp, error)
	EnqueueBatch(ctx context.Context, req *disheapv1.EnqueueBatchReq) (*disheapv1.EnqueueBatchResp, error)
	Ack(ctx context.Context, req *disheapv1.AckReq) error
	Nack(ctx context.Context, req *disheapv1.NackReq) error
	Extend(ctx context.Context, req *disheapv1.ExtendReq) error
	Peek(ctx context.Context, req *disheapv1.PeekReq) (*disheapv1.PeekResp, error)

	// Connection management
	Close() error
	IsHealthy() bool
}

// Config holds configuration for the engine client
type Config struct {
	// Engine endpoints (can be multiple for load balancing)
	Endpoints []string
	// Connection timeout
	ConnectTimeout time.Duration
	// Request timeout for individual calls
	RequestTimeout time.Duration
	// TLS configuration
	TLSEnabled bool
	// Keepalive settings
	KeepAlive KeepAliveConfig
	// Retry configuration
	Retry RetryConfig
}

// KeepAliveConfig holds gRPC keepalive configuration
type KeepAliveConfig struct {
	Time    time.Duration
	Timeout time.Duration
}

// RetryConfig holds retry configuration
type RetryConfig struct {
	MaxAttempts       int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
}

// DefaultConfig returns a default client configuration
func DefaultConfig() Config {
	return Config{
		Endpoints:      []string{"localhost:8080"},
		ConnectTimeout: 10 * time.Second,
		RequestTimeout: 30 * time.Second,
		TLSEnabled:     false,
		KeepAlive: KeepAliveConfig{
			Time:    30 * time.Second,
			Timeout: 5 * time.Second,
		},
		Retry: RetryConfig{
			MaxAttempts:       3,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        5 * time.Second,
			BackoffMultiplier: 2.0,
		},
	}
}

// client implements the EngineClient interface
type client struct {
	config     Config
	conn       *grpc.ClientConn
	grpcClient disheapv1.DisheapClient
	mu         sync.RWMutex
	closed     bool
}

// New creates a new engine client
func New(config Config) (EngineClient, error) {
	if len(config.Endpoints) == 0 {
		return nil, fmt.Errorf("at least one endpoint must be specified")
	}

	c := &client{
		config: config,
	}

	if err := c.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to engine: %w", err)
	}

	return c, nil
}

// connect establishes a connection to the engine
func (c *client) connect() error {
	var opts []grpc.DialOption

	// Configure keepalive
	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:    c.config.KeepAlive.Time,
		Timeout: c.config.KeepAlive.Timeout,
	}))

	// Configure TLS
	if c.config.TLSEnabled {
		// TODO: Add proper TLS credentials
		// For now, use insecure for development
	}
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Add OpenTelemetry instrumentation
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))

	// For simplicity, connect to the first endpoint
	// TODO: Implement proper load balancing across multiple endpoints
	ctx, cancel := context.WithTimeout(context.Background(), c.config.ConnectTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, c.config.Endpoints[0], opts...)
	if err != nil {
		return fmt.Errorf("failed to dial %s: %w", c.config.Endpoints[0], err)
	}

	c.conn = conn
	c.grpcClient = disheapv1.NewDisheapClient(conn)

	return nil
}

// Close closes the client connection
func (c *client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// IsHealthy checks if the client connection is healthy
func (c *client) IsHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed || c.conn == nil {
		return false
	}

	return c.conn.GetState().String() == "READY"
}

// withTimeout creates a context with the configured request timeout
func (c *client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		// If context already has a deadline, use it
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.config.RequestTimeout)
}

// Admin operations

func (c *client) MakeHeap(ctx context.Context, req *disheapv1.MakeHeapReq) (*disheapv1.MakeHeapResp, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	return c.grpcClient.MakeHeap(ctx, req)
}

func (c *client) DeleteHeap(ctx context.Context, req *disheapv1.DeleteHeapReq) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	_, err := c.grpcClient.DeleteHeap(ctx, req)
	return err
}

func (c *client) UpdateHeapConfig(ctx context.Context, req *disheapv1.UpdateHeapConfigReq) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	_, err := c.grpcClient.UpdateHeapConfig(ctx, req)
	return err
}

func (c *client) ListHeaps(ctx context.Context, req *disheapv1.ListHeapsReq) (*disheapv1.ListHeapsResp, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	return c.grpcClient.ListHeaps(ctx, req)
}

func (c *client) Stats(ctx context.Context, req *disheapv1.StatsReq) (*disheapv1.StatsResp, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	return c.grpcClient.Stats(ctx, req)
}

func (c *client) Purge(ctx context.Context, req *disheapv1.PurgeReq) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	_, err := c.grpcClient.Purge(ctx, req)
	return err
}

func (c *client) MoveToDLQ(ctx context.Context, req *disheapv1.MoveToDlqReq) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	_, err := c.grpcClient.MoveToDLQ(ctx, req)
	return err
}

// Data operations

func (c *client) Enqueue(ctx context.Context, req *disheapv1.EnqueueReq) (*disheapv1.EnqueueResp, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	return c.grpcClient.Enqueue(ctx, req)
}

func (c *client) EnqueueBatch(ctx context.Context, req *disheapv1.EnqueueBatchReq) (*disheapv1.EnqueueBatchResp, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	return c.grpcClient.EnqueueBatch(ctx, req)
}

func (c *client) Ack(ctx context.Context, req *disheapv1.AckReq) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	_, err := c.grpcClient.Ack(ctx, req)
	return err
}

func (c *client) Nack(ctx context.Context, req *disheapv1.NackReq) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	_, err := c.grpcClient.Nack(ctx, req)
	return err
}

func (c *client) Extend(ctx context.Context, req *disheapv1.ExtendReq) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	_, err := c.grpcClient.Extend(ctx, req)
	return err
}

func (c *client) Peek(ctx context.Context, req *disheapv1.PeekReq) (*disheapv1.PeekResp, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	return c.grpcClient.Peek(ctx, req)
}
