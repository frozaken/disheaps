package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// PoolConfig holds configuration for connection pooling
type PoolConfig struct {
	// Maximum number of connections per endpoint
	MaxConnections int
	// Maximum idle time before closing connection
	MaxIdleTime time.Duration
	// Health check interval
	HealthCheckInterval time.Duration
}

// DefaultPoolConfig returns default pool configuration
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxConnections:      10,
		MaxIdleTime:         5 * time.Minute,
		HealthCheckInterval: 30 * time.Second,
	}
}

// Connection represents a pooled gRPC connection
type Connection struct {
	conn      *grpc.ClientConn
	client    disheapv1.DisheapClient
	endpoint  string
	createdAt time.Time
	lastUsed  time.Time
	inUse     bool
	mu        sync.RWMutex
}

// IsHealthy checks if the connection is healthy
func (c *Connection) IsHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn == nil {
		return false
	}

	state := c.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// Use marks the connection as in use
func (c *Connection) Use() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inUse = true
	c.lastUsed = time.Now()
}

// Release marks the connection as available
func (c *Connection) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inUse = false
}

// IsIdle returns true if the connection has been idle for too long
func (c *Connection) IsIdle(maxIdleTime time.Duration) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.inUse && time.Since(c.lastUsed) > maxIdleTime
}

// Close closes the connection
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ConnectionPool manages a pool of gRPC connections
type ConnectionPool struct {
	config      Config
	poolConfig  PoolConfig
	connections map[string][]*Connection
	mu          sync.RWMutex
	closed      bool
	stopCh      chan struct{}
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(config Config, poolConfig PoolConfig) *ConnectionPool {
	pool := &ConnectionPool{
		config:      config,
		poolConfig:  poolConfig,
		connections: make(map[string][]*Connection),
		stopCh:      make(chan struct{}),
	}

	// Start background health checker
	go pool.healthChecker()

	return pool
}

// GetConnection gets a connection from the pool for the given endpoint
func (p *ConnectionPool) GetConnection(ctx context.Context, endpoint string) (*Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, fmt.Errorf("connection pool is closed")
	}

	// Look for available connection
	connections, exists := p.connections[endpoint]
	if exists {
		for _, conn := range connections {
			if !conn.inUse && conn.IsHealthy() {
				conn.Use()
				return conn, nil
			}
		}
	}

	// Create new connection if pool not full
	if !exists || len(connections) < p.poolConfig.MaxConnections {
		conn, err := p.createConnection(ctx, endpoint)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection: %w", err)
		}

		if !exists {
			p.connections[endpoint] = []*Connection{conn}
		} else {
			p.connections[endpoint] = append(p.connections[endpoint], conn)
		}

		conn.Use()
		return conn, nil
	}

	// Pool is full, wait for available connection or create temporary one
	return p.createConnection(ctx, endpoint)
}

// ReleaseConnection releases a connection back to the pool
func (p *ConnectionPool) ReleaseConnection(conn *Connection) {
	if conn != nil {
		conn.Release()
	}
}

// createConnection creates a new gRPC connection
func (p *ConnectionPool) createConnection(ctx context.Context, endpoint string) (*Connection, error) {
	var opts []grpc.DialOption

	// Configure keepalive
	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:    p.config.KeepAlive.Time,
		Timeout: p.config.KeepAlive.Timeout,
	}))

	// Configure TLS
	if p.config.TLSEnabled {
		// TODO: Add proper TLS credentials
	}
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Add OpenTelemetry instrumentation
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))

	// Create connection
	dialCtx, cancel := context.WithTimeout(ctx, p.config.ConnectTimeout)
	defer cancel()

	grpcConn, err := grpc.DialContext(dialCtx, endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", endpoint, err)
	}

	conn := &Connection{
		conn:      grpcConn,
		client:    disheapv1.NewDisheapClient(grpcConn),
		endpoint:  endpoint,
		createdAt: time.Now(),
		lastUsed:  time.Now(),
		inUse:     false,
	}

	return conn, nil
}

// healthChecker runs periodic health checks and cleanup
func (p *ConnectionPool) healthChecker() {
	ticker := time.NewTicker(p.poolConfig.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.cleanup()
		}
	}
}

// cleanup removes unhealthy and idle connections
func (p *ConnectionPool) cleanup() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for endpoint, connections := range p.connections {
		var healthy []*Connection

		for _, conn := range connections {
			if !conn.IsHealthy() || conn.IsIdle(p.poolConfig.MaxIdleTime) {
				// Close unhealthy or idle connection
				conn.Close()
			} else {
				healthy = append(healthy, conn)
			}
		}

		if len(healthy) == 0 {
			delete(p.connections, endpoint)
		} else {
			p.connections[endpoint] = healthy
		}
	}
}

// Close closes all connections in the pool
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	close(p.stopCh)

	// Close all connections
	for _, connections := range p.connections {
		for _, conn := range connections {
			conn.Close()
		}
	}

	p.connections = make(map[string][]*Connection)
	return nil
}

// Stats returns pool statistics
func (p *ConnectionPool) Stats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make(map[string]interface{})
	totalConnections := 0
	inUseConnections := 0

	for endpoint, connections := range p.connections {
		endpointStats := map[string]interface{}{
			"total":   len(connections),
			"in_use":  0,
			"healthy": 0,
		}

		for _, conn := range connections {
			if conn.inUse {
				inUseConnections++
				endpointStats["in_use"] = endpointStats["in_use"].(int) + 1
			}
			if conn.IsHealthy() {
				endpointStats["healthy"] = endpointStats["healthy"].(int) + 1
			}
		}

		totalConnections += len(connections)
		stats[endpoint] = endpointStats
	}

	stats["total_connections"] = totalConnections
	stats["in_use_connections"] = inUseConnections

	return stats
}
