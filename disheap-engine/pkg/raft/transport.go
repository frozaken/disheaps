package raft

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

// TransportConfig configures the Raft network transport
type TransportConfig struct {
	// Bind address for this node
	BindAddr string
	// Maximum number of connections to pool per target
	MaxPool int
	// Timeout for transport operations
	Timeout time.Duration
	// TCP keep-alive interval
	TCPKeepAlive time.Duration
	// TCP user timeout (Linux only)
	TCPUserTimeout time.Duration
	// Enable TCP no delay
	TCPNoDelay bool
}

// DefaultTransportConfig returns a default transport configuration
func DefaultTransportConfig(bindAddr string) *TransportConfig {
	return &TransportConfig{
		BindAddr:       bindAddr,
		MaxPool:        3,
		Timeout:        10 * time.Second,
		TCPKeepAlive:   30 * time.Second,
		TCPUserTimeout: 60 * time.Second,
		TCPNoDelay:     true,
	}
}

// TransportManager manages Raft network transport
type TransportManager struct {
	config    *TransportConfig
	transport raft.Transport
	listener  net.Listener
	logger    *zap.Logger
	mu        sync.RWMutex
}

// NewTransportManager creates a new transport manager
func NewTransportManager(config *TransportConfig, logger *zap.Logger) *TransportManager {
	if config == nil {
		config = DefaultTransportConfig(":8300")
	}

	return &TransportManager{
		config: config,
		logger: logger.Named("raft-transport"),
	}
}

// Start initializes and starts the transport
func (tm *TransportManager) Start() (raft.Transport, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.transport != nil {
		return tm.transport, nil
	}

	tm.logger.Info("starting raft transport",
		zap.String("bind_addr", tm.config.BindAddr))

	// Resolve the bind address
	addr, err := net.ResolveTCPAddr("tcp", tm.config.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve bind address: %w", err)
	}

	// Create TCP transport - using basic TCP transport
	transport, err := raft.NewTCPTransport(
		tm.config.BindAddr,
		addr,
		tm.config.MaxPool,
		tm.config.Timeout,
		os.Stderr, // Use stderr for now instead of custom logger
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP transport: %w", err)
	}

	tm.transport = transport

	tm.logger.Info("raft transport started successfully",
		zap.String("local_addr", string(transport.LocalAddr())))

	return transport, nil
}

// Stop gracefully stops the transport
func (tm *TransportManager) Stop() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.transport == nil {
		return nil
	}

	tm.logger.Info("stopping raft transport")

	// Transport doesn't have Close() method in this version
	tm.transport = nil

	tm.logger.Info("raft transport stopped")
	return nil
}

// GetTransport returns the current transport instance
func (tm *TransportManager) GetTransport() raft.Transport {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.transport
}

// GetLocalAddr returns the local bind address as a string
func (tm *TransportManager) GetLocalAddr() string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if tm.transport == nil {
		return ""
	}

	return string(tm.transport.LocalAddr())
}

func (tm *TransportManager) createDialer() func(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return func(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
		dialer := &net.Dialer{
			Timeout:   timeout,
			KeepAlive: tm.config.TCPKeepAlive,
		}

		conn, err := dialer.Dial("tcp", string(addr))
		if err != nil {
			return nil, err
		}

		// Configure TCP socket options
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetNoDelay(tm.config.TCPNoDelay); err != nil {
				tm.logger.Warn("failed to set TCP_NODELAY", zap.Error(err))
			}

			if err := tcpConn.SetKeepAlive(true); err != nil {
				tm.logger.Warn("failed to enable TCP keep-alive", zap.Error(err))
			}

			if err := tcpConn.SetKeepAlivePeriod(tm.config.TCPKeepAlive); err != nil {
				tm.logger.Warn("failed to set TCP keep-alive period", zap.Error(err))
			}
		}

		return conn, nil
	}
}

// TransportLogger adapts zap.Logger for Raft transport logging
type TransportLogger struct {
	logger *zap.Logger
}

// NewTransportLogger creates a logger adapter for transport
func NewTransportLogger(logger *zap.Logger) *TransportLogger {
	return &TransportLogger{logger: logger.Named("transport")}
}

func (tl *TransportLogger) Write(p []byte) (int, error) {
	// Log transport messages at debug level
	msg := string(p)
	if len(msg) > 0 && msg[len(msg)-1] == '\n' {
		msg = msg[:len(msg)-1] // Remove trailing newline
	}
	tl.logger.Debug("transport", zap.String("message", msg))
	return len(p), nil
}

// ConnPool manages connections to other Raft nodes
type ConnPool struct {
	config *TransportConfig
	pools  map[raft.ServerAddress]*connectionPool
	logger *zap.Logger
	mu     sync.RWMutex
}

type connectionPool struct {
	addr        raft.ServerAddress
	connections chan net.Conn
	mu          sync.Mutex
}

// NewConnPool creates a new connection pool
func NewConnPool(config *TransportConfig, logger *zap.Logger) *ConnPool {
	return &ConnPool{
		config: config,
		pools:  make(map[raft.ServerAddress]*connectionPool),
		logger: logger.Named("conn-pool"),
	}
}

// Get gets a connection to the specified address
func (cp *ConnPool) Get(addr raft.ServerAddress) (net.Conn, error) {
	cp.mu.RLock()
	pool, exists := cp.pools[addr]
	cp.mu.RUnlock()

	if !exists {
		cp.mu.Lock()
		// Double check after acquiring write lock
		if pool, exists = cp.pools[addr]; !exists {
			pool = &connectionPool{
				addr:        addr,
				connections: make(chan net.Conn, cp.config.MaxPool),
			}
			cp.pools[addr] = pool
		}
		cp.mu.Unlock()
	}

	// Try to get from pool first
	select {
	case conn := <-pool.connections:
		// Verify connection is still good
		if err := cp.verifyConnection(conn); err == nil {
			return conn, nil
		}
		conn.Close() // Connection is bad, close it
	default:
		// No pooled connections available
	}

	// Create new connection
	return cp.createConnection(addr)
}

// Put returns a connection to the pool
func (cp *ConnPool) Put(addr raft.ServerAddress, conn net.Conn) error {
	cp.mu.RLock()
	pool, exists := cp.pools[addr]
	cp.mu.RUnlock()

	if !exists {
		conn.Close()
		return fmt.Errorf("no pool for address %s", addr)
	}

	// Verify connection before pooling
	if err := cp.verifyConnection(conn); err != nil {
		conn.Close()
		return err
	}

	select {
	case pool.connections <- conn:
		return nil
	default:
		// Pool is full, close the connection
		conn.Close()
		return nil
	}
}

// Close closes all connections and pools
func (cp *ConnPool) Close() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	var lastErr error

	for addr, pool := range cp.pools {
		close(pool.connections)

		// Close all pooled connections
		for conn := range pool.connections {
			if err := conn.Close(); err != nil {
				cp.logger.Warn("error closing pooled connection",
					zap.String("addr", string(addr)),
					zap.Error(err))
				lastErr = err
			}
		}

		delete(cp.pools, addr)
	}

	return lastErr
}

func (cp *ConnPool) verifyConnection(conn net.Conn) error {
	// Set a short deadline for verification
	if err := conn.SetDeadline(time.Now().Add(1 * time.Second)); err != nil {
		return err
	}

	// Clear the deadline after verification
	defer conn.SetDeadline(time.Time{})

	// For TCP connections, we can check if the socket is still open
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// Try to set socket options to verify it's still valid
		if err := tcpConn.SetNoDelay(true); err != nil {
			return fmt.Errorf("connection verification failed: %w", err)
		}
	}

	return nil
}

func (cp *ConnPool) createConnection(addr raft.ServerAddress) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout:   cp.config.Timeout,
		KeepAlive: cp.config.TCPKeepAlive,
	}

	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	// Configure TCP options
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(cp.config.TCPNoDelay); err != nil {
			cp.logger.Warn("failed to set TCP_NODELAY", zap.Error(err))
		}

		if err := tcpConn.SetKeepAlive(true); err != nil {
			cp.logger.Warn("failed to enable TCP keep-alive", zap.Error(err))
		}

		if err := tcpConn.SetKeepAlivePeriod(cp.config.TCPKeepAlive); err != nil {
			cp.logger.Warn("failed to set keep-alive period", zap.Error(err))
		}
	}

	return conn, nil
}
