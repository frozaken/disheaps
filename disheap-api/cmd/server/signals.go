package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// SignalHandler manages graceful shutdown of the application
type SignalHandler struct {
	shutdownFuncs []func(context.Context) error
	shutdownOnce  sync.Once
	done          chan struct{}
}

// NewSignalHandler creates a new signal handler
func NewSignalHandler() *SignalHandler {
	return &SignalHandler{
		shutdownFuncs: make([]func(context.Context) error, 0),
		done:          make(chan struct{}),
	}
}

// RegisterShutdownFunc registers a function to be called during shutdown
func (sh *SignalHandler) RegisterShutdownFunc(fn func(context.Context) error) {
	sh.shutdownFuncs = append(sh.shutdownFuncs, fn)
}

// WaitForSignal waits for termination signals and handles graceful shutdown
func (sh *SignalHandler) WaitForSignal(timeout time.Duration) {
	// Create channel for OS signals
	signalChan := make(chan os.Signal, 1)

	// Register the channel to receive specific signals
	signal.Notify(signalChan,
		syscall.SIGINT,  // Interrupt (Ctrl+C)
		syscall.SIGTERM, // Termination request
		syscall.SIGQUIT, // Quit request
		syscall.SIGHUP,  // Hangup (often used for config reload)
	)

	// Wait for signal
	sig := <-signalChan
	log.Printf("Received signal: %s", sig)

	// Handle the signal
	switch sig {
	case syscall.SIGHUP:
		log.Println("Received SIGHUP - configuration reload not implemented yet")
		// TODO: Implement configuration reload
		return
	default:
		// For all other signals, initiate graceful shutdown
		sh.gracefulShutdown(timeout)
	}
}

// gracefulShutdown performs graceful shutdown with timeout
func (sh *SignalHandler) gracefulShutdown(timeout time.Duration) {
	sh.shutdownOnce.Do(func() {
		log.Printf("Initiating graceful shutdown (timeout: %v)", timeout)

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// Execute all shutdown functions
		for i, shutdownFunc := range sh.shutdownFuncs {
			log.Printf("Executing shutdown function %d/%d", i+1, len(sh.shutdownFuncs))

			if err := shutdownFunc(ctx); err != nil {
				log.Printf("Error in shutdown function %d: %v", i+1, err)
			}
		}

		log.Println("Graceful shutdown completed")
		close(sh.done)
	})
}

// Done returns a channel that is closed when shutdown is complete
func (sh *SignalHandler) Done() <-chan struct{} {
	return sh.done
}

// ForceShutdown can be called to force shutdown without waiting for signals
func (sh *SignalHandler) ForceShutdown(timeout time.Duration) {
	log.Println("Force shutdown requested")
	sh.gracefulShutdown(timeout)
}

// IsShuttingDown returns true if shutdown has been initiated
func (sh *SignalHandler) IsShuttingDown() bool {
	select {
	case <-sh.done:
		return true
	default:
		return false
	}
}

// ShutdownManager is a global instance for managing application shutdown
type ShutdownManager struct {
	handler *SignalHandler
	mu      sync.RWMutex
	started bool
}

// NewShutdownManager creates a new shutdown manager
func NewShutdownManager() *ShutdownManager {
	return &ShutdownManager{
		handler: NewSignalHandler(),
	}
}

// RegisterShutdownFunc registers a function to be called during shutdown
func (sm *ShutdownManager) RegisterShutdownFunc(fn func(context.Context) error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.started {
		log.Println("Warning: Registering shutdown function after signal handling started")
	}

	sm.handler.RegisterShutdownFunc(fn)
}

// Start begins signal handling (blocking call)
func (sm *ShutdownManager) Start(timeout time.Duration) {
	sm.mu.Lock()
	sm.started = true
	sm.mu.Unlock()

	log.Println("Signal handler started - press Ctrl+C to shutdown")
	sm.handler.WaitForSignal(timeout)
}

// ForceShutdown forces shutdown without waiting for signals
func (sm *ShutdownManager) ForceShutdown(timeout time.Duration) {
	sm.handler.ForceShutdown(timeout)
}

// Global shutdown manager instance
var globalShutdownManager = NewShutdownManager()

// RegisterShutdownFunc registers a global shutdown function
func RegisterShutdownFunc(fn func(context.Context) error) {
	globalShutdownManager.RegisterShutdownFunc(fn)
}

// StartSignalHandler starts the global signal handler
func StartSignalHandler(timeout time.Duration) {
	globalShutdownManager.Start(timeout)
}

// Example usage functions for common shutdown scenarios

// RegisterServerShutdown registers a server for graceful shutdown
func RegisterServerShutdown(server interface{ Shutdown(context.Context) error }, name string) {
	RegisterShutdownFunc(func(ctx context.Context) error {
		log.Printf("Shutting down %s server...", name)
		return server.Shutdown(ctx)
	})
}

// RegisterClientShutdown registers a client for graceful shutdown
func RegisterClientShutdown(client interface{ Close() error }, name string) {
	RegisterShutdownFunc(func(ctx context.Context) error {
		log.Printf("Closing %s client...", name)
		return client.Close()
	})
}

// RegisterResourceCleanup registers a cleanup function
func RegisterResourceCleanup(cleanup func() error, name string) {
	RegisterShutdownFunc(func(ctx context.Context) error {
		log.Printf("Cleaning up %s...", name)
		return cleanup()
	})
}

// RegisterDatabaseShutdown registers database connection pool shutdown
func RegisterDatabaseShutdown(db interface{ Close() error }, name string) {
	RegisterShutdownFunc(func(ctx context.Context) error {
		log.Printf("Closing %s database connection...", name)
		return db.Close()
	})
}
