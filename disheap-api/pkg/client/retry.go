package client

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryableFunc defines a function that can be retried
type RetryableFunc func() error

// IsRetryable determines if an error is retryable based on gRPC status code
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Get gRPC status code
	st, ok := status.FromError(err)
	if !ok {
		// Non-gRPC error, consider retryable
		return true
	}

	switch st.Code() {
	case codes.Unavailable,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.Internal:
		return true
	case codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.FailedPrecondition,
		codes.OutOfRange,
		codes.Unimplemented,
		codes.Unauthenticated:
		return false
	default:
		return false
	}
}

// Retry executes a function with exponential backoff retry logic
func Retry(ctx context.Context, config RetryConfig, fn RetryableFunc) error {
	var lastErr error

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Execute the function
		err := fn()
		if err == nil {
			return nil // Success
		}

		lastErr = err

		// Don't retry on last attempt
		if attempt == config.MaxAttempts {
			break
		}

		// Check if error is retryable
		if !IsRetryable(err) {
			return err
		}

		// Calculate backoff delay
		backoff := calculateBackoff(attempt-1, config)

		// Wait for backoff period or context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			// Continue to next attempt
		}
	}

	return fmt.Errorf("failed after %d attempts: %w", config.MaxAttempts, lastErr)
}

// calculateBackoff calculates the backoff duration for a given attempt
func calculateBackoff(attempt int, config RetryConfig) time.Duration {
	if attempt < 0 {
		return config.InitialBackoff
	}

	// Calculate exponential backoff
	backoff := float64(config.InitialBackoff) * math.Pow(config.BackoffMultiplier, float64(attempt))

	// Apply jitter (±25%)
	jitter := 1.0 + (rand.Float64()-0.5)*0.5
	backoff *= jitter

	// Cap at maximum backoff
	if backoff > float64(config.MaxBackoff) {
		backoff = float64(config.MaxBackoff)
	}

	return time.Duration(backoff)
}

// WithRetry wraps a client method call with retry logic
func (c *client) withRetry(ctx context.Context, fn RetryableFunc) error {
	return Retry(ctx, c.config.Retry, fn)
}

// Example of how to wrap client calls with retry logic
// This would be used to enhance the client methods for critical operations

// MakeHeapWithRetry creates a heap with retry logic
func (c *client) MakeHeapWithRetry(ctx context.Context, req *disheapv1.MakeHeapReq) (*disheapv1.MakeHeapResp, error) {
	var result *disheapv1.MakeHeapResp
	var resultErr error

	err := c.withRetry(ctx, func() error {
		var err error
		result, err = c.MakeHeap(ctx, req)
		resultErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return result, resultErr
}

// EnqueueWithRetry enqueues a message with retry logic
func (c *client) EnqueueWithRetry(ctx context.Context, req *disheapv1.EnqueueReq) (*disheapv1.EnqueueResp, error) {
	var result *disheapv1.EnqueueResp
	var resultErr error

	err := c.withRetry(ctx, func() error {
		var err error
		result, err = c.Enqueue(ctx, req)
		resultErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return result, resultErr
}
