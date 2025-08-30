# Delivery Semantics Package

This package implements exactly-once delivery guarantees, exponential backoff with jitter, and dead letter queue (DLQ) management for the disheap message queuing system.

## Overview

The delivery package provides three main components that work together to ensure reliable message delivery:

- **DeliveryManager**: Handles exactly-once delivery through message leasing
- **BackoffManager**: Implements exponential backoff strategies with jitter for retry logic
- **DLQManager**: Manages dead letter queues for messages that exceed retry limits

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  DeliveryManager│    │  BackoffManager  │    │   DLQManager    │
│                 │    │                  │    │                 │
│ • Message Leases│    │ • Retry Delays   │    │ • Failed Msgs   │
│ • Ack/Nack/Extend│   │ • Jitter Types   │    │ • Replay/Delete │
│ • Timeout Cleanup│   │ • Linear/Exp     │    │ • Compaction    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Components

### DeliveryManager

Manages exactly-once delivery semantics by creating leases for messages that prevent concurrent processing.

**Key Features:**
- Secure lease token generation
- Configurable lease timeouts and extensions
- Automatic cleanup of expired leases
- Thread-safe operations
- Comprehensive metrics

**Usage:**

```go
import "github.com/disheap/disheap/disheap-engine/pkg/delivery"

// Create delivery manager
config := &DeliveryConfig{
    MaxLeaseExtensions:  10,
    MaxLeaseExtension:   5 * time.Minute,
    DefaultLeaseTimeout: 30 * time.Second,
}
dm := NewDeliveryManager(config, logger)

// Lease a message
lease, err := dm.LeaseMessage(ctx, messageID, "consumer-1", 30*time.Second)
if err != nil {
    // Handle error (e.g., already leased)
}

// Acknowledge successful processing
err = dm.AckMessage(ctx, messageID, lease.Token)

// Or reject with failure
err = dm.NackMessage(ctx, messageID, lease.Token)

// Extend lease if processing takes longer
extendedLease, err := dm.ExtendLease(ctx, messageID, lease.Token, 30*time.Second)
```

### BackoffManager

Implements configurable backoff strategies for handling message retry delays.

**Supported Strategies:**
- **Exponential Backoff**: `delay = baseDelay * multiplier^(attempts-1)`
- **Linear Backoff**: `delay = baseDelay + (attempts-1) * increment`

**Jitter Types:**
- **NoJitter**: Use calculated delay exactly
- **FullJitter**: Random delay between 0 and calculated delay
- **EqualJitter**: Half calculated + random half
- **DecorrelatedJitter**: Random between calculated and 3x calculated

**Usage:**

```go
// Create exponential backoff strategy
config := &BackoffConfig{
    BaseDelay:  1 * time.Second,
    MaxDelay:   5 * time.Minute,
    Multiplier: 2.0,
    Jitter:     FullJitter,
}
strategy := NewExponentialBackoff(config, logger)
bm := NewBackoffManager(strategy, logger)

// Calculate retry delay
delay := bm.CalculateBackoff(attempts, true) // with jitter
retryAt := bm.ScheduleRetry(attempts, true)

// Check if should retry
if bm.ShouldRetry(attempts, maxRetries) {
    // Schedule retry
}
```

### DLQManager

Manages dead letter queues for messages that have exceeded their retry limits.

**Features:**
- Per-topic DLQ heaps
- Configurable retention policies
- Capacity limits and backpressure
- Message replay capabilities
- Automatic compaction
- Statistics and monitoring

**Usage:**

```go
// Create DLQ manager
config := &DLQConfig{
    MaxCapacity:        10000,
    RetentionTime:      7 * 24 * time.Hour,
    EnableCompaction:   true,
    CompactionInterval: 1 * time.Hour,
}
dlq := NewDLQManager(config, logger)

// Move failed message to DLQ
err := dlq.MoveToDLQ(ctx, "topic-1", message, "max retries exceeded")

// Peek at DLQ messages (non-destructive)
messages, err := dlq.PeekDLQ(ctx, "topic-1", 10)

// Replay messages back to main queue
replayed, err := dlq.ReplayFromDLQ(ctx, "topic-1", messageIDs)

// Permanently delete messages
err := dlq.DeleteFromDLQ(ctx, "topic-1", messageIDs)

// Get DLQ statistics
stats, err := dlq.GetDLQStats("topic-1")
fmt.Printf("DLQ has %d messages, %d total moved\n", 
    stats.MessageCount, stats.TotalMoved)
```

## Configuration

### DeliveryConfig

```go
type DeliveryConfig struct {
    MaxLeaseExtensions  uint32        // Max number of extensions per lease
    MaxLeaseExtension   time.Duration // Max duration for a single extension
    DefaultLeaseTimeout time.Duration // Default lease timeout
}
```

### BackoffConfig

```go
type BackoffConfig struct {
    BaseDelay  time.Duration   // Initial delay
    MaxDelay   time.Duration   // Maximum delay cap
    Multiplier float64         // Exponential multiplier (>1.0)
    Jitter     JitterStrategy  // Jitter type to apply
}
```

### DLQConfig

```go
type DLQConfig struct {
    MaxCapacity        uint64        // Max messages per DLQ
    RetentionTime      time.Duration // Message retention period
    EnableCompaction   bool          // Auto cleanup expired messages
    CompactionInterval time.Duration // How often to compact
}
```

## Error Handling

The package defines several specific error types:

```go
var (
    ErrMessageNotFound    = errors.New("message not found")
    ErrInvalidLease       = errors.New("invalid lease token")
    ErrLeaseExpired       = errors.New("lease has expired")
    ErrAlreadyLeased      = errors.New("message already leased")
    ErrDeliveryTimeout    = errors.New("delivery timeout")
    ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")
    ErrDLQNotEnabled      = errors.New("DLQ not enabled for topic")
    ErrDLQFull            = errors.New("DLQ has reached capacity limit")
)
```

## Testing

Run the comprehensive test suite:

```bash
# Run all delivery package tests
go test ./pkg/delivery -v

# Run with race detection
go test ./pkg/delivery -v -race

# Run specific test
go test ./pkg/delivery -v -run TestDeliveryManager_LeaseMessage

# Run benchmarks
go test ./pkg/delivery -v -bench=.
```

## Examples

### Complete Message Processing Flow

```go
package main

import (
    "context"
    "fmt"
    "time"
    
    "github.com/disheap/disheap/disheap-engine/pkg/delivery"
    "github.com/disheap/disheap/disheap-engine/pkg/models"
    "go.uber.org/zap"
)

func processMessage(ctx context.Context) error {
    logger, _ := zap.NewDevelopment()
    
    // Initialize managers
    dm := delivery.NewDeliveryManager(nil, logger)
    bm := delivery.NewBackoffManager(nil, logger)
    dlq := delivery.NewDLQManager(nil, logger)
    
    messageID := models.MessageID("msg-123")
    topic := "orders"
    maxRetries := uint32(3)
    var attempts uint32 = 1
    
    // Try to lease the message
    lease, err := dm.LeaseMessage(ctx, messageID, "worker-1", 30*time.Second)
    if err != nil {
        return fmt.Errorf("failed to lease message: %w", err)
    }
    
    // Simulate message processing
    if processBusinessLogic() {
        // Success - acknowledge
        return dm.AckMessage(ctx, messageID, lease.Token)
    } else {
        // Failure - reject
        err := dm.NackMessage(ctx, messageID, lease.Token)
        if err != nil {
            return err
        }
        
        // Check if we should retry
        if bm.ShouldRetry(attempts, maxRetries) {
            // Calculate backoff and schedule retry
            delay := bm.CalculateBackoff(attempts, true)
            fmt.Printf("Retrying after %v\n", delay)
            return nil // Retry scheduled
        } else {
            // Move to DLQ
            message := &models.Message{ID: messageID, Attempts: attempts}
            return dlq.MoveToDLQ(ctx, topic, message, "max retries exceeded")
        }
    }
}

func processBusinessLogic() bool {
    // Simulate processing that might fail
    return time.Now().UnixNano()%2 == 0
}
```

### Monitoring and Metrics

```go
// Get delivery statistics
deliveryStats := dm.Stats()
fmt.Printf("Active leases: %d, Acked: %d, Nacked: %d, Timeouts: %d\n",
    deliveryStats.ActiveLeases,
    deliveryStats.AckedCount,
    deliveryStats.NackedCount,
    deliveryStats.TimeoutCount)

// Get DLQ statistics for monitoring
dlqStats, _ := dlq.GetDLQStats("orders")
fmt.Printf("DLQ: %d messages, oldest: %v\n",
    dlqStats.MessageCount,
    dlqStats.OldestMessage)

// Check for retention violations
if dlqStats.RetentionViolations > 0 {
    fmt.Printf("WARNING: %d messages past retention deadline\n",
        dlqStats.RetentionViolations)
}
```

## Integration

This package integrates with other disheap components:

- **Storage Layer**: Persists lease information and DLQ messages
- **Partition Manager**: Coordinates message lifecycle
- **Heap Structures**: Receives retry messages and DLQ replays
- **Metrics System**: Exports delivery statistics

## Thread Safety

All managers in this package are thread-safe and can be used concurrently from multiple goroutines. Internal synchronization is handled using read-write mutexes for optimal performance.

## Observability

The package provides structured logging using Zap with consistent component tags for easy filtering and monitoring. All operations are logged at appropriate levels with relevant context.

Log example:
```json
{
  "level": "debug",
  "component": "delivery_manager",
  "message_id": "msg-123",
  "holder": "consumer-1",
  "token": "abc123...",
  "msg": "Message leased"
}
```
