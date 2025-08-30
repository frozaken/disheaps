# Transaction Package (`pkg/txn`)

This package implements **Two-Phase Commit (2PC)** protocol for atomic batch operations across multiple partitions in the Disheap Engine.

## Overview

The transaction package provides distributed transaction support with ACID guarantees, allowing atomic operations across multiple topics and partitions. It follows the classic Two-Phase Commit protocol for ensuring consistency in distributed environments.

## Architecture

### Components

1. **Coordinator** (`coordinator.go`)
   - Manages distributed transactions using 2PC protocol
   - Orchestrates prepare and commit phases across participants
   - Handles transaction timeouts and cleanup
   - Maintains transaction state and recovery information

2. **Participant** (`participant.go`)  
   - Implements participant side of 2PC protocol
   - Integrates with partition operations (enqueue, ack, nack)
   - Manages local transaction state and validation
   - Handles auto-abort for timed-out prepared transactions

3. **Timeout Manager** (`timeout.go`)
   - Efficient timeout scheduling using min-heap
   - Asynchronous callback processing with panic protection
   - Memory-safe cleanup of expired timeouts

## Two-Phase Commit Protocol

### Phase 1: Prepare
1. **Coordinator** sends `Prepare` requests to all participants
2. **Participants** validate operations and prepare for commit
3. **Participants** respond with success/failure
4. If any participant fails, coordinator initiates abort

### Phase 2: Commit/Abort
1. **Coordinator** sends `Commit` (if all prepared) or `Abort` requests
2. **Participants** apply operations (commit) or clean up (abort)  
3. **Participants** acknowledge completion
4. **Coordinator** marks transaction as complete

## Usage Example

```go
// Create coordinator
config := DefaultCoordinatorConfig()
coordinator := NewCoordinator(config, logger)

// Register participants for topics/partitions
participant1 := NewPartitionParticipant(partConfig, partition1, logger)
coordinator.RegisterParticipant(ParticipantID{Topic: "orders", Partition: 0}, participant1)

// Begin distributed transaction
operations := []*models.BatchOperation{
    {Type: models.BatchOperationEnqueue, Topic: "orders", Partition: 0, Message: msg1},
    {Type: models.BatchOperationEnqueue, Topic: "inventory", Partition: 1, Message: msg2},
}

txn, err := coordinator.BeginTransaction(ctx, operations)
if err != nil {
    return err
}

// Execute 2PC protocol
err = coordinator.CommitTransaction(ctx, txn.ID)
if err != nil {
    // Transaction failed - participants have been notified to abort
    return err
}
```

## Configuration

### Coordinator Configuration
- `DefaultTimeout`: Maximum transaction lifetime (default: 30s)
- `MaxConcurrentTxns`: Limit on concurrent transactions (default: 1000)
- `ParticipantTimeout`: Timeout for participant operations (default: 10s)
- `CleanupInterval`: How often to clean completed transactions (default: 5m)

### Participant Configuration
- `MaxConcurrentTxns`: Limit on concurrent transactions per participant (default: 100)
- `PreparedTimeout`: How long to keep prepared state before auto-abort (default: 5m)
- `CleanupInterval`: Cleanup frequency for completed transactions (default: 1m)

## Transaction States

### Coordinator States
- `StateInit`: Transaction created
- `StatePreparing`: Prepare phase in progress
- `StatePrepared`: All participants prepared successfully
- `StateCommitting`: Commit phase in progress
- `StateCommitted`: Transaction committed successfully
- `StateAborting`: Abort phase in progress  
- `StateAborted`: Transaction aborted

### Participant States
- `ParticipantIdle`: No active transaction
- `ParticipantPreparing`: Validating operations
- `ParticipantPrepared`: Ready to commit
- `ParticipantCommitting`: Applying operations
- `ParticipantCommitted`: Operations applied successfully
- `ParticipantAborting`: Cleaning up prepared state
- `ParticipantAborted`: Cleanup complete

## Error Handling

### Prepare Phase Failures
- Any participant failure causes entire transaction to abort
- All participants are notified to abort their prepared state
- Coordinator returns prepare error to client

### Commit Phase Failures  
- Transaction is still considered committed if any participant prepared
- Individual commit failures are logged but don't fail the transaction
- This maintains consistency - prepared participants must commit

### Timeout Handling
- Transactions automatically abort after `DefaultTimeout`
- Prepared participants auto-abort after `PreparedTimeout`
- Prevents resource leaks from abandoned transactions

## Concurrency & Safety

### Thread Safety
- All operations are thread-safe using appropriate mutexes
- Coordinator uses global mutex for transaction state
- Participants use per-transaction synchronization

### Race Condition Prevention
- Atomic state transitions prevent inconsistent states  
- Timeout cleanup is synchronized with active operations
- Proper cleanup prevents memory/resource leaks

## Monitoring & Observability

### Logging
- Structured logging with zap for all operations
- Debug-level logs for protocol steps
- Error-level logs for failures and timeouts

### Metrics (Future)
- Transaction latency and throughput
- Success/failure rates by reason
- Active transaction counts
- Timeout occurrence rates

## Integration

### With Partition Package
- Participants wrap partition operations in 2PC protocol
- Operations validated during prepare phase
- Actual changes applied only during commit phase

### With Storage Package  
- Transaction log entries can be persisted for recovery
- Coordinator state can survive restarts
- Participant prepared state survives node failures

## Testing

Comprehensive test suite covers:
- **End-to-end transaction success** scenarios
- **Prepare phase failures** and rollback
- **Commit phase failures** with partial success
- **Transaction timeouts** and auto-abort  
- **Concurrent transaction safety**
- **Integration testing** with mock partitions

## Performance Characteristics

- **Latency**: ~2x RTT (prepare + commit phases)
- **Throughput**: Limited by slowest participant
- **Scalability**: Linear with participant count
- **Memory**: O(transactions) + O(operations per transaction)

## Future Enhancements

- **Recovery**: Persistent transaction log for coordinator restart recovery
- **Optimization**: Single-phase commit optimization for single-partition transactions  
- **Batching**: Group multiple transactions for better throughput
- **Metrics**: Comprehensive observability and monitoring
