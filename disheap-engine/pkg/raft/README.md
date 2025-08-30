# Disheap Engine - Raft Consensus

[![Go Reference](https://pkg.go.dev/badge/github.com/disheap/disheap/disheap-engine/pkg/raft.svg)](https://pkg.go.dev/github.com/disheap/disheap/disheap-engine/pkg/raft)
[![Go Report Card](https://goreportcard.com/badge/github.com/disheap/disheap/disheap-engine/pkg/raft)](https://goreportcard.com/report/github.com/disheap/disheap/disheap-engine/pkg/raft)

This package provides Raft consensus and state machine replication for the Disheap distributed priority messaging system.

## Overview

The Raft integration enables distributed consensus across multiple Disheap nodes, ensuring strong consistency for critical operations while maintaining high availability and partition tolerance.

### Key Features

- **Distributed Consensus** - HashiCorp Raft-based leader election and log replication
- **State Machine Replication** - Consistent application of operations across all nodes
- **Transactional Support** - Two-phase commit protocol for atomic batch operations
- **Snapshot Management** - Efficient state recovery and log compaction
- **Cluster Management** - Dynamic node membership and automatic failover

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Disheap Node  │    │   Disheap Node  │    │   Disheap Node  │
│                 │    │                 │    │                 │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │   gRPC    │  │    │  │   gRPC    │  │    │  │   gRPC    │  │
│  │  Service  │  │    │  │  Service  │  │    │  │  Service  │  │
│  └─────┬─────┘  │    │  └─────┬─────┘  │    │  └─────┬─────┘  │
│        │        │    │        │        │    │        │        │
│  ┌─────▼─────┐  │    │  ┌─────▼─────┐  │    │  ┌─────▼─────┐  │
│  │ Raft FSM  │  │    │  │ Raft FSM  │  │    │  │ Raft FSM  │  │
│  └─────┬─────┘  │    │  └─────┬─────┘  │    │  └─────┬─────┘  │
│        │        │    │        │        │    │        │        │
│  ┌─────▼─────┐  │    │  ┌─────▼─────┐  │    │  ┌─────▼─────┐  │
│  │  Storage  │  │    │  │  Storage  │  │    │  │  Storage  │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                        ┌────────▼────────┐
                        │  Raft Consensus │
                        │   (Leader)      │
                        └─────────────────┘
```

## Components

### Core Files

| File | Description |
|------|-------------|
| `node.go` | Raft node lifecycle and cluster management |
| `fsm.go` | Finite state machine for operation application |
| `log.go` | Log entry definitions and serialization |
| `snapshot.go` | State snapshots and recovery |
| `cluster.go` | Cluster membership and health management |
| `transport.go` | Network transport configuration |
| `compaction.go` | Log compaction and cleanup |
| `recovery.go` | Disaster recovery and state restoration |

### Test Files

| File | Description |
|------|-------------|
| `fsm_test.go` | FSM operation and snapshot testing |
| `node_test.go` | Node lifecycle and cluster testing |
| `recovery_test.go` | Recovery and restoration testing |

## Log Entry Types

The Raft log supports the following operation types:

```go
type EntryType int

const (
    EntryEnqueue        // Message enqueue operations
    EntryAck            // Message acknowledgments  
    EntryNack           // Negative acknowledgments
    EntryLeaseExtend    // Lease extension operations
    EntryBatchPrepare   // Two-phase commit prepare
    EntryBatchCommit    // Two-phase commit commit
    EntryBatchAbort     // Two-phase commit abort
    EntryConfigUpdate   // Topic/partition configuration
    EntrySnapshotMark   // Snapshot marker entries
)
```

## Usage

### Basic Setup

```go
package main

import (
    "log"
    
    disheap_raft "github.com/disheap/disheap/disheap-engine/pkg/raft"
    "github.com/disheap/disheap/disheap-engine/pkg/storage"
    "go.uber.org/zap"
)

func main() {
    // Initialize storage and logger
    storage := storage.NewBadgerStorage("/data/disheap")
    logger := zap.NewProduction()
    
    // Create Raft node configuration
    config := disheap_raft.NodeConfig{
        NodeID:           "node-1",
        RaftBindAddr:     ":8300",
        RaftAdvertiseAddr: "node-1:8300",
        DataDir:          "/data/raft",
        Bootstrap:        true, // Only for initial node
    }
    
    // Create and start Raft node
    node, err := disheap_raft.NewNode(config, storage, logger)
    if err != nil {
        log.Fatal(err)
    }
    
    if err := node.Start(); err != nil {
        log.Fatal(err)
    }
    
    // Node is now ready to participate in consensus
}
```

### Applying Operations

```go
// Create a log entry for topic creation
entry, err := disheap_raft.NewLogEntry(
    disheap_raft.EntryConfigUpdate,
    "my-topic",
    0,
    &disheap_raft.ConfigUpdateEntry{
        ConfigType: "topic",
        ConfigID:   "my-topic",
        NewConfig:  topicConfig,
    },
)
if err != nil {
    return err
}

// Apply through Raft consensus
if err := node.Apply(entry, 10*time.Second); err != nil {
    return err
}
```

### Cluster Management

```go
// Join an existing cluster
joinAddresses := []string{"node-1:8300", "node-2:8300"}
if err := node.JoinCluster(joinAddresses); err != nil {
    return err
}

// Check cluster status
status := node.GetStatus()
fmt.Printf("Leader: %s, State: %s\n", status.Leader, status.State)
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NODE_ID` | Unique node identifier | `hostname` |
| `RAFT_BIND` | Raft bind address | `:8300` |
| `RAFT_ADVERTISE` | Raft advertise address | `hostname:8300` |
| `BOOTSTRAP` | Bootstrap new cluster | `false` |
| `RAFT_JOIN_ADDRESSES` | Comma-separated join addresses | `""` |

### Docker Compose Example

```yaml
version: '3.8'
services:
  disheap-engine-1:
    image: disheap/engine:latest
    environment:
      - NODE_ID=node-1
      - RAFT_BIND=:8300
      - RAFT_ADVERTISE=disheap-engine-1:8300
      - BOOTSTRAP=true
    volumes:
      - disheap-data-1:/home/disheap/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  disheap-engine-2:
    image: disheap/engine:latest
    environment:
      - NODE_ID=node-2
      - RAFT_BIND=:8300
      - RAFT_ADVERTISE=disheap-engine-2:8300
      - BOOTSTRAP=false
      - RAFT_JOIN_ADDRESSES=disheap-engine-1:8300
    volumes:
      - disheap-data-2:/home/disheap/data
    depends_on:
      disheap-engine-1:
        condition: service_healthy

volumes:
  disheap-data-1:
  disheap-data-2:
```

## Performance

### Benchmarks

```
BenchmarkHeapFSM_ApplyEnqueue    17683    64480 ns/op    28269 B/op    589 allocs/op
BenchmarkHeapFSM_Snapshot       124545    18839 ns/op     4449 B/op     34 allocs/op
BenchmarkNode_Apply              12456    89234 ns/op    31245 B/op    678 allocs/op
```

### Tuning Parameters

- **Heartbeat Timeout**: `1s` (adjust based on network latency)
- **Election Timeout**: `5s` (balance between split-brain and failover speed)
- **Commit Timeout**: `500ms` (balance between throughput and latency)
- **Snapshot Threshold**: `8192` log entries (adjust based on memory constraints)

## Monitoring

### Metrics

The Raft implementation exposes the following metrics:

- `raft_applied_entries_total` - Total applied log entries
- `raft_failed_entries_total` - Total failed log entries  
- `raft_snapshots_total` - Total snapshots created
- `raft_cluster_size` - Current cluster size
- `raft_leader_changes_total` - Total leader changes

### Health Checks

```bash
# Check node health
curl http://localhost:8080/health

# Check Raft status
curl http://localhost:8080/raft/status

# Check cluster statistics
curl http://localhost:9090/stats
```

## Testing

Run the test suite:

```bash
go test ./pkg/raft/...
```

Run benchmarks:

```bash
go test -bench=. ./pkg/raft/...
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](../../LICENSE) file for details.

## Dependencies

- [HashiCorp Raft](https://github.com/hashicorp/raft) - Core Raft consensus library
- [BadgerDB](https://github.com/dgraph-io/badger) - Embedded key-value store
- [Zap](https://github.com/uber-go/zap) - Structured logging
- [gRPC](https://grpc.io/) - RPC framework

---

**Disheap Engine Raft Package** - Production-ready distributed consensus for priority messaging systems.