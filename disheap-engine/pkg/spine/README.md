# Spine Index Package

The spine package implements the **Hierarchical Spine Index** - a sophisticated distributed system component that enables global top-K operations across multiple partitions with bounded staleness guarantees.

## Overview

The Spine Index is a key architectural component from the disheap specification that solves the challenge of maintaining global priority ordering across partitioned message heaps. It provides **bounded staleness** guarantees, ensuring that when you request the top-K messages globally, you get messages that are provably within the top-K*F range (where F is the fan-out factor).

## Architecture

```
                    ┌─────────────────────────────────────┐
                    │         Spine Index                 │
                    │   (Global Coordinator Heap)        │
                    └─────────────────────────────────────┘
                               │         │         │
                    ┌──────────┴─┐  ┌────┴─────┐  ┌┴──────────┐
                    │ Partition 1 │  │Partition 2│  │Partition 3│
                    │Candidate Set│  │Candidate  │  │Candidate  │
                    │  [Top F]    │  │Set [Top F]│  │Set [Top F]│
                    └─────────────┘  └───────────┘  └───────────┘
                           │              │              │
                    ┌──────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐
                    │   Local     │ │   Local    │ │   Local    │
                    │Binary Heap  │ │Binary Heap │ │Binary Heap │
                    │(Full Data)  │ │(Full Data) │ │(Full Data) │
                    └─────────────┘ └────────────┘ └────────────┘
```

## Key Features

### 🎯 **Bounded Staleness Guarantee**
- **Property**: Any message selected by `SelectBest(K)` is guaranteed to be within the global top-K*F messages
- **Proof**: Each partition exposes its top-F candidates to the Spine Index, so the coordinator heap contains ≤P*F messages representing the best from each partition
- **Trade-off**: Perfect global ordering (staleness=0) vs performance and scalability

### 📊 **Multi-Strategy Selection**
- **BestFirst**: Strict priority ordering (default)
- **RoundRobin**: Fair partition rotation for load balancing  
- **Weighted**: Custom partition weights for heterogeneous workloads

### 🚦 **Throttling Support**
- **Suspend partitions** during rebalancing/maintenance
- **Graceful degradation** - continue serving from healthy partitions
- **No client errors** - transparent to SDK users

### ⚡ **High Performance**
- **O(log P)** selection complexity (P = partitions)
- **Lock-free coordinator heap** rebuilding
- **Microsecond latency** selection (verified by benchmarks)

## Core Components

### 1. SpineIndex Interface

The main interface for hierarchical partition coordination:

```go
type SpineIndex interface {
    UpdateCandidates(partitionID uint32, candidates []*models.Message) error
    SelectBest(count int) ([]*models.Message, error)
    GetBounds() (topK int, actualK int)
    AddPartition(partitionID uint32) error
    RemovePartition(partitionID uint32) error
    SetPartitionThrottled(partitionID uint32, throttled bool, reason string) error
    GetStats() SpineIndexStats
    Close() error
}
```

### 2. PartitionCandidateSet

Manages the top-F candidates from each partition:

```go
type PartitionCandidateSet struct {
    partitionID uint32
    fanOut      int                // F - number of candidates to expose
    candidates  []*Candidate       // Ordered by priority (best first) 
    mode        models.HeapMode    // MIN or MAX heap
    // ... other fields
}
```

### 3. GlobalSelector  

Implements sophisticated selection algorithms with staleness policies:

```go
type GlobalSelector struct {
    policy SelectionPolicy    // Strategy, staleness, fairness settings
    // ... selection state and counters
}
```

### 4. Selection Strategies

#### Best-First (Default)
```go
policy := DefaultSelectionPolicy()  
policy.Strategy = StrategyBestFirst
```
- Selects candidates in strict priority order
- **Use case**: Applications requiring optimal priority ordering
- **Trade-off**: May favor hot partitions

#### Round-Robin
```go  
policy.Strategy = StrategyRoundRobin
```
- Rotates through partitions to ensure fairness
- **Use case**: Workloads requiring partition load balancing
- **Trade-off**: Slightly suboptimal priority ordering for fairness

#### Weighted
```go
policy.Strategy = StrategyWeighted
policy.PartitionWeights = map[uint32]float64{
    1: 3.0,  // Partition 1 gets 3x weight
    2: 1.0,  // Partition 2 gets 1x weight
}
```
- Custom partition weights for heterogeneous resources
- **Use case**: Partitions with different capacities or priorities  
- **Trade-off**: Configuration complexity for optimal resource usage

## Configuration

### SpineIndexConfig

```go
config := SpineIndexConfig{
    TopK:   100,                    // Target top-K bound (K)
    FanOut: 4,                      // Candidates per partition (F)  
    Mode:   models.MinHeap,         // MIN or MAX heap
    MaxAge: 30 * time.Second,       // Max candidate age for freshness
}
```

### SelectionPolicy  

```go
policy := SelectionPolicy{
    Strategy:       StrategyBestFirst,
    MaxStaleness:   5 * time.Second,   // Filter candidates older than this
    MinFreshness:   100 * time.Millisecond, // Filter candidates newer than this  
    FairnessWeight: 0.1,               // Fairness vs priority trade-off
    PartitionWeights: map[uint32]float64{}, // Custom partition weights
}
```

## Usage Examples

### Basic Usage

```go
import "github.com/disheap/disheap/disheap-engine/pkg/spine"

// Create spine index
config := spine.SpineIndexConfig{
    TopK:   50,
    FanOut: 3,
    Mode:   models.MinHeap,
}

spineIndex, err := spine.NewSpineIndex(config, logger)
if err != nil {
    log.Fatal(err)
}

// Add partitions  
spineIndex.AddPartition(1)
spineIndex.AddPartition(2)
spineIndex.AddPartition(3)

// Update candidates from each partition
candidates1 := []*models.Message{/* top 3 from partition 1 */}
candidates2 := []*models.Message{/* top 3 from partition 2 */}  
candidates3 := []*models.Message{/* top 3 from partition 3 */}

spineIndex.UpdateCandidates(1, candidates1)
spineIndex.UpdateCandidates(2, candidates2)  
spineIndex.UpdateCandidates(3, candidates3)

// Select global top-10 messages
topMessages, err := spineIndex.SelectBest(10)
if err != nil {
    log.Fatal(err)
}

// Process selected messages...
for _, msg := range topMessages {
    fmt.Printf("Selected message: ID=%s, Priority=%d\n", msg.ID, msg.Priority)
}
```

### Advanced Selection with Custom Policy

```go
// Create selector with custom policy
policy := spine.SelectionPolicy{
    Strategy:     spine.StrategyWeighted,
    MaxStaleness: 2 * time.Second,
    MinFreshness: 0,
    PartitionWeights: map[uint32]float64{
        1: 2.0,  // Partition 1: high priority
        2: 1.0,  // Partition 2: normal priority  
        3: 0.5,  // Partition 3: low priority
    },
}

selector := spine.NewGlobalSelector(policy, logger)

// Get candidates from spine index (internal operation)
candidates := getAllCandidatesFromSpineIndex()

// Select with weighted strategy
selected, err := selector.SelectCandidates(ctx, candidates, 20)
if err != nil {
    log.Fatal(err)
}
```

### Partition Throttling

```go
// Throttle partition during maintenance
err := spineIndex.SetPartitionThrottled(2, true, "rebalancing")
if err != nil {
    log.Fatal(err)
}

// Continue serving from healthy partitions
// (partition 2 won't contribute candidates)
topMessages, _ := spineIndex.SelectBest(10)

// Unthrottle when maintenance is complete
spineIndex.SetPartitionThrottled(2, false, "")
```

### Monitoring and Statistics

```go
// Get spine index statistics
stats := spineIndex.GetStats()
fmt.Printf("Partitions: %d/%d available\n", stats.AvailablePartitions, stats.PartitionCount)
fmt.Printf("Coordinator heap size: %d\n", stats.CoordinatorHeapSize) 
fmt.Printf("Total updates: %d\n", stats.TotalUpdates)

// Get per-partition details  
for partitionID, partStats := range stats.PartitionStats {
    fmt.Printf("Partition %d: %d candidates, %d updates\n", 
        partitionID, partStats.CandidateCount, partStats.TotalUpdates)
}

// Get selector statistics
selectorStats := selector.GetStats()
fmt.Printf("Selection strategy: %v\n", selectorStats.Strategy)
fmt.Printf("Total selections: %d\n", selectorStats.TotalSelections)
fmt.Printf("Staleness violations: %d\n", selectorStats.StalenessViolations)
```

## Performance Characteristics

Based on benchmark results:

```
BenchmarkSpineIndexSelect-10       661194    2428 ns/op    1592 B/op    9 allocs/op
BenchmarkGlobalSelectorBestFirst-10  1000000  1290 ns/op   1094 B/op    4 allocs/op
```

### Performance Analysis

- **Selection Latency**: ~2.4μs for selecting from 40 candidates (10 partitions × 4 fan-out)
- **Memory Efficiency**: ~1.6KB per selection operation
- **Allocation Efficiency**: Only 9 allocations per selection
- **Throughput**: ~400K selections/second per core

### Scaling Characteristics  

- **Partitions (P)**: Linear growth in memory (O(P*F)), logarithmic in selection time
- **Fan-out (F)**: Linear impact on both memory and selection time
- **Selection Count (K)**: Linear only in result copying, not in coordination

## Bounded Staleness Theory

The Spine Index provides **bounded staleness** guarantees based on the following theorem:

### Theorem: Bounded Staleness Guarantee

**Given:**
- P partitions, each exposing F candidates
- Global heap with N total messages  
- Spine Index contains ≤P*F candidates

**Property:** 
Any message `m` selected by `SelectBest(K)` satisfies:
```
rank_global(m) ≤ K*F
```

**Proof Sketch:**
1. Each partition contributes its top-F messages to the spine
2. The spine coordinator heap maintains the best P*F candidates globally
3. When selecting K messages, we choose from this bounded candidate set
4. Therefore, selected messages are provably within the top-K*F globally

### Staleness Bound Configuration

```go
// For tighter bounds, increase fan-out
config.FanOut = 8  // Better staleness, more memory

// For looser bounds, decrease fan-out  
config.FanOut = 2  // Less memory, more staleness

// Staleness factor = FanOut
// If FanOut=4, then SelectBest(10) gives messages within global top-40
```

## Integration with disheap-engine

The Spine Index integrates with other disheap components:

### With Partition Manager
```go
// Partition manager updates spine with top candidates
func (pm *PartitionManager) updateSpineIndex(partitionID uint32) {
    topCandidates := pm.partitions[partitionID].GetTopMessages(fanOut)
    pm.spineIndex.UpdateCandidates(partitionID, topCandidates)  
}
```

### With gRPC Service  
```go
// gRPC PopStream uses spine for global selection
func (s *DisheapService) PopStream(stream disheapv1.Disheap_PopStreamServer) error {
    // ... stream setup ...
    
    // Get global top-K candidates
    candidates, err := s.spineIndex.SelectBest(batchSize)
    if err != nil {
        return err
    }
    
    // Stream candidates to client...
}
```

### With Coordinator
```go  
// Cross-partition coordinator leverages spine for global operations
func (c *Coordinator) GlobalPop(topicID string, count int) ([]*models.Message, error) {
    return c.spineIndex.SelectBest(count)
}
```

## Thread Safety

All components in the spine package are **thread-safe**:

- **SpineIndex**: Uses `sync.RWMutex` for concurrent reads/exclusive writes
- **PartitionCandidateSet**: Thread-safe candidate updates and retrievals  
- **GlobalSelector**: Thread-safe selection with atomic statistics

## Error Handling

The spine package defines specific error types:

```go
// Spine-specific errors
ErrSpineClosed           // Operations on closed spine index
ErrPartitionNotFound     // Partition doesn't exist in spine  
ErrNoAvailableCandidates // All partitions throttled or empty
ErrInvalidCandidate      // Candidate validation failed
ErrSpineIndexFull        // Spine reached capacity limit
```

## Testing

Comprehensive test coverage includes:

- **Unit Tests**: Individual component functionality
- **Integration Tests**: Multi-component interaction  
- **Bounded Staleness Tests**: Verify theoretical guarantees
- **Performance Tests**: Benchmarks for latency/throughput
- **Chaos Tests**: Partition throttling, failover scenarios

Run tests:
```bash
go test ./pkg/spine -v           # All tests
go test ./pkg/spine -bench=.     # Benchmarks
go test ./pkg/spine -race        # Race condition detection
```

## Future Enhancements

Potential improvements for future versions:

1. **Adaptive Fan-out**: Dynamically adjust F based on staleness metrics
2. **Hierarchical Spine**: Multi-level spine for very large partition counts  
3. **Predictive Caching**: Pre-fetch candidates based on access patterns
4. **Cross-DC Spine**: Geo-distributed spine coordination
5. **ML-based Selection**: Learning-based partition weight optimization

## Conclusion

The Spine Index package provides a sophisticated, production-ready implementation of hierarchical partition coordination with bounded staleness guarantees. It balances the trade-offs between global ordering optimality, system performance, and operational simplicity - making it ideal for distributed priority queue systems like disheap.

The implementation provides microsecond-level selection latency while maintaining theoretical correctness guarantees, supporting the disheap system's goal of scalable, high-performance message queuing with bounded staleness semantics.
