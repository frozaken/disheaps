package coordinator

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"sync"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// PartitionRouter handles message routing to partitions using rendezvous hashing
type PartitionRouter interface {
	// Route determines the target partition for a message based on its partition key
	Route(topic string, partitionKey string) (models.PartitionID, error)

	// GetPartitions returns all partition IDs for a topic
	GetPartitions(topic string) ([]models.PartitionID, error)

	// AddPartition adds a new partition to the routing table
	AddPartition(topic string, partitionID models.PartitionID) error

	// RemovePartition removes a partition from the routing table
	RemovePartition(topic string, partitionID models.PartitionID) error

	// UpdatePartitions updates the complete partition set for a topic
	UpdatePartitions(topic string, partitions []models.PartitionID) error

	// GetTopology returns the current partition topology
	GetTopology() map[string][]models.PartitionID

	// GetStats returns routing statistics
	GetStats() RoutingStats
}

// RendezvousRouter implements consistent partition routing using rendezvous hashing
// This provides stable routing during partition changes with minimal redistribution
type RendezvousRouter struct {
	mu       sync.RWMutex
	topology map[string][]models.PartitionID // topic -> partition list
	logger   *zap.Logger

	// Statistics
	routingCalls    uint64
	topologyChanges uint64
}

// NewRendezvousRouter creates a new rendezvous-based partition router
func NewRendezvousRouter(logger *zap.Logger) PartitionRouter {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &RendezvousRouter{
		topology: make(map[string][]models.PartitionID),
		logger:   logger,
	}
}

// Route determines the target partition using rendezvous hashing (highest random weight)
func (rr *RendezvousRouter) Route(topic string, partitionKey string) (models.PartitionID, error) {
	rr.mu.RLock()
	defer rr.mu.RUnlock()

	partitions, exists := rr.topology[topic]
	if !exists {
		return 0, fmt.Errorf("topic %s not found in routing topology", topic)
	}

	if len(partitions) == 0 {
		return 0, fmt.Errorf("no partitions available for topic %s", topic)
	}

	// Single partition - no hashing needed
	if len(partitions) == 1 {
		rr.routingCalls++
		return partitions[0], nil
	}

	// Rendezvous hashing: find partition with highest hash(key + partition_id)
	maxWeight := float64(-1)
	selectedPartition := partitions[0]

	keyBytes := []byte(partitionKey)

	for _, partitionID := range partitions {
		weight := rr.calculateWeight(keyBytes, partitionID)
		if weight > maxWeight {
			maxWeight = weight
			selectedPartition = partitionID
		}
	}

	rr.routingCalls++

	rr.logger.Debug("Routed message to partition",
		zap.String("component", "rendezvous_router"),
		zap.String("topic", topic),
		zap.String("partition_key", partitionKey),
		zap.Uint32("partition_id", uint32(selectedPartition)),
		zap.Float64("weight", maxWeight))

	return selectedPartition, nil
}

// GetPartitions returns all partition IDs for a topic
func (rr *RendezvousRouter) GetPartitions(topic string) ([]models.PartitionID, error) {
	rr.mu.RLock()
	defer rr.mu.RUnlock()

	partitions, exists := rr.topology[topic]
	if !exists {
		return nil, fmt.Errorf("topic %s not found in routing topology", topic)
	}

	// Return a copy to prevent external modification
	result := make([]models.PartitionID, len(partitions))
	copy(result, partitions)
	return result, nil
}

// AddPartition adds a new partition to the routing table
func (rr *RendezvousRouter) AddPartition(topic string, partitionID models.PartitionID) error {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	partitions, exists := rr.topology[topic]
	if !exists {
		rr.topology[topic] = []models.PartitionID{partitionID}
	} else {
		// Check if partition already exists
		for _, existing := range partitions {
			if existing == partitionID {
				return nil // Already exists, no-op
			}
		}

		// Add and sort partitions for consistent ordering
		partitions = append(partitions, partitionID)
		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i] < partitions[j]
		})
		rr.topology[topic] = partitions
	}

	rr.topologyChanges++

	rr.logger.Info("Added partition to routing topology",
		zap.String("component", "rendezvous_router"),
		zap.String("topic", topic),
		zap.Uint32("partition_id", uint32(partitionID)),
		zap.Int("total_partitions", len(rr.topology[topic])))

	return nil
}

// RemovePartition removes a partition from the routing table
func (rr *RendezvousRouter) RemovePartition(topic string, partitionID models.PartitionID) error {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	partitions, exists := rr.topology[topic]
	if !exists {
		return fmt.Errorf("topic %s not found in routing topology", topic)
	}

	// Find and remove the partition
	for i, existing := range partitions {
		if existing == partitionID {
			// Remove partition by slicing
			rr.topology[topic] = append(partitions[:i], partitions[i+1:]...)
			rr.topologyChanges++

			rr.logger.Info("Removed partition from routing topology",
				zap.String("component", "rendezvous_router"),
				zap.String("topic", topic),
				zap.Uint32("partition_id", uint32(partitionID)),
				zap.Int("remaining_partitions", len(rr.topology[topic])))

			return nil
		}
	}

	return fmt.Errorf("partition %d not found for topic %s", partitionID, topic)
}

// UpdatePartitions updates the complete partition set for a topic
func (rr *RendezvousRouter) UpdatePartitions(topic string, partitions []models.PartitionID) error {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	// Create sorted copy for consistent ordering
	newPartitions := make([]models.PartitionID, len(partitions))
	copy(newPartitions, partitions)
	sort.Slice(newPartitions, func(i, j int) bool {
		return newPartitions[i] < newPartitions[j]
	})

	rr.topology[topic] = newPartitions
	rr.topologyChanges++

	rr.logger.Info("Updated partition topology",
		zap.String("component", "rendezvous_router"),
		zap.String("topic", topic),
		zap.Int("partition_count", len(newPartitions)))

	return nil
}

// GetTopology returns the current partition topology
func (rr *RendezvousRouter) GetTopology() map[string][]models.PartitionID {
	rr.mu.RLock()
	defer rr.mu.RUnlock()

	// Return deep copy to prevent external modification
	topology := make(map[string][]models.PartitionID)
	for topic, partitions := range rr.topology {
		topicPartitions := make([]models.PartitionID, len(partitions))
		copy(topicPartitions, partitions)
		topology[topic] = topicPartitions
	}

	return topology
}

// GetStats returns routing statistics
func (rr *RendezvousRouter) GetStats() RoutingStats {
	rr.mu.RLock()
	defer rr.mu.RUnlock()

	stats := RoutingStats{
		RoutingCalls:    rr.routingCalls,
		TopologyChanges: rr.topologyChanges,
		TopicCount:      len(rr.topology),
		TopicPartitions: make(map[string]int),
	}

	for topic, partitions := range rr.topology {
		stats.TopicPartitions[topic] = len(partitions)
	}

	return stats
}

// calculateWeight computes the rendezvous hash weight for a key-partition pair
func (rr *RendezvousRouter) calculateWeight(keyBytes []byte, partitionID models.PartitionID) float64 {
	// Create hash input: key + partition_id
	h := sha256.New()
	h.Write(keyBytes)

	// Add partition ID as bytes
	partitionBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(partitionBytes, uint32(partitionID))
	h.Write(partitionBytes)

	hashBytes := h.Sum(nil)

	// Convert first 8 bytes of hash to uint64, then normalize to [0,1)
	hashValue := binary.LittleEndian.Uint64(hashBytes[:8])
	weight := float64(hashValue) / float64(math.MaxUint64)

	return weight
}

// HashRouter provides simple hash-based routing (for comparison/testing)
type HashRouter struct {
	mu       sync.RWMutex
	topology map[string][]models.PartitionID
	logger   *zap.Logger

	// Statistics
	routingCalls uint64
}

// NewHashRouter creates a simple hash-based router
func NewHashRouter(logger *zap.Logger) PartitionRouter {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &HashRouter{
		topology: make(map[string][]models.PartitionID),
		logger:   logger,
	}
}

// Route uses simple hash modulo for partition selection
func (hr *HashRouter) Route(topic string, partitionKey string) (models.PartitionID, error) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	partitions, exists := hr.topology[topic]
	if !exists {
		return 0, fmt.Errorf("topic %s not found in routing topology", topic)
	}

	if len(partitions) == 0 {
		return 0, fmt.Errorf("no partitions available for topic %s", topic)
	}

	// Simple hash modulo
	h := fnv.New64a()
	h.Write([]byte(partitionKey))
	hashValue := h.Sum64()

	partitionIndex := int(hashValue % uint64(len(partitions)))
	selectedPartition := partitions[partitionIndex]

	hr.routingCalls++
	return selectedPartition, nil
}

// GetPartitions returns all partition IDs for a topic (HashRouter)
func (hr *HashRouter) GetPartitions(topic string) ([]models.PartitionID, error) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	partitions, exists := hr.topology[topic]
	if !exists {
		return nil, fmt.Errorf("topic %s not found in routing topology", topic)
	}

	result := make([]models.PartitionID, len(partitions))
	copy(result, partitions)
	return result, nil
}

// AddPartition adds a partition (HashRouter)
func (hr *HashRouter) AddPartition(topic string, partitionID models.PartitionID) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	partitions := hr.topology[topic]
	partitions = append(partitions, partitionID)
	hr.topology[topic] = partitions
	return nil
}

// RemovePartition removes a partition (HashRouter)
func (hr *HashRouter) RemovePartition(topic string, partitionID models.PartitionID) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	partitions, exists := hr.topology[topic]
	if !exists {
		return fmt.Errorf("topic %s not found", topic)
	}

	for i, p := range partitions {
		if p == partitionID {
			hr.topology[topic] = append(partitions[:i], partitions[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("partition %d not found", partitionID)
}

// UpdatePartitions updates partitions (HashRouter)
func (hr *HashRouter) UpdatePartitions(topic string, partitions []models.PartitionID) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	newPartitions := make([]models.PartitionID, len(partitions))
	copy(newPartitions, partitions)
	hr.topology[topic] = newPartitions
	return nil
}

// GetTopology returns topology (HashRouter)
func (hr *HashRouter) GetTopology() map[string][]models.PartitionID {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	topology := make(map[string][]models.PartitionID)
	for topic, partitions := range hr.topology {
		topicPartitions := make([]models.PartitionID, len(partitions))
		copy(topicPartitions, partitions)
		topology[topic] = topicPartitions
	}
	return topology
}

// GetStats returns stats (HashRouter)
func (hr *HashRouter) GetStats() RoutingStats {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	return RoutingStats{
		RoutingCalls:    hr.routingCalls,
		TopologyChanges: 0, // HashRouter doesn't track topology changes
		TopicCount:      len(hr.topology),
		TopicPartitions: make(map[string]int),
	}
}

// RoutingStats holds statistics about partition routing
type RoutingStats struct {
	RoutingCalls    uint64         `json:"routing_calls"`
	TopologyChanges uint64         `json:"topology_changes"`
	TopicCount      int            `json:"topic_count"`
	TopicPartitions map[string]int `json:"topic_partitions"` // topic -> partition count
}

// PartitionKeyGenerator provides utilities for generating partition keys
type PartitionKeyGenerator struct{}

// GenerateKey generates a partition key for a message
func (pkg *PartitionKeyGenerator) GenerateKey(msg *models.Message) string {
	// Default: use message topic as partition key (all messages for a topic go to same partition)
	// In practice, this could be customized based on:
	// - Producer ID (for ordering per producer)
	// - Message attributes (for custom sharding)
	// - Hash of payload (for even distribution)

	return msg.Topic
}

// GenerateKeyFromProducer generates a partition key based on producer ID
func (pkg *PartitionKeyGenerator) GenerateKeyFromProducer(topic string, producerID string) string {
	if producerID == "" {
		return topic // Fallback to topic-based partitioning
	}

	return fmt.Sprintf("%s:%s", topic, producerID)
}

// GenerateKeyFromPayload generates a partition key by hashing the payload
func (pkg *PartitionKeyGenerator) GenerateKeyFromPayload(topic string, payload []byte) string {
	h := fnv.New64a()
	h.Write(payload)
	return fmt.Sprintf("%s:%016x", topic, h.Sum64())
}
