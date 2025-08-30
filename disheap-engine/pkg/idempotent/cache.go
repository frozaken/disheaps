package idempotent

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
)

// DeduplicationCache implements an in-memory LRU cache for deduplication entries
// with sliding window sequence tracking per producer
type DeduplicationCache struct {
	mu sync.RWMutex

	// Configuration
	maxMemoryMB int
	windowSize  uint64
	maxEntries  int

	// LRU list and map for cache entries
	lru     *list.List
	entries map[string]*cacheEntry

	// Statistics
	hits      uint64
	misses    uint64
	evictions uint64

	// Producer sequence windows
	producers map[string]*ProducerWindow
}

// cacheEntry represents a single cache entry
type cacheEntry struct {
	key       string
	messageID models.MessageID
	timestamp time.Time
	element   *list.Element // Reference to LRU list element
}

// ProducerWindow tracks sequence numbers for a specific producer within a sliding window
type ProducerWindow struct {
	producerKey models.ProducerKey
	topic       string
	sequences   map[uint64]models.MessageID // sequence -> message_id
	minSeq      uint64                      // Minimum sequence in window
	maxSeq      uint64                      // Maximum sequence in window
	lastAccess  time.Time
}

// NewDeduplicationCache creates a new deduplication cache
func NewDeduplicationCache(maxMemoryMB int, windowSize uint64) *DeduplicationCache {
	// Estimate max entries based on memory limit
	// Rough estimate: 100 bytes per entry (key + message_id + overhead)
	avgEntrySize := 100
	maxEntries := (maxMemoryMB * 1024 * 1024) / avgEntrySize

	return &DeduplicationCache{
		maxMemoryMB: maxMemoryMB,
		windowSize:  windowSize,
		maxEntries:  maxEntries,
		lru:         list.New(),
		entries:     make(map[string]*cacheEntry),
		producers:   make(map[string]*ProducerWindow),
	}
}

// Check checks if a sequence exists in the cache
func (c *DeduplicationCache) Check(topic string, producerKey models.ProducerKey, sequence uint64) (models.MessageID, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := c.buildKey(topic, producerKey, sequence)
	entry, exists := c.entries[key]
	if !exists {
		c.misses++
		return "", false
	}

	// Move to front of LRU list (mark as recently used)
	c.lru.MoveToFront(entry.element)
	c.hits++

	return entry.messageID, true
}

// Store stores a sequence and message ID in the cache
func (c *DeduplicationCache) Store(topic string, producerKey models.ProducerKey, sequence uint64, messageID models.MessageID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.buildKey(topic, producerKey, sequence)

	// Check if entry already exists
	if entry, exists := c.entries[key]; exists {
		// Update existing entry
		entry.messageID = messageID
		entry.timestamp = time.Now()
		c.lru.MoveToFront(entry.element)
		return
	}

	// Ensure we have space for new entry
	c.evictIfNeeded()

	// Create new entry
	entry := &cacheEntry{
		key:       key,
		messageID: messageID,
		timestamp: time.Now(),
	}

	// Add to LRU list
	element := c.lru.PushFront(entry)
	entry.element = element

	// Add to map
	c.entries[key] = entry

	// Update producer window
	c.updateProducerWindow(topic, producerKey, sequence, messageID)
}

// updateProducerWindow updates the sliding window for a producer
func (c *DeduplicationCache) updateProducerWindow(topic string, producerKey models.ProducerKey, sequence uint64, messageID models.MessageID) {
	windowKey := c.buildProducerKey(topic, producerKey)

	window, exists := c.producers[windowKey]
	if !exists {
		window = &ProducerWindow{
			producerKey: producerKey,
			topic:       topic,
			sequences:   make(map[uint64]models.MessageID),
			minSeq:      sequence,
			maxSeq:      sequence,
			lastAccess:  time.Now(),
		}
		c.producers[windowKey] = window
	}

	window.sequences[sequence] = messageID
	window.lastAccess = time.Now()

	// Update min/max sequence numbers
	if sequence < window.minSeq {
		window.minSeq = sequence
	}
	if sequence > window.maxSeq {
		window.maxSeq = sequence
	}

	// Enforce sliding window size
	c.enforceWindowSize(window)
}

// enforceWindowSize ensures the producer window doesn't exceed the configured size
func (c *DeduplicationCache) enforceWindowSize(window *ProducerWindow) {
	if uint64(len(window.sequences)) <= c.windowSize {
		return
	}

	// Remove oldest sequences (lowest sequence numbers)
	currentSize := uint64(len(window.sequences))
	toRemove := currentSize - c.windowSize

	// Find sequences to remove (starting from minSeq)
	removed := uint64(0)
	newMinSeq := window.minSeq

	for seq := window.minSeq; removed < toRemove && seq <= window.maxSeq; seq++ {
		if _, exists := window.sequences[seq]; exists {
			// Remove from cache entries as well
			key := c.buildKey(window.topic, window.producerKey, seq)
			if entry, exists := c.entries[key]; exists {
				c.lru.Remove(entry.element)
				delete(c.entries, key)
			}

			delete(window.sequences, seq)
			removed++
			newMinSeq = seq + 1
		}
	}

	window.minSeq = newMinSeq
}

// evictIfNeeded evicts entries if the cache is at capacity
func (c *DeduplicationCache) evictIfNeeded() {
	for len(c.entries) >= c.maxEntries {
		c.evictLRU()
	}
}

// evictLRU evicts the least recently used entry
func (c *DeduplicationCache) evictLRU() {
	element := c.lru.Back()
	if element == nil {
		return
	}

	entry := element.Value.(*cacheEntry)
	c.lru.Remove(element)
	delete(c.entries, entry.key)
	c.evictions++

	// Update producer window if needed
	topic, producerKey, sequence, err := c.parseKey(entry.key)
	if err == nil {
		c.removeFromProducerWindow(topic, producerKey, sequence)
	}
}

// removeFromProducerWindow removes a sequence from a producer window
func (c *DeduplicationCache) removeFromProducerWindow(topic string, producerKey models.ProducerKey, sequence uint64) {
	windowKey := c.buildProducerKey(topic, producerKey)
	window, exists := c.producers[windowKey]
	if !exists {
		return
	}

	delete(window.sequences, sequence)

	// If window is empty, remove it
	if len(window.sequences) == 0 {
		delete(c.producers, windowKey)
		return
	}

	// Recalculate min/max sequences
	window.minSeq = uint64(^uint64(0)) // max uint64
	window.maxSeq = 0

	for seq := range window.sequences {
		if seq < window.minSeq {
			window.minSeq = seq
		}
		if seq > window.maxSeq {
			window.maxSeq = seq
		}
	}
}

// EvictExpired removes expired entries based on timestamp
func (c *DeduplicationCache) EvictExpired() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	cutoff := time.Now().Add(-24 * time.Hour) // Default TTL from config
	evicted := 0

	// Walk through LRU list from back (oldest entries)
	for element := c.lru.Back(); element != nil; {
		entry := element.Value.(*cacheEntry)
		if entry.timestamp.After(cutoff) {
			break // Entries are ordered by access time
		}

		// Remove this entry
		prev := element.Prev()
		c.lru.Remove(element)
		delete(c.entries, entry.key)
		evicted++
		c.evictions++

		// Update producer window
		topic, producerKey, sequence, err := c.parseKey(entry.key)
		if err == nil {
			c.removeFromProducerWindow(topic, producerKey, sequence)
		}

		element = prev
	}

	// Also evict old producer windows
	producerEvicted := c.evictOldProducers(cutoff)

	return evicted + producerEvicted
}

// evictOldProducers removes producer windows that haven't been accessed recently
func (c *DeduplicationCache) evictOldProducers(cutoff time.Time) int {
	evicted := 0

	for key, window := range c.producers {
		if window.lastAccess.Before(cutoff) {
			delete(c.producers, key)
			evicted++
		}
	}

	return evicted
}

// GetStats returns cache statistics
func (c *DeduplicationCache) GetStats() *CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &CacheStats{
		Hits:       c.hits,
		Misses:     c.misses,
		Size:       uint64(len(c.entries)),
		Evictions:  c.evictions,
		MaxSize:    uint64(c.maxEntries),
		Producers:  uint64(len(c.producers)),
		WindowSize: c.windowSize,
	}
}

// CacheStats holds cache performance statistics
type CacheStats struct {
	Hits       uint64 `json:"hits"`
	Misses     uint64 `json:"misses"`
	Size       uint64 `json:"size"`
	Evictions  uint64 `json:"evictions"`
	MaxSize    uint64 `json:"max_size"`
	Producers  uint64 `json:"producers"`
	WindowSize uint64 `json:"window_size"`
}

// UpdateConfig updates the cache configuration
func (c *DeduplicationCache) UpdateConfig(maxMemoryMB int, windowSize uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxMemoryMB = maxMemoryMB
	c.windowSize = windowSize

	// Recalculate max entries
	avgEntrySize := 100
	newMaxEntries := (maxMemoryMB * 1024 * 1024) / avgEntrySize
	c.maxEntries = newMaxEntries

	// Evict if we're over the new limit
	for len(c.entries) > c.maxEntries {
		c.evictLRU()
	}

	// Update all producer windows to enforce new window size
	for _, window := range c.producers {
		c.enforceWindowSize(window)
	}
}

// Clear removes all entries from the cache
func (c *DeduplicationCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lru.Init()
	c.entries = make(map[string]*cacheEntry)
	c.producers = make(map[string]*ProducerWindow)
	c.hits = 0
	c.misses = 0
	c.evictions = 0
}

// buildKey builds a cache key from topic, producer key, and sequence
func (c *DeduplicationCache) buildKey(topic string, producerKey models.ProducerKey, sequence uint64) string {
	return fmt.Sprintf("%s:%s:%d:%d", topic, producerKey.ID, producerKey.Epoch, sequence)
}

// buildProducerKey builds a key for producer window tracking
func (c *DeduplicationCache) buildProducerKey(topic string, producerKey models.ProducerKey) string {
	return fmt.Sprintf("%s:%s:%d", topic, producerKey.ID, producerKey.Epoch)
}

// parseKey parses a cache key back into its components
func (c *DeduplicationCache) parseKey(key string) (topic string, producerKey models.ProducerKey, sequence uint64, err error) {
	var producerID string
	var epoch uint64

	n, parseErr := fmt.Sscanf(key, "%s:%s:%d:%d", &topic, &producerID, &epoch, &sequence)
	if n != 4 || parseErr != nil {
		return "", models.ProducerKey{}, 0, fmt.Errorf("invalid cache key format: %s", key)
	}

	producerKey = models.ProducerKey{
		ID:    producerID,
		Epoch: epoch,
	}

	return topic, producerKey, sequence, nil
}

// GetProducerWindows returns information about current producer windows
func (c *DeduplicationCache) GetProducerWindows() map[string]*ProducerWindowInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	windows := make(map[string]*ProducerWindowInfo)

	for key, window := range c.producers {
		windows[key] = &ProducerWindowInfo{
			Topic:         window.topic,
			ProducerID:    window.producerKey.ID,
			Epoch:         window.producerKey.Epoch,
			SequenceCount: len(window.sequences),
			MinSequence:   window.minSeq,
			MaxSequence:   window.maxSeq,
			LastAccess:    window.lastAccess,
		}
	}

	return windows
}

// ProducerWindowInfo contains information about a producer window
type ProducerWindowInfo struct {
	Topic         string    `json:"topic"`
	ProducerID    string    `json:"producer_id"`
	Epoch         uint64    `json:"epoch"`
	SequenceCount int       `json:"sequence_count"`
	MinSequence   uint64    `json:"min_sequence"`
	MaxSequence   uint64    `json:"max_sequence"`
	LastAccess    time.Time `json:"last_access"`
}
