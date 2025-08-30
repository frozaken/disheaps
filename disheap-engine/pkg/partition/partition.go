package partition

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/heap"
	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Partition interface defines the core operations for a single partition
// Following ENGINE_IMPLEMENTATION_PLAN.md interface contract
type Partition interface {
	// Message operations
	Enqueue(ctx context.Context, msg *models.Message) error
	Pop(ctx context.Context, holder string, timeout time.Duration) (*models.Message, error)
	Ack(ctx context.Context, msgID models.MessageID, token models.LeaseToken) error
	Nack(ctx context.Context, msgID models.MessageID, token models.LeaseToken, backoff *time.Duration) error

	// Lease management
	ExtendLease(ctx context.Context, token models.LeaseToken, extension time.Duration) error

	// Admin operations
	Peek(ctx context.Context, limit int) ([]*models.Message, error)
	Stats() models.PartitionStats

	// Spine index integration
	GetCandidates(count int) []*models.Message

	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	IsHealthy() bool
	Throttle(reason string)
	Unthrottle()
}

// LocalPartition implements a single partition with heap operations
type LocalPartition struct {
	// Configuration
	partitionID models.PartitionID
	topic       string
	config      *models.TopicConfig

	// Core components
	mainHeap      heap.Heap
	timerHeap     heap.TimerHeap
	scheduler     *heap.MessageScheduler
	storage       storage.Storage
	leaseRegistry *models.LeaseRegistry

	// Observability (following INTERFACE_CONTRACTS.md)
	logger  *zap.Logger
	meter   metric.Meter
	metrics *PartitionMetrics

	// State management
	mu             sync.RWMutex
	isRunning      bool
	isHealthy      bool
	isThrottled    bool
	throttleReason string

	// Background context
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Statistics
	stats models.PartitionStats
}

// PartitionMetrics holds OpenTelemetry metrics for observability
type PartitionMetrics struct {
	// Core operation metrics (from INTERFACE_CONTRACTS.md)
	EnqueueLatency metric.Float64Histogram
	PopLatency     metric.Float64Histogram
	Inflight       metric.Int64UpDownCounter
	Backlog        metric.Int64UpDownCounter
	Retries        metric.Int64Counter
	DLQTotal       metric.Int64Counter
	LeaseTimeouts  metric.Int64Counter

	// Additional partition metrics
	ThrottledOperations metric.Int64Counter
	HealthChecks        metric.Int64Counter
}

// PartitionConfig holds partition configuration
type PartitionConfig struct {
	// Basic settings
	PartitionID models.PartitionID
	Topic       string
	TopicConfig *models.TopicConfig

	// Component settings
	Storage       storage.Storage
	CheckInterval time.Duration

	// Lease settings
	DefaultLeaseTimeout time.Duration
	MaxLeaseExtensions  int

	// Health and throttling
	HealthCheckInterval time.Duration
	MaxBacklogSize      int
	ThrottleThreshold   int
}

// NewLocalPartition creates a new local partition instance
func NewLocalPartition(config *PartitionConfig, logger *zap.Logger) (*LocalPartition, error) {
	if config == nil {
		return nil, fmt.Errorf("partition config cannot be nil")
	}

	if config.TopicConfig == nil {
		return nil, fmt.Errorf("topic config cannot be nil")
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create heaps
	mainHeap := heap.NewBinaryHeap(config.TopicConfig.Mode)
	timerHeap := heap.NewBinaryTimerHeap(logger)
	scheduler := heap.NewMessageScheduler(timerHeap, mainHeap, logger)

	// Set scheduler check interval
	if config.CheckInterval > 0 {
		scheduler.SetCheckInterval(config.CheckInterval)
	}

	// Create lease registry
	leaseRegistry := models.NewLeaseRegistry(logger)

	// Set up metrics
	meter := otel.Meter("disheap.engine.partition")
	metrics, err := setupMetrics(meter)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to setup metrics: %w", err)
	}

	partition := &LocalPartition{
		partitionID:   config.PartitionID,
		topic:         config.Topic,
		config:        config.TopicConfig,
		mainHeap:      mainHeap,
		timerHeap:     timerHeap,
		scheduler:     scheduler,
		storage:       config.Storage,
		leaseRegistry: leaseRegistry,
		isHealthy:     true, // Healthy when properly initialized
		logger: logger.With(
			zap.String("component", "partition"),
			zap.String("topic", config.Topic),
			zap.Uint32("partition_id", uint32(config.PartitionID)),
		),
		meter:   meter,
		metrics: metrics,
		ctx:     ctx,
		cancel:  cancel,
		stats: models.PartitionStats{
			Topic:       config.Topic,
			PartitionID: config.PartitionID,
			LastUpdated: time.Now(),
		},
	}

	return partition, nil
}

// Start initializes and starts the partition
func (p *LocalPartition) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isRunning {
		return fmt.Errorf("partition is already running")
	}

	// Start scheduler
	p.scheduler.Start()

	// Start background tasks
	p.wg.Add(1)
	go p.backgroundTasks()

	p.isRunning = true

	p.logger.Info("Partition started",
		zap.String("topic", p.topic),
		zap.Uint32("partition_id", uint32(p.partitionID)),
		zap.String("heap_mode", p.config.Mode.String()),
	)

	return nil
}

// Stop gracefully stops the partition
func (p *LocalPartition) Stop(ctx context.Context) error {
	p.mu.Lock()
	if !p.isRunning {
		p.mu.Unlock()
		return nil
	}
	p.isRunning = false
	p.mu.Unlock()

	// Stop scheduler
	p.scheduler.Stop()

	// Stop background tasks
	p.cancel()
	p.wg.Wait()

	p.logger.Info("Partition stopped",
		zap.String("topic", p.topic),
		zap.Uint32("partition_id", uint32(p.partitionID)),
	)

	return nil
}

// Enqueue adds a message to the partition
func (p *LocalPartition) Enqueue(ctx context.Context, msg *models.Message) error {
	if !p.checkHealthy() {
		return fmt.Errorf("partition is not healthy")
	}

	if p.isThrottled {
		p.metrics.ThrottledOperations.Add(ctx, 1,
			metric.WithAttributes(attribute.String("operation", "enqueue")))
		return fmt.Errorf("partition is throttled: %s", p.throttleReason)
	}

	start := time.Now()
	defer func() {
		duration := float64(time.Since(start).Nanoseconds()) / 1e6
		p.metrics.EnqueueLatency.Record(ctx, duration,
			metric.WithAttributes(attribute.String("topic", p.topic)))
	}()

	// Validate message
	maxPayload := p.config.MaxPayloadBytes
	if maxPayload == 0 {
		maxPayload = 1024 * 1024 // Default 1MB
	}
	if err := msg.Validate(maxPayload); err != nil {
		return fmt.Errorf("message validation failed: %w", err)
	}

	// Store message for durability
	if err := p.storage.StoreMessage(ctx, msg); err != nil {
		p.logger.Error("Failed to store message",
			zap.String("message_id", string(msg.ID)),
			zap.Error(err),
		)
		return fmt.Errorf("failed to store message: %w", err)
	}

	// Check if message should be delayed
	if msg.NotBefore != nil && msg.NotBefore.After(time.Now()) {
		// Schedule for later processing
		if err := p.timerHeap.Schedule(msg, *msg.NotBefore); err != nil {
			p.logger.Error("Failed to schedule delayed message",
				zap.String("message_id", string(msg.ID)),
				zap.Time("not_before", *msg.NotBefore),
				zap.Error(err),
			)
			return fmt.Errorf("failed to schedule delayed message: %w", err)
		}

		// Trigger scheduler check to optimize timing
		p.scheduler.TriggerCheck()

		p.logger.Debug("Message scheduled for delayed processing",
			zap.String("message_id", string(msg.ID)),
			zap.Time("not_before", *msg.NotBefore),
			zap.Duration("delay", time.Until(*msg.NotBefore)),
		)
	} else {
		// Add directly to main heap
		if err := p.mainHeap.Insert(msg); err != nil {
			p.logger.Error("Failed to insert message into heap",
				zap.String("message_id", string(msg.ID)),
				zap.Error(err),
			)
			return fmt.Errorf("failed to insert message into heap: %w", err)
		}

		p.logger.Debug("Message enqueued to main heap",
			zap.String("message_id", string(msg.ID)),
			zap.Int64("priority", msg.Priority),
		)
	}

	// Update metrics and stats
	p.metrics.Backlog.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("topic", p.topic),
			attribute.String("partition", fmt.Sprintf("%d", p.partitionID))))

	p.mu.Lock()
	p.stats.MessageCount++
	p.stats.LastUpdated = time.Now()
	p.mu.Unlock()

	return nil
}

// setupMetrics initializes OpenTelemetry metrics
func setupMetrics(meter metric.Meter) (*PartitionMetrics, error) {
	enqueueLatency, err := meter.Float64Histogram(
		"enqueue_latency_ms",
		metric.WithDescription("Message enqueue latency in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	popLatency, err := meter.Float64Histogram(
		"pop_latency_ms",
		metric.WithDescription("Message pop latency in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	inflight, err := meter.Int64UpDownCounter(
		"inflight",
		metric.WithDescription("Number of messages currently leased"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	backlog, err := meter.Int64UpDownCounter(
		"backlog",
		metric.WithDescription("Number of messages waiting in partition"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	retries, err := meter.Int64Counter(
		"retries_total",
		metric.WithDescription("Total number of message retries"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	dlqTotal, err := meter.Int64Counter(
		"dlq_total",
		metric.WithDescription("Total number of messages sent to DLQ"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	leaseTimeouts, err := meter.Int64Counter(
		"lease_timeouts_total",
		metric.WithDescription("Total number of lease timeouts"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	throttledOps, err := meter.Int64Counter(
		"throttled_operations_total",
		metric.WithDescription("Total number of throttled operations"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	healthChecks, err := meter.Int64Counter(
		"health_checks_total",
		metric.WithDescription("Total number of health checks performed"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	return &PartitionMetrics{
		EnqueueLatency:      enqueueLatency,
		PopLatency:          popLatency,
		Inflight:            inflight,
		Backlog:             backlog,
		Retries:             retries,
		DLQTotal:            dlqTotal,
		LeaseTimeouts:       leaseTimeouts,
		ThrottledOperations: throttledOps,
		HealthChecks:        healthChecks,
	}, nil
}

// checkHealthy verifies partition health
func (p *LocalPartition) checkHealthy() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isRunning && p.isHealthy
}

// backgroundTasks runs periodic maintenance tasks
func (p *LocalPartition) backgroundTasks() {
	defer p.wg.Done()

	ticker := time.NewTicker(30 * time.Second) // Health check interval
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.performHealthCheck()
		}
	}
}

// performHealthCheck checks partition health
func (p *LocalPartition) performHealthCheck() {
	ctx := context.Background()
	p.metrics.HealthChecks.Add(ctx, 1)

	// Check if backlog is too large
	backlogSize := p.mainHeap.Size() + p.timerHeap.Size()

	p.mu.Lock()
	p.stats.MessageCount = uint64(backlogSize)
	p.stats.InflightCount = uint64(p.leaseRegistry.ActiveLeases())
	p.stats.LastUpdated = time.Now()
	p.mu.Unlock()

	p.logger.Debug("Health check completed",
		zap.Int("backlog_size", backlogSize),
		zap.Int("active_leases", p.leaseRegistry.ActiveLeases()),
		zap.Bool("is_healthy", p.isHealthy),
		zap.Bool("is_throttled", p.isThrottled),
	)
}
