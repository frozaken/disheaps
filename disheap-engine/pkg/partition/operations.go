package partition

import (
	"context"
	"fmt"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Pop retrieves the next message from the partition with a lease
func (p *LocalPartition) Pop(ctx context.Context, holder string, timeout time.Duration) (*models.Message, error) {
	if !p.checkHealthy() {
		return nil, fmt.Errorf("partition is not healthy")
	}

	if p.isThrottled {
		p.metrics.ThrottledOperations.Add(ctx, 1,
			metric.WithAttributes(attribute.String("operation", "pop")))
		return nil, fmt.Errorf("partition is throttled: %s", p.throttleReason)
	}

	start := time.Now()
	defer func() {
		duration := float64(time.Since(start).Nanoseconds()) / 1e6
		p.metrics.PopLatency.Record(ctx, duration,
			metric.WithAttributes(attribute.String("topic", p.topic)))
	}()

	// Extract message from main heap
	msg, err := p.mainHeap.Extract()
	if err != nil {
		// No messages available
		return nil, nil
	}

	// Create lease for the message
	leaseTimeout := p.config.VisibilityTimeoutDefault
	if leaseTimeout == 0 {
		leaseTimeout = 30 * time.Second // Default visibility timeout
	}

	lease := models.NewLease(msg.ID, holder, leaseTimeout)

	// Grant lease in registry
	if err := p.leaseRegistry.Grant(ctx, lease); err != nil {
		// Failed to grant lease - put message back in heap
		if insertErr := p.mainHeap.Insert(msg); insertErr != nil {
			p.logger.Error("Failed to reinsert message after lease failure",
				zap.String("message_id", string(msg.ID)),
				zap.Error(insertErr),
			)
		}
		return nil, fmt.Errorf("failed to grant lease: %w", err)
	}

	// Store lease for durability
	if err := p.storage.StoreLease(ctx, p.topic, uint32(p.partitionID), lease); err != nil {
		p.logger.Error("Failed to store lease",
			zap.String("message_id", string(msg.ID)),
			zap.String("lease_token", string(lease.Token)),
			zap.Error(err),
		)
		// Continue - lease is granted in memory, storage failure is non-fatal
	}

	// Set lease in message
	msg.Lease = &models.MessageLease{
		Token:      lease.Token,
		Holder:     lease.Holder,
		GrantedAt:  lease.GrantedAt,
		Deadline:   lease.Deadline,
		Extensions: 0,
	}

	// Update metrics
	p.metrics.Inflight.Add(ctx, 1,
		metric.WithAttributes(attribute.String("topic", p.topic)))
	p.metrics.Backlog.Add(ctx, -1,
		metric.WithAttributes(
			attribute.String("topic", p.topic),
			attribute.String("partition", fmt.Sprintf("%d", p.partitionID))))

	// Update stats
	p.mu.Lock()
	p.stats.InflightCount++
	p.stats.LastUpdated = time.Now()
	p.mu.Unlock()

	p.logger.Debug("Message popped with lease",
		zap.String("message_id", string(msg.ID)),
		zap.String("holder", holder),
		zap.String("lease_token", string(lease.Token)),
		zap.Duration("lease_timeout", leaseTimeout),
	)

	return msg, nil
}

// Ack acknowledges a message, removing it permanently
func (p *LocalPartition) Ack(ctx context.Context, msgID models.MessageID, token models.LeaseToken) error {
	if !p.checkHealthy() {
		return fmt.Errorf("partition is not healthy")
	}

	// Verify and revoke lease
	lease, err := p.leaseRegistry.Revoke(ctx, msgID, token)
	if err != nil {
		return fmt.Errorf("failed to revoke lease: %w", err)
	}

	// Delete message from storage
	if err := p.storage.DeleteMessage(ctx, p.topic, uint32(p.partitionID), msgID); err != nil {
		p.logger.Error("Failed to delete acknowledged message from storage",
			zap.String("message_id", string(msgID)),
			zap.Error(err),
		)
		// Continue - message is acked in memory
	}

	// Delete lease from storage
	if err := p.storage.DeleteLease(ctx, p.topic, uint32(p.partitionID), msgID); err != nil {
		p.logger.Error("Failed to delete lease from storage",
			zap.String("message_id", string(msgID)),
			zap.Error(err),
		)
	}

	// Update metrics
	p.metrics.Inflight.Add(ctx, -1,
		metric.WithAttributes(attribute.String("topic", p.topic)))

	// Update stats
	p.mu.Lock()
	p.stats.InflightCount--
	p.stats.AckedCount++
	p.stats.LastUpdated = time.Now()
	p.mu.Unlock()

	p.logger.Debug("Message acknowledged",
		zap.String("message_id", string(msgID)),
		zap.String("holder", lease.Holder),
		zap.Duration("lease_duration", time.Since(lease.GrantedAt)),
	)

	return nil
}

// Nack negative-acknowledges a message, returning it to the queue
func (p *LocalPartition) Nack(ctx context.Context, msgID models.MessageID, token models.LeaseToken, backoff *time.Duration) error {
	if !p.checkHealthy() {
		return fmt.Errorf("partition is not healthy")
	}

	// Verify and revoke lease
	_, err := p.leaseRegistry.Revoke(ctx, msgID, token)
	if err != nil {
		return fmt.Errorf("failed to revoke lease: %w", err)
	}

	// Get message from storage
	msg, err := p.storage.GetMessage(ctx, p.topic, uint32(p.partitionID), msgID)
	if err != nil {
		return fmt.Errorf("failed to get message for nack: %w", err)
	}

	// Increment retry count
	msg.RetryCount++

	// Check if message should go to DLQ
	if p.shouldMoveToDLQ(msg) {
		if err := p.moveToDLQ(ctx, msg, "max_retries_exceeded"); err != nil {
			p.logger.Error("Failed to move message to DLQ",
				zap.String("message_id", string(msgID)),
				zap.Error(err),
			)
			return fmt.Errorf("failed to move message to DLQ: %w", err)
		}

		p.metrics.DLQTotal.Add(ctx, 1,
			metric.WithAttributes(attribute.String("topic", p.topic)))

		p.mu.Lock()
		p.stats.DLQCount++
		p.mu.Unlock()

		p.logger.Info("Message moved to DLQ after max retries",
			zap.String("message_id", string(msgID)),
			zap.Int32("retry_count", msg.RetryCount),
		)

		return nil
	}

	// Update message in storage with new retry count
	if err := p.storage.StoreMessage(ctx, msg); err != nil {
		p.logger.Error("Failed to update message retry count",
			zap.String("message_id", string(msgID)),
			zap.Error(err),
		)
	}

	// Calculate backoff time
	var scheduleTime time.Time
	if backoff != nil {
		scheduleTime = time.Now().Add(*backoff)
	} else {
		// Calculate exponential backoff with jitter
		scheduleTime = p.calculateBackoffTime(msg.RetryCount)
	}

	// Schedule message for retry
	if scheduleTime.After(time.Now()) {
		// Add to timer heap for delayed retry
		if err := p.timerHeap.Schedule(msg, scheduleTime); err != nil {
			p.logger.Error("Failed to schedule message for retry",
				zap.String("message_id", string(msgID)),
				zap.Time("schedule_time", scheduleTime),
				zap.Error(err),
			)
			return fmt.Errorf("failed to schedule message for retry: %w", err)
		}

		p.scheduler.TriggerCheck()

		p.logger.Debug("Message scheduled for retry",
			zap.String("message_id", string(msgID)),
			zap.Int32("retry_count", msg.RetryCount),
			zap.Time("schedule_time", scheduleTime),
			zap.Duration("backoff", time.Until(scheduleTime)),
		)
	} else {
		// Add back to main heap immediately
		if err := p.mainHeap.Insert(msg); err != nil {
			p.logger.Error("Failed to reinsert message into heap",
				zap.String("message_id", string(msgID)),
				zap.Error(err),
			)
			return fmt.Errorf("failed to reinsert message: %w", err)
		}

		p.logger.Debug("Message returned to main heap",
			zap.String("message_id", string(msgID)),
			zap.Int32("retry_count", msg.RetryCount),
		)
	}

	// Delete lease from storage
	if err := p.storage.DeleteLease(ctx, p.topic, uint32(p.partitionID), msgID); err != nil {
		p.logger.Error("Failed to delete lease from storage",
			zap.String("message_id", string(msgID)),
			zap.Error(err),
		)
	}

	// Update metrics
	p.metrics.Inflight.Add(ctx, -1,
		metric.WithAttributes(attribute.String("topic", p.topic)))
	p.metrics.Retries.Add(ctx, 1,
		metric.WithAttributes(attribute.String("topic", p.topic)))

	if scheduleTime.After(time.Now()) {
		// Message went to timer heap
		p.metrics.Backlog.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("topic", p.topic),
				attribute.String("partition", fmt.Sprintf("%d", p.partitionID))))
	} else {
		// Message went back to main heap
		p.metrics.Backlog.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("topic", p.topic),
				attribute.String("partition", fmt.Sprintf("%d", p.partitionID))))
	}

	// Update stats
	p.mu.Lock()
	p.stats.InflightCount--
	p.stats.NackedCount++
	p.stats.LastUpdated = time.Now()
	p.mu.Unlock()

	return nil
}

// ExtendLease extends the lease duration for a message
func (p *LocalPartition) ExtendLease(ctx context.Context, token models.LeaseToken, extension time.Duration) error {
	if !p.checkHealthy() {
		return fmt.Errorf("partition is not healthy")
	}

	// Extend lease in registry
	lease, err := p.leaseRegistry.Extend(ctx, token, extension)
	if err != nil {
		return fmt.Errorf("failed to extend lease: %w", err)
	}

	// Update lease in storage
	if err := p.storage.StoreLease(ctx, p.topic, uint32(p.partitionID), lease); err != nil {
		p.logger.Error("Failed to update lease in storage",
			zap.String("message_id", string(lease.MessageID)),
			zap.String("lease_token", string(lease.Token)),
			zap.Error(err),
		)
		// Continue - lease is extended in memory
	}

	p.logger.Debug("Lease extended",
		zap.String("message_id", string(lease.MessageID)),
		zap.String("holder", lease.Holder),
		zap.Duration("extension", extension),
		zap.Time("new_deadline", lease.Deadline),
	)

	return nil
}

// Peek returns the top messages without removing them
func (p *LocalPartition) Peek(ctx context.Context, limit int) ([]*models.Message, error) {
	if !p.checkHealthy() {
		return nil, fmt.Errorf("partition is not healthy")
	}

	if limit <= 0 {
		limit = 10 // Default limit
	}

	// Get top N messages from main heap
	messages := p.mainHeap.TopN(limit)

	p.logger.Debug("Peeked at messages",
		zap.Int("requested", limit),
		zap.Int("returned", len(messages)),
	)

	return messages, nil
}

// Stats returns current partition statistics
func (p *LocalPartition) Stats() models.PartitionStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Update real-time stats
	stats := p.stats
	stats.MessageCount = uint64(p.mainHeap.Size() + p.timerHeap.Size())
	stats.InflightCount = uint64(p.leaseRegistry.ActiveLeases())
	stats.IsHealthy = p.isHealthy
	stats.IsThrottled = p.isThrottled
	if p.isThrottled {
		stats.ThrottleReason = p.throttleReason
	}
	stats.LastUpdated = time.Now()

	return stats
}

// GetCandidates returns messages for spine index selection
func (p *LocalPartition) GetCandidates(count int) []*models.Message {
	if count <= 0 {
		return nil
	}

	return p.mainHeap.TopN(count)
}

// IsHealthy returns the current health status
func (p *LocalPartition) IsHealthy() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isHealthy
}

// Throttle enables throttling with a reason
func (p *LocalPartition) Throttle(reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.isThrottled = true
	p.throttleReason = reason

	p.logger.Warn("Partition throttled",
		zap.String("reason", reason),
	)
}

// Unthrottle disables throttling
func (p *LocalPartition) Unthrottle() {
	p.mu.Lock()
	defer p.mu.Unlock()

	wasThrottled := p.isThrottled
	p.isThrottled = false
	p.throttleReason = ""

	if wasThrottled {
		p.logger.Info("Partition throttling removed")
	}
}

// shouldMoveToDLQ determines if a message should be moved to DLQ
func (p *LocalPartition) shouldMoveToDLQ(msg *models.Message) bool {
	if p.config.DLQPolicy.Type == models.DLQPolicyNone {
		return false
	}

	switch p.config.DLQPolicy.Type {
	case models.DLQPolicyMaxRetries:
		return msg.RetryCount >= p.config.DLQPolicy.MaxRetries
	case models.DLQPolicyTTL:
		return time.Since(msg.EnqueuedAt) > p.config.DLQPolicy.MaxTTL
	case models.DLQPolicyNone:
		return false
	default:
		return false
	}
}

// moveToDLQ moves a message to the dead letter queue
func (p *LocalPartition) moveToDLQ(ctx context.Context, msg *models.Message, reason string) error {
	// For now, we'll delete the message and log it
	// In a full implementation, this would move to a separate DLQ topic

	if err := p.storage.DeleteMessage(ctx, p.topic, uint32(p.partitionID), msg.ID); err != nil {
		return fmt.Errorf("failed to delete message when moving to DLQ: %w", err)
	}

	p.logger.Warn("Message moved to DLQ",
		zap.String("message_id", string(msg.ID)),
		zap.String("reason", reason),
		zap.Int32("retry_count", msg.RetryCount),
		zap.Duration("age", time.Since(msg.EnqueuedAt)),
	)

	return nil
}

// calculateBackoffTime calculates exponential backoff with jitter
func (p *LocalPartition) calculateBackoffTime(retryCount int32) time.Time {
	// Base delay of 1 second, doubles each retry, max 5 minutes
	baseDelay := time.Second
	maxDelay := 5 * time.Minute

	// Exponential backoff: delay = base * 2^retryCount
	delay := time.Duration(int64(baseDelay) * (1 << uint(retryCount)))
	if delay > maxDelay {
		delay = maxDelay
	}

	// Add jitter (±25%)
	jitter := delay / 4
	delay = delay - jitter + time.Duration(float64(jitter)*2*p.random())

	return time.Now().Add(delay)
}

// random returns a pseudo-random number between 0 and 1
// In a real implementation, this should use a proper random source
func (p *LocalPartition) random() float64 {
	// Simple linear congruential generator for demonstration
	// In production, use crypto/rand or math/rand with proper seeding
	return 0.5 // For now, return constant value
}
