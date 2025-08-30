package models

import (
	"errors"
	"time"
)

// PartitionStats represents statistics for a partition
type PartitionStats struct {
	Topic          string      `json:"topic"`
	PartitionID    PartitionID `json:"partition_id"`
	MessageCount   uint64      `json:"message_count"`
	InflightCount  uint64      `json:"inflight_count"`
	AckedCount     uint64      `json:"acked_count"`
	NackedCount    uint64      `json:"nacked_count"`
	DLQCount       uint64      `json:"dlq_count"`
	IsHealthy      bool        `json:"is_healthy"`
	IsThrottled    bool        `json:"is_throttled"`
	ThrottleReason string      `json:"throttle_reason,omitempty"`
	LastUpdated    time.Time   `json:"last_updated"`
}

// MessageLease represents lease information attached to a message
type MessageLease struct {
	Token      LeaseToken `json:"token"`
	Holder     string     `json:"holder"`
	GrantedAt  time.Time  `json:"granted_at"`
	Deadline   time.Time  `json:"deadline"`
	Extensions int        `json:"extensions"`
}

// Validate validates the message lease
func (ml *MessageLease) Validate() error {
	if ml.Token == "" {
		return errors.New("lease token cannot be empty")
	}
	if ml.Holder == "" {
		return errors.New("lease holder cannot be empty")
	}
	if ml.GrantedAt.IsZero() {
		return errors.New("lease granted time cannot be zero")
	}
	if ml.Deadline.IsZero() {
		return errors.New("lease deadline cannot be zero")
	}
	if ml.Deadline.Before(ml.GrantedAt) {
		return errors.New("lease deadline cannot be before granted time")
	}
	return nil
}

// IsExpired returns true if the lease has expired
func (ml *MessageLease) IsExpired() bool {
	return time.Now().After(ml.Deadline)
}
