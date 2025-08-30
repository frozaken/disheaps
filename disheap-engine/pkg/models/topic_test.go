package models

import (
	"testing"
	"time"
)

func TestTopicNameValidation(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{"valid_topic", "valid-topic", false},
		{"valid_with_underscore", "valid_topic", false},
		{"valid_alphanumeric", "topic123", false},
		{"valid_mixed", "topic-123_test", false},
		{"empty_topic", "", true},
		{"invalid_characters", "topic.invalid", true},
		{"invalid_characters_space", "topic invalid", true},
		{"invalid_characters_dot", "topic.name", true},
		{"too_long", "this-is-a-very-long-topic-name-that-exceeds-the-maximum-allowed-length-for-topic-names-by-being-over-255-characters-long-and-should-definitely-fail-validation-because-it-is-way-too-long-for-any-reasonable-use-case-and-would-cause-issues-in-the-system-and-really-needs-to-be-longer", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewTopicConfig(tt.topic, MinHeap, 1, 1)
			err := config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("TopicConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDLQPolicy_Validate(t *testing.T) {
	tests := []struct {
		name    string
		policy  DLQPolicy
		wantErr bool
	}{
		{
			name:    "none policy",
			policy:  DLQPolicy{Type: DLQPolicyNone, RetentionTime: 24 * time.Hour},
			wantErr: false,
		},
		{
			name:    "max retries with valid values",
			policy:  DLQPolicy{Type: DLQPolicyMaxRetries, MaxRetries: 3, RetentionTime: 24 * time.Hour},
			wantErr: false,
		},
		{
			name:    "max retries with zero retries",
			policy:  DLQPolicy{Type: DLQPolicyMaxRetries, MaxRetries: 0, RetentionTime: 24 * time.Hour},
			wantErr: true,
		},
		{
			name:    "ttl with valid values",
			policy:  DLQPolicy{Type: DLQPolicyTTL, MaxTTL: time.Hour, RetentionTime: 24 * time.Hour},
			wantErr: false,
		},
		{
			name:    "ttl with zero TTL",
			policy:  DLQPolicy{Type: DLQPolicyTTL, MaxTTL: 0, RetentionTime: 24 * time.Hour},
			wantErr: true,
		},
		{
			name:    "negative retention",
			policy:  DLQPolicy{Type: DLQPolicyNone, RetentionTime: -time.Hour},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.policy.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("DLQPolicy.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTopicConfig_Validate(t *testing.T) {
	tests := []struct {
		name   string
		config *TopicConfig
		want   bool
	}{
		{
			name:   "valid config",
			config: NewTopicConfig("test-topic", MinHeap, 1, 1),
			want:   true,
		},
		{
			name: "invalid empty name",
			config: &TopicConfig{
				Name:                     "",
				Mode:                     MinHeap,
				TopKBound:                100,
				Partitions:               3,
				ReplicationFactor:        2,
				RetentionTime:            24 * time.Hour,
				VisibilityTimeoutDefault: 30 * time.Second,
				MaxRetriesDefault:        3,
				MaxPayloadBytes:          1048576,
				CompressionEnabled:       true,
				DLQPolicy: DLQPolicy{
					Type:          DLQPolicyMaxRetries,
					MaxRetries:    3,
					RetentionTime: 7 * 24 * time.Hour,
				},
			},
			want: false,
		},
		{
			name: "invalid zero partitions",
			config: &TopicConfig{
				Name:                     "test-topic",
				Mode:                     MinHeap,
				TopKBound:                100,
				Partitions:               0,
				ReplicationFactor:        2,
				RetentionTime:            24 * time.Hour,
				VisibilityTimeoutDefault: 30 * time.Second,
				MaxRetriesDefault:        3,
				MaxPayloadBytes:          1048576,
				CompressionEnabled:       true,
				DLQPolicy: DLQPolicy{
					Type:          DLQPolicyMaxRetries,
					MaxRetries:    3,
					RetentionTime: 7 * 24 * time.Hour,
				},
			},
			want: false,
		},
		{
			name: "invalid zero replication factor",
			config: &TopicConfig{
				Name:                     "test-topic",
				Mode:                     MinHeap,
				TopKBound:                100,
				Partitions:               3,
				ReplicationFactor:        0,
				RetentionTime:            24 * time.Hour,
				VisibilityTimeoutDefault: 30 * time.Second,
				MaxRetriesDefault:        3,
				MaxPayloadBytes:          1048576,
				CompressionEnabled:       true,
				DLQPolicy: DLQPolicy{
					Type:          DLQPolicyMaxRetries,
					MaxRetries:    3,
					RetentionTime: 7 * 24 * time.Hour,
				},
			},
			want: false,
		},
		{
			name: "invalid negative retention time",
			config: &TopicConfig{
				Name:                     "test-topic",
				Mode:                     MinHeap,
				TopKBound:                100,
				Partitions:               3,
				ReplicationFactor:        2,
				RetentionTime:            -time.Hour,
				VisibilityTimeoutDefault: 30 * time.Second,
				MaxRetriesDefault:        3,
				MaxPayloadBytes:          1048576,
				CompressionEnabled:       true,
				DLQPolicy: DLQPolicy{
					Type:          DLQPolicyMaxRetries,
					MaxRetries:    3,
					RetentionTime: 7 * 24 * time.Hour,
				},
			},
			want: false,
		},
		{
			name: "invalid zero visibility timeout",
			config: &TopicConfig{
				Name:                     "test-topic",
				Mode:                     MinHeap,
				TopKBound:                100,
				Partitions:               3,
				ReplicationFactor:        2,
				RetentionTime:            24 * time.Hour,
				VisibilityTimeoutDefault: 0,
				MaxRetriesDefault:        3,
				MaxPayloadBytes:          1048576,
				CompressionEnabled:       true,
				DLQPolicy: DLQPolicy{
					Type:          DLQPolicyMaxRetries,
					MaxRetries:    3,
					RetentionTime: 7 * 24 * time.Hour,
				},
			},
			want: false,
		},
		{
			name: "invalid zero payload size",
			config: &TopicConfig{
				Name:                     "test-topic",
				Mode:                     MinHeap,
				TopKBound:                100,
				Partitions:               3,
				ReplicationFactor:        2,
				RetentionTime:            24 * time.Hour,
				VisibilityTimeoutDefault: 30 * time.Second,
				MaxRetriesDefault:        3,
				MaxPayloadBytes:          0,
				CompressionEnabled:       true,
				DLQPolicy: DLQPolicy{
					Type:          DLQPolicyMaxRetries,
					MaxRetries:    3,
					RetentionTime: 7 * 24 * time.Hour,
				},
			},
			want: false,
		},
		{
			name: "invalid DLQ policy",
			config: &TopicConfig{
				Name:                     "test-topic",
				Mode:                     MinHeap,
				TopKBound:                100,
				Partitions:               3,
				ReplicationFactor:        2,
				RetentionTime:            24 * time.Hour,
				VisibilityTimeoutDefault: 30 * time.Second,
				MaxRetriesDefault:        3,
				MaxPayloadBytes:          1048576,
				CompressionEnabled:       true,
				DLQPolicy: DLQPolicy{
					Type:          DLQPolicyMaxRetries,
					MaxRetries:    0, // Invalid: zero retries
					RetentionTime: 7 * 24 * time.Hour,
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			got := err == nil
			if got != tt.want {
				t.Errorf("TopicConfig.Validate() = %v, want %v, error = %v", got, tt.want, err)
			}
		})
	}
}

func TestPartitionInfo_Creation(t *testing.T) {
	id := PartitionID(1)
	topic := "test-topic"
	leaderNode := "node-1"
	replicaNodes := []string{"node-1", "node-2", "node-3"}

	info := NewPartitionInfo(id, topic, leaderNode, replicaNodes)

	if info.ID != id {
		t.Errorf("ID = %v, want %v", info.ID, id)
	}
	if info.Topic != topic {
		t.Errorf("Topic = %v, want %v", info.Topic, topic)
	}
	if info.LeaderNode != leaderNode {
		t.Errorf("LeaderNode = %v, want %v", info.LeaderNode, leaderNode)
	}
	if len(info.ReplicaNodes) != len(replicaNodes) {
		t.Errorf("ReplicaNodes length = %v, want %v", len(info.ReplicaNodes), len(replicaNodes))
	}
	if !info.IsHealthy {
		t.Error("New partition info should be healthy by default")
	}
}
