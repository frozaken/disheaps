package proto_test

import (
	"testing"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestMakeHeapReq tests MakeHeapReq message serialization and validation
func TestMakeHeapReq(t *testing.T) {
	tests := []struct {
		name    string
		req     *disheapv1.MakeHeapReq
		wantErr bool
	}{
		{
			name: "valid min heap request",
			req: &disheapv1.MakeHeapReq{
				Topic:                    "test-topic",
				Mode:                     disheapv1.Mode_MIN,
				Partitions:               4,
				ReplicationFactor:        3,
				TopKBound:                100,
				RetentionTime:            durationpb.New(24 * time.Hour),
				VisibilityTimeoutDefault: durationpb.New(30 * time.Second),
				MaxRetriesDefault:        3,
				MaxPayloadBytes:          1048576, // 1MB
				CompressionEnabled:       true,
				DlqPolicy: &disheapv1.DLQPolicy{
					Enabled:       true,
					RetentionTime: durationpb.New(7 * 24 * time.Hour), // 7 days
				},
			},
			wantErr: false,
		},
		{
			name: "valid max heap request",
			req: &disheapv1.MakeHeapReq{
				Topic:                    "max-heap",
				Mode:                     disheapv1.Mode_MAX,
				Partitions:               8,
				ReplicationFactor:        2,
				TopKBound:                50,
				RetentionTime:            durationpb.New(48 * time.Hour),
				VisibilityTimeoutDefault: durationpb.New(60 * time.Second),
				MaxRetriesDefault:        5,
				MaxPayloadBytes:          2097152, // 2MB
				CompressionEnabled:       false,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test serialization
			data, err := proto.Marshal(tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("MakeHeapReq marshal error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			// Test deserialization
			var decoded disheapv1.MakeHeapReq
			if err := proto.Unmarshal(data, &decoded); err != nil {
				t.Errorf("MakeHeapReq unmarshal error = %v", err)
				return
			}

			// Verify fields
			if decoded.Topic != tt.req.Topic {
				t.Errorf("Topic = %v, want %v", decoded.Topic, tt.req.Topic)
			}
			if decoded.Mode != tt.req.Mode {
				t.Errorf("Mode = %v, want %v", decoded.Mode, tt.req.Mode)
			}
			if decoded.Partitions != tt.req.Partitions {
				t.Errorf("Partitions = %v, want %v", decoded.Partitions, tt.req.Partitions)
			}
			if decoded.ReplicationFactor != tt.req.ReplicationFactor {
				t.Errorf("ReplicationFactor = %v, want %v", decoded.ReplicationFactor, tt.req.ReplicationFactor)
			}
		})
	}
}

// TestEnqueueReq tests EnqueueReq message handling
func TestEnqueueReq(t *testing.T) {
	req := &disheapv1.EnqueueReq{
		Topic:        "test-topic",
		Payload:      []byte("test payload"),
		Priority:     100,
		PartitionKey: proto.String("partition-key-123"),
		ProducerId:   proto.String("producer-1"),
		Epoch:        proto.Uint64(1),
		Sequence:     proto.Uint64(42),
		MaxRetries:   proto.Uint32(5),
		NotBefore:    durationpb.New(10 * time.Second),
	}

	// Test serialization
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("EnqueueReq marshal error = %v", err)
	}

	// Test deserialization
	var decoded disheapv1.EnqueueReq
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("EnqueueReq unmarshal error = %v", err)
	}

	// Verify fields
	if decoded.Topic != req.Topic {
		t.Errorf("Topic = %v, want %v", decoded.Topic, req.Topic)
	}
	if string(decoded.Payload) != string(req.Payload) {
		t.Errorf("Payload = %v, want %v", string(decoded.Payload), string(req.Payload))
	}
	if decoded.Priority != req.Priority {
		t.Errorf("Priority = %v, want %v", decoded.Priority, req.Priority)
	}
	if decoded.GetProducerId() != req.GetProducerId() {
		t.Errorf("ProducerId = %v, want %v", decoded.GetProducerId(), req.GetProducerId())
	}
	if decoded.GetEpoch() != req.GetEpoch() {
		t.Errorf("Epoch = %v, want %v", decoded.GetEpoch(), req.GetEpoch())
	}
	if decoded.GetSequence() != req.GetSequence() {
		t.Errorf("Sequence = %v, want %v", decoded.GetSequence(), req.GetSequence())
	}
}

// TestPopItem tests PopItem message handling
func TestPopItem(t *testing.T) {
	now := time.Now()
	item := &disheapv1.PopItem{
		MessageId:     "msg-123",
		Topic:         "test-topic",
		PartitionId:   2,
		Payload:       []byte("message payload"),
		Priority:      1000,
		EnqueuedTime:  timestamppb.New(now),
		LeaseToken:    "lease-token-abc",
		LeaseDeadline: timestamppb.New(now.Add(30 * time.Second)),
		Attempts:      1,
		MaxRetries:    3,
		ProducerId:    proto.String("producer-1"),
		Epoch:         proto.Uint64(5),
		Sequence:      proto.Uint64(999),
	}

	// Test serialization
	data, err := proto.Marshal(item)
	if err != nil {
		t.Fatalf("PopItem marshal error = %v", err)
	}

	// Test deserialization
	var decoded disheapv1.PopItem
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("PopItem unmarshal error = %v", err)
	}

	// Verify fields
	if decoded.MessageId != item.MessageId {
		t.Errorf("MessageId = %v, want %v", decoded.MessageId, item.MessageId)
	}
	if decoded.Topic != item.Topic {
		t.Errorf("Topic = %v, want %v", decoded.Topic, item.Topic)
	}
	if decoded.PartitionId != item.PartitionId {
		t.Errorf("PartitionId = %v, want %v", decoded.PartitionId, item.PartitionId)
	}
	if decoded.Priority != item.Priority {
		t.Errorf("Priority = %v, want %v", decoded.Priority, item.Priority)
	}
	if decoded.LeaseToken != item.LeaseToken {
		t.Errorf("LeaseToken = %v, want %v", decoded.LeaseToken, item.LeaseToken)
	}
	if decoded.Attempts != item.Attempts {
		t.Errorf("Attempts = %v, want %v", decoded.Attempts, item.Attempts)
	}

	// Verify timestamps
	if !decoded.EnqueuedTime.AsTime().Equal(item.EnqueuedTime.AsTime()) {
		t.Errorf("EnqueuedTime = %v, want %v", decoded.EnqueuedTime.AsTime(), item.EnqueuedTime.AsTime())
	}
	if !decoded.LeaseDeadline.AsTime().Equal(item.LeaseDeadline.AsTime()) {
		t.Errorf("LeaseDeadline = %v, want %v", decoded.LeaseDeadline.AsTime(), item.LeaseDeadline.AsTime())
	}
}

// TestFlowControl tests streaming flow control messages
func TestFlowControl(t *testing.T) {
	fc := &disheapv1.FlowControl{
		Credits: 10,
	}

	popOpen := &disheapv1.PopOpen{
		Kind: &disheapv1.PopOpen_FlowControl{
			FlowControl: fc,
		},
	}

	// Test serialization
	data, err := proto.Marshal(popOpen)
	if err != nil {
		t.Fatalf("PopOpen marshal error = %v", err)
	}

	// Test deserialization
	var decoded disheapv1.PopOpen
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("PopOpen unmarshal error = %v", err)
	}

	// Verify oneof field
	switch kind := decoded.Kind.(type) {
	case *disheapv1.PopOpen_FlowControl:
		if kind.FlowControl.Credits != fc.Credits {
			t.Errorf("FlowControl.Credits = %v, want %v", kind.FlowControl.Credits, fc.Credits)
		}
	default:
		t.Errorf("Expected FlowControl, got %T", kind)
	}
}

// TestEnumValues tests enum value handling
func TestEnumValues(t *testing.T) {
	tests := []struct {
		name string
		mode disheapv1.Mode
		want string
	}{
		{"min mode", disheapv1.Mode_MIN, "MIN"},
		{"max mode", disheapv1.Mode_MAX, "MAX"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mode.String(); got != tt.want {
				t.Errorf("Mode.String() = %v, want %v", got, tt.want)
			}

			// Test enum value in message
			req := &disheapv1.MakeHeapReq{
				Topic: "test",
				Mode:  tt.mode,
			}

			data, err := proto.Marshal(req)
			if err != nil {
				t.Fatalf("Marshal error = %v", err)
			}

			var decoded disheapv1.MakeHeapReq
			if err := proto.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("Unmarshal error = %v", err)
			}

			if decoded.Mode != tt.mode {
				t.Errorf("Decoded mode = %v, want %v", decoded.Mode, tt.mode)
			}
		})
	}
}

// TestBatchOperations tests batch enqueue request/response
func TestBatchOperations(t *testing.T) {
	batchReq := &disheapv1.EnqueueBatchReq{
		Requests: []*disheapv1.EnqueueReq{
			{
				Topic:    "topic-1",
				Payload:  []byte("payload-1"),
				Priority: 100,
			},
			{
				Topic:    "topic-1",
				Payload:  []byte("payload-2"),
				Priority: 200,
			},
		},
		TransactionId: proto.String("txn-123"),
	}

	// Test serialization
	data, err := proto.Marshal(batchReq)
	if err != nil {
		t.Fatalf("EnqueueBatchReq marshal error = %v", err)
	}

	// Test deserialization
	var decoded disheapv1.EnqueueBatchReq
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("EnqueueBatchReq unmarshal error = %v", err)
	}

	if len(decoded.Requests) != len(batchReq.Requests) {
		t.Errorf("Requests length = %v, want %v", len(decoded.Requests), len(batchReq.Requests))
	}

	for i, req := range decoded.Requests {
		original := batchReq.Requests[i]
		if req.Topic != original.Topic {
			t.Errorf("Request[%d].Topic = %v, want %v", i, req.Topic, original.Topic)
		}
		if req.Priority != original.Priority {
			t.Errorf("Request[%d].Priority = %v, want %v", i, req.Priority, original.Priority)
		}
	}

	if decoded.GetTransactionId() != batchReq.GetTransactionId() {
		t.Errorf("TransactionId = %v, want %v", decoded.GetTransactionId(), batchReq.GetTransactionId())
	}
}

// TestStatsMessages tests stats request/response handling
func TestStatsMessages(t *testing.T) {
	statsResp := &disheapv1.StatsResp{
		GlobalStats: &disheapv1.HeapStats{
			TotalMessages:    1000,
			InflightMessages: 50,
			DlqMessages:      10,
			TotalEnqueues:    1200,
			TotalPops:        1100,
			TotalAcks:        1050,
			TotalNacks:       40,
			TotalRetries:     15,
			TotalTimeouts:    5,
			PartitionStats: map[uint32]*disheapv1.PartitionStats{
				0: {
					PartitionId:  0,
					Messages:     500,
					Inflight:     25,
					LeaderNode:   "node-1",
					ReplicaNodes: []string{"node-2", "node-3"},
					IsHealthy:    true,
				},
				1: {
					PartitionId:  1,
					Messages:     500,
					Inflight:     25,
					LeaderNode:   "node-2",
					ReplicaNodes: []string{"node-1", "node-3"},
					IsHealthy:    true,
				},
			},
		},
		TopicStats: map[string]*disheapv1.HeapStats{
			"topic-1": {
				TotalMessages:    800,
				InflightMessages: 40,
				DlqMessages:      8,
			},
			"topic-2": {
				TotalMessages:    200,
				InflightMessages: 10,
				DlqMessages:      2,
			},
		},
	}

	// Test serialization
	data, err := proto.Marshal(statsResp)
	if err != nil {
		t.Fatalf("StatsResp marshal error = %v", err)
	}

	// Test deserialization
	var decoded disheapv1.StatsResp
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("StatsResp unmarshal error = %v", err)
	}

	// Verify global stats
	if decoded.GlobalStats.TotalMessages != statsResp.GlobalStats.TotalMessages {
		t.Errorf("GlobalStats.TotalMessages = %v, want %v",
			decoded.GlobalStats.TotalMessages, statsResp.GlobalStats.TotalMessages)
	}

	// Verify partition stats
	if len(decoded.GlobalStats.PartitionStats) != len(statsResp.GlobalStats.PartitionStats) {
		t.Errorf("PartitionStats length = %v, want %v",
			len(decoded.GlobalStats.PartitionStats), len(statsResp.GlobalStats.PartitionStats))
	}

	// Verify topic stats
	if len(decoded.TopicStats) != len(statsResp.TopicStats) {
		t.Errorf("TopicStats length = %v, want %v",
			len(decoded.TopicStats), len(statsResp.TopicStats))
	}

	for topic, stats := range decoded.TopicStats {
		originalStats := statsResp.TopicStats[topic]
		if stats.TotalMessages != originalStats.TotalMessages {
			t.Errorf("TopicStats[%s].TotalMessages = %v, want %v",
				topic, stats.TotalMessages, originalStats.TotalMessages)
		}
	}
}
