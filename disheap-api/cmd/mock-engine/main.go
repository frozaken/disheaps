package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockEngine implements the Disheap gRPC service for testing
type mockEngine struct {
	disheapv1.UnimplementedDisheapServer
	heaps map[string]*disheapv1.HeapInfo // Simple in-memory heap storage
}

// NewMockEngine creates a new mock engine
func NewMockEngine() *mockEngine {
	return &mockEngine{
		heaps: make(map[string]*disheapv1.HeapInfo),
	}
}

// MakeHeap creates a new heap
func (e *mockEngine) MakeHeap(ctx context.Context, req *disheapv1.MakeHeapReq) (*disheapv1.MakeHeapResp, error) {
	log.Printf("MakeHeap called: topic=%s, mode=%s, partitions=%d", req.Topic, req.Mode, req.Partitions)

	// Store heap info
	e.heaps[req.Topic] = &disheapv1.HeapInfo{
		Topic:             req.Topic,
		Mode:              req.Mode,
		Partitions:        req.Partitions,
		ReplicationFactor: req.ReplicationFactor,
		TopKBound:         req.TopKBound,
		RetentionTime:     req.RetentionTime,
	}

	return &disheapv1.MakeHeapResp{
		Topic:  req.Topic,
		HeapId: fmt.Sprintf("heap_%s_%d", req.Topic, time.Now().Unix()),
	}, nil
}

// DeleteHeap deletes a heap
func (e *mockEngine) DeleteHeap(ctx context.Context, req *disheapv1.DeleteHeapReq) (*emptypb.Empty, error) {
	log.Printf("DeleteHeap called: topic=%s", req.Topic)

	delete(e.heaps, req.Topic)
	return &emptypb.Empty{}, nil
}

// UpdateHeapConfig updates heap configuration
func (e *mockEngine) UpdateHeapConfig(ctx context.Context, req *disheapv1.UpdateHeapConfigReq) (*emptypb.Empty, error) {
	log.Printf("UpdateHeapConfig called: topic=%s", req.Topic)
	return &emptypb.Empty{}, nil
}

// ListHeaps lists all heaps
func (e *mockEngine) ListHeaps(ctx context.Context, req *disheapv1.ListHeapsReq) (*disheapv1.ListHeapsResp, error) {
	log.Printf("ListHeaps called: page_size=%d, page_token=%s", req.PageSize, req.PageToken)

	heaps := make([]*disheapv1.HeapInfo, 0, len(e.heaps))
	for _, heap := range e.heaps {
		heaps = append(heaps, heap)
	}

	return &disheapv1.ListHeapsResp{
		Heaps:         heaps,
		NextPageToken: "", // No pagination in mock
	}, nil
}

// Stats returns heap statistics
func (e *mockEngine) Stats(ctx context.Context, req *disheapv1.StatsReq) (*disheapv1.StatsResp, error) {
	log.Printf("Stats called: topic=%v", req.Topic)

	globalStats := &disheapv1.HeapStats{
		TotalMessages:    100,
		InflightMessages: 5,
		DlqMessages:      2,
		TotalEnqueues:    500,
		TotalPops:        450,
		TotalAcks:        400,
		TotalNacks:       50,
		TotalRetries:     25,
		TotalTimeouts:    5,
	}

	topicStats := make(map[string]*disheapv1.HeapStats)

	// If specific topic requested, return its stats
	if req.Topic != nil {
		topicStats[*req.Topic] = &disheapv1.HeapStats{
			TotalMessages:    25,
			InflightMessages: 3,
			DlqMessages:      1,
			TotalEnqueues:    150,
			TotalPops:        125,
			TotalAcks:        120,
			TotalNacks:       5,
			TotalRetries:     3,
			TotalTimeouts:    1,
		}
	} else {
		// Return stats for all heaps
		for topic := range e.heaps {
			topicStats[topic] = &disheapv1.HeapStats{
				TotalMessages:    25,
				InflightMessages: 3,
				DlqMessages:      1,
				TotalEnqueues:    150,
				TotalPops:        125,
				TotalAcks:        120,
				TotalNacks:       5,
				TotalRetries:     3,
				TotalTimeouts:    1,
			}
		}
	}

	return &disheapv1.StatsResp{
		GlobalStats: globalStats,
		TopicStats:  topicStats,
	}, nil
}

// Purge purges messages from a heap
func (e *mockEngine) Purge(ctx context.Context, req *disheapv1.PurgeReq) (*emptypb.Empty, error) {
	log.Printf("Purge called: topic=%s", req.Topic)
	return &emptypb.Empty{}, nil
}

// MoveToDLQ moves messages to DLQ
func (e *mockEngine) MoveToDLQ(ctx context.Context, req *disheapv1.MoveToDlqReq) (*emptypb.Empty, error) {
	log.Printf("MoveToDLQ called: topic=%s, message_ids=%v", req.Topic, req.MessageIds)
	return &emptypb.Empty{}, nil
}

// Enqueue enqueues a message
func (e *mockEngine) Enqueue(ctx context.Context, req *disheapv1.EnqueueReq) (*disheapv1.EnqueueResp, error) {
	log.Printf("Enqueue called: topic=%s, priority=%d, payload_size=%d", req.Topic, req.Priority, len(req.Payload))

	messageID := fmt.Sprintf("msg_%s_%d", req.Topic, time.Now().UnixNano())

	return &disheapv1.EnqueueResp{
		MessageId: messageID,
		Timestamp: timestamppb.Now(),
	}, nil
}

// EnqueueBatch enqueues multiple messages
func (e *mockEngine) EnqueueBatch(ctx context.Context, req *disheapv1.EnqueueBatchReq) (*disheapv1.EnqueueBatchResp, error) {
	log.Printf("EnqueueBatch called: topic=%s, message_count=%d", req.Topic, len(req.Messages))

	results := make([]*disheapv1.BatchResult, len(req.Messages))
	for i := range req.Messages {
		messageID := fmt.Sprintf("msg_%s_%d_%d", req.Topic, time.Now().UnixNano(), i)
		results[i] = &disheapv1.BatchResult{
			Success:   true,
			MessageId: messageID,
			Error:     "",
		}
	}

	return &disheapv1.EnqueueBatchResp{
		Results: results,
	}, nil
}

// Ack acknowledges messages
func (e *mockEngine) Ack(ctx context.Context, req *disheapv1.AckReq) (*emptypb.Empty, error) {
	log.Printf("Ack called: topic=%s, message_ids=%v", req.Topic, req.MessageIds)
	return &emptypb.Empty{}, nil
}

// Nack negatively acknowledges messages
func (e *mockEngine) Nack(ctx context.Context, req *disheapv1.NackReq) (*emptypb.Empty, error) {
	log.Printf("Nack called: topic=%s, message_ids=%v, increment_retry=%t", req.Topic, req.MessageIds, req.IncrementRetryCount)
	return &emptypb.Empty{}, nil
}

// Extend extends message lease
func (e *mockEngine) Extend(ctx context.Context, req *disheapv1.ExtendReq) (*emptypb.Empty, error) {
	log.Printf("Extend called: topic=%s, message_id=%s", req.Topic, req.MessageId)
	return &emptypb.Empty{}, nil
}

// Peek peeks at messages
func (e *mockEngine) Peek(ctx context.Context, req *disheapv1.PeekReq) (*disheapv1.PeekResp, error) {
	log.Printf("Peek called: topic=%s, limit=%d", req.Topic, req.Limit)

	// Return some mock messages
	messages := []*disheapv1.Message{
		{
			Id:       "msg_1",
			Payload:  []byte("Hello World 1"),
			Priority: 100,
			Metadata: map[string]string{
				"created_at": time.Now().Format(time.RFC3339),
			},
		},
		{
			Id:       "msg_2",
			Payload:  []byte("Hello World 2"),
			Priority: 200,
			Metadata: map[string]string{
				"created_at": time.Now().Add(-time.Minute).Format(time.RFC3339),
			},
		},
	}

	return &disheapv1.PeekResp{
		Messages: messages,
	}, nil
}

func main() {
	port := ":9090"
	log.Printf("Starting mock disheap-engine on port %s", port)

	// Create listener
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create gRPC server
	server := grpc.NewServer()

	// Register mock engine service
	mockEngineService := NewMockEngine()
	disheapv1.RegisterDisheapServer(server, mockEngineService)

	log.Printf("Mock engine server listening on %s", port)
	log.Println("Available endpoints:")
	log.Println("  - MakeHeap, DeleteHeap, UpdateHeapConfig, ListHeaps")
	log.Println("  - Stats, Purge, MoveToDLQ")
	log.Println("  - Enqueue, EnqueueBatch, Ack, Nack, Extend, Peek")

	// Start server
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
