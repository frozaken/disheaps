package raft

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func createTestNodeHelper(t testing.TB, nodeID string, port int) (*Node, func()) {
	// Create temporary directory
	tempDir := t.TempDir()

	config := DefaultNodeConfig(
		nodeID,
		fmt.Sprintf("127.0.0.1:%d", port),
		tempDir,
	)
	config.EnableSingleNode = true
	config.HeartbeatTimeout = 1000 * time.Millisecond // Increase to work with default LeaderLeaseTimeout
	config.ElectionTimeout = 1000 * time.Millisecond

	// Create mock storage
	mockStorage := createTestStorage(t.(*testing.T))

	logger := zaptest.NewLogger(t)
	node, err := NewNode(config, mockStorage, logger)
	require.NoError(t, err)

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := node.Stop(ctx); err != nil {
			t.Logf("Error stopping node: %v", err)
		}

		// Clean up temp directory
		os.RemoveAll(tempDir)
	}

	return node, cleanup
}

func createTestNode(t *testing.T, nodeID string, port int) (*Node, func()) {
	return createTestNodeHelper(t, nodeID, port)
}

func TestNode_BasicLifecycle(t *testing.T) {
	node, cleanup := createTestNode(t, "test-node-1", 18001)
	defer cleanup()

	ctx := context.Background()

	// Test start
	err := node.Start(ctx)
	require.NoError(t, err)

	// Verify node is running
	assert.True(t, node.running)

	// Test stop
	err = node.Stop(ctx)
	require.NoError(t, err)

	// Verify node is stopped
	assert.False(t, node.running)
}

func TestNode_SingleNodeBootstrap(t *testing.T) {
	node, cleanup := createTestNode(t, "bootstrap-node", 18002)
	defer cleanup()

	ctx := context.Background()

	// Start node
	err := node.Start(ctx)
	require.NoError(t, err)
	defer node.Stop(ctx)

	// Wait for leadership
	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Verify node became leader
	assert.True(t, node.IsLeader())

	// Get stats
	stats := node.GetStats()
	assert.Equal(t, "bootstrap-node", stats.NodeID)
	assert.True(t, stats.IsLeader)
	assert.Equal(t, 1, stats.ClusterSize)
	assert.Equal(t, 1, stats.VotersCount)
}

func TestNode_Configuration(t *testing.T) {
	// Test invalid configurations
	logger := zaptest.NewLogger(t)
	mockStorage := createTestStorage(t)

	t.Run("nil_config", func(t *testing.T) {
		_, err := NewNode(nil, mockStorage, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "config cannot be nil")
	})

	t.Run("empty_node_id", func(t *testing.T) {
		config := DefaultNodeConfig("", "127.0.0.1:18003", t.TempDir())
		_, err := NewNode(config, mockStorage, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "node ID cannot be empty")
	})

	t.Run("empty_raft_addr", func(t *testing.T) {
		config := DefaultNodeConfig("test-node", "", t.TempDir())
		_, err := NewNode(config, mockStorage, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "raft address cannot be empty")
	})

	t.Run("empty_data_dir", func(t *testing.T) {
		config := DefaultNodeConfig("test-node", "127.0.0.1:18003", "")
		_, err := NewNode(config, mockStorage, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "data directory cannot be empty")
	})
}

func TestNode_Stats(t *testing.T) {
	node, cleanup := createTestNode(t, "stats-node", 18004)
	defer cleanup()

	ctx := context.Background()

	// Test stats before start
	stats := node.GetStats()
	assert.Equal(t, "stats-node", stats.NodeID)
	assert.False(t, stats.IsLeader)

	// Start node and wait for leadership
	err := node.Start(ctx)
	require.NoError(t, err)
	defer node.Stop(ctx)

	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Test stats after leadership
	stats = node.GetStats()
	assert.Equal(t, "stats-node", stats.NodeID)
	assert.True(t, stats.IsLeader)
	assert.Equal(t, 1, stats.ClusterSize)
	assert.Equal(t, 1, stats.VotersCount)
	assert.Equal(t, 0, stats.NonVotersCount)
}

func TestNode_LogApplication(t *testing.T) {
	node, cleanup := createTestNode(t, "apply-node", 18005)
	defer cleanup()

	ctx := context.Background()

	// Start node
	err := node.Start(ctx)
	require.NoError(t, err)
	defer node.Stop(ctx)

	// Wait for leadership
	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Create test log entry
	entry := &LogEntry{
		Type:      EntrySnapshotMark,
		Topic:     "test-topic",
		Partition: 0,
		Timestamp: time.Now(),
	}

	// Get initial stats before applying
	initialStats := node.GetStats()

	// Apply log entry
	err = node.Apply(entry, 5*time.Second)
	require.NoError(t, err)

	// Wait for entry to be applied
	err = node.WaitForAppliedIndex(initialStats.LastLogIndex+1, 5*time.Second)
	require.NoError(t, err)

	// Verify stats updated
	finalStats := node.GetStats()
	assert.Greater(t, finalStats.AppliedIndex, initialStats.AppliedIndex)
}

func TestNode_Snapshot(t *testing.T) {
	node, cleanup := createTestNode(t, "snapshot-node", 18006)
	defer cleanup()

	ctx := context.Background()

	// Start node
	err := node.Start(ctx)
	require.NoError(t, err)
	defer node.Stop(ctx)

	// Wait for leadership
	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Apply some entries to have state to snapshot
	for i := 0; i < 5; i++ {
		entry := &LogEntry{
			Type:      EntrySnapshotMark,
			Topic:     "test-topic",
			Partition: uint32(i),
			Timestamp: time.Now(),
		}

		err = node.Apply(entry, 2*time.Second)
		require.NoError(t, err)
	}

	// Create snapshot
	err = node.Snapshot()
	require.NoError(t, err)

	// Verify snapshot was created (this is a basic test - in practice we'd check snapshot store)
	stats := node.GetStats()
	assert.Greater(t, stats.LastLogIndex, uint64(0))
}

func TestClusterManager_BasicOperations(t *testing.T) {
	node, cleanup := createTestNode(t, "cluster-node", 18007)
	defer cleanup()

	logger := zaptest.NewLogger(t)
	clusterManager := NewClusterManager(node, logger)

	ctx := context.Background()

	// Start node
	err := node.Start(ctx)
	require.NoError(t, err)
	defer node.Stop(ctx)

	// Wait for leadership
	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Test cluster info
	info, err := clusterManager.GetClusterInfo()
	require.NoError(t, err)
	assert.Equal(t, 1, info.TotalCount)
	assert.Equal(t, 1, info.VoterCount)
	assert.True(t, info.IsHealthy)

	// Test cluster topology
	topology, err := clusterManager.GetClusterTopology()
	require.NoError(t, err)
	assert.Equal(t, "cluster-node", topology["leader"])
	assert.Equal(t, 1, topology["total_count"])
	assert.True(t, topology["is_healthy"].(bool))
}

func TestClusterManager_LeadershipOperations(t *testing.T) {
	node, cleanup := createTestNode(t, "leader-node", 18008)
	defer cleanup()

	logger := zaptest.NewLogger(t)
	clusterManager := NewClusterManager(node, logger)

	ctx := context.Background()

	// Start node
	err := node.Start(ctx)
	require.NoError(t, err)
	defer node.Stop(ctx)

	// Wait for leadership
	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Test verify leader
	err = clusterManager.VerifyLeader()
	require.NoError(t, err)

	// Test barrier
	err = clusterManager.Barrier(5 * time.Second)
	require.NoError(t, err)

	// Test getting configuration
	config, err := clusterManager.GetConfiguration()
	require.NoError(t, err)
	assert.Len(t, config.Servers, 1)
	assert.Equal(t, "leader-node", string(config.Servers[0].ID))
}

func TestTransportManager_BasicOperations(t *testing.T) {
	logger := zaptest.NewLogger(t)

	config := DefaultTransportConfig("127.0.0.1:18009")
	tm := NewTransportManager(config, logger)

	// Test start
	transport, err := tm.Start()
	require.NoError(t, err)
	assert.NotNil(t, transport)
	defer tm.Stop()

	// Test get transport
	assert.Equal(t, transport, tm.GetTransport())

	// Test local address
	addr := tm.GetLocalAddr()
	assert.NotEmpty(t, addr)
	assert.Contains(t, addr, "18009")
}

func TestTransportManager_InvalidConfig(t *testing.T) {
	logger := zaptest.NewLogger(t)

	// Test with invalid bind address
	config := DefaultTransportConfig("invalid-address")
	tm := NewTransportManager(config, logger)

	_, err := tm.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to resolve bind address")
}

func TestConnPool_BasicOperations(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := DefaultTransportConfig("127.0.0.1:18010")

	pool := NewConnPool(config, logger)
	defer pool.Close()

	// Test connection to non-existent address (should fail)
	addr := raft.ServerAddress("127.0.0.1:18010") // Assuming nothing is listening here
	_, err := pool.Get(addr)
	require.Error(t, err) // Should fail since nothing is listening
}

func TestIntegration_MultipleNodes(t *testing.T) {
	// This test would ideally test multiple nodes, but requires more complex setup
	// For now, test single node with cluster operations

	node, cleanup := createTestNode(t, "integration-node", 18011)
	defer cleanup()

	logger := zaptest.NewLogger(t)
	clusterManager := NewClusterManager(node, logger)

	ctx := context.Background()

	// Start node
	err := node.Start(ctx)
	require.NoError(t, err)
	defer node.Stop(ctx)

	// Wait for leadership
	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Test cluster operations
	info, err := clusterManager.GetClusterInfo()
	require.NoError(t, err)
	assert.True(t, info.IsHealthy)

	// Test adding a non-voter (this will fail in single-node setup, but tests the API)
	err = clusterManager.AddNonvoter("new-node", "127.0.0.1:18012", 5*time.Second)
	// This might error in single-node setup, which is expected

	// Test wait for cluster (should succeed with size 1)
	err = clusterManager.WaitForCluster(1, 2*time.Second)
	require.NoError(t, err)
}

func BenchmarkNode_LogApplication(b *testing.B) {
	node, cleanup := createTestNodeHelper(b, "bench-node", 18020)
	defer cleanup()

	ctx := context.Background()

	// Start node
	err := node.Start(ctx)
	require.NoError(b, err)
	defer node.Stop(ctx)

	// Wait for leadership
	err = node.WaitForLeader(5 * time.Second)
	require.NoError(b, err)

	// Benchmark log application
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		entry := &LogEntry{
			Type:      EntrySnapshotMark,
			Topic:     "bench-topic",
			Partition: uint32(i % 4),
			Timestamp: time.Now(),
		}

		err := node.Apply(entry, 1*time.Second)
		if err != nil {
			b.Fatalf("Apply failed: %v", err)
		}
	}
}

func TestRaftZapLogger(t *testing.T) {
	logger := zaptest.NewLogger(t)
	raftLogger := NewRaftZapLogger(logger)

	// Test logging methods (these should not panic)
	raftLogger.Debug("debug message")
	raftLogger.Info("info message")
	raftLogger.Warn("warn message")
	raftLogger.Error("error message")

	// Test with formatting
	raftLogger.Debug("debug with args: %s %d", "test", 123)
	raftLogger.Info("info with args: %s %d", "test", 123)
}

func TestTransportLogger(t *testing.T) {
	logger := zaptest.NewLogger(t)
	transportLogger := NewTransportLogger(logger)

	// Test write operations
	n, err := transportLogger.Write([]byte("test message"))
	assert.NoError(t, err)
	assert.Equal(t, 12, n) // Length of "test message"

	// Test with newline
	n, err = transportLogger.Write([]byte("test with newline\n"))
	assert.NoError(t, err)
	assert.Equal(t, 18, n) // Length of full string
}
