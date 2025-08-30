package raft

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func createTestRecoveryManager(t *testing.T) (*RecoveryManager, func()) {
	node, nodeCleanup := createTestNode(t, "recovery-node", 18030)
	mockStorage := createTestStorage(t)
	logger := zaptest.NewLogger(t)

	rm := NewRecoveryManager(node, mockStorage, logger)

	cleanup := func() {
		nodeCleanup()
	}

	return rm, cleanup
}

func createTestRecoveryManagerForBenchmark(b *testing.B) (*RecoveryManager, func()) {
	node, nodeCleanup := createTestNodeHelper(b, "recovery-node", 18030)
	mockStorage := &MockStorage{} // Use the existing MockStorage
	logger := zaptest.NewLogger(b)

	rm := NewRecoveryManager(node, mockStorage, logger)

	cleanup := func() {
		nodeCleanup()
	}

	return rm, cleanup
}

func TestRecoveryManager_BasicOperations(t *testing.T) {
	rm, cleanup := createTestRecoveryManager(t)
	defer cleanup()

	// Test recovery config
	config := DefaultRecoveryConfig()
	assert.Equal(t, 30*time.Second, config.ConsensusTimeout)
	assert.Equal(t, 5*time.Second, config.RetryInterval)
	assert.Equal(t, 5, config.MaxRetries)
	assert.True(t, config.VerifyIntegrity)

	// Test getting available snapshots (should work even without snapshots)
	snapshots, err := rm.GetAvailableSnapshots()
	require.NoError(t, err)
	assert.NotNil(t, snapshots)
}

func TestRecoveryManager_VerifyIntegrity(t *testing.T) {
	rm, cleanup := createTestRecoveryManager(t)
	defer cleanup()

	ctx := context.Background()

	// Start the node first
	err := rm.node.Start(ctx)
	require.NoError(t, err)
	defer rm.node.Stop(ctx)

	// Wait for leadership
	err = rm.node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Test integrity verification
	err = rm.VerifyIntegrity(ctx)
	require.NoError(t, err)
}

func TestRecoveryManager_RecoverPartitions(t *testing.T) {
	rm, cleanup := createTestRecoveryManager(t)
	defer cleanup()

	ctx := context.Background()

	// Test partition recovery (should work with empty storage)
	err := rm.RecoverPartitions(ctx)
	require.NoError(t, err)
}

func TestRecoveryManager_ValidateClusterState(t *testing.T) {
	rm, cleanup := createTestRecoveryManager(t)
	defer cleanup()

	ctx := context.Background()

	// Start the node
	err := rm.node.Start(ctx)
	require.NoError(t, err)
	defer rm.node.Stop(ctx)

	// Test cluster state validation
	err = rm.ValidateClusterState(ctx)
	require.NoError(t, err)
}

func TestRecoveryManager_DiagnoseIssues(t *testing.T) {
	rm, cleanup := createTestRecoveryManager(t)
	defer cleanup()

	ctx := context.Background()

	// Test diagnosis without started node
	diagnosis := rm.DiagnoseRecoveryIssues(ctx)
	assert.Contains(t, diagnosis, "node_state")
	assert.Contains(t, diagnosis, "storage_accessible")

	// Start node and test again
	err := rm.node.Start(ctx)
	require.NoError(t, err)
	defer rm.node.Stop(ctx)

	err = rm.node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	diagnosis = rm.DiagnoseRecoveryIssues(ctx)
	assert.True(t, diagnosis["storage_accessible"].(bool))
	assert.Equal(t, 0, diagnosis["fsm_partitions"])
}

func TestRecoveryManager_CrashRecovery(t *testing.T) {
	rm, cleanup := createTestRecoveryManager(t)
	defer cleanup()

	ctx := context.Background()

	// Start the node first
	err := rm.node.Start(ctx)
	require.NoError(t, err)
	defer rm.node.Stop(ctx)

	// Wait for leadership
	err = rm.node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Apply some log entries to create state
	for i := 0; i < 3; i++ {
		entry := &LogEntry{
			Type:      EntrySnapshotMark,
			Topic:     "recovery-topic",
			Partition: uint32(i),
			Timestamp: time.Now(),
		}

		err = rm.node.Apply(entry, 2*time.Second)
		require.NoError(t, err)
	}

	// Perform crash recovery
	config := DefaultRecoveryConfig()
	config.MaxRetries = 2 // Reduce for testing
	config.ConsensusTimeout = 10 * time.Second

	stats, err := rm.RecoverFromCrash(ctx, config)
	require.NoError(t, err)
	assert.True(t, stats.SuccessfulRecovery)
	assert.Greater(t, stats.RecoveryAttempts, 0)
	assert.True(t, stats.IntegrityCheckPassed)
}

func TestCompactionManager_BasicOperations(t *testing.T) {
	node, cleanup := createTestNode(t, "compaction-node", 18031)
	defer cleanup()

	logger := zaptest.NewLogger(t)
	config := DefaultCompactionConfig()
	config.EnableAutoCompaction = false // Disable for testing

	cm := NewCompactionManager(node, config, logger)
	ctx := context.Background()

	// Test start/stop
	err := cm.Start(ctx)
	require.NoError(t, err)

	err = cm.Stop(ctx)
	require.NoError(t, err)
}

func TestCompactionManager_ShouldCompact(t *testing.T) {
	node, cleanup := createTestNode(t, "should-compact-node", 18032)
	defer cleanup()

	logger := zaptest.NewLogger(t)
	config := DefaultCompactionConfig()
	config.LogCountThreshold = 5 // Low threshold for testing

	cm := NewCompactionManager(node, config, logger)
	ctx := context.Background()

	// Start components
	err := cm.Start(ctx)
	require.NoError(t, err)
	defer cm.Stop(ctx)

	err = node.Start(ctx)
	require.NoError(t, err)
	defer node.Stop(ctx)

	// Wait for leadership
	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Initially should not need compaction
	should, reason := cm.ShouldCompact()
	assert.False(t, should)
	assert.Contains(t, reason, "no compaction needed")

	// Apply entries to trigger threshold
	for i := 0; i < 10; i++ {
		entry := &LogEntry{
			Type:      EntrySnapshotMark,
			Topic:     "compact-topic",
			Partition: uint32(i % 2),
			Timestamp: time.Now(),
		}

		err = node.Apply(entry, 1*time.Second)
		require.NoError(t, err)
	}

	// Now should need compaction
	should, reason = cm.ShouldCompact()
	assert.True(t, should)
	assert.Contains(t, reason, "log count")
}

func TestCompactionManager_CompactNow(t *testing.T) {
	node, cleanup := createTestNode(t, "compact-now-node", 18033)
	defer cleanup()

	logger := zaptest.NewLogger(t)
	config := DefaultCompactionConfig()

	cm := NewCompactionManager(node, config, logger)
	ctx := context.Background()

	// Start components
	err := cm.Start(ctx)
	require.NoError(t, err)
	defer cm.Stop(ctx)

	err = node.Start(ctx)
	require.NoError(t, err)
	defer node.Stop(ctx)

	// Wait for leadership
	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Apply some entries
	for i := 0; i < 5; i++ {
		entry := &LogEntry{
			Type:      EntrySnapshotMark,
			Topic:     "compact-topic",
			Partition: uint32(i),
			Timestamp: time.Now(),
		}

		err = node.Apply(entry, 1*time.Second)
		require.NoError(t, err)
	}

	// Test manual compaction
	err = cm.CompactNow(ctx)
	require.NoError(t, err)

	// Verify stats updated
	stats := cm.GetStats()
	assert.Greater(t, stats.CompactionCount, uint64(0))
}

func TestCompactionManager_GetStats(t *testing.T) {
	node, cleanup := createTestNode(t, "stats-node", 18034)
	defer cleanup()

	logger := zaptest.NewLogger(t)
	config := DefaultCompactionConfig()

	cm := NewCompactionManager(node, config, logger)

	// Test initial stats
	stats := cm.GetStats()
	assert.Equal(t, uint64(0), stats.CompactionCount)
	assert.Equal(t, uint64(0), stats.BytesReclaimed)
}

func TestCompactionManager_GetCompactionInfo(t *testing.T) {
	node, cleanup := createTestNode(t, "info-node", 18035)
	defer cleanup()

	logger := zaptest.NewLogger(t)
	config := DefaultCompactionConfig()

	cm := NewCompactionManager(node, config, logger)

	// Test compaction info
	info := cm.GetCompactionInfo()
	assert.Contains(t, info, "auto_compaction_enabled")
	assert.Contains(t, info, "current_log_count")
	assert.Contains(t, info, "should_compact")
	assert.Contains(t, info, "thresholds")
}

func TestCompactionManager_EstimateCompactionBenefit(t *testing.T) {
	node, cleanup := createTestNode(t, "estimate-node", 18036)
	defer cleanup()

	logger := zaptest.NewLogger(t)
	config := DefaultCompactionConfig()

	cm := NewCompactionManager(node, config, logger)

	// Test estimation
	benefit, err := cm.EstimateCompactionBenefit()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, benefit, uint64(0))
}

func TestRecoveryManager_ConcurrentRecovery(t *testing.T) {
	// Test that recovery operations are thread-safe
	rm, cleanup := createTestRecoveryManager(t)
	defer cleanup()

	ctx := context.Background()

	// Start the node
	err := rm.node.Start(ctx)
	require.NoError(t, err)
	defer rm.node.Stop(ctx)

	err = rm.node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Run multiple recovery operations concurrently
	errCh := make(chan error, 3)

	go func() {
		errCh <- rm.VerifyIntegrity(ctx)
	}()

	go func() {
		errCh <- rm.RecoverPartitions(ctx)
	}()

	go func() {
		errCh <- rm.ValidateClusterState(ctx)
	}()

	// Wait for all operations to complete
	for i := 0; i < 3; i++ {
		err := <-errCh
		assert.NoError(t, err)
	}
}

func BenchmarkRecoveryManager_VerifyIntegrity(b *testing.B) {
	rm, cleanup := createTestRecoveryManagerForBenchmark(b)
	defer cleanup()

	ctx := context.Background()

	// Start the node
	err := rm.node.Start(ctx)
	require.NoError(b, err)
	defer rm.node.Stop(ctx)

	err = rm.node.WaitForLeader(5 * time.Second)
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := rm.VerifyIntegrity(ctx)
		if err != nil {
			b.Fatalf("VerifyIntegrity failed: %v", err)
		}
	}
}

func BenchmarkCompactionManager_ShouldCompact(b *testing.B) {
	node, cleanup := createTestNodeHelper(b, "bench-compact-node", 18037)
	defer cleanup()

	logger := zaptest.NewLogger(b)
	config := DefaultCompactionConfig()

	cm := NewCompactionManager(node, config, logger)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = cm.ShouldCompact()
	}
}
