// Package raft provides distributed consensus and state machine replication
// for the Disheap priority messaging system using HashiCorp Raft.
package raft

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/zap"

	"github.com/disheap/disheap/disheap-engine/pkg/partition"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
)

// NodeConfig configures a Raft node
type NodeConfig struct {
	// Node identification
	NodeID            string // Unique node identifier
	RaftAddr          string // Raft bind address (host:port)
	RaftAdvertiseAddr string // Raft advertise address (what peers dial)
	DataDir           string // Directory for Raft data

	// Raft configuration
	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	CommitTimeout     time.Duration
	MaxAppendEntries  int
	BatchApply        bool
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
	TrailingLogs      uint64

	// Network configuration
	TransportTimeout time.Duration
	MaxPool          int

	// Recovery configuration
	EnableSingleNode bool     // Bootstrap as single-node cluster
	JoinAddresses    []string // Existing cluster addresses to join
}

// DefaultNodeConfig returns a production-ready node configuration
func DefaultNodeConfig(nodeID, raftAddr, dataDir string) *NodeConfig {
	return DefaultNodeConfigWithAddrs(nodeID, raftAddr, raftAddr, dataDir)
}

// DefaultNodeConfigWithAddrs returns a production-ready node configuration with separate bind/advertise
func DefaultNodeConfigWithAddrs(nodeID, raftBindAddr, raftAdvertiseAddr, dataDir string) *NodeConfig {
	return &NodeConfig{
		NodeID:            nodeID,
		RaftAddr:          raftBindAddr,      // Where we bind/listen
		RaftAdvertiseAddr: raftAdvertiseAddr, // What peers should dial
		DataDir:           dataDir,

		// Raft timeouts
		HeartbeatTimeout:  1000 * time.Millisecond,
		ElectionTimeout:   1000 * time.Millisecond,
		CommitTimeout:     50 * time.Millisecond,
		MaxAppendEntries:  64,
		BatchApply:        true,
		SnapshotInterval:  120 * time.Second,
		SnapshotThreshold: 8192,
		TrailingLogs:      10240,

		// Network configuration
		TransportTimeout: 10 * time.Second,
		MaxPool:          3,

		// Recovery
		EnableSingleNode: false,
		JoinAddresses:    []string{},
	}
}

// Node represents a single Raft node in the disheap cluster
type Node struct {
	config         *NodeConfig
	logger         *zap.Logger
	fsm            *HeapFSM
	transport      raft.Transport
	raft           *raft.Raft
	clusterManager *ClusterManager

	// Storage components
	logStore    raft.LogStore
	stableStore raft.StableStore
	snapStore   raft.SnapshotStore

	// Lifecycle management
	mu        sync.RWMutex
	running   bool
	stopCh    chan struct{}
	stoppedCh chan struct{}
}

// NodeStats contains Raft node statistics
type NodeStats struct {
	NodeID         string
	State          raft.RaftState
	Term           uint64
	LastLogIndex   uint64
	LastLogTerm    uint64
	CommitIndex    uint64
	AppliedIndex   uint64
	LastContact    time.Time
	LastSnapshot   time.Time
	IsLeader       bool
	LeaderID       raft.ServerID
	ClusterSize    int
	VotersCount    int
	NonVotersCount int
}

// NewNode creates a new Raft node
func NewNode(
	config *NodeConfig,
	storage storage.Storage,
	logger *zap.Logger,
) (*Node, error) {
	if config == nil {
		return nil, fmt.Errorf("node config cannot be nil")
	}

	if config.NodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}

	if config.RaftAddr == "" {
		return nil, fmt.Errorf("raft address cannot be empty")
	}

	if config.DataDir == "" {
		return nil, fmt.Errorf("data directory cannot be empty")
	}

	// Create FSM
	fsm := NewHeapFSM(storage, logger.Named("fsm"))

	node := &Node{
		config:    config,
		logger:    logger.Named("raft-node"),
		fsm:       fsm,
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}

	// Initialize cluster manager
	node.clusterManager = NewClusterManager(node, logger)

	return node, nil
}

// Start initializes and starts the Raft node
func (n *Node) Start(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.running {
		return fmt.Errorf("node is already running")
	}

	n.logger.Info("starting raft node",
		zap.String("node_id", n.config.NodeID),
		zap.String("raft_addr", n.config.RaftAddr),
		zap.String("data_dir", n.config.DataDir))

	// Create data directory
	if err := os.MkdirAll(n.config.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Setup Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(n.config.NodeID)
	raftConfig.HeartbeatTimeout = n.config.HeartbeatTimeout
	raftConfig.ElectionTimeout = n.config.ElectionTimeout
	raftConfig.CommitTimeout = n.config.CommitTimeout
	raftConfig.MaxAppendEntries = n.config.MaxAppendEntries
	// Note: BatchApply is not available in current Raft version
	raftConfig.SnapshotInterval = n.config.SnapshotInterval
	raftConfig.SnapshotThreshold = n.config.SnapshotThreshold
	raftConfig.TrailingLogs = n.config.TrailingLogs
	// Use default logger for now - custom logger integration can be improved later
	// raftConfig.Logger = NewRaftZapLogger(n.logger.Named("raft-lib"))

	// Setup transport - CRITICAL: bind vs advertise separation
	// Create TCP transport: first param is bind string, second is advertise TCPAddr
	advertiseAddr, err := net.ResolveTCPAddr("tcp", n.config.RaftAdvertiseAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve raft advertise address: %w", err)
	}

	transport, err := raft.NewTCPTransport(
		n.config.RaftAddr, // bind address string for listener
		advertiseAddr,     // advertised address peers will dial
		n.config.MaxPool,
		n.config.TransportTimeout,
		os.Stderr, // Raft library output
	)
	if err != nil {
		return fmt.Errorf("failed to create transport: %w", err)
	}
	n.transport = transport

	// Setup log store
	logPath := filepath.Join(n.config.DataDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logPath)
	if err != nil {
		return fmt.Errorf("failed to create log store: %w", err)
	}
	n.logStore = logStore

	// Setup stable store
	stablePath := filepath.Join(n.config.DataDir, "raft-stable.db")
	stableStore, err := raftboltdb.NewBoltStore(stablePath)
	if err != nil {
		return fmt.Errorf("failed to create stable store: %w", err)
	}
	n.stableStore = stableStore

	// Setup snapshot store
	snapDir := filepath.Join(n.config.DataDir, "snapshots")
	snapStore, err := raft.NewFileSnapshotStore(snapDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %w", err)
	}
	n.snapStore = snapStore

	// Create Raft instance
	raftNode, err := raft.NewRaft(
		raftConfig,
		n.fsm,
		n.logStore,
		n.stableStore,
		n.snapStore,
		n.transport,
	)
	if err != nil {
		return fmt.Errorf("failed to create raft node: %w", err)
	}
	n.raft = raftNode

	// Bootstrap cluster if needed
	if err := n.bootstrap(); err != nil {
		return fmt.Errorf("failed to bootstrap cluster: %w", err)
	}

	n.running = true

	// Start background monitoring
	go n.monitor()

	n.logger.Info("raft node started successfully",
		zap.String("node_id", n.config.NodeID),
		zap.String("raft_addr", n.config.RaftAddr))

	return nil
}

// Stop gracefully stops the Raft node
func (n *Node) Stop(ctx context.Context) error {
	n.mu.Lock()
	if !n.running {
		n.mu.Unlock()
		return nil
	}
	n.running = false
	n.mu.Unlock()

	n.logger.Info("stopping raft node")

	// Signal stop and wait
	close(n.stopCh)

	// Shutdown Raft
	shutdownFuture := n.raft.Shutdown()
	if err := shutdownFuture.Error(); err != nil {
		n.logger.Error("error during raft shutdown", zap.Error(err))
	}

	// Transport doesn't have a Close method in this version
	// Stores are managed by Raft and don't need explicit closing

	// Wait for monitor to stop
	<-n.stoppedCh

	n.logger.Info("raft node stopped")
	return nil
}

// Apply applies a log entry through Raft consensus
func (n *Node) Apply(entry *LogEntry, timeout time.Duration) error {
	if !n.IsLeader() {
		return fmt.Errorf("node is not leader")
	}

	data, err := entry.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode log entry: %w", err)
	}

	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	return nil
}

// GetStats returns node statistics
func (n *Node) GetStats() NodeStats {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if !n.running || n.raft == nil {
		return NodeStats{
			NodeID: n.config.NodeID,
			State:  raft.Shutdown,
		}
	}

	state := n.raft.State()
	stats := n.raft.Stats()

	// Parse stats
	var term, lastLogIndex, lastLogTerm, commitIndex, appliedIndex uint64
	fmt.Sscanf(stats["term"], "%d", &term)
	fmt.Sscanf(stats["last_log_index"], "%d", &lastLogIndex)
	fmt.Sscanf(stats["last_log_term"], "%d", &lastLogTerm)
	fmt.Sscanf(stats["commit_index"], "%d", &commitIndex)
	fmt.Sscanf(stats["applied_index"], "%d", &appliedIndex)

	var lastContact time.Time
	if contactStr, exists := stats["last_contact"]; exists {
		if contactNano, err := time.ParseDuration(contactStr + "ns"); err == nil {
			lastContact = time.Now().Add(-contactNano)
		}
	}

	// Get cluster configuration
	configFuture := n.raft.GetConfiguration()
	var clusterSize, votersCount, nonVotersCount int
	var leaderID raft.ServerID

	if configFuture.Error() == nil {
		config := configFuture.Configuration()
		clusterSize = len(config.Servers)

		for _, server := range config.Servers {
			if server.Suffrage == raft.Voter {
				votersCount++
			} else {
				nonVotersCount++
			}
		}
	}

	if state == raft.Leader {
		leaderID = raft.ServerID(n.config.NodeID)
	} else {
		// Leader() returns ServerAddress, but we need ServerID
		// For now, we'll use the address as the ID (they're often the same)
		leaderAddr := n.raft.Leader()
		leaderID = raft.ServerID(leaderAddr)
	}

	return NodeStats{
		NodeID:         n.config.NodeID,
		State:          state,
		Term:           term,
		LastLogIndex:   lastLogIndex,
		LastLogTerm:    lastLogTerm,
		CommitIndex:    commitIndex,
		AppliedIndex:   appliedIndex,
		LastContact:    lastContact,
		IsLeader:       state == raft.Leader,
		LeaderID:       leaderID,
		ClusterSize:    clusterSize,
		VotersCount:    votersCount,
		NonVotersCount: nonVotersCount,
	}
}

// IsLeader returns true if this node is the Raft leader
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if !n.running || n.raft == nil {
		return false
	}

	return n.raft.State() == raft.Leader
}

// GetClusterManager returns the cluster manager for join operations
func (n *Node) GetClusterManager() *ClusterManager {
	return n.clusterManager
}

// GetLeader returns the current leader address
func (n *Node) GetLeader() raft.ServerAddress {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if !n.running || n.raft == nil {
		return ""
	}

	return n.raft.Leader()
}

// AddServer adds a new server to the cluster
func (n *Node) AddServer(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration) error {
	if !n.IsLeader() {
		return fmt.Errorf("not leader")
	}

	future := n.raft.AddVoter(id, address, prevIndex, timeout)
	return future.Error()
}

// RemoveServer removes a server from the cluster
func (n *Node) RemoveServer(id raft.ServerID, prevIndex uint64, timeout time.Duration) error {
	if !n.IsLeader() {
		return fmt.Errorf("not leader")
	}

	future := n.raft.RemoveServer(id, prevIndex, timeout)
	return future.Error()
}

// AddPartition adds a partition to the FSM
func (n *Node) AddPartition(topic string, partitionID uint32, partition partition.Partition) {
	n.fsm.AddPartition(topic, partitionID, partition)
}

// RemovePartition removes a partition from the FSM
func (n *Node) RemovePartition(topic string, partitionID uint32) {
	n.fsm.RemovePartition(topic, partitionID)
}

// Snapshot triggers a manual snapshot
func (n *Node) Snapshot() error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if !n.running || n.raft == nil {
		return fmt.Errorf("node is not running")
	}

	future := n.raft.Snapshot()
	return future.Error()
}

// WaitForLeader waits for a leader to be elected
func (n *Node) WaitForLeader(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for leader")
		case <-ticker.C:
			if leader := n.GetLeader(); leader != "" {
				return nil
			}
		}
	}
}

// WaitForAppliedIndex waits for the given index to be applied
func (n *Node) WaitForAppliedIndex(index uint64, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for applied index %d", index)
		case <-ticker.C:
			stats := n.GetStats()
			if stats.AppliedIndex >= index {
				return nil
			}
		}
	}
}

func (n *Node) bootstrap() error {
	// Check if already bootstrapped
	hasState, err := raft.HasExistingState(n.logStore, n.stableStore, n.snapStore)
	if err != nil {
		return fmt.Errorf("failed to check existing state: %w", err)
	}

	if hasState {
		n.logger.Info("node has existing state, skipping bootstrap")
		return nil
	}

	// Bootstrap options
	if n.config.EnableSingleNode {
		// Bootstrap as single-node cluster
		n.logger.Info("bootstrapping single-node cluster")

		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(n.config.NodeID),
					Address: raft.ServerAddress(n.config.RaftAdvertiseAddr),
				},
			},
		}

		if err := n.raft.BootstrapCluster(configuration).Error(); err != nil {
			return fmt.Errorf("failed to bootstrap cluster: %w", err)
		}
	} else if len(n.config.JoinAddresses) > 0 {
		// Join via internal path (kept for completeness, but not used in compose)
		n.logger.Info("attempting to join existing cluster",
			zap.Strings("addresses", n.config.JoinAddresses))

		joinTimeout := 30 * time.Second // Configurable timeout
		if err := n.clusterManager.JoinCluster(n.config.JoinAddresses, joinTimeout); err != nil {
			return fmt.Errorf("failed to join cluster: %w", err)
		}
	} else {
		n.logger.Info("no bootstrap configuration provided, waiting for cluster setup")
	}

	return nil
}

func (n *Node) monitor() {
	defer close(n.stoppedCh)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.logStats()
		}
	}
}

func (n *Node) logStats() {
	stats := n.GetStats()

	n.logger.Debug("raft node stats",
		zap.String("node_id", stats.NodeID),
		zap.String("state", stats.State.String()),
		zap.Uint64("term", stats.Term),
		zap.Uint64("last_log_index", stats.LastLogIndex),
		zap.Uint64("commit_index", stats.CommitIndex),
		zap.Uint64("applied_index", stats.AppliedIndex),
		zap.Bool("is_leader", stats.IsLeader),
		zap.String("leader_id", string(stats.LeaderID)),
		zap.Int("cluster_size", stats.ClusterSize))
}

// NewRaftZapLogger creates a logger adapter for HashiCorp Raft
func NewRaftZapLogger(logger *zap.Logger) *RaftZapLogger {
	return &RaftZapLogger{logger: logger}
}

// RaftZapLogger adapts zap.Logger to raft.Logger interface
type RaftZapLogger struct {
	logger *zap.Logger
}

func (r *RaftZapLogger) Debug(msg string, args ...interface{}) {
	if len(args) > 0 {
		r.logger.Debug(fmt.Sprintf(msg, args...))
	} else {
		r.logger.Debug(msg)
	}
}

func (r *RaftZapLogger) Info(msg string, args ...interface{}) {
	if len(args) > 0 {
		r.logger.Info(fmt.Sprintf(msg, args...))
	} else {
		r.logger.Info(msg)
	}
}

func (r *RaftZapLogger) Warn(msg string, args ...interface{}) {
	if len(args) > 0 {
		r.logger.Warn(fmt.Sprintf(msg, args...))
	} else {
		r.logger.Warn(msg)
	}
}

func (r *RaftZapLogger) Error(msg string, args ...interface{}) {
	if len(args) > 0 {
		r.logger.Error(fmt.Sprintf(msg, args...))
	} else {
		r.logger.Error(msg)
	}
}
