package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"bytes"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

// ClusterManager manages Raft cluster membership and operations
type ClusterManager struct {
	node   *Node
	logger *zap.Logger
	mu     sync.RWMutex
}

// ServerInfo represents information about a cluster member
type ServerInfo struct {
	ID       raft.ServerID
	Address  raft.ServerAddress
	Suffrage raft.ServerSuffrage
	IsLeader bool
	IsOnline bool
}

// ClusterInfo represents the current cluster state
type ClusterInfo struct {
	LeaderID   raft.ServerID
	LeaderAddr raft.ServerAddress
	Term       uint64
	Servers    []ServerInfo
	VoterCount int
	TotalCount int
	IsHealthy  bool
}

// NewClusterManager creates a new cluster manager
func NewClusterManager(node *Node, logger *zap.Logger) *ClusterManager {
	return &ClusterManager{
		node:   node,
		logger: logger.Named("cluster-manager"),
	}
}

// JoinCluster attempts to join an existing cluster
func (cm *ClusterManager) JoinCluster(addresses []string, timeout time.Duration) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.node.IsLeader() {
		return fmt.Errorf("cannot join cluster: node is already a leader")
	}

	cm.logger.Info("attempting to join cluster",
		zap.Strings("addresses", addresses),
		zap.Duration("timeout", timeout))

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Try each address until one succeeds
	var lastErr error
	for _, addr := range addresses {
		cm.logger.Debug("trying to contact cluster member", zap.String("address", addr))

		// Create temporary transport for join request
		if err := cm.requestJoin(ctx, addr); err != nil {
			lastErr = err
			cm.logger.Warn("failed to join via address",
				zap.String("address", addr),
				zap.Error(err))
			continue
		}

		cm.logger.Info("successfully joined cluster via address", zap.String("address", addr))
		return nil
	}

	return fmt.Errorf("failed to join cluster via any address: %w", lastErr)
}

// AddVoter adds a voting member to the cluster
func (cm *ClusterManager) AddVoter(nodeID string, address string, timeout time.Duration) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.node.IsLeader() {
		return fmt.Errorf("not leader")
	}

	cm.logger.Info("adding voter to cluster",
		zap.String("node_id", nodeID),
		zap.String("address", address))

	future := cm.node.raft.AddVoter(
		raft.ServerID(nodeID),
		raft.ServerAddress(address),
		0, // prevIndex - use 0 for latest
		timeout,
	)

	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	cm.logger.Info("successfully added voter to cluster",
		zap.String("node_id", nodeID))

	return nil
}

// AddNonvoter adds a non-voting member to the cluster
func (cm *ClusterManager) AddNonvoter(nodeID string, address string, timeout time.Duration) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.node.IsLeader() {
		return fmt.Errorf("not leader")
	}

	cm.logger.Info("adding nonvoter to cluster",
		zap.String("node_id", nodeID),
		zap.String("address", address))

	future := cm.node.raft.AddNonvoter(
		raft.ServerID(nodeID),
		raft.ServerAddress(address),
		0, // prevIndex - use 0 for latest
		timeout,
	)

	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add nonvoter: %w", err)
	}

	cm.logger.Info("successfully added nonvoter to cluster",
		zap.String("node_id", nodeID))

	return nil
}

// RemoveServer removes a member from the cluster
func (cm *ClusterManager) RemoveServer(nodeID string, timeout time.Duration) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.node.IsLeader() {
		return fmt.Errorf("not leader")
	}

	cm.logger.Info("removing server from cluster", zap.String("node_id", nodeID))

	future := cm.node.raft.RemoveServer(
		raft.ServerID(nodeID),
		0, // prevIndex - use 0 for latest
		timeout,
	)

	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to remove server: %w", err)
	}

	cm.logger.Info("successfully removed server from cluster",
		zap.String("node_id", nodeID))

	return nil
}

// DemoteVoter demotes a voter to a nonvoter
func (cm *ClusterManager) DemoteVoter(nodeID string, timeout time.Duration) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.node.IsLeader() {
		return fmt.Errorf("not leader")
	}

	cm.logger.Info("demoting voter", zap.String("node_id", nodeID))

	future := cm.node.raft.DemoteVoter(
		raft.ServerID(nodeID),
		0, // prevIndex - use 0 for latest
		timeout,
	)

	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to demote voter: %w", err)
	}

	cm.logger.Info("successfully demoted voter", zap.String("node_id", nodeID))
	return nil
}

// GetClusterInfo returns current cluster information
func (cm *ClusterManager) GetClusterInfo() (*ClusterInfo, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if !cm.node.running || cm.node.raft == nil {
		return nil, fmt.Errorf("node is not running")
	}

	// Get configuration
	configFuture := cm.node.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return nil, fmt.Errorf("failed to get configuration: %w", err)
	}

	config := configFuture.Configuration()
	stats := cm.node.GetStats()

	// Build server info
	servers := make([]ServerInfo, len(config.Servers))
	voterCount := 0

	for i, server := range config.Servers {
		isVoter := server.Suffrage == raft.Voter
		if isVoter {
			voterCount++
		}

		servers[i] = ServerInfo{
			ID:       server.ID,
			Address:  server.Address,
			Suffrage: server.Suffrage,
			IsLeader: server.ID == stats.LeaderID,
			IsOnline: true, // Health status determined by Raft state
		}
	}

	// Determine cluster health
	isHealthy := stats.LeaderID != "" && voterCount >= (len(servers)/2+1)

	return &ClusterInfo{
		LeaderID:   stats.LeaderID,
		LeaderAddr: cm.node.GetLeader(),
		Term:       stats.Term,
		Servers:    servers,
		VoterCount: voterCount,
		TotalCount: len(servers),
		IsHealthy:  isHealthy,
	}, nil
}

// TransferLeadership attempts to transfer leadership to another node
func (cm *ClusterManager) TransferLeadership(targetNodeID string, timeout time.Duration) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.node.IsLeader() {
		return fmt.Errorf("not leader")
	}

	cm.logger.Info("transferring leadership",
		zap.String("target_node_id", targetNodeID))

	future := cm.node.raft.LeadershipTransfer()

	// Wait for leadership transfer to complete
	if err := future.Error(); err != nil {
		return fmt.Errorf("leadership transfer failed: %w", err)
	}

	cm.logger.Info("leadership transfer completed")
	return nil
}

// VerifyLeader verifies that this node is still the leader
func (cm *ClusterManager) VerifyLeader() error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if !cm.node.running || cm.node.raft == nil {
		return fmt.Errorf("node is not running")
	}

	future := cm.node.raft.VerifyLeader()
	return future.Error()
}

// Barrier ensures all prior operations have been applied
func (cm *ClusterManager) Barrier(timeout time.Duration) error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if !cm.node.IsLeader() {
		return fmt.Errorf("not leader")
	}

	future := cm.node.raft.Barrier(timeout)
	return future.Error()
}

// GetConfiguration returns the current cluster configuration
func (cm *ClusterManager) GetConfiguration() (*raft.Configuration, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if !cm.node.running || cm.node.raft == nil {
		return nil, fmt.Errorf("node is not running")
	}

	future := cm.node.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}

	config := future.Configuration()
	return &config, nil
}

// WaitForCluster waits for the cluster to have the specified number of members
func (cm *ClusterManager) WaitForCluster(minMembers int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for cluster with %d members", minMembers)
		case <-ticker.C:
			info, err := cm.GetClusterInfo()
			if err != nil {
				cm.logger.Debug("error getting cluster info while waiting", zap.Error(err))
				continue
			}

			if info.TotalCount >= minMembers {
				cm.logger.Info("cluster size requirement met",
					zap.Int("current_size", info.TotalCount),
					zap.Int("min_required", minMembers))
				return nil
			}

			cm.logger.Debug("waiting for more cluster members",
				zap.Int("current_size", info.TotalCount),
				zap.Int("min_required", minMembers))
		}
	}
}

func (cm *ClusterManager) requestJoin(ctx context.Context, address string) error {
	cm.logger.Info("requesting join from cluster member", zap.String("address", address))

	// Wait a bit for the Raft instance to be ready
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}

	// Build our identity
	myID := cm.node.config.NodeID
	myAddr := cm.node.config.RaftAdvertiseAddr

	// Extract host from join address (strip any port)
	leaderHost := address
	if idx := strings.Index(address, ":"); idx != -1 {
		leaderHost = address[:idx]
	}
	joinURL := fmt.Sprintf("http://%s:%d/join", leaderHost, 8080)

	cm.logger.Info("attempting HTTP join",
		zap.String("url", joinURL),
		zap.String("node_id", myID),
		zap.String("node_addr", myAddr))

	// Try HTTP join against the target's HTTP port
	maxAttempts := 10
	for attempt := 0; attempt < maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		payload := map[string]string{
			"node_id": myID,
			"address": myAddr,
		}
		buf := new(bytes.Buffer)
		if err := json.NewEncoder(buf).Encode(payload); err != nil {
			return fmt.Errorf("failed to encode join payload: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL, buf)
		if err != nil {
			return fmt.Errorf("failed to create join request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cm.logger.Info("join request failed, retrying",
				zap.Int("attempt", attempt+1),
				zap.Error(err))
			time.Sleep(3 * time.Second)
			continue
		}
		if resp.Body != nil {
			resp.Body.Close()
		}

		if resp.StatusCode == http.StatusOK {
			cm.logger.Info("successfully joined cluster via leader HTTP",
				zap.String("url", joinURL))
			return nil
		}

		cm.logger.Info("join rejected by leader, retrying",
			zap.Int("attempt", attempt+1),
			zap.Int("status", resp.StatusCode))
		time.Sleep(3 * time.Second)
	}

	return fmt.Errorf("failed to join cluster after %d attempts", maxAttempts)
}

// IsClusterHealthy returns true if the cluster has a quorum and leader
func (cm *ClusterManager) IsClusterHealthy() (bool, error) {
	info, err := cm.GetClusterInfo()
	if err != nil {
		return false, err
	}

	return info.IsHealthy, nil
}

// GetClusterTopology returns detailed cluster topology information
func (cm *ClusterManager) GetClusterTopology() (map[string]interface{}, error) {
	info, err := cm.GetClusterInfo()
	if err != nil {
		return nil, err
	}

	topology := map[string]interface{}{
		"leader":      string(info.LeaderID),
		"leader_addr": string(info.LeaderAddr),
		"term":        info.Term,
		"voter_count": info.VoterCount,
		"total_count": info.TotalCount,
		"is_healthy":  info.IsHealthy,
		"servers":     make([]map[string]interface{}, len(info.Servers)),
	}

	for i, server := range info.Servers {
		topology["servers"].([]map[string]interface{})[i] = map[string]interface{}{
			"id":        string(server.ID),
			"address":   string(server.Address),
			"suffrage":  server.Suffrage.String(),
			"is_leader": server.IsLeader,
			"is_online": server.IsOnline,
		}
	}

	return topology, nil
}
