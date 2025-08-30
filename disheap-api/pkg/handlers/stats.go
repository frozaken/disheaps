package handlers

import (
	"net/http"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"github.com/disheap/disheap/disheap-api/pkg/auth"
	"github.com/disheap/disheap/disheap-api/pkg/client"
	"github.com/gin-gonic/gin"
)

// StatsHandler handles statistics and monitoring endpoints
type StatsHandler struct {
	engineClient client.EngineClient
}

// NewStatsHandler creates a new stats handler
func NewStatsHandler(engineClient client.EngineClient) *StatsHandler {
	return &StatsHandler{
		engineClient: engineClient,
	}
}

// GetGlobalStats handles GET /v1/stats
func (h *StatsHandler) GetGlobalStats(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": map[string]interface{}{
				"code":    "UNAUTHENTICATED",
				"message": "Authentication required",
				"details": []map[string]interface{}{
					{
						"field": "authorization",
						"issue": "missing or invalid credentials",
					},
				},
			},
		})
		return
	}

	// Create stats request
	req := &disheapv1.StatsReq{}

	// Call engine
	resp, err := h.engineClient.Stats(c.Request.Context(), req)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to get global statistics")
		return
	}

	// Convert to JSON response
	c.JSON(http.StatusOK, map[string]interface{}{
		"global_stats": convertHeapStatsToJSON(resp.GlobalStats),
		"topic_stats":  convertTopicStatsToJSON(resp.TopicStats),
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
	})
}

// GetTopicStats handles GET /v1/stats/{topic}
func (h *StatsHandler) GetTopicStats(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": map[string]interface{}{
				"code":    "UNAUTHENTICATED",
				"message": "Authentication required",
				"details": []map[string]interface{}{
					{
						"field": "authorization",
						"issue": "missing or invalid credentials",
					},
				},
			},
		})
		return
	}

	topic := c.Param("topic")
	if topic == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": map[string]interface{}{
				"code":    "INVALID_ARGUMENT",
				"message": "Topic parameter is required",
				"details": []map[string]interface{}{
					{
						"field": "topic",
						"issue": "missing topic parameter in URL path",
					},
				},
			},
		})
		return
	}

	// Create stats request for specific topic
	req := &disheapv1.StatsReq{
		Topic: &topic,
	}

	// Call engine
	resp, err := h.engineClient.Stats(c.Request.Context(), req)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to get topic statistics")
		return
	}

	// Get topic-specific stats
	topicStats, exists := resp.TopicStats[topic]
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{
			"error": map[string]interface{}{
				"code":    "NOT_FOUND",
				"message": "Topic not found",
				"details": []map[string]interface{}{
					{
						"field": "topic",
						"issue": "specified topic does not exist",
					},
				},
			},
		})
		return
	}

	// Convert to JSON response
	c.JSON(http.StatusOK, map[string]interface{}{
		"topic":     topic,
		"stats":     convertHeapStatsToJSON(topicStats),
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// GetHealthStats handles GET /v1/health/stats (internal health statistics)
func (h *StatsHandler) GetHealthStats(c *gin.Context) {
	stats := map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"engine": map[string]interface{}{
			"connected": h.engineClient.IsHealthy(),
		},
		"server": map[string]interface{}{
			"uptime": "unknown", // TODO: Track actual uptime
			"status": "healthy",
		},
	}

	statusCode := http.StatusOK
	if !h.engineClient.IsHealthy() {
		statusCode = http.StatusServiceUnavailable
		stats["engine"].(map[string]interface{})["status"] = "unhealthy"
	}

	c.JSON(statusCode, stats)
}

// convertHeapStatsToJSON converts protobuf HeapStats to JSON
func convertHeapStatsToJSON(stats *disheapv1.HeapStats) map[string]interface{} {
	if stats == nil {
		return nil
	}

	result := map[string]interface{}{
		"total_messages":    stats.TotalMessages,
		"inflight_messages": stats.InflightMessages,
		"dlq_messages":      stats.DlqMessages,
		"total_enqueues":    stats.TotalEnqueues,
		"total_pops":        stats.TotalPops,
		"total_acks":        stats.TotalAcks,
		"total_nacks":       stats.TotalNacks,
		"total_retries":     stats.TotalRetries,
		"total_timeouts":    stats.TotalTimeouts,
	}

	// Convert partition stats
	if len(stats.PartitionStats) > 0 {
		partitionStats := make(map[string]interface{})
		for partitionID, partStats := range stats.PartitionStats {
			partitionStats[string(rune(partitionID))] = map[string]interface{}{
				"partition_id":  partStats.PartitionId,
				"messages":      partStats.Messages,
				"inflight":      partStats.Inflight,
				"leader_node":   partStats.LeaderNode,
				"replica_nodes": partStats.ReplicaNodes,
				"is_healthy":    partStats.IsHealthy,
			}
		}
		result["partition_stats"] = partitionStats
	}

	return result
}

// convertTopicStatsToJSON converts topic stats map to JSON
func convertTopicStatsToJSON(topicStats map[string]*disheapv1.HeapStats) map[string]interface{} {
	if topicStats == nil {
		return nil
	}

	result := make(map[string]interface{})
	for topic, stats := range topicStats {
		result[topic] = convertHeapStatsToJSON(stats)
	}

	return result
}

// handleGRPCError handles gRPC errors and converts them to appropriate HTTP responses
func (h *StatsHandler) handleGRPCError(c *gin.Context, err error, message string) {
	// TODO: Implement proper gRPC status code to HTTP status code mapping
	// For now, return a generic internal server error
	c.JSON(http.StatusInternalServerError, gin.H{
		"error": map[string]interface{}{
			"code":    "INTERNAL",
			"message": message,
			"details": []map[string]interface{}{
				{
					"field": "grpc_call",
					"issue": err.Error(),
				},
			},
		},
	})
}
