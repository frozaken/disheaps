package handlers

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	disheapv1 "github.com/disheap/disheap"
	"github.com/disheap/disheap/disheap-api/pkg/auth"
	"github.com/disheap/disheap/disheap-api/pkg/client"
	"github.com/gin-gonic/gin"
)

// AdminHandler handles administrative operations for heaps
type AdminHandler struct {
	engineClient client.EngineClient
}

// NewAdminHandler creates a new admin handler
func NewAdminHandler(engineClient client.EngineClient) *AdminHandler {
	return &AdminHandler{
		engineClient: engineClient,
	}
}

// CreateHeapRequest represents a request to create a new heap
type CreateHeapRequest struct {
	Topic             string `json:"topic" binding:"required"`
	Mode              string `json:"mode" binding:"required"` // "MIN" or "MAX"
	Partitions        uint32 `json:"partitions"`              // optional, defaults to 1
	ReplicationFactor uint32 `json:"replication_factor"`      // optional, defaults to 1
	TopKBound         uint32 `json:"top_k_bound"`             // optional
	Description       string `json:"description"`             // optional
}

// UpdateHeapRequest represents a request to update an existing heap
type UpdateHeapRequest struct {
	TopKBound   *uint32 `json:"top_k_bound,omitempty"` // optional
	Description *string `json:"description,omitempty"` // optional
}

// HeapResponse represents a heap response
type HeapResponse struct {
	Topic             string             `json:"topic"`
	Mode              string             `json:"mode"`
	Partitions        uint32             `json:"partitions"`
	ReplicationFactor uint32             `json:"replication_factor"`
	TopKBound         uint32             `json:"top_k_bound"`
	Description       string             `json:"description"`
	CreatedAt         string             `json:"created_at"`
	Stats             *HeapStatsResponse `json:"stats,omitempty"`
}

// HeapStatsResponse represents heap statistics
type HeapStatsResponse struct {
	TotalMessages    uint64 `json:"total_messages"`
	InflightMessages uint64 `json:"inflight_messages"`
	DLQMessages      uint64 `json:"dlq_messages"`
	TotalEnqueues    uint64 `json:"total_enqueues"`
	TotalPops        uint64 `json:"total_pops"`
	TotalAcks        uint64 `json:"total_acks"`
	TotalNacks       uint64 `json:"total_nacks"`
}

// ListHeapsResponse represents a list of heaps response
type ListHeapsResponse struct {
	Heaps      []HeapResponse `json:"heaps"`
	PageSize   uint32         `json:"page_size"`
	PageToken  string         `json:"page_token,omitempty"`
	NextToken  string         `json:"next_token,omitempty"`
	TotalCount uint32         `json:"total_count,omitempty"`
}

// CreateHeap handles POST /v1/heaps
func (h *AdminHandler) CreateHeap(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		h.respondUnauthorized(c, "Authentication required")
		return
	}

	// Parse request body
	var req CreateHeapRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.respondBadRequest(c, "Invalid request body", err.Error())
		return
	}

	// Validate request
	if err := h.validateCreateHeapRequest(&req); err != nil {
		h.respondBadRequest(c, "Invalid request parameters", err.Error())
		return
	}

	// Convert mode to protobuf enum
	mode, err := h.convertModeToProto(req.Mode)
	if err != nil {
		h.respondBadRequest(c, "Invalid mode", err.Error())
		return
	}

	// Create heap request for engine
	makeReq := &disheapv1.MakeHeapReq{
		Topic:             req.Topic,
		Mode:              mode,
		Partitions:        req.Partitions,
		ReplicationFactor: req.ReplicationFactor,
		TopKBound:         req.TopKBound,
		// TODO: Add other configuration options as needed
	}

	// Call engine
	resp, err := h.engineClient.MakeHeap(c.Request.Context(), makeReq)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to create heap")
		return
	}

	// Convert response (MakeHeapResp only returns Topic and HeapId)
	heapResponse := &HeapResponse{
		Topic:             resp.Topic,
		Mode:              req.Mode,
		Partitions:        req.Partitions,
		ReplicationFactor: req.ReplicationFactor,
		TopKBound:         req.TopKBound,
		Description:       req.Description,
		CreatedAt:         "", // Not available from MakeHeapResp
	}

	c.JSON(http.StatusCreated, heapResponse)
}

// ListHeaps handles GET /v1/heaps
func (h *AdminHandler) ListHeaps(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		h.respondUnauthorized(c, "Authentication required")
		return
	}

	// Parse query parameters
	pageSize := uint32(50) // default
	if pageSizeStr := c.Query("page_size"); pageSizeStr != "" {
		if ps, err := strconv.ParseUint(pageSizeStr, 10, 32); err == nil && ps > 0 && ps <= 1000 {
			pageSize = uint32(ps)
		}
	}

	pageToken := c.Query("page_token")

	// Create list request for engine
	listReq := &disheapv1.ListHeapsReq{
		PageSize:  pageSize,
		PageToken: pageToken,
	}

	// Call engine
	resp, err := h.engineClient.ListHeaps(c.Request.Context(), listReq)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to list heaps")
		return
	}

	// Convert response
	heaps := make([]HeapResponse, len(resp.Heaps))
	for i, heap := range resp.Heaps {
		heaps[i] = HeapResponse{
			Topic:             heap.Topic,
			Mode:              h.convertModeFromProto(heap.Mode),
			Partitions:        heap.Partitions,
			ReplicationFactor: heap.ReplicationFactor,
			TopKBound:         heap.TopKBound,
			Description:       "", // Not available in HeapInfo
			CreatedAt:         "", // Not available in HeapInfo
		}
	}

	listResponse := &ListHeapsResponse{
		Heaps:     heaps,
		PageSize:  pageSize,
		PageToken: pageToken,
		NextToken: resp.NextPageToken,
	}

	c.JSON(http.StatusOK, listResponse)
}

// GetHeap handles GET /v1/heaps/{topic}
func (h *AdminHandler) GetHeap(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		h.respondUnauthorized(c, "Authentication required")
		return
	}

	topic := c.Param("topic")
	if topic == "" {
		h.respondBadRequest(c, "Topic parameter is required", "missing topic in URL path")
		return
	}

	// For now, we'll use the stats endpoint to get heap information
	// TODO: Add dedicated GetHeap endpoint to the engine when available
	statsReq := &disheapv1.StatsReq{
		Topic: &topic,
	}

	statsResp, err := h.engineClient.Stats(c.Request.Context(), statsReq)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to get heap information")
		return
	}

	// Check if topic exists in stats
	topicStats, exists := statsResp.TopicStats[topic]
	if !exists {
		h.respondNotFound(c, "Heap not found", "specified topic does not exist")
		return
	}

	// Create response (with limited info since we don't have a dedicated GetHeap endpoint)
	heapResponse := &HeapResponse{
		Topic:       topic,
		Mode:        "UNKNOWN", // TODO: Get from actual heap config when endpoint available
		Partitions:  1,         // TODO: Get from actual heap config
		TopKBound:   0,         // TODO: Get from actual heap config
		Description: "",        // TODO: Get from actual heap config
		CreatedAt:   "",        // TODO: Get from actual heap config
		Stats: &HeapStatsResponse{
			TotalMessages:    topicStats.TotalMessages,
			InflightMessages: topicStats.InflightMessages,
			DLQMessages:      topicStats.DlqMessages,
			TotalEnqueues:    topicStats.TotalEnqueues,
			TotalPops:        topicStats.TotalPops,
			TotalAcks:        topicStats.TotalAcks,
			TotalNacks:       topicStats.TotalNacks,
		},
	}

	c.JSON(http.StatusOK, heapResponse)
}

// UpdateHeap handles PATCH /v1/heaps/{topic}
func (h *AdminHandler) UpdateHeap(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		h.respondUnauthorized(c, "Authentication required")
		return
	}

	topic := c.Param("topic")
	if topic == "" {
		h.respondBadRequest(c, "Topic parameter is required", "missing topic in URL path")
		return
	}

	// Parse request body
	var req UpdateHeapRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.respondBadRequest(c, "Invalid request body", err.Error())
		return
	}

	// Create update request for engine
	updateReq := &disheapv1.UpdateHeapConfigReq{
		Topic: topic,
	}

	// Note: UpdateHeapConfigReq doesn't have TopKBound field
	// Only retention time and other config options are supported
	if req.TopKBound != nil {
		// TODO: Handle TopKBound updates when supported by the engine
		h.respondBadRequest(c, "TopKBound updates not yet supported", "engine API limitation")
		return
	}

	// Call engine
	err := h.engineClient.UpdateHeapConfig(c.Request.Context(), updateReq)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to update heap configuration")
		return
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{
		"message": "Heap configuration updated successfully",
		"topic":   topic,
	})
}

// DeleteHeap handles DELETE /v1/heaps/{topic}
func (h *AdminHandler) DeleteHeap(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		h.respondUnauthorized(c, "Authentication required")
		return
	}

	topic := c.Param("topic")
	if topic == "" {
		h.respondBadRequest(c, "Topic parameter is required", "missing topic in URL path")
		return
	}

	// TODO: Check for confirmation token for destructive operations
	// For now, we'll require a confirmation query parameter
	confirmation := c.Query("confirm")
	if confirmation != "true" {
		h.respondBadRequest(c, "Confirmation required", "add ?confirm=true to delete the heap")
		return
	}

	// Create delete request for engine
	deleteReq := &disheapv1.DeleteHeapReq{
		Topic: topic,
	}

	// Call engine
	err := h.engineClient.DeleteHeap(c.Request.Context(), deleteReq)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to delete heap")
		return
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{
		"message": "Heap deleted successfully",
		"topic":   topic,
	})
}

// PurgeHeap handles POST /v1/heaps/{topic}:purge
func (h *AdminHandler) PurgeHeap(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		h.respondUnauthorized(c, "Authentication required")
		return
	}

	topic := c.Param("topic")
	if topic == "" {
		h.respondBadRequest(c, "Topic parameter is required", "missing topic in URL path")
		return
	}

	// TODO: Check for confirmation token for destructive operations
	// For now, we'll require a confirmation query parameter
	confirmation := c.Query("confirm")
	if confirmation != "true" {
		h.respondBadRequest(c, "Confirmation required", "add ?confirm=true to purge the heap")
		return
	}

	// Create purge request for engine
	purgeReq := &disheapv1.PurgeReq{
		Topic: topic,
	}

	// Call engine
	err := h.engineClient.Purge(c.Request.Context(), purgeReq)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to purge heap")
		return
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{
		"message": "Heap purged successfully",
		"topic":   topic,
	})
}

// Helper methods

// validateCreateHeapRequest validates the create heap request
func (h *AdminHandler) validateCreateHeapRequest(req *CreateHeapRequest) error {
	// Validate topic name
	if len(req.Topic) == 0 || len(req.Topic) > 255 {
		return errors.New("topic name must be between 1 and 255 characters")
	}

	// Topic name should be alphanumeric with hyphens and underscores
	for _, char := range req.Topic {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_') {
			return errors.New("topic name can only contain alphanumeric characters, hyphens, and underscores")
		}
	}

	// Validate mode
	mode := strings.ToUpper(req.Mode)
	if mode != "MIN" && mode != "MAX" {
		return errors.New("mode must be either 'MIN' or 'MAX'")
	}

	// Validate partitions
	if req.Partitions == 0 {
		req.Partitions = 1 // default
	}
	if req.Partitions > 1000 {
		return errors.New("partitions cannot exceed 1000")
	}

	// Validate replication factor
	if req.ReplicationFactor == 0 {
		req.ReplicationFactor = 1 // default
	}
	if req.ReplicationFactor > req.Partitions {
		return errors.New("replication factor cannot exceed number of partitions")
	}

	return nil
}

// convertModeToProto converts string mode to protobuf enum
func (h *AdminHandler) convertModeToProto(mode string) (disheapv1.Mode, error) {
	switch strings.ToUpper(mode) {
	case "MIN":
		return disheapv1.Mode_MIN, nil
	case "MAX":
		return disheapv1.Mode_MAX, nil
	default:
		return disheapv1.Mode_MIN, errors.New("invalid mode: must be 'MIN' or 'MAX'")
	}
}

// convertModeFromProto converts protobuf enum to string
func (h *AdminHandler) convertModeFromProto(mode disheapv1.Mode) string {
	switch mode {
	case disheapv1.Mode_MIN:
		return "MIN"
	case disheapv1.Mode_MAX:
		return "MAX"
	default:
		return "UNKNOWN"
	}
}

// Error response helpers

func (h *AdminHandler) respondUnauthorized(c *gin.Context, message string) {
	c.JSON(http.StatusUnauthorized, gin.H{
		"error": map[string]interface{}{
			"code":    "UNAUTHENTICATED",
			"message": message,
			"details": []map[string]interface{}{
				{
					"field": "authorization",
					"issue": "authentication required",
				},
			},
		},
	})
}

func (h *AdminHandler) respondBadRequest(c *gin.Context, message, details string) {
	c.JSON(http.StatusBadRequest, gin.H{
		"error": map[string]interface{}{
			"code":    "INVALID_ARGUMENT",
			"message": message,
			"details": []map[string]interface{}{
				{
					"field": "request",
					"issue": details,
				},
			},
		},
	})
}

func (h *AdminHandler) respondNotFound(c *gin.Context, message, details string) {
	c.JSON(http.StatusNotFound, gin.H{
		"error": map[string]interface{}{
			"code":    "NOT_FOUND",
			"message": message,
			"details": []map[string]interface{}{
				{
					"field": "resource",
					"issue": details,
				},
			},
		},
	})
}

// handleGRPCError handles gRPC errors and converts them to appropriate HTTP responses
func (h *AdminHandler) handleGRPCError(c *gin.Context, err error, message string) {
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
