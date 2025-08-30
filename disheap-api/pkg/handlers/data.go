package handlers

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"github.com/disheap/disheap/disheap-api/pkg/auth"
	"github.com/disheap/disheap/disheap-api/pkg/client"
	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/types/known/durationpb"
)

// DataHandler handles data operations (enqueue, ack, nack, peek, etc.)
type DataHandler struct {
	engineClient client.EngineClient
}

// NewDataHandler creates a new data handler
func NewDataHandler(engineClient client.EngineClient) *DataHandler {
	return &DataHandler{
		engineClient: engineClient,
	}
}

// EnqueueRequest represents a request to enqueue a message
type EnqueueRequest struct {
	Topic        string  `json:"topic" binding:"required"`
	Payload      string  `json:"payload" binding:"required"` // Base64 encoded payload
	Priority     int64   `json:"priority"`                   // Priority for the message
	PartitionKey *string `json:"partition_key,omitempty"`    // Optional partition key
	ProducerID   *string `json:"producer_id,omitempty"`      // Optional producer ID
}

// EnqueueBatchRequest represents a batch enqueue request
type EnqueueBatchRequest struct {
	Messages      []EnqueueRequest `json:"messages" binding:"required,min=1,max=1000"`
	TransactionID *string          `json:"transaction_id,omitempty"` // Optional transaction ID
}

// EnqueueResponse represents the response from enqueue operations
type EnqueueResponse struct {
	MessageID string `json:"message_id"`
	Topic     string `json:"topic"`
	Priority  int64  `json:"priority"`
}

// EnqueueBatchResponse represents the response from batch enqueue operations
type EnqueueBatchResponse struct {
	Messages  []BatchEnqueueResult `json:"messages"`
	Succeeded int                  `json:"succeeded"`
	Failed    int                  `json:"failed"`
}

// BatchEnqueueResult represents the result of a single message in a batch enqueue
type BatchEnqueueResult struct {
	ID        string `json:"id,omitempty"`         // Original ID from request
	MessageID string `json:"message_id,omitempty"` // Generated message ID on success
	Priority  int64  `json:"priority,omitempty"`   // Message priority
	Error     string `json:"error,omitempty"`      // Error message on failure
}

// AckRequest represents a request to acknowledge messages
type AckRequest struct {
	Topic      string `json:"topic" binding:"required"`
	MessageID  string `json:"message_id" binding:"required"`
	LeaseToken string `json:"lease_token" binding:"required"`
}

// NackRequest represents a request to negatively acknowledge messages
type NackRequest struct {
	Topic           string  `json:"topic" binding:"required"`
	MessageID       string  `json:"message_id" binding:"required"`
	LeaseToken      string  `json:"lease_token" binding:"required"`
	BackoffOverride *int64  `json:"backoff_override,omitempty"` // Backoff override in seconds
	Reason          *string `json:"reason,omitempty"`           // Reason for nack
}

// ExtendLeaseRequest represents a request to extend message lease
type ExtendLeaseRequest struct {
	Topic      string `json:"topic" binding:"required"`
	LeaseToken string `json:"lease_token" binding:"required"`
	Extension  int64  `json:"extension" binding:"required"` // Extension duration in seconds
}

// PeekRequest represents a request to peek at messages
type PeekRequest struct {
	Topic string `json:"topic" binding:"required"`
	Limit uint32 `json:"limit,omitempty"` // Optional limit, defaults to 10
}

// PeekResponse represents the response from a peek operation
type PeekResponse struct {
	Topic    string        `json:"topic"`
	Messages []PeekMessage `json:"messages"`
	Count    int           `json:"count"`
}

// PeekMessage represents a message in a peek response
type PeekMessage struct {
	ID       string            `json:"id"`
	Payload  string            `json:"payload"` // Base64 encoded payload
	Priority int64             `json:"priority"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Enqueue handles POST /v1/enqueue
func (h *DataHandler) Enqueue(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		h.respondUnauthorized(c, "Authentication required")
		return
	}

	// Parse request body
	var req EnqueueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.respondBadRequest(c, "Invalid request body", err.Error())
		return
	}

	// Validate request
	if err := h.validateEnqueueRequest(&req); err != nil {
		h.respondBadRequest(c, "Invalid request parameters", err.Error())
		return
	}

	// Decode base64 payload
	payload, err := base64.StdEncoding.DecodeString(req.Payload)
	if err != nil {
		h.respondBadRequest(c, "Invalid payload encoding", "payload must be base64 encoded")
		return
	}

	// Create enqueue request for engine
	enqReq := &disheapv1.EnqueueReq{
		Topic:    req.Topic,
		Payload:  payload,
		Priority: req.Priority,
	}

	// Add optional parameters
	if req.PartitionKey != nil {
		enqReq.PartitionKey = req.PartitionKey
	}
	if req.ProducerID != nil {
		enqReq.ProducerId = req.ProducerID
	}

	// Call engine
	resp, err := h.engineClient.Enqueue(c.Request.Context(), enqReq)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to enqueue message")
		return
	}

	// Return response
	response := &EnqueueResponse{
		MessageID: resp.MessageId,
		Topic:     req.Topic,
		Priority:  req.Priority,
	}

	c.JSON(http.StatusOK, response)
}

// EnqueueBatch handles POST /v1/enqueue:batch
func (h *DataHandler) EnqueueBatch(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		h.respondUnauthorized(c, "Authentication required")
		return
	}

	// Parse request body
	var req EnqueueBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.respondBadRequest(c, "Invalid request body", err.Error())
		return
	}

	// Validate request
	if err := h.validateEnqueueBatchRequest(&req); err != nil {
		h.respondBadRequest(c, "Invalid request parameters", err.Error())
		return
	}

	// Prepare batch requests for engine
	requests := make([]*disheapv1.EnqueueReq, len(req.Messages))
	for i, msg := range req.Messages {
		// Decode payload
		payload, err := base64.StdEncoding.DecodeString(msg.Payload)
		if err != nil {
			h.respondBadRequest(c, "Invalid payload encoding", "message "+strconv.Itoa(i)+": payload must be base64 encoded")
			return
		}

		// Create enqueue request
		enqReq := &disheapv1.EnqueueReq{
			Topic:    msg.Topic,
			Payload:  payload,
			Priority: msg.Priority,
		}

		// Add optional parameters
		if msg.PartitionKey != nil {
			enqReq.PartitionKey = msg.PartitionKey
		}
		if msg.ProducerID != nil {
			enqReq.ProducerId = msg.ProducerID
		}

		requests[i] = enqReq
	}

	// Create batch enqueue request for engine
	batchReq := &disheapv1.EnqueueBatchReq{
		Requests: requests,
	}

	// Add optional transaction ID
	if req.TransactionID != nil {
		batchReq.TransactionId = req.TransactionID
	}

	// Call engine
	resp, err := h.engineClient.EnqueueBatch(c.Request.Context(), batchReq)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to enqueue batch")
		return
	}

	// Process batch response
	results := make([]BatchEnqueueResult, len(req.Messages))
	succeeded := 0
	failed := 0

	for i, enqResp := range resp.Responses {
		batchResult := BatchEnqueueResult{}

		if i < len(req.Messages) {
			batchResult.Priority = req.Messages[i].Priority
		}

		// EnqueueResp should contain MessageId and possibly error info
		if enqResp != nil {
			batchResult.MessageID = enqResp.MessageId
			succeeded++
		} else {
			batchResult.Error = "No response received"
			failed++
		}

		results[i] = batchResult
	}

	// Return response
	response := &EnqueueBatchResponse{
		Messages:  results,
		Succeeded: succeeded,
		Failed:    failed,
	}

	c.JSON(http.StatusOK, response)
}

// Ack handles POST /v1/ack
func (h *DataHandler) Ack(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		h.respondUnauthorized(c, "Authentication required")
		return
	}

	// Parse request body
	var req AckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.respondBadRequest(c, "Invalid request body", err.Error())
		return
	}

	// Validate request
	if err := h.validateAckRequest(&req); err != nil {
		h.respondBadRequest(c, "Invalid request parameters", err.Error())
		return
	}

	// Create ack request for engine
	ackReq := &disheapv1.AckReq{
		Topic:      req.Topic,
		MessageId:  req.MessageID,
		LeaseToken: req.LeaseToken,
	}

	// Call engine
	err := h.engineClient.Ack(c.Request.Context(), ackReq)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to acknowledge messages")
		return
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{
		"message":    "Message acknowledged successfully",
		"topic":      req.Topic,
		"message_id": req.MessageID,
	})
}

// Nack handles POST /v1/nack
func (h *DataHandler) Nack(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		h.respondUnauthorized(c, "Authentication required")
		return
	}

	// Parse request body
	var req NackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.respondBadRequest(c, "Invalid request body", err.Error())
		return
	}

	// Validate request
	if err := h.validateNackRequest(&req); err != nil {
		h.respondBadRequest(c, "Invalid request parameters", err.Error())
		return
	}

	// Create nack request for engine
	nackReq := &disheapv1.NackReq{
		Topic:      req.Topic,
		MessageId:  req.MessageID,
		LeaseToken: req.LeaseToken,
	}

	// Add optional parameters
	if req.BackoffOverride != nil {
		nackReq.BackoffOverride = durationpb.New(time.Duration(*req.BackoffOverride) * time.Second)
	}
	if req.Reason != nil {
		nackReq.Reason = req.Reason
	}

	// Call engine
	err := h.engineClient.Nack(c.Request.Context(), nackReq)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to nack messages")
		return
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{
		"message":    "Message nacked successfully",
		"topic":      req.Topic,
		"message_id": req.MessageID,
	})
}

// ExtendLease handles POST /v1/extend
func (h *DataHandler) ExtendLease(c *gin.Context) {
	// Verify authentication
	if !auth.IsAuthenticated(c) {
		h.respondUnauthorized(c, "Authentication required")
		return
	}

	// Parse request body
	var req ExtendLeaseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.respondBadRequest(c, "Invalid request body", err.Error())
		return
	}

	// Validate request
	if err := h.validateExtendLeaseRequest(&req); err != nil {
		h.respondBadRequest(c, "Invalid request parameters", err.Error())
		return
	}

	// Create extend request for engine
	extendReq := &disheapv1.ExtendReq{
		Topic:      req.Topic,
		LeaseToken: req.LeaseToken,
		Extension:  durationpb.New(time.Duration(req.Extension) * time.Second),
	}

	// Call engine
	err := h.engineClient.Extend(c.Request.Context(), extendReq)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to extend lease")
		return
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{
		"message":     "Lease extended successfully",
		"topic":       req.Topic,
		"lease_token": req.LeaseToken,
		"extension":   req.Extension,
	})
}

// Peek handles GET /v1/peek/:topic
func (h *DataHandler) Peek(c *gin.Context) {
	// Verify authentication (handled by middleware)

	topic := c.Param("topic")
	if topic == "" {
		h.respondBadRequest(c, "Missing topic parameter", "topic is required")
		return
	}

	// Parse query parameters
	var req PeekRequest
	req.Topic = topic
	req.Limit = 10 // default

	if limitStr := c.Query("limit"); limitStr != "" {
		if limit, err := strconv.ParseUint(limitStr, 10, 32); err == nil && limit <= 1000 {
			req.Limit = uint32(limit)
		}
	}

	// Validate request
	if err := h.validatePeekRequest(&req); err != nil {
		h.respondBadRequest(c, "Invalid request parameters", err.Error())
		return
	}

	// Create peek request for engine
	peekReq := &disheapv1.PeekReq{
		Topic: req.Topic,
		Limit: &req.Limit,
	}

	// Call engine
	resp, err := h.engineClient.Peek(c.Request.Context(), peekReq)
	if err != nil {
		h.handleGRPCError(c, err, "Failed to peek messages")
		return
	}

	// Convert items to response format
	messages := make([]PeekMessage, len(resp.Items))
	for i, item := range resp.Items {
		metadata := make(map[string]string)
		if item.EnqueuedTime != nil {
			metadata["enqueued_time"] = item.EnqueuedTime.AsTime().Format(time.RFC3339)
		}
		if item.LeaseDeadline != nil {
			metadata["lease_deadline"] = item.LeaseDeadline.AsTime().Format(time.RFC3339)
		}
		metadata["attempts"] = strconv.FormatUint(uint64(item.Attempts), 10)
		metadata["max_retries"] = strconv.FormatUint(uint64(item.MaxRetries), 10)
		metadata["is_leased"] = strconv.FormatBool(item.IsLeased)
		metadata["in_dlq"] = strconv.FormatBool(item.InDlq)
		metadata["partition_id"] = strconv.FormatUint(uint64(item.PartitionId), 10)

		messages[i] = PeekMessage{
			ID:       item.MessageId,
			Payload:  base64.StdEncoding.EncodeToString(item.Payload),
			Priority: item.Priority,
			Metadata: metadata,
		}
	}

	// Return response
	response := &PeekResponse{
		Topic:    req.Topic,
		Messages: messages,
		Count:    len(messages),
	}

	c.JSON(http.StatusOK, response)
}

// Helper methods

// validateEnqueueRequest validates the enqueue request
func (h *DataHandler) validateEnqueueRequest(req *EnqueueRequest) error {
	if len(req.Topic) == 0 || len(req.Topic) > 255 {
		return errors.New("topic name must be between 1 and 255 characters")
	}
	if len(req.Payload) == 0 {
		return errors.New("payload cannot be empty")
	}
	if req.Priority < -9223372036854775808 || req.Priority > 9223372036854775807 {
		return errors.New("priority must be within int64 range")
	}
	return nil
}

// validateEnqueueBatchRequest validates the batch enqueue request
func (h *DataHandler) validateEnqueueBatchRequest(req *EnqueueBatchRequest) error {
	if len(req.Messages) == 0 {
		return errors.New("messages array cannot be empty")
	}
	if len(req.Messages) > 1000 {
		return errors.New("batch size cannot exceed 1000 messages")
	}

	for i, msg := range req.Messages {
		if err := h.validateEnqueueRequest(&msg); err != nil {
			return errors.New("message " + strconv.Itoa(i) + ": " + err.Error())
		}
	}
	return nil
}

// validateAckRequest validates the ack request
func (h *DataHandler) validateAckRequest(req *AckRequest) error {
	if len(req.Topic) == 0 || len(req.Topic) > 255 {
		return errors.New("topic name must be between 1 and 255 characters")
	}
	if len(req.MessageID) == 0 {
		return errors.New("message_id cannot be empty")
	}
	if len(req.LeaseToken) == 0 {
		return errors.New("lease_token cannot be empty")
	}
	return nil
}

// validateNackRequest validates the nack request
func (h *DataHandler) validateNackRequest(req *NackRequest) error {
	if len(req.Topic) == 0 || len(req.Topic) > 255 {
		return errors.New("topic name must be between 1 and 255 characters")
	}
	if len(req.MessageID) == 0 {
		return errors.New("message_id cannot be empty")
	}
	if len(req.LeaseToken) == 0 {
		return errors.New("lease_token cannot be empty")
	}
	if req.BackoffOverride != nil && *req.BackoffOverride < 0 {
		return errors.New("backoff_override cannot be negative")
	}
	return nil
}

// validateExtendLeaseRequest validates the extend lease request
func (h *DataHandler) validateExtendLeaseRequest(req *ExtendLeaseRequest) error {
	if len(req.Topic) == 0 || len(req.Topic) > 255 {
		return errors.New("topic name must be between 1 and 255 characters")
	}
	if len(req.LeaseToken) == 0 {
		return errors.New("lease_token cannot be empty")
	}
	if req.Extension <= 0 {
		return errors.New("extension must be positive")
	}
	if req.Extension > 43200 { // 12 hours
		return errors.New("extension cannot exceed 12 hours (43200 seconds)")
	}
	return nil
}

// validatePeekRequest validates the peek request
func (h *DataHandler) validatePeekRequest(req *PeekRequest) error {
	if len(req.Topic) == 0 || len(req.Topic) > 255 {
		return errors.New("topic name must be between 1 and 255 characters")
	}
	if req.Limit == 0 {
		req.Limit = 10 // Set default
	}
	if req.Limit > 1000 {
		return errors.New("limit cannot exceed 1000 messages")
	}
	return nil
}

// Error response helpers

func (h *DataHandler) respondUnauthorized(c *gin.Context, message string) {
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

func (h *DataHandler) respondBadRequest(c *gin.Context, message, details string) {
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

// handleGRPCError handles gRPC errors and converts them to appropriate HTTP responses
func (h *DataHandler) handleGRPCError(c *gin.Context, err error, message string) {
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
