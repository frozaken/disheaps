package server

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// HealthResponse represents a health check response
type HealthResponse struct {
	Status    string                 `json:"status"`
	Timestamp time.Time              `json:"timestamp"`
	Reason    string                 `json:"reason,omitempty"`
	Details   map[string]interface{} `json:"details,omitempty"`
}

// ReadinessResponse represents a readiness check response
type ReadinessResponse struct {
	Status    string                   `json:"status"`
	Timestamp time.Time                `json:"timestamp"`
	Services  map[string]ServiceStatus `json:"services,omitempty"`
}

// ServiceStatus represents the status of an individual service
type ServiceStatus struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// HealthHandler handles health check requests
func (s *Server) HealthHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		response := HealthResponse{
			Timestamp: time.Now().UTC(),
			Details:   make(map[string]interface{}),
		}

		// Check engine connectivity
		if s.engineClient != nil && s.engineClient.IsHealthy() {
			response.Status = "healthy"
			response.Details["engine"] = "connected"
		} else {
			response.Status = "unhealthy"
			response.Reason = "engine unavailable"
			response.Details["engine"] = "disconnected"
		}

		// Set appropriate HTTP status code
		statusCode := http.StatusOK
		if response.Status != "healthy" {
			statusCode = http.StatusServiceUnavailable
		}

		c.JSON(statusCode, response)
	}
}

// ReadinessHandler handles readiness check requests
func (s *Server) ReadinessHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		response := ReadinessResponse{
			Status:    "ready",
			Timestamp: time.Now().UTC(),
			Services:  make(map[string]ServiceStatus),
		}

		// Check individual service components
		allReady := true

		// Check engine client
		if s.engineClient != nil && s.engineClient.IsHealthy() {
			response.Services["engine"] = ServiceStatus{
				Status:  "ready",
				Message: "Engine client is healthy and connected",
			}
		} else {
			response.Services["engine"] = ServiceStatus{
				Status:  "not_ready",
				Message: "Engine client is not healthy or not connected",
			}
			allReady = false
		}

		// Add other service checks as needed
		// response.Services["database"] = ServiceStatus{...}
		// response.Services["cache"] = ServiceStatus{...}

		// Set overall status
		if allReady {
			response.Status = "ready"
		} else {
			response.Status = "not_ready"
		}

		// Set appropriate HTTP status code
		statusCode := http.StatusOK
		if response.Status != "ready" {
			statusCode = http.StatusServiceUnavailable
		}

		c.JSON(statusCode, response)
	}
}

// LivenessHandler handles liveness probe requests
// This is a simple endpoint that indicates the application is alive
func (s *Server) LivenessHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "alive",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	}
}
