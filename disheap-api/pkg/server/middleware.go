package server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

// Logger middleware provides structured logging
func Logger() gin.HandlerFunc {
	return gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		return fmt.Sprintf(`{"timestamp":"%s","method":"%s","path":"%s","status":%d,"latency":"%s","ip":"%s","user_agent":"%s"}%s`,
			param.TimeStamp.Format(time.RFC3339),
			param.Method,
			param.Path,
			param.StatusCode,
			param.Latency,
			param.ClientIP,
			param.Request.UserAgent(),
			"\n",
		)
	})
}

// CORS middleware handles Cross-Origin Resource Sharing
func CORS(config CORSConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")

		// Check if origin is allowed
		originAllowed := false
		for _, allowedOrigin := range config.AllowedOrigins {
			if allowedOrigin == "*" || allowedOrigin == origin {
				originAllowed = true
				break
			}
		}

		if originAllowed {
			c.Header("Access-Control-Allow-Origin", origin)
		}

		c.Header("Access-Control-Allow-Methods", strings.Join(config.AllowedMethods, ", "))
		c.Header("Access-Control-Allow-Headers", strings.Join(config.AllowedHeaders, ", "))
		c.Header("Access-Control-Expose-Headers", strings.Join(config.ExposeHeaders, ", "))
		c.Header("Access-Control-Max-Age", strconv.Itoa(int(config.MaxAge.Seconds())))

		if config.AllowCredentials {
			c.Header("Access-Control-Allow-Credentials", "true")
		}

		// Handle preflight requests
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// rateLimitEntry holds rate limiting information for a client
type rateLimitEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// rateLimitStore manages rate limiters for different clients
type rateLimitStore struct {
	mu      sync.RWMutex
	clients map[string]*rateLimitEntry
	cleanup time.Duration
}

// globalRateLimitStore is the global instance for rate limiting
var globalRateLimitStore = &rateLimitStore{
	clients: make(map[string]*rateLimitEntry),
	cleanup: 10 * time.Minute,
}

// init starts the cleanup goroutine
func init() {
	go globalRateLimitStore.cleanupRoutine()
}

// cleanupRoutine removes expired rate limiters
func (s *rateLimitStore) cleanupRoutine() {
	ticker := time.NewTicker(s.cleanup)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		cutoff := time.Now().Add(-s.cleanup)
		for clientID, entry := range s.clients {
			if entry.lastSeen.Before(cutoff) {
				delete(s.clients, clientID)
			}
		}
		s.mu.Unlock()
	}
}

// getLimiter gets or creates a rate limiter for a client
func (s *rateLimitStore) getLimiter(clientID string, requestsPerMinute int, burstSize int) *rate.Limiter {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.clients[clientID]
	if !exists {
		// Create new rate limiter
		limiter := rate.NewLimiter(rate.Limit(requestsPerMinute)/60, burstSize) // Convert per minute to per second
		entry = &rateLimitEntry{
			limiter:  limiter,
			lastSeen: time.Now(),
		}
		s.clients[clientID] = entry
	} else {
		entry.lastSeen = time.Now()
	}

	return entry.limiter
}

// RateLimit middleware provides rate limiting functionality
func RateLimit(config RateLimitConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get client identifier (IP address for now, could be enhanced with API keys)
		clientID := c.ClientIP()

		// Get rate limiter for this client
		limiter := globalRateLimitStore.getLimiter(clientID, config.RequestsPerMinute, config.BurstSize)

		// Check if request is allowed
		if !limiter.Allow() {
			// Calculate when the client can make another request
			reservation := limiter.Reserve()
			resetTime := time.Now().Add(reservation.Delay())
			reservation.Cancel() // Cancel the reservation since we're rejecting the request

			// Set rate limit headers
			c.Header("X-RateLimit-Limit", strconv.Itoa(config.RequestsPerMinute))
			c.Header("X-RateLimit-Remaining", "0")
			c.Header("X-RateLimit-Reset", strconv.FormatInt(resetTime.Unix(), 10))

			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": map[string]interface{}{
					"code":    "RATE_LIMIT_EXCEEDED",
					"message": "Rate limit exceeded. Please try again later.",
					"details": []map[string]interface{}{
						{
							"field": "rate_limit",
							"issue": fmt.Sprintf("Exceeded %d requests per minute", config.RequestsPerMinute),
						},
					},
				},
			})
			c.Abort()
			return
		}

		// Set rate limit headers for successful requests
		// Note: These are approximations since we can't easily get exact remaining count from rate.Limiter
		c.Header("X-RateLimit-Limit", strconv.Itoa(config.RequestsPerMinute))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(config.BurstSize-1)) // Approximate
		c.Header("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10))

		c.Next()
	}
}

// ErrorHandler middleware handles panics and errors in a consistent way
func ErrorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				// Log the panic (in a real implementation, use structured logging)
				fmt.Printf("Panic recovered: %v\n", r)

				c.JSON(http.StatusInternalServerError, gin.H{
					"error": map[string]interface{}{
						"code":    "INTERNAL",
						"message": "An internal server error occurred",
						"details": []map[string]interface{}{},
					},
				})
				c.Abort()
				return
			}
		}()

		c.Next()

		// Handle errors that were set during request processing
		if len(c.Errors) > 0 {
			// Get the last error
			err := c.Errors.Last()

			// Map error to appropriate HTTP status and response
			// This follows the error format from INTERFACE_CONTRACTS.md
			switch err.Type {
			case gin.ErrorTypeBind:
				c.JSON(http.StatusBadRequest, gin.H{
					"error": map[string]interface{}{
						"code":    "INVALID_ARGUMENT",
						"message": "Invalid request data",
						"details": []map[string]interface{}{
							{
								"field": "request_body",
								"issue": err.Error(),
							},
						},
					},
				})
			case gin.ErrorTypePublic:
				c.JSON(http.StatusBadRequest, gin.H{
					"error": map[string]interface{}{
						"code":    "INVALID_ARGUMENT",
						"message": err.Error(),
						"details": []map[string]interface{}{},
					},
				})
			default:
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": map[string]interface{}{
						"code":    "INTERNAL",
						"message": "An internal server error occurred",
						"details": []map[string]interface{}{},
					},
				})
			}

			c.Abort()
		}
	}
}
