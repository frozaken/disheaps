package middleware

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-api/pkg/config"
	"github.com/disheap/disheap/disheap-api/pkg/errors"
	"github.com/gin-gonic/gin"
)

// RateLimiter implements a simple in-memory rate limiter
type RateLimiter struct {
	apiKeyLimits map[string]*RateLimit
	userLimits   map[string]*RateLimit
	ipLimits     map[string]*RateLimit
	mutex        sync.RWMutex

	// Configuration
	apiKeyLimit int
	userLimit   int
	ipLimit     int
	window      time.Duration
}

// RateLimit tracks rate limiting for a specific key
type RateLimit struct {
	Count       int
	WindowStart time.Time
	Limit       int
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(apiKeyLimit, userLimit, ipLimit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		apiKeyLimits: make(map[string]*RateLimit),
		userLimits:   make(map[string]*RateLimit),
		ipLimits:     make(map[string]*RateLimit),
		apiKeyLimit:  apiKeyLimit,
		userLimit:    userLimit,
		ipLimit:      ipLimit,
		window:       window,
	}
}

// checkRateLimit checks if a request is within rate limits
func (rl *RateLimiter) checkRateLimit(key string, limits map[string]*RateLimit, limit int) (bool, int, time.Time) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	now := time.Now()

	rateLimitData, exists := limits[key]
	if !exists {
		rateLimitData = &RateLimit{
			Count:       0,
			WindowStart: now,
			Limit:       limit,
		}
		limits[key] = rateLimitData
	}

	// Check if we need to reset the window
	if now.Sub(rateLimitData.WindowStart) >= rl.window {
		rateLimitData.Count = 0
		rateLimitData.WindowStart = now
	}

	// Check if request is allowed
	if rateLimitData.Count >= limit {
		nextReset := rateLimitData.WindowStart.Add(rl.window)
		return false, limit - rateLimitData.Count, nextReset
	}

	// Increment count
	rateLimitData.Count++
	nextReset := rateLimitData.WindowStart.Add(rl.window)

	return true, limit - rateLimitData.Count, nextReset
}

// CheckAPIKey checks rate limit for API key
func (rl *RateLimiter) CheckAPIKey(apiKeyID string) (bool, int, time.Time) {
	return rl.checkRateLimit(fmt.Sprintf("api:%s", apiKeyID), rl.apiKeyLimits, rl.apiKeyLimit)
}

// CheckUser checks rate limit for user
func (rl *RateLimiter) CheckUser(userID string) (bool, int, time.Time) {
	return rl.checkRateLimit(fmt.Sprintf("user:%s", userID), rl.userLimits, rl.userLimit)
}

// CheckIP checks rate limit for IP address
func (rl *RateLimiter) CheckIP(ip string) (bool, int, time.Time) {
	return rl.checkRateLimit(fmt.Sprintf("ip:%s", ip), rl.ipLimits, rl.ipLimit)
}

var (
	defaultRateLimiter *RateLimiter
	rateLimiterOnce    sync.Once
)

// RateLimit middleware enforces rate limiting according to interface contracts
func RateLimit(cfg *config.Config) gin.HandlerFunc {
	// Initialize rate limiter once
	rateLimiterOnce.Do(func() {
		defaultRateLimiter = NewRateLimiter(
			1000, // API key limit: 1000 req/min
			100,  // User limit: 100 req/min
			60,   // IP limit: 60 req/min for unauthenticated requests
			time.Minute,
		)
	})

	return func(c *gin.Context) {
		var allowed bool
		var remaining int
		var resetTime time.Time
		var limit int

		// Get auth context if available
		authContext, hasAuth := GetAuthContext(c)

		if hasAuth {
			if authContext.IsAPIKey {
				// Rate limit by API key
				allowed, remaining, resetTime = defaultRateLimiter.CheckAPIKey(authContext.APIKeyID)
				limit = 1000
			} else {
				// Rate limit by user ID
				allowed, remaining, resetTime = defaultRateLimiter.CheckUser(authContext.UserID)
				limit = 100
			}
		} else {
			// Rate limit by IP for unauthenticated requests
			clientIP := c.ClientIP()
			allowed, remaining, resetTime = defaultRateLimiter.CheckIP(clientIP)
			limit = 60
		}

		// Set rate limit headers
		c.Header("X-RateLimit-Limit", strconv.Itoa(limit))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(remaining))
		c.Header("X-RateLimit-Reset", strconv.FormatInt(resetTime.Unix(), 10))

		if !allowed {
			c.JSON(http.StatusTooManyRequests, errors.NewAPIError(
				"RESOURCE_EXHAUSTED",
				"Rate limit exceeded",
				map[string]interface{}{
					"retry_after": resetTime.Sub(time.Now()).Seconds(),
				},
			))
			c.Abort()
			return
		}

		c.Next()
	}
}
