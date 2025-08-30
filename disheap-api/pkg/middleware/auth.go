package middleware

import (
	"net/http"
	"strings"

	"github.com/disheap/disheap/disheap-api/pkg/auth"
	"github.com/disheap/disheap/disheap-api/pkg/errors"
	"github.com/gin-gonic/gin"
)

const (
	AuthContextKey = "auth_context"
)

// Auth middleware handles both JWT and API key authentication
func Auth(authService *auth.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		var authContext *auth.AuthContext
		var err error

		// Check for API key first (X-API-Key header or Authorization: Bearer)
		apiKey := c.GetHeader("X-API-Key")
		if apiKey == "" {
			// Check Authorization header for API key
			authHeader := c.GetHeader("Authorization")
			if strings.HasPrefix(authHeader, "Bearer dh_") {
				apiKey = strings.TrimPrefix(authHeader, "Bearer ")
			}
		}

		if apiKey != "" {
			authContext, err = authService.ValidateAPIKey(apiKey)
			if err != nil {
				c.JSON(http.StatusUnauthorized, errors.NewAPIError(
					"UNAUTHENTICATED",
					"Invalid API key",
					nil,
				))
				c.Abort()
				return
			}
		} else {
			// Try JWT authentication
			authHeader := c.GetHeader("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				c.JSON(http.StatusUnauthorized, errors.NewAPIError(
					"UNAUTHENTICATED",
					"Missing authentication credentials",
					nil,
				))
				c.Abort()
				return
			}

			token := strings.TrimPrefix(authHeader, "Bearer ")
			authContext, err = authService.ValidateJWT(token)
			if err != nil {
				c.JSON(http.StatusUnauthorized, errors.NewAPIError(
					"UNAUTHENTICATED",
					"Invalid JWT token",
					nil,
				))
				c.Abort()
				return
			}
		}

		// Store auth context
		c.Set(AuthContextKey, authContext)
		c.Next()
	}
}

// JWTAuth middleware requires JWT authentication specifically (for UI routes)
func JWTAuth(authService *auth.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			c.JSON(http.StatusUnauthorized, errors.NewAPIError(
				"UNAUTHENTICATED",
				"Missing JWT token",
				nil,
			))
			c.Abort()
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")

		// Reject if it looks like an API key
		if strings.HasPrefix(token, "dh_") {
			c.JSON(http.StatusUnauthorized, errors.NewAPIError(
				"UNAUTHENTICATED",
				"API keys not allowed for this endpoint",
				nil,
			))
			c.Abort()
			return
		}

		authContext, err := authService.ValidateJWT(token)
		if err != nil {
			c.JSON(http.StatusUnauthorized, errors.NewAPIError(
				"UNAUTHENTICATED",
				"Invalid JWT token",
				nil,
			))
			c.Abort()
			return
		}

		c.Set(AuthContextKey, authContext)
		c.Next()
	}
}

// GetAuthContext extracts the auth context from the request
func GetAuthContext(c *gin.Context) (*auth.AuthContext, bool) {
	authContext, exists := c.Get(AuthContextKey)
	if !exists {
		return nil, false
	}

	ctx, ok := authContext.(*auth.AuthContext)
	return ctx, ok
}
