package auth

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// AuthContext holds authentication context information
type AuthContext struct {
	APIKeyID    string
	UserID      string
	AuthMethod  string // "api_key" or "jwt"
	Permissions []string
}

// ContextKey is used for context keys to avoid collisions
type ContextKey string

const (
	// AuthContextKey is the context key for auth information
	AuthContextKey ContextKey = "auth_context"
)

// APIKeyStore defines the interface for API key storage operations
type APIKeyStore interface {
	GetAPIKeyByID(ctx context.Context, keyID string) (*APIKey, error)
	UpdateAPIKeyLastUsed(ctx context.Context, keyID string, lastUsed time.Time) error
}

// UserStore defines the interface for user storage operations
type UserStore interface {
	GetUserByID(ctx context.Context, userID string) (*User, error)
}

// AuthService provides authentication functionality
type AuthService struct {
	apiKeyManager *APIKeyManager
	apiKeyStore   APIKeyStore
	userStore     UserStore
	hasher        Hasher
}

// NewAuthService creates a new authentication service
func NewAuthService(apiKeyStore APIKeyStore, userStore UserStore) *AuthService {
	return &AuthService{
		apiKeyManager: NewAPIKeyManager(),
		apiKeyStore:   apiKeyStore,
		userStore:     userStore,
		hasher:        NewSecureHasher(),
	}
}

// NewAuthServiceWithHasher creates a new authentication service with custom hasher
func NewAuthServiceWithHasher(apiKeyStore APIKeyStore, userStore UserStore, hasher Hasher) *AuthService {
	return &AuthService{
		apiKeyManager: NewAPIKeyManager(),
		apiKeyStore:   apiKeyStore,
		userStore:     userStore,
		hasher:        hasher,
	}
}

// APIKeyAuthMiddleware provides API key authentication middleware
func (s *AuthService) APIKeyAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get Authorization header
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			s.respondUnauthorized(c, "Missing Authorization header")
			return
		}

		// Check for Bearer token format
		if !strings.HasPrefix(authHeader, "Bearer ") {
			s.respondUnauthorized(c, "Invalid Authorization header format")
			return
		}

		// Extract API key
		apiKey := strings.TrimPrefix(authHeader, "Bearer ")
		if apiKey == "" {
			s.respondUnauthorized(c, "Missing API key")
			return
		}

		// Validate API key format
		keyID, _, err := ParseAPIKey(apiKey)
		if err != nil {
			s.respondUnauthorized(c, "Invalid API key format")
			return
		}

		// Get API key from storage
		storedKey, err := s.apiKeyStore.GetAPIKeyByID(c.Request.Context(), keyID)
		if err != nil {
			s.respondUnauthorized(c, "Invalid API key")
			return
		}

		if storedKey == nil {
			s.respondUnauthorized(c, "Invalid API key")
			return
		}

		// Verify API key
		valid, err := s.apiKeyManager.VerifyAPIKey(apiKey, storedKey)
		if err != nil || !valid {
			s.respondUnauthorized(c, "Invalid API key")
			return
		}

		// Update last used timestamp (fire and forget)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s.apiKeyStore.UpdateAPIKeyLastUsed(ctx, keyID, time.Now().UTC())
		}()

		// Set auth context
		authCtx := &AuthContext{
			APIKeyID:   keyID,
			AuthMethod: "api_key",
			// Add permissions based on API key if needed
			Permissions: []string{}, // For now, API keys have full access
		}

		// Add to Gin context
		c.Set(string(AuthContextKey), authCtx)

		// Add to request context
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), AuthContextKey, authCtx))

		c.Next()
	}
}

// JWTAuthMiddleware provides JWT authentication middleware (placeholder)
func (s *AuthService) JWTAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// TODO: Implement JWT authentication in Phase 2.2
		s.respondUnauthorized(c, "JWT authentication not implemented yet")
		c.Abort()
	}
}

// AuthMiddleware provides flexible authentication (API key or JWT)
func (s *AuthService) AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			s.respondUnauthorized(c, "Missing Authorization header")
			return
		}

		if strings.HasPrefix(authHeader, "Bearer dh.") {
			// API key authentication
			s.APIKeyAuthMiddleware()(c)
		} else if strings.HasPrefix(authHeader, "Bearer ") {
			// JWT authentication
			s.JWTAuthMiddleware()(c)
		} else {
			s.respondUnauthorized(c, "Invalid Authorization header format")
		}
	}
}

// RequireAuth is a helper middleware that requires authentication
func (s *AuthService) RequireAuth() gin.HandlerFunc {
	return s.AuthMiddleware()
}

// RequireAPIKey is a helper middleware that specifically requires API key auth
func (s *AuthService) RequireAPIKey() gin.HandlerFunc {
	return s.APIKeyAuthMiddleware()
}

// GetAuthContext extracts auth context from Gin context
func GetAuthContext(c *gin.Context) (*AuthContext, bool) {
	value, exists := c.Get(string(AuthContextKey))
	if !exists {
		return nil, false
	}

	authCtx, ok := value.(*AuthContext)
	return authCtx, ok
}

// GetAuthContextFromRequest extracts auth context from request context
func GetAuthContextFromRequest(ctx context.Context) (*AuthContext, bool) {
	value := ctx.Value(AuthContextKey)
	if value == nil {
		return nil, false
	}

	authCtx, ok := value.(*AuthContext)
	return authCtx, ok
}

// IsAuthenticated checks if the request is authenticated
func IsAuthenticated(c *gin.Context) bool {
	_, exists := GetAuthContext(c)
	return exists
}

// IsAPIKeyAuth checks if the request is authenticated with API key
func IsAPIKeyAuth(c *gin.Context) bool {
	authCtx, exists := GetAuthContext(c)
	return exists && authCtx.AuthMethod == "api_key"
}

// IsJWTAuth checks if the request is authenticated with JWT
func IsJWTAuth(c *gin.Context) bool {
	authCtx, exists := GetAuthContext(c)
	return exists && authCtx.AuthMethod == "jwt"
}

// respondUnauthorized sends a standardized unauthorized response
func (s *AuthService) respondUnauthorized(c *gin.Context, message string) {
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
	c.Abort()
}

// respondForbidden sends a standardized forbidden response
func (s *AuthService) respondForbidden(c *gin.Context, message string) {
	c.JSON(http.StatusForbidden, gin.H{
		"error": map[string]interface{}{
			"code":    "PERMISSION_DENIED",
			"message": message,
			"details": []map[string]interface{}{
				{
					"field": "permissions",
					"issue": "insufficient permissions",
				},
			},
		},
	})
	c.Abort()
}
