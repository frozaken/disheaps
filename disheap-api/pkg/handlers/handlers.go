package handlers

import (
	"github.com/disheap/disheap/disheap-api/pkg/auth"
	"github.com/disheap/disheap/disheap-api/pkg/client"
	"github.com/gin-gonic/gin"
)

// Handlers aggregates all HTTP handlers for the API
type Handlers struct {
	engineClient client.EngineClient
	authService  *auth.AuthService

	// Handler components
	StatsHandler *StatsHandler
	AdminHandler *AdminHandler
	DataHandler  *DataHandler
	// TODO: Add other handlers as they're implemented
	// AuthHandler  *AuthHandler
}

// NewHandlers creates a new handlers instance
func NewHandlers(engineClient client.EngineClient, authService *auth.AuthService) *Handlers {
	return &Handlers{
		engineClient: engineClient,
		authService:  authService,
		StatsHandler: NewStatsHandler(engineClient),
		AdminHandler: NewAdminHandler(engineClient),
		DataHandler:  NewDataHandler(engineClient),
	}
}

// RegisterRoutes registers all API routes with the given router group
func (h *Handlers) RegisterRoutes(v1 *gin.RouterGroup) {
	// Statistics endpoints (require authentication)
	statsGroup := v1.Group("/stats")
	statsGroup.Use(h.authService.AuthMiddleware())
	{
		statsGroup.GET("", h.StatsHandler.GetGlobalStats)
		statsGroup.GET("/:topic", h.StatsHandler.GetTopicStats)
	}

	// Health statistics (no auth required for monitoring)
	healthGroup := v1.Group("/health")
	{
		healthGroup.GET("/stats", h.StatsHandler.GetHealthStats)
	}

	// Admin operations (require authentication)
	heapsGroup := v1.Group("/heaps")
	heapsGroup.Use(h.authService.AuthMiddleware())
	{
		heapsGroup.POST("", h.AdminHandler.CreateHeap)
		heapsGroup.GET("", h.AdminHandler.ListHeaps)
		heapsGroup.GET("/:topic", h.AdminHandler.GetHeap)
		heapsGroup.PATCH("/:topic", h.AdminHandler.UpdateHeap)
		heapsGroup.DELETE("/:topic", h.AdminHandler.DeleteHeap)
		heapsGroup.POST("/:topic/purge", h.AdminHandler.PurgeHeap)
	}

	// Data operations (require authentication)
	dataGroup := v1.Group("")
	dataGroup.Use(h.authService.AuthMiddleware())
	{
		dataGroup.POST("/enqueue", h.DataHandler.Enqueue)
		dataGroup.POST("/enqueue/batch", h.DataHandler.EnqueueBatch)
		dataGroup.POST("/ack", h.DataHandler.Ack)
		dataGroup.POST("/nack", h.DataHandler.Nack)
		dataGroup.POST("/extend", h.DataHandler.ExtendLease)
		dataGroup.GET("/peek/:topic", h.DataHandler.Peek)
	}

	// TODO: Add other route groups as handlers are implemented
	// h.registerAuthRoutes(v1)
}

// TODO: Implement these as handlers are created

// func (h *Handlers) registerAdminRoutes(v1 *gin.RouterGroup) {
//     adminGroup := v1.Group("/heaps")
//     adminGroup.Use(h.authService.AuthMiddleware())
//     {
//         adminGroup.POST("", h.AdminHandler.CreateHeap)
//         adminGroup.GET("", h.AdminHandler.ListHeaps)
//         adminGroup.GET("/:topic", h.AdminHandler.GetHeap)
//         adminGroup.PATCH("/:topic", h.AdminHandler.UpdateHeap)
//         adminGroup.DELETE("/:topic", h.AdminHandler.DeleteHeap)
//         adminGroup.POST("/:topic:purge", h.AdminHandler.PurgeHeap)
//     }
// }

// func (h *Handlers) registerDataRoutes(v1 *gin.RouterGroup) {
//     dataGroup := v1.Group("")
//     dataGroup.Use(h.authService.AuthMiddleware())
//     {
//         dataGroup.POST("/enqueue", h.DataHandler.Enqueue)
//         dataGroup.POST("/enqueue:batch", h.DataHandler.EnqueueBatch)
//         dataGroup.GET("/peek/:topic", h.DataHandler.Peek)
//         dataGroup.POST("/ack", h.DataHandler.Ack)
//         dataGroup.POST("/nack", h.DataHandler.Nack)
//         dataGroup.POST("/extend", h.DataHandler.ExtendLease)
//     }
// }

// func (h *Handlers) registerAuthRoutes(v1 *gin.RouterGroup) {
//     // Authentication routes (no auth required)
//     authGroup := v1.Group("/auth")
//     {
//         authGroup.POST("/login", h.AuthHandler.Login)
//         authGroup.POST("/logout", h.AuthHandler.Logout)
//         authGroup.POST("/refresh", h.AuthHandler.RefreshToken)
//         authGroup.GET("/me", h.authService.JWTAuthMiddleware(), h.AuthHandler.GetCurrentUser)
//     }
//
//     // API Key management (JWT auth required)
//     keysGroup := v1.Group("/keys")
//     keysGroup.Use(h.authService.JWTAuthMiddleware())
//     {
//         keysGroup.POST("", h.AuthHandler.CreateAPIKey)
//         keysGroup.GET("", h.AuthHandler.ListAPIKeys)
//         keysGroup.GET("/:keyId", h.AuthHandler.GetAPIKey)
//         keysGroup.DELETE("/:keyId", h.AuthHandler.RevokeAPIKey)
//     }
// }
