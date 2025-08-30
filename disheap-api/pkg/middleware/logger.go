package middleware

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// LogEntry represents a structured log entry
type LogEntry struct {
	Timestamp     time.Time `json:"timestamp"`
	Level         string    `json:"level"`
	Message       string    `json:"message"`
	Component     string    `json:"component"`
	Method        string    `json:"method"`
	Path          string    `json:"path"`
	Status        int       `json:"status"`
	Duration      float64   `json:"duration_ms"`
	ClientIP      string    `json:"client_ip"`
	UserAgent     string    `json:"user_agent"`
	CorrelationID string    `json:"correlation_id"`
	UserID        string    `json:"user_id,omitempty"`
	APIKeyID      string    `json:"api_key_id,omitempty"`
	ErrorMessage  string    `json:"error,omitempty"`
}

// Logger middleware provides structured logging
func Logger(format string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Generate correlation ID
		correlationID := uuid.New().String()
		c.Set("correlation_id", correlationID)
		c.Header("X-Correlation-ID", correlationID)

		// Start timer
		start := time.Now()

		// Process request
		c.Next()

		// Calculate duration
		duration := time.Since(start).Seconds() * 1000 // Convert to milliseconds

		// Get auth context if available
		var userID, apiKeyID string
		if authContext, exists := GetAuthContext(c); exists {
			userID = authContext.UserID
			if authContext.IsAPIKey {
				apiKeyID = authContext.APIKeyID
			}
		}

		// Determine log level based on status code
		level := "INFO"
		if c.Writer.Status() >= 400 {
			level = "WARN"
		}
		if c.Writer.Status() >= 500 {
			level = "ERROR"
		}

		// Get error message if any
		var errorMessage string
		if len(c.Errors) > 0 {
			errorMessage = c.Errors.String()
		}

		// Create log entry
		logEntry := LogEntry{
			Timestamp:     start,
			Level:         level,
			Message:       "HTTP request processed",
			Component:     "api",
			Method:        c.Request.Method,
			Path:          c.Request.URL.Path,
			Status:        c.Writer.Status(),
			Duration:      duration,
			ClientIP:      c.ClientIP(),
			UserAgent:     c.Request.UserAgent(),
			CorrelationID: correlationID,
			UserID:        userID,
			APIKeyID:      apiKeyID,
			ErrorMessage:  errorMessage,
		}

		// Output based on format
		if format == "json" {
			logJSON, _ := json.Marshal(logEntry)
			gin.DefaultWriter.Write(logJSON)
			gin.DefaultWriter.Write([]byte("\n"))
		} else {
			// Simple text format for development
			gin.DefaultWriter.Write([]byte(formatTextLog(logEntry)))
		}
	}
}

// formatTextLog formats a log entry as human-readable text
func formatTextLog(entry LogEntry) string {
	var buf bytes.Buffer

	buf.WriteString(entry.Timestamp.Format("2006-01-02 15:04:05"))
	buf.WriteString(" [" + entry.Level + "] ")
	buf.WriteString(entry.Method + " " + entry.Path)
	buf.WriteString(" - " + string(rune(entry.Status)))
	buf.WriteString(" - " + formatDuration(entry.Duration) + "ms")
	buf.WriteString(" - " + entry.ClientIP)

	if entry.UserID != "" {
		buf.WriteString(" - user:" + entry.UserID)
	}

	if entry.APIKeyID != "" {
		buf.WriteString(" - key:" + entry.APIKeyID)
	}

	if entry.ErrorMessage != "" {
		buf.WriteString(" - error: " + entry.ErrorMessage)
	}

	buf.WriteString("\n")
	return buf.String()
}

// formatDuration formats duration with appropriate precision
func formatDuration(ms float64) string {
	if ms < 1 {
		return "0.1"
	} else if ms < 10 {
		return fmt.Sprintf("%.1f", ms)
	} else {
		return fmt.Sprintf("%.0f", ms)
	}
}
