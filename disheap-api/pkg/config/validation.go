package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// ValidationError represents a configuration validation error
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s': %s", e.Field, e.Message)
}

// ValidationErrors represents multiple validation errors
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return "no validation errors"
	}
	if len(e) == 1 {
		return e[0].Error()
	}

	var messages []string
	for _, err := range e {
		messages = append(messages, err.Error())
	}
	return fmt.Sprintf("multiple validation errors: %s", strings.Join(messages, "; "))
}

// ValidateDetailed performs detailed validation and returns all validation errors
func (c *Config) ValidateDetailed() ValidationErrors {
	var errors ValidationErrors

	// Server validation
	errors = append(errors, c.validateServer()...)

	// Engine validation
	errors = append(errors, c.validateEngine()...)

	// Auth validation
	errors = append(errors, c.validateAuth()...)

	// Storage validation
	errors = append(errors, c.validateStorage()...)

	// Logging validation
	errors = append(errors, c.validateLogging()...)

	// Observability validation
	errors = append(errors, c.validateObservability()...)

	return errors
}

// validateServer validates server configuration
func (c *Config) validateServer() ValidationErrors {
	var errors ValidationErrors

	if c.Server.Host == "" {
		errors = append(errors, ValidationError{
			Field:   "server.host",
			Message: "cannot be empty",
		})
	}

	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		errors = append(errors, ValidationError{
			Field:   "server.port",
			Message: "must be between 1 and 65535",
		})
	}

	if c.Server.ReadTimeout <= 0 {
		errors = append(errors, ValidationError{
			Field:   "server.read_timeout",
			Message: "must be positive",
		})
	}

	if c.Server.WriteTimeout <= 0 {
		errors = append(errors, ValidationError{
			Field:   "server.write_timeout",
			Message: "must be positive",
		})
	}

	if c.Server.IdleTimeout <= 0 {
		errors = append(errors, ValidationError{
			Field:   "server.idle_timeout",
			Message: "must be positive",
		})
	}

	// TLS validation
	if c.Server.TLSCertFile != "" && c.Server.TLSKeyFile == "" {
		errors = append(errors, ValidationError{
			Field:   "server.tls_key_file",
			Message: "must be specified when tls_cert_file is provided",
		})
	}
	if c.Server.TLSKeyFile != "" && c.Server.TLSCertFile == "" {
		errors = append(errors, ValidationError{
			Field:   "server.tls_cert_file",
			Message: "must be specified when tls_key_file is provided",
		})
	}

	// CORS validation
	errors = append(errors, c.validateCORS()...)

	// Rate limit validation
	errors = append(errors, c.validateRateLimit()...)

	return errors
}

// validateEngine validates engine configuration
func (c *Config) validateEngine() ValidationErrors {
	var errors ValidationErrors

	if len(c.Engine.Endpoints) == 0 {
		errors = append(errors, ValidationError{
			Field:   "engine.endpoints",
			Message: "cannot be empty",
		})
	}

	for i, endpoint := range c.Engine.Endpoints {
		if endpoint == "" {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("engine.endpoints[%d]", i),
				Message: "cannot be empty",
			})
			continue
		}

		// Validate endpoint format (host:port)
		if !c.isValidEndpoint(endpoint) {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("engine.endpoints[%d]", i),
				Message: fmt.Sprintf("invalid endpoint format '%s', expected host:port", endpoint),
			})
		}
	}

	if c.Engine.ConnectTimeout <= 0 {
		errors = append(errors, ValidationError{
			Field:   "engine.connect_timeout",
			Message: "must be positive",
		})
	}

	if c.Engine.RequestTimeout <= 0 {
		errors = append(errors, ValidationError{
			Field:   "engine.request_timeout",
			Message: "must be positive",
		})
	}

	if c.Engine.MaxRetries < 0 {
		errors = append(errors, ValidationError{
			Field:   "engine.max_retries",
			Message: "cannot be negative",
		})
	}

	if c.Engine.RetryBackoffMin <= 0 {
		errors = append(errors, ValidationError{
			Field:   "engine.retry_backoff_min",
			Message: "must be positive",
		})
	}

	if c.Engine.RetryBackoffMax <= 0 {
		errors = append(errors, ValidationError{
			Field:   "engine.retry_backoff_max",
			Message: "must be positive",
		})
	}

	if c.Engine.RetryBackoffMax <= c.Engine.RetryBackoffMin {
		errors = append(errors, ValidationError{
			Field:   "engine.retry_backoff_max",
			Message: "must be greater than retry_backoff_min",
		})
	}

	if c.Engine.MaxConnections <= 0 {
		errors = append(errors, ValidationError{
			Field:   "engine.max_connections",
			Message: "must be positive",
		})
	}

	if c.Engine.IdleTimeout <= 0 {
		errors = append(errors, ValidationError{
			Field:   "engine.idle_timeout",
			Message: "must be positive",
		})
	}

	if c.Engine.KeepAlive.Time <= 0 {
		errors = append(errors, ValidationError{
			Field:   "engine.keep_alive.time",
			Message: "must be positive",
		})
	}

	if c.Engine.KeepAlive.Timeout <= 0 {
		errors = append(errors, ValidationError{
			Field:   "engine.keep_alive.timeout",
			Message: "must be positive",
		})
	}

	return errors
}

// validateAuth validates authentication configuration
func (c *Config) validateAuth() ValidationErrors {
	var errors ValidationErrors

	if c.Auth.JWTSecret == "" {
		errors = append(errors, ValidationError{
			Field:   "auth.jwt_secret",
			Message: "cannot be empty",
		})
	} else if c.Auth.JWTSecret == "change-me-in-production" {
		errors = append(errors, ValidationError{
			Field:   "auth.jwt_secret",
			Message: "must be changed from default value in production",
		})
	} else if len(c.Auth.JWTSecret) < 32 {
		errors = append(errors, ValidationError{
			Field:   "auth.jwt_secret",
			Message: "should be at least 32 characters for security",
		})
	}

	if c.Auth.JWTExpiry <= 0 {
		errors = append(errors, ValidationError{
			Field:   "auth.jwt_expiry",
			Message: "must be positive",
		})
	}

	if c.Auth.JWTIssuer == "" {
		errors = append(errors, ValidationError{
			Field:   "auth.jwt_issuer",
			Message: "cannot be empty",
		})
	}

	// Argon2 validation
	if c.Auth.ArgonMemory < 1024 { // Minimum 1MB
		errors = append(errors, ValidationError{
			Field:   "auth.argon_memory",
			Message: "should be at least 1024 KB for security",
		})
	}

	if c.Auth.ArgonTime < 1 {
		errors = append(errors, ValidationError{
			Field:   "auth.argon_time",
			Message: "must be at least 1",
		})
	}

	if c.Auth.ArgonThreads < 1 {
		errors = append(errors, ValidationError{
			Field:   "auth.argon_threads",
			Message: "must be at least 1",
		})
	}

	return errors
}

// validateStorage validates storage configuration
func (c *Config) validateStorage() ValidationErrors {
	var errors ValidationErrors

	validTypes := map[string]bool{
		"sqlite":   true,
		"postgres": true,
		"memory":   true,
	}

	if c.Storage.Type == "" {
		errors = append(errors, ValidationError{
			Field:   "storage.type",
			Message: "cannot be empty",
		})
	} else if !validTypes[c.Storage.Type] {
		errors = append(errors, ValidationError{
			Field:   "storage.type",
			Message: "must be one of: sqlite, postgres, memory",
		})
	}

	if c.Storage.ConnectionString == "" {
		errors = append(errors, ValidationError{
			Field:   "storage.connection_string",
			Message: "cannot be empty",
		})
	}

	if c.Storage.MaxConnections <= 0 {
		errors = append(errors, ValidationError{
			Field:   "storage.max_connections",
			Message: "must be positive",
		})
	}

	if c.Storage.MaxIdleConns < 0 {
		errors = append(errors, ValidationError{
			Field:   "storage.max_idle_conns",
			Message: "cannot be negative",
		})
	}

	if c.Storage.MaxIdleConns > c.Storage.MaxConnections {
		errors = append(errors, ValidationError{
			Field:   "storage.max_idle_conns",
			Message: "cannot be greater than max_connections",
		})
	}

	return errors
}

// validateLogging validates logging configuration
func (c *Config) validateLogging() ValidationErrors {
	var errors ValidationErrors

	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}

	if !validLevels[c.Logging.Level] {
		errors = append(errors, ValidationError{
			Field:   "logging.level",
			Message: "must be one of: debug, info, warn, error",
		})
	}

	validFormats := map[string]bool{
		"json": true,
		"text": true,
	}

	if !validFormats[c.Logging.Format] {
		errors = append(errors, ValidationError{
			Field:   "logging.format",
			Message: "must be one of: json, text",
		})
	}

	return errors
}

// validateObservability validates observability configuration
func (c *Config) validateObservability() ValidationErrors {
	var errors ValidationErrors

	if c.Observability.MetricsPath == "" {
		errors = append(errors, ValidationError{
			Field:   "observability.metrics_path",
			Message: "cannot be empty when metrics are enabled",
		})
	} else if !strings.HasPrefix(c.Observability.MetricsPath, "/") {
		errors = append(errors, ValidationError{
			Field:   "observability.metrics_path",
			Message: "must start with '/'",
		})
	}

	if c.Observability.TracingEnabled && c.Observability.TracingEndpoint == "" {
		errors = append(errors, ValidationError{
			Field:   "observability.tracing_endpoint",
			Message: "cannot be empty when tracing is enabled",
		})
	}

	// Validate log level and format for observability (same rules as logging)
	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}

	if !validLevels[c.Observability.LogLevel] {
		errors = append(errors, ValidationError{
			Field:   "observability.log_level",
			Message: "must be one of: debug, info, warn, error",
		})
	}

	validFormats := map[string]bool{
		"json": true,
		"text": true,
	}

	if !validFormats[c.Observability.LogFormat] {
		errors = append(errors, ValidationError{
			Field:   "observability.log_format",
			Message: "must be one of: json, text",
		})
	}

	return errors
}

// validateCORS validates CORS configuration
func (c *Config) validateCORS() ValidationErrors {
	var errors ValidationErrors

	if c.Server.CORS.Enabled {
		if len(c.Server.CORS.AllowedOrigins) == 0 {
			errors = append(errors, ValidationError{
				Field:   "server.cors.allowed_origins",
				Message: "cannot be empty when CORS is enabled",
			})
		}

		if len(c.Server.CORS.AllowedMethods) == 0 {
			errors = append(errors, ValidationError{
				Field:   "server.cors.allowed_methods",
				Message: "cannot be empty when CORS is enabled",
			})
		}

		if c.Server.CORS.MaxAge < 0 {
			errors = append(errors, ValidationError{
				Field:   "server.cors.max_age",
				Message: "cannot be negative",
			})
		}
	}

	return errors
}

// validateRateLimit validates rate limiting configuration
func (c *Config) validateRateLimit() ValidationErrors {
	var errors ValidationErrors

	if c.Server.RateLimit.Enabled {
		if c.Server.RateLimit.RequestsPerMinute <= 0 {
			errors = append(errors, ValidationError{
				Field:   "server.rate_limit.requests_per_minute",
				Message: "must be positive when rate limiting is enabled",
			})
		}

		if c.Server.RateLimit.BurstSize <= 0 {
			errors = append(errors, ValidationError{
				Field:   "server.rate_limit.burst_size",
				Message: "must be positive when rate limiting is enabled",
			})
		}

		if c.Server.RateLimit.BurstSize > c.Server.RateLimit.RequestsPerMinute {
			errors = append(errors, ValidationError{
				Field:   "server.rate_limit.burst_size",
				Message: "should not exceed requests_per_minute",
			})
		}
	}

	return errors
}

// isValidEndpoint checks if an endpoint has valid host:port format
func (c *Config) isValidEndpoint(endpoint string) bool {
	host, portStr, err := net.SplitHostPort(endpoint)
	if err != nil {
		return false
	}

	// Validate host (can be IP or hostname)
	if host == "" {
		return false
	}

	// Validate port
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return false
	}

	return port > 0 && port <= 65535
}
