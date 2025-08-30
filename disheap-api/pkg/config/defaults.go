package config

import (
	"os"
	"time"
)

// DefaultValues contains all default configuration values
var DefaultValues = struct {
	// Server defaults
	ServerHost         string
	ServerPort         int
	ServerReadTimeout  time.Duration
	ServerWriteTimeout time.Duration
	ServerIdleTimeout  time.Duration

	// Engine defaults
	EngineEndpoints        []string
	EngineConnectTimeout   time.Duration
	EngineRequestTimeout   time.Duration
	EngineMaxRetries       int
	EngineRetryBackoffMin  time.Duration
	EngineRetryBackoffMax  time.Duration
	EngineMaxConnections   int
	EngineIdleTimeout      time.Duration
	EngineKeepAliveTime    time.Duration
	EngineKeepAliveTimeout time.Duration

	// Auth defaults
	AuthJWTExpiry    time.Duration
	AuthJWTIssuer    string
	AuthArgonMemory  uint32
	AuthArgonTime    uint32
	AuthArgonThreads uint8

	// Storage defaults
	StorageType             string
	StorageConnectionString string
	StorageMaxConnections   int
	StorageMaxIdleConns     int
	StorageConnMaxLifetime  time.Duration

	// Logging defaults
	LoggingLevel  string
	LoggingFormat string

	// Observability defaults
	ObservabilityMetricsEnabled bool
	ObservabilityMetricsPath    string
	ObservabilityLogLevel       string
	ObservabilityLogFormat      string

	// CORS defaults
	CORSEnabled          bool
	CORSAllowedOrigins   []string
	CORSAllowedMethods   []string
	CORSAllowedHeaders   []string
	CORSExposeHeaders    []string
	CORSAllowCredentials bool
	CORSMaxAge           time.Duration

	// Rate limiting defaults
	RateLimitEnabled           bool
	RateLimitRequestsPerMinute int
	RateLimitBurstSize         int
}{
	// Server defaults
	ServerHost:         "0.0.0.0",
	ServerPort:         8080,
	ServerReadTimeout:  30 * time.Second,
	ServerWriteTimeout: 30 * time.Second,
	ServerIdleTimeout:  60 * time.Second,

	// Engine defaults
	EngineEndpoints:        []string{"localhost:9090"},
	EngineConnectTimeout:   10 * time.Second,
	EngineRequestTimeout:   30 * time.Second,
	EngineMaxRetries:       3,
	EngineRetryBackoffMin:  100 * time.Millisecond,
	EngineRetryBackoffMax:  5 * time.Second,
	EngineMaxConnections:   10,
	EngineIdleTimeout:      5 * time.Minute,
	EngineKeepAliveTime:    30 * time.Second,
	EngineKeepAliveTimeout: 5 * time.Second,

	// Auth defaults
	AuthJWTExpiry:    24 * time.Hour,
	AuthJWTIssuer:    "disheap-api",
	AuthArgonMemory:  64 * 1024, // 64 MB
	AuthArgonTime:    3,         // 3 iterations
	AuthArgonThreads: 2,         // 2 threads

	// Storage defaults
	StorageType:             "sqlite",
	StorageConnectionString: "./disheap.db",
	StorageMaxConnections:   10,
	StorageMaxIdleConns:     5,
	StorageConnMaxLifetime:  time.Hour,

	// Logging defaults
	LoggingLevel:  "info",
	LoggingFormat: "json",

	// Observability defaults
	ObservabilityMetricsEnabled: true,
	ObservabilityMetricsPath:    "/metrics",
	ObservabilityLogLevel:       "info",
	ObservabilityLogFormat:      "json",

	// CORS defaults
	CORSEnabled:          true,
	CORSAllowedOrigins:   []string{"*"},
	CORSAllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
	CORSAllowedHeaders:   []string{"*"},
	CORSExposeHeaders:    []string{"X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"},
	CORSAllowCredentials: false,
	CORSMaxAge:           12 * time.Hour,

	// Rate limiting defaults
	RateLimitEnabled:           true,
	RateLimitRequestsPerMinute: 1000,
	RateLimitBurstSize:         100,
}

// NewDefaultConfig creates a new configuration with all default values
func NewDefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:         DefaultValues.ServerHost,
			Port:         DefaultValues.ServerPort,
			ReadTimeout:  DefaultValues.ServerReadTimeout,
			WriteTimeout: DefaultValues.ServerWriteTimeout,
			IdleTimeout:  DefaultValues.ServerIdleTimeout,
			TLSCertFile:  "",
			TLSKeyFile:   "",
			CORS: CORSConfig{
				Enabled:          DefaultValues.CORSEnabled,
				AllowedOrigins:   append([]string{}, DefaultValues.CORSAllowedOrigins...),
				AllowedMethods:   append([]string{}, DefaultValues.CORSAllowedMethods...),
				AllowedHeaders:   append([]string{}, DefaultValues.CORSAllowedHeaders...),
				ExposeHeaders:    append([]string{}, DefaultValues.CORSExposeHeaders...),
				AllowCredentials: DefaultValues.CORSAllowCredentials,
				MaxAge:           DefaultValues.CORSMaxAge,
			},
			RateLimit: RateLimitConfig{
				Enabled:           DefaultValues.RateLimitEnabled,
				RequestsPerMinute: DefaultValues.RateLimitRequestsPerMinute,
				BurstSize:         DefaultValues.RateLimitBurstSize,
			},
		},
		Engine: EngineConfig{
			Endpoints:       append([]string{}, DefaultValues.EngineEndpoints...),
			ConnectTimeout:  DefaultValues.EngineConnectTimeout,
			RequestTimeout:  DefaultValues.EngineRequestTimeout,
			TLSEnabled:      false,
			MaxRetries:      DefaultValues.EngineMaxRetries,
			RetryBackoffMin: DefaultValues.EngineRetryBackoffMin,
			RetryBackoffMax: DefaultValues.EngineRetryBackoffMax,
			MaxConnections:  DefaultValues.EngineMaxConnections,
			IdleTimeout:     DefaultValues.EngineIdleTimeout,
			KeepAlive: KeepAliveConfig{
				Time:    DefaultValues.EngineKeepAliveTime,
				Timeout: DefaultValues.EngineKeepAliveTimeout,
			},
		},
		Auth: AuthConfig{
			JWTSecret:    generateDefaultJWTSecret(),
			JWTExpiry:    DefaultValues.AuthJWTExpiry,
			JWTIssuer:    DefaultValues.AuthJWTIssuer,
			ArgonMemory:  DefaultValues.AuthArgonMemory,
			ArgonTime:    DefaultValues.AuthArgonTime,
			ArgonThreads: DefaultValues.AuthArgonThreads,
		},
		Storage: StorageConfig{
			Type:             DefaultValues.StorageType,
			ConnectionString: DefaultValues.StorageConnectionString,
			MaxConnections:   DefaultValues.StorageMaxConnections,
			MaxIdleConns:     DefaultValues.StorageMaxIdleConns,
			ConnMaxLifetime:  DefaultValues.StorageConnMaxLifetime,
		},
		Logging: LoggingConfig{
			Level:  DefaultValues.LoggingLevel,
			Format: DefaultValues.LoggingFormat,
		},
		Observability: ObservabilityConfig{
			MetricsEnabled:  DefaultValues.ObservabilityMetricsEnabled,
			MetricsPath:     DefaultValues.ObservabilityMetricsPath,
			TracingEnabled:  false,
			TracingEndpoint: "",
			LogLevel:        DefaultValues.ObservabilityLogLevel,
			LogFormat:       DefaultValues.ObservabilityLogFormat,
		},
	}
}

// generateDefaultJWTSecret generates a default JWT secret for development
// In production, this should always be overridden
func generateDefaultJWTSecret() string {
	// In development, use a predictable but secure-looking secret
	if isDevelopment() {
		return "dev-jwt-secret-change-in-production-32chars-min"
	}
	// In production, force the user to set their own secret
	return "change-me-in-production"
}

// isDevelopment checks if we're running in development mode
func isDevelopment() bool {
	env := os.Getenv("ENVIRONMENT")
	if env == "" {
		env = os.Getenv("ENV")
	}
	if env == "" {
		env = os.Getenv("GO_ENV")
	}

	return env == "development" || env == "dev" || env == "local"
}

// ApplyDefaults applies default values to any unset configuration fields
func (c *Config) ApplyDefaults() {
	// Server defaults
	if c.Server.Host == "" {
		c.Server.Host = DefaultValues.ServerHost
	}
	if c.Server.Port == 0 {
		c.Server.Port = DefaultValues.ServerPort
	}
	if c.Server.ReadTimeout == 0 {
		c.Server.ReadTimeout = DefaultValues.ServerReadTimeout
	}
	if c.Server.WriteTimeout == 0 {
		c.Server.WriteTimeout = DefaultValues.ServerWriteTimeout
	}
	if c.Server.IdleTimeout == 0 {
		c.Server.IdleTimeout = DefaultValues.ServerIdleTimeout
	}

	// Engine defaults
	if len(c.Engine.Endpoints) == 0 {
		c.Engine.Endpoints = append([]string{}, DefaultValues.EngineEndpoints...)
	}
	if c.Engine.ConnectTimeout == 0 {
		c.Engine.ConnectTimeout = DefaultValues.EngineConnectTimeout
	}
	if c.Engine.RequestTimeout == 0 {
		c.Engine.RequestTimeout = DefaultValues.EngineRequestTimeout
	}
	if c.Engine.MaxRetries == 0 {
		c.Engine.MaxRetries = DefaultValues.EngineMaxRetries
	}
	if c.Engine.RetryBackoffMin == 0 {
		c.Engine.RetryBackoffMin = DefaultValues.EngineRetryBackoffMin
	}
	if c.Engine.RetryBackoffMax == 0 {
		c.Engine.RetryBackoffMax = DefaultValues.EngineRetryBackoffMax
	}
	if c.Engine.MaxConnections == 0 {
		c.Engine.MaxConnections = DefaultValues.EngineMaxConnections
	}
	if c.Engine.IdleTimeout == 0 {
		c.Engine.IdleTimeout = DefaultValues.EngineIdleTimeout
	}
	if c.Engine.KeepAlive.Time == 0 {
		c.Engine.KeepAlive.Time = DefaultValues.EngineKeepAliveTime
	}
	if c.Engine.KeepAlive.Timeout == 0 {
		c.Engine.KeepAlive.Timeout = DefaultValues.EngineKeepAliveTimeout
	}

	// Auth defaults
	if c.Auth.JWTSecret == "" {
		c.Auth.JWTSecret = generateDefaultJWTSecret()
	}
	if c.Auth.JWTExpiry == 0 {
		c.Auth.JWTExpiry = DefaultValues.AuthJWTExpiry
	}
	if c.Auth.JWTIssuer == "" {
		c.Auth.JWTIssuer = DefaultValues.AuthJWTIssuer
	}
	if c.Auth.ArgonMemory == 0 {
		c.Auth.ArgonMemory = DefaultValues.AuthArgonMemory
	}
	if c.Auth.ArgonTime == 0 {
		c.Auth.ArgonTime = DefaultValues.AuthArgonTime
	}
	if c.Auth.ArgonThreads == 0 {
		c.Auth.ArgonThreads = DefaultValues.AuthArgonThreads
	}

	// Storage defaults
	if c.Storage.Type == "" {
		c.Storage.Type = DefaultValues.StorageType
	}
	if c.Storage.ConnectionString == "" {
		c.Storage.ConnectionString = DefaultValues.StorageConnectionString
	}
	if c.Storage.MaxConnections == 0 {
		c.Storage.MaxConnections = DefaultValues.StorageMaxConnections
	}
	if c.Storage.MaxIdleConns == 0 {
		c.Storage.MaxIdleConns = DefaultValues.StorageMaxIdleConns
	}
	if c.Storage.ConnMaxLifetime == 0 {
		c.Storage.ConnMaxLifetime = DefaultValues.StorageConnMaxLifetime
	}

	// Logging defaults
	if c.Logging.Level == "" {
		c.Logging.Level = DefaultValues.LoggingLevel
	}
	if c.Logging.Format == "" {
		c.Logging.Format = DefaultValues.LoggingFormat
	}

	// Observability defaults
	if c.Observability.MetricsPath == "" {
		c.Observability.MetricsPath = DefaultValues.ObservabilityMetricsPath
	}
	if c.Observability.LogLevel == "" {
		c.Observability.LogLevel = DefaultValues.ObservabilityLogLevel
	}
	if c.Observability.LogFormat == "" {
		c.Observability.LogFormat = DefaultValues.ObservabilityLogFormat
	}

	// CORS defaults
	if len(c.Server.CORS.AllowedOrigins) == 0 {
		c.Server.CORS.AllowedOrigins = append([]string{}, DefaultValues.CORSAllowedOrigins...)
	}
	if len(c.Server.CORS.AllowedMethods) == 0 {
		c.Server.CORS.AllowedMethods = append([]string{}, DefaultValues.CORSAllowedMethods...)
	}
	if len(c.Server.CORS.AllowedHeaders) == 0 {
		c.Server.CORS.AllowedHeaders = append([]string{}, DefaultValues.CORSAllowedHeaders...)
	}
	if len(c.Server.CORS.ExposeHeaders) == 0 {
		c.Server.CORS.ExposeHeaders = append([]string{}, DefaultValues.CORSExposeHeaders...)
	}
	if c.Server.CORS.MaxAge == 0 {
		c.Server.CORS.MaxAge = DefaultValues.CORSMaxAge
	}

	// Rate limit defaults
	if c.Server.RateLimit.RequestsPerMinute == 0 {
		c.Server.RateLimit.RequestsPerMinute = DefaultValues.RateLimitRequestsPerMinute
	}
	if c.Server.RateLimit.BurstSize == 0 {
		c.Server.RateLimit.BurstSize = DefaultValues.RateLimitBurstSize
	}
}

// GetConfigFromEnvironmentWithDefaults creates a config from environment variables with defaults applied
func GetConfigFromEnvironmentWithDefaults() (*Config, error) {
	config := NewDefaultConfig()

	if err := config.LoadFromEnvironment(); err != nil {
		return nil, err
	}

	config.ApplyDefaults()

	return config, nil
}
