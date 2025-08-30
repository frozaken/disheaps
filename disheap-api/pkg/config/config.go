package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/disheap/disheap/disheap-api/pkg/client"
	"github.com/disheap/disheap/disheap-api/pkg/server"
)

// Config holds the complete application configuration
type Config struct {
	Server        ServerConfig        `yaml:"server" mapstructure:"server"`
	Engine        EngineConfig        `yaml:"engine" mapstructure:"engine"`
	Auth          AuthConfig          `yaml:"auth" mapstructure:"auth"`
	Storage       StorageConfig       `yaml:"storage" mapstructure:"storage"`
	Logging       LoggingConfig       `yaml:"logging" mapstructure:"logging"`
	Observability ObservabilityConfig `yaml:"observability" mapstructure:"observability"`
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Host         string          `yaml:"host" mapstructure:"host"`
	Port         int             `yaml:"port" mapstructure:"port"`
	ReadTimeout  time.Duration   `yaml:"read_timeout" mapstructure:"read_timeout"`
	WriteTimeout time.Duration   `yaml:"write_timeout" mapstructure:"write_timeout"`
	IdleTimeout  time.Duration   `yaml:"idle_timeout" mapstructure:"idle_timeout"`
	TLSCertFile  string          `yaml:"tls_cert_file" mapstructure:"tls_cert_file"`
	TLSKeyFile   string          `yaml:"tls_key_file" mapstructure:"tls_key_file"`
	CORS         CORSConfig      `yaml:"cors" mapstructure:"cors"`
	RateLimit    RateLimitConfig `yaml:"rate_limit" mapstructure:"rate_limit"`
}

// EngineConfig holds engine client configuration
type EngineConfig struct {
	Endpoints       []string        `yaml:"endpoints" mapstructure:"endpoints"`
	ConnectTimeout  time.Duration   `yaml:"connect_timeout" mapstructure:"connect_timeout"`
	RequestTimeout  time.Duration   `yaml:"request_timeout" mapstructure:"request_timeout"`
	TLSEnabled      bool            `yaml:"tls_enabled" mapstructure:"tls_enabled"`
	MaxRetries      int             `yaml:"max_retries" mapstructure:"max_retries"`
	RetryBackoffMin time.Duration   `yaml:"retry_backoff_min" mapstructure:"retry_backoff_min"`
	RetryBackoffMax time.Duration   `yaml:"retry_backoff_max" mapstructure:"retry_backoff_max"`
	MaxConnections  int             `yaml:"max_connections" mapstructure:"max_connections"`
	IdleTimeout     time.Duration   `yaml:"idle_timeout" mapstructure:"idle_timeout"`
	KeepAlive       KeepAliveConfig `yaml:"keep_alive" mapstructure:"keep_alive"`
}

// KeepAliveConfig holds gRPC keepalive configuration
type KeepAliveConfig struct {
	Time    time.Duration `yaml:"time" mapstructure:"time"`
	Timeout time.Duration `yaml:"timeout" mapstructure:"timeout"`
}

// AuthConfig holds authentication configuration
type AuthConfig struct {
	JWTSecret    string        `yaml:"jwt_secret" mapstructure:"jwt_secret"`
	JWTExpiry    time.Duration `yaml:"jwt_expiry" mapstructure:"jwt_expiry"`
	JWTIssuer    string        `yaml:"jwt_issuer" mapstructure:"jwt_issuer"`
	ArgonMemory  uint32        `yaml:"argon_memory" mapstructure:"argon_memory"`
	ArgonTime    uint32        `yaml:"argon_time" mapstructure:"argon_time"`
	ArgonThreads uint8         `yaml:"argon_threads" mapstructure:"argon_threads"`
}

// CORSConfig holds CORS configuration
type CORSConfig struct {
	Enabled          bool          `yaml:"enabled" mapstructure:"enabled"`
	AllowedOrigins   []string      `yaml:"allowed_origins" mapstructure:"allowed_origins"`
	AllowedMethods   []string      `yaml:"allowed_methods" mapstructure:"allowed_methods"`
	AllowedHeaders   []string      `yaml:"allowed_headers" mapstructure:"allowed_headers"`
	ExposeHeaders    []string      `yaml:"expose_headers" mapstructure:"expose_headers"`
	AllowCredentials bool          `yaml:"allow_credentials" mapstructure:"allow_credentials"`
	MaxAge           time.Duration `yaml:"max_age" mapstructure:"max_age"`
}

// RateLimitConfig holds rate limiting configuration
type RateLimitConfig struct {
	Enabled           bool `yaml:"enabled" mapstructure:"enabled"`
	RequestsPerMinute int  `yaml:"requests_per_minute" mapstructure:"requests_per_minute"`
	BurstSize         int  `yaml:"burst_size" mapstructure:"burst_size"`
}

// StorageConfig holds database configuration
type StorageConfig struct {
	Type             string        `yaml:"type" mapstructure:"type"` // "sqlite", "postgres", "memory"
	ConnectionString string        `yaml:"connection_string" mapstructure:"connection_string"`
	MaxConnections   int           `yaml:"max_connections" mapstructure:"max_connections"`
	MaxIdleConns     int           `yaml:"max_idle_conns" mapstructure:"max_idle_conns"`
	ConnMaxLifetime  time.Duration `yaml:"conn_max_lifetime" mapstructure:"conn_max_lifetime"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level  string `yaml:"level" mapstructure:"level"`   // "debug", "info", "warn", "error"
	Format string `yaml:"format" mapstructure:"format"` // "json", "text"
}

// ObservabilityConfig holds observability configuration
type ObservabilityConfig struct {
	MetricsEnabled  bool   `yaml:"metrics_enabled" mapstructure:"metrics_enabled"`
	MetricsPath     string `yaml:"metrics_path" mapstructure:"metrics_path"`
	TracingEnabled  bool   `yaml:"tracing_enabled" mapstructure:"tracing_enabled"`
	TracingEndpoint string `yaml:"tracing_endpoint" mapstructure:"tracing_endpoint"`
	LogLevel        string `yaml:"log_level" mapstructure:"log_level"`
	LogFormat       string `yaml:"log_format" mapstructure:"log_format"`
}

// Default returns a default configuration
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Host:         "0.0.0.0",
			Port:         8080,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
			TLSCertFile:  "",
			TLSKeyFile:   "",
			CORS: CORSConfig{
				Enabled:          true,
				AllowedOrigins:   []string{"*"},
				AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
				AllowedHeaders:   []string{"*"},
				ExposeHeaders:    []string{"X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"},
				AllowCredentials: false,
				MaxAge:           12 * time.Hour,
			},
			RateLimit: RateLimitConfig{
				Enabled:           true,
				RequestsPerMinute: 1000,
				BurstSize:         100,
			},
		},
		Engine: EngineConfig{
			Endpoints:       []string{"localhost:9090"},
			ConnectTimeout:  10 * time.Second,
			RequestTimeout:  30 * time.Second,
			TLSEnabled:      false,
			MaxRetries:      3,
			RetryBackoffMin: 100 * time.Millisecond,
			RetryBackoffMax: 5 * time.Second,
			MaxConnections:  10,
			IdleTimeout:     5 * time.Minute,
			KeepAlive: KeepAliveConfig{
				Time:    30 * time.Second,
				Timeout: 5 * time.Second,
			},
		},
		Auth: AuthConfig{
			JWTSecret:    "change-me-in-production",
			JWTExpiry:    24 * time.Hour,
			JWTIssuer:    "disheap-api",
			ArgonMemory:  64 * 1024, // 64 MB
			ArgonTime:    3,         // 3 iterations
			ArgonThreads: 2,         // 2 threads
		},
		Storage: StorageConfig{
			Type:             "sqlite",
			ConnectionString: "./disheap.db",
			MaxConnections:   10,
			MaxIdleConns:     5,
			ConnMaxLifetime:  time.Hour,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
		},
		Observability: ObservabilityConfig{
			MetricsEnabled:  true,
			MetricsPath:     "/metrics",
			TracingEnabled:  false,
			TracingEndpoint: "",
			LogLevel:        "info",
			LogFormat:       "json",
		},
	}
}

// LoadFromEnvironment loads configuration from environment variables
func (c *Config) LoadFromEnvironment() error {
	// Server configuration
	if host := os.Getenv("DISHEAP_API_HOST"); host != "" {
		c.Server.Host = host
	}
	if port := os.Getenv("DISHEAP_API_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			c.Server.Port = p
		}
	}
	if readTimeout := os.Getenv("DISHEAP_READ_TIMEOUT"); readTimeout != "" {
		if d, err := time.ParseDuration(readTimeout); err == nil {
			c.Server.ReadTimeout = d
		}
	}
	if writeTimeout := os.Getenv("DISHEAP_WRITE_TIMEOUT"); writeTimeout != "" {
		if d, err := time.ParseDuration(writeTimeout); err == nil {
			c.Server.WriteTimeout = d
		}
	}
	if idleTimeout := os.Getenv("DISHEAP_IDLE_TIMEOUT"); idleTimeout != "" {
		if d, err := time.ParseDuration(idleTimeout); err == nil {
			c.Server.IdleTimeout = d
		}
	}
	c.Server.TLSCertFile = os.Getenv("DISHEAP_API_TLS_CERT_FILE")
	c.Server.TLSKeyFile = os.Getenv("DISHEAP_API_TLS_KEY_FILE")

	// Engine configuration
	if endpoints := os.Getenv("DISHEAP_ENGINE_ENDPOINTS"); endpoints != "" {
		c.Engine.Endpoints = strings.Split(endpoints, ",")
	} else if endpoint := os.Getenv("DISHEAP_ENGINE_ENDPOINT"); endpoint != "" {
		c.Engine.Endpoints = []string{endpoint}
	}
	if connectTimeout := os.Getenv("DISHEAP_ENGINE_CONNECT_TIMEOUT"); connectTimeout != "" {
		if d, err := time.ParseDuration(connectTimeout); err == nil {
			c.Engine.ConnectTimeout = d
		}
	}
	if requestTimeout := os.Getenv("DISHEAP_ENGINE_REQUEST_TIMEOUT"); requestTimeout != "" {
		if d, err := time.ParseDuration(requestTimeout); err == nil {
			c.Engine.RequestTimeout = d
		}
	}
	if tlsEnabled := os.Getenv("DISHEAP_ENGINE_TLS_ENABLED"); tlsEnabled != "" {
		c.Engine.TLSEnabled = tlsEnabled == "true" || tlsEnabled == "1"
	}
	if maxRetries := os.Getenv("DISHEAP_ENGINE_MAX_RETRIES"); maxRetries != "" {
		if r, err := strconv.Atoi(maxRetries); err == nil {
			c.Engine.MaxRetries = r
		}
	}

	// Auth configuration
	if jwtSecret := os.Getenv("DISHEAP_JWT_SECRET"); jwtSecret != "" {
		c.Auth.JWTSecret = jwtSecret
	}
	if jwtExpiry := os.Getenv("DISHEAP_JWT_EXPIRY"); jwtExpiry != "" {
		if d, err := time.ParseDuration(jwtExpiry); err == nil {
			c.Auth.JWTExpiry = d
		}
	}

	// Storage configuration
	if storageType := os.Getenv("DISHEAP_STORAGE_TYPE"); storageType != "" {
		c.Storage.Type = storageType
	}
	if connString := os.Getenv("DISHEAP_STORAGE_CONNECTION_STRING"); connString != "" {
		c.Storage.ConnectionString = connString
	}

	// CORS configuration
	if corsEnabled := os.Getenv("DISHEAP_CORS_ENABLED"); corsEnabled != "" {
		c.Server.CORS.Enabled = corsEnabled == "true" || corsEnabled == "1"
	}
	if origins := os.Getenv("DISHEAP_CORS_ALLOWED_ORIGINS"); origins != "" {
		c.Server.CORS.AllowedOrigins = strings.Split(origins, ",")
	}
	if credentials := os.Getenv("DISHEAP_CORS_ALLOW_CREDENTIALS"); credentials != "" {
		c.Server.CORS.AllowCredentials = credentials == "true" || credentials == "1"
	}

	// Rate limit configuration
	if rateLimitEnabled := os.Getenv("DISHEAP_RATE_LIMIT_ENABLED"); rateLimitEnabled != "" {
		c.Server.RateLimit.Enabled = rateLimitEnabled == "true" || rateLimitEnabled == "1"
	}
	if requestsPerMinute := os.Getenv("DISHEAP_RATE_LIMIT_REQUESTS_PER_MINUTE"); requestsPerMinute != "" {
		if rpm, err := strconv.Atoi(requestsPerMinute); err == nil {
			c.Server.RateLimit.RequestsPerMinute = rpm
		}
	}
	if burstSize := os.Getenv("DISHEAP_RATE_LIMIT_BURST_SIZE"); burstSize != "" {
		if bs, err := strconv.Atoi(burstSize); err == nil {
			c.Server.RateLimit.BurstSize = bs
		}
	}

	// Logging configuration
	if logLevel := os.Getenv("DISHEAP_LOG_LEVEL"); logLevel != "" {
		c.Logging.Level = logLevel
		c.Observability.LogLevel = logLevel
	}
	if logFormat := os.Getenv("DISHEAP_LOG_FORMAT"); logFormat != "" {
		c.Logging.Format = logFormat
		c.Observability.LogFormat = logFormat
	}

	// Observability configuration
	if metricsEnabled := os.Getenv("DISHEAP_METRICS_ENABLED"); metricsEnabled != "" {
		c.Observability.MetricsEnabled = metricsEnabled == "true" || metricsEnabled == "1"
	}
	if tracingEnabled := os.Getenv("DISHEAP_TRACING_ENABLED"); tracingEnabled != "" {
		c.Observability.TracingEnabled = tracingEnabled == "true" || tracingEnabled == "1"
	}

	return nil
}

// ToServerConfig converts to server.Config
func (c *Config) ToServerConfig() server.Config {
	return server.Config{
		Host:         c.Server.Host,
		Port:         c.Server.Port,
		ReadTimeout:  c.Server.ReadTimeout,
		WriteTimeout: c.Server.WriteTimeout,
		IdleTimeout:  c.Server.IdleTimeout,
		TLSCertFile:  c.Server.TLSCertFile,
		TLSKeyFile:   c.Server.TLSKeyFile,
		CORS: server.CORSConfig{
			Enabled:          c.Server.CORS.Enabled,
			AllowedOrigins:   c.Server.CORS.AllowedOrigins,
			AllowedMethods:   c.Server.CORS.AllowedMethods,
			AllowedHeaders:   c.Server.CORS.AllowedHeaders,
			ExposeHeaders:    c.Server.CORS.ExposeHeaders,
			AllowCredentials: c.Server.CORS.AllowCredentials,
			MaxAge:           c.Server.CORS.MaxAge,
		},
		RateLimit: server.RateLimitConfig{
			Enabled:           c.Server.RateLimit.Enabled,
			RequestsPerMinute: c.Server.RateLimit.RequestsPerMinute,
			BurstSize:         c.Server.RateLimit.BurstSize,
		},
	}
}

// ToClientConfig converts to client.Config
func (c *Config) ToClientConfig() client.Config {
	return client.Config{
		Endpoints:      c.Engine.Endpoints,
		ConnectTimeout: c.Engine.ConnectTimeout,
		RequestTimeout: c.Engine.RequestTimeout,
		TLSEnabled:     c.Engine.TLSEnabled,
		KeepAlive: client.KeepAliveConfig{
			Time:    c.Engine.KeepAlive.Time,
			Timeout: c.Engine.KeepAlive.Timeout,
		},
		Retry: client.RetryConfig{
			MaxAttempts:       c.Engine.MaxRetries,
			InitialBackoff:    c.Engine.RetryBackoffMin,
			MaxBackoff:        c.Engine.RetryBackoffMax,
			BackoffMultiplier: 2.0,
		},
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Server validation
	if c.Server.Host == "" {
		return fmt.Errorf("server.host cannot be empty")
	}
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("server.port must be between 1 and 65535")
	}
	if c.Server.ReadTimeout <= 0 {
		return fmt.Errorf("server.read_timeout must be positive")
	}
	if c.Server.WriteTimeout <= 0 {
		return fmt.Errorf("server.write_timeout must be positive")
	}

	// Engine validation
	if len(c.Engine.Endpoints) == 0 {
		return fmt.Errorf("engine.endpoints cannot be empty")
	}
	for _, endpoint := range c.Engine.Endpoints {
		if endpoint == "" {
			return fmt.Errorf("engine endpoints cannot contain empty strings")
		}
	}
	if c.Engine.ConnectTimeout <= 0 {
		return fmt.Errorf("engine.connect_timeout must be positive")
	}
	if c.Engine.RequestTimeout <= 0 {
		return fmt.Errorf("engine.request_timeout must be positive")
	}
	if c.Engine.MaxRetries < 0 {
		return fmt.Errorf("engine.max_retries cannot be negative")
	}

	// Auth validation
	if c.Auth.JWTSecret == "" {
		return fmt.Errorf("auth.jwt_secret cannot be empty")
	}
	if c.Auth.JWTSecret == "change-me-in-production" {
		return fmt.Errorf("auth.jwt_secret must be changed from default value")
	}
	if c.Auth.JWTExpiry <= 0 {
		return fmt.Errorf("auth.jwt_expiry must be positive")
	}

	// Storage validation
	if c.Storage.Type == "" {
		return fmt.Errorf("storage.type cannot be empty")
	}
	validStorageTypes := map[string]bool{
		"sqlite":   true,
		"postgres": true,
		"memory":   true,
	}
	if !validStorageTypes[c.Storage.Type] {
		return fmt.Errorf("storage.type must be one of: sqlite, postgres, memory")
	}
	if c.Storage.ConnectionString == "" {
		return fmt.Errorf("storage.connection_string cannot be empty")
	}

	// Logging validation
	validLogLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLogLevels[c.Logging.Level] {
		return fmt.Errorf("logging.level must be one of: debug, info, warn, error")
	}
	validLogFormats := map[string]bool{
		"json": true,
		"text": true,
	}
	if !validLogFormats[c.Logging.Format] {
		return fmt.Errorf("logging.format must be one of: json, text")
	}

	return nil
}

// IsDevelopment returns true if running in development mode
func (c *Config) IsDevelopment() bool {
	return c.Logging.Level == "debug" || c.Storage.Type == "memory"
}

// IsProduction returns true if running in production mode
func (c *Config) IsProduction() bool {
	return !c.IsDevelopment()
}
