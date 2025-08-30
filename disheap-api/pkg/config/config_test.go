package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := Default()

	assert.Equal(t, "0.0.0.0", config.Server.Host)
	assert.Equal(t, 8080, config.Server.Port)
	assert.Equal(t, 30*time.Second, config.Server.ReadTimeout)
	assert.Equal(t, 30*time.Second, config.Server.WriteTimeout)
	assert.Equal(t, 60*time.Second, config.Server.IdleTimeout)

	assert.Equal(t, []string{"localhost:9090"}, config.Engine.Endpoints)
	assert.Equal(t, 10*time.Second, config.Engine.ConnectTimeout)
	assert.Equal(t, 30*time.Second, config.Engine.RequestTimeout)
	assert.False(t, config.Engine.TLSEnabled)
	assert.Equal(t, 3, config.Engine.MaxRetries)

	assert.Equal(t, "change-me-in-production", config.Auth.JWTSecret)
	assert.Equal(t, 24*time.Hour, config.Auth.JWTExpiry)
	assert.Equal(t, "disheap-api", config.Auth.JWTIssuer)

	assert.Equal(t, "sqlite", config.Storage.Type)
	assert.Equal(t, "./disheap.db", config.Storage.ConnectionString)

	assert.Equal(t, "info", config.Logging.Level)
	assert.Equal(t, "json", config.Logging.Format)

	assert.True(t, config.Server.CORS.Enabled)
	assert.True(t, config.Server.RateLimit.Enabled)
	assert.True(t, config.Observability.MetricsEnabled)
}

func TestNewDefaultConfig(t *testing.T) {
	config := NewDefaultConfig()

	// Should have all default values
	assert.Equal(t, DefaultValues.ServerHost, config.Server.Host)
	assert.Equal(t, DefaultValues.ServerPort, config.Server.Port)
	assert.Equal(t, DefaultValues.EngineEndpoints[0], config.Engine.Endpoints[0])
	assert.Equal(t, DefaultValues.AuthJWTIssuer, config.Auth.JWTIssuer)
	assert.Equal(t, DefaultValues.StorageType, config.Storage.Type)
}

func TestLoadFromEnvironment(t *testing.T) {
	// Save original environment
	originalEnv := make(map[string]string)
	envVars := []string{
		"DISHEAP_API_HOST",
		"DISHEAP_API_PORT",
		"DISHEAP_ENGINE_ENDPOINT",
		"DISHEAP_JWT_SECRET",
		"DISHEAP_STORAGE_TYPE",
		"DISHEAP_CORS_ENABLED",
		"DISHEAP_RATE_LIMIT_ENABLED",
	}

	for _, env := range envVars {
		originalEnv[env] = os.Getenv(env)
	}

	// Clean up after test
	defer func() {
		for _, env := range envVars {
			if originalEnv[env] == "" {
				os.Unsetenv(env)
			} else {
				os.Setenv(env, originalEnv[env])
			}
		}
	}()

	// Set test environment variables
	os.Setenv("DISHEAP_API_HOST", "127.0.0.1")
	os.Setenv("DISHEAP_API_PORT", "9999")
	os.Setenv("DISHEAP_ENGINE_ENDPOINT", "localhost:8888")
	os.Setenv("DISHEAP_JWT_SECRET", "test-secret-key-for-testing-purposes")
	os.Setenv("DISHEAP_STORAGE_TYPE", "postgres")
	os.Setenv("DISHEAP_CORS_ENABLED", "false")
	os.Setenv("DISHEAP_RATE_LIMIT_ENABLED", "false")

	config := Default()
	err := config.LoadFromEnvironment()
	require.NoError(t, err)

	assert.Equal(t, "127.0.0.1", config.Server.Host)
	assert.Equal(t, 9999, config.Server.Port)
	assert.Equal(t, []string{"localhost:8888"}, config.Engine.Endpoints)
	assert.Equal(t, "test-secret-key-for-testing-purposes", config.Auth.JWTSecret)
	assert.Equal(t, "postgres", config.Storage.Type)
	assert.False(t, config.Server.CORS.Enabled)
	assert.False(t, config.Server.RateLimit.Enabled)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		expectErr bool
		errField  string
	}{
		{
			name:      "valid default config",
			config:    func() *Config { c := Default(); c.Auth.JWTSecret = "valid-32-character-secret-key-here"; return c }(),
			expectErr: false,
		},
		{
			name:      "empty server host",
			config:    func() *Config { c := Default(); c.Server.Host = ""; return c }(),
			expectErr: true,
			errField:  "server.host",
		},
		{
			name:      "invalid server port",
			config:    func() *Config { c := Default(); c.Server.Port = 0; return c }(),
			expectErr: true,
			errField:  "server.port",
		},
		{
			name:      "empty engine endpoints",
			config:    func() *Config { c := Default(); c.Engine.Endpoints = []string{}; return c }(),
			expectErr: true,
			errField:  "engine.endpoints",
		},
		{
			name:      "default jwt secret",
			config:    Default(),
			expectErr: true,
			errField:  "auth.jwt_secret",
		},
		{
			name:      "empty storage type",
			config:    func() *Config { c := Default(); c.Storage.Type = ""; return c }(),
			expectErr: true,
			errField:  "storage.type",
		},
		{
			name:      "invalid storage type",
			config:    func() *Config { c := Default(); c.Storage.Type = "invalid"; return c }(),
			expectErr: true,
			errField:  "storage.type",
		},
		{
			name:      "invalid log level",
			config:    func() *Config { c := Default(); c.Logging.Level = "invalid"; return c }(),
			expectErr: true,
			errField:  "logging.level",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.expectErr {
				assert.Error(t, err)
				if tt.errField != "" {
					assert.Contains(t, err.Error(), tt.errField)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateDetailed(t *testing.T) {
	config := &Config{
		Server: ServerConfig{
			Host: "", // Invalid
			Port: 0,  // Invalid
		},
		Engine: EngineConfig{
			Endpoints: []string{}, // Invalid
		},
		Auth: AuthConfig{
			JWTSecret: "", // Invalid
		},
		Storage: StorageConfig{
			Type: "invalid", // Invalid
		},
		Logging: LoggingConfig{
			Level: "invalid", // Invalid
		},
	}

	errors := config.ValidateDetailed()
	assert.NotEmpty(t, errors)

	// Should have multiple errors
	assert.True(t, len(errors) >= 5)

	// Check that it implements error interface
	assert.Contains(t, errors.Error(), "multiple validation errors")
}

func TestToServerConfig(t *testing.T) {
	config := Default()
	serverConfig := config.ToServerConfig()

	assert.Equal(t, config.Server.Host, serverConfig.Host)
	assert.Equal(t, config.Server.Port, serverConfig.Port)
	assert.Equal(t, config.Server.ReadTimeout, serverConfig.ReadTimeout)
	assert.Equal(t, config.Server.WriteTimeout, serverConfig.WriteTimeout)
	assert.Equal(t, config.Server.IdleTimeout, serverConfig.IdleTimeout)
	assert.Equal(t, config.Server.TLSCertFile, serverConfig.TLSCertFile)
	assert.Equal(t, config.Server.TLSKeyFile, serverConfig.TLSKeyFile)
}

func TestToClientConfig(t *testing.T) {
	config := Default()
	clientConfig := config.ToClientConfig()

	assert.Equal(t, config.Engine.Endpoints, clientConfig.Endpoints)
	assert.Equal(t, config.Engine.ConnectTimeout, clientConfig.ConnectTimeout)
	assert.Equal(t, config.Engine.RequestTimeout, clientConfig.RequestTimeout)
	assert.Equal(t, config.Engine.TLSEnabled, clientConfig.TLSEnabled)
	assert.Equal(t, config.Engine.KeepAlive.Time, clientConfig.KeepAlive.Time)
	assert.Equal(t, config.Engine.KeepAlive.Timeout, clientConfig.KeepAlive.Timeout)
	assert.Equal(t, config.Engine.MaxRetries, clientConfig.Retry.MaxAttempts)
	assert.Equal(t, config.Engine.RetryBackoffMin, clientConfig.Retry.InitialBackoff)
	assert.Equal(t, config.Engine.RetryBackoffMax, clientConfig.Retry.MaxBackoff)
	assert.Equal(t, 2.0, clientConfig.Retry.BackoffMultiplier)
}

func TestIsDevelopment(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
		isDev  bool
	}{
		{
			name:   "debug logging",
			config: func() *Config { c := Default(); c.Logging.Level = "debug"; return c }(),
			isDev:  true,
		},
		{
			name:   "memory storage",
			config: func() *Config { c := Default(); c.Storage.Type = "memory"; return c }(),
			isDev:  true,
		},
		{
			name:   "info logging with sqlite",
			config: Default(),
			isDev:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.isDev, tt.config.IsDevelopment())
			assert.Equal(t, !tt.isDev, tt.config.IsProduction())
		})
	}
}

func TestApplyDefaults(t *testing.T) {
	config := &Config{}

	config.ApplyDefaults()

	// Should now have all default values
	assert.Equal(t, DefaultValues.ServerHost, config.Server.Host)
	assert.Equal(t, DefaultValues.ServerPort, config.Server.Port)
	assert.Equal(t, DefaultValues.EngineEndpoints, config.Engine.Endpoints)
	assert.Equal(t, DefaultValues.AuthJWTExpiry, config.Auth.JWTExpiry)
	assert.Equal(t, DefaultValues.StorageType, config.Storage.Type)
	assert.Equal(t, DefaultValues.LoggingLevel, config.Logging.Level)
}

func TestGetConfigFromEnvironmentWithDefaults(t *testing.T) {
	// Save and restore environment
	originalPort := os.Getenv("DISHEAP_API_PORT")
	defer func() {
		if originalPort == "" {
			os.Unsetenv("DISHEAP_API_PORT")
		} else {
			os.Setenv("DISHEAP_API_PORT", originalPort)
		}
	}()

	// Set a test environment variable
	os.Setenv("DISHEAP_API_PORT", "7777")

	config, err := GetConfigFromEnvironmentWithDefaults()
	require.NoError(t, err)

	// Should have environment override
	assert.Equal(t, 7777, config.Server.Port)

	// Should have defaults for unset values
	assert.Equal(t, DefaultValues.ServerHost, config.Server.Host)
	assert.Equal(t, DefaultValues.EngineEndpoints, config.Engine.Endpoints)
}

func TestValidationError(t *testing.T) {
	err := ValidationError{
		Field:   "test.field",
		Message: "test message",
	}

	assert.Equal(t, "validation error for field 'test.field': test message", err.Error())
}

func TestValidationErrors(t *testing.T) {
	// Test empty errors
	var errors ValidationErrors
	assert.Equal(t, "no validation errors", errors.Error())

	// Test single error
	errors = ValidationErrors{
		{Field: "field1", Message: "message1"},
	}
	assert.Contains(t, errors.Error(), "field1")
	assert.Contains(t, errors.Error(), "message1")

	// Test multiple errors
	errors = ValidationErrors{
		{Field: "field1", Message: "message1"},
		{Field: "field2", Message: "message2"},
	}
	assert.Contains(t, errors.Error(), "multiple validation errors")
	assert.Contains(t, errors.Error(), "field1")
	assert.Contains(t, errors.Error(), "field2")
}

func TestEnvironmentVariableParsing(t *testing.T) {
	tests := []struct {
		name     string
		envVar   string
		envValue string
		test     func(*Config) bool
	}{
		{
			name:     "boolean true (true)",
			envVar:   "DISHEAP_CORS_ENABLED",
			envValue: "true",
			test:     func(c *Config) bool { return c.Server.CORS.Enabled },
		},
		{
			name:     "boolean true (1)",
			envVar:   "DISHEAP_CORS_ENABLED",
			envValue: "1",
			test:     func(c *Config) bool { return c.Server.CORS.Enabled },
		},
		{
			name:     "boolean false",
			envVar:   "DISHEAP_CORS_ENABLED",
			envValue: "false",
			test:     func(c *Config) bool { return !c.Server.CORS.Enabled },
		},
		{
			name:     "duration parsing",
			envVar:   "DISHEAP_READ_TIMEOUT",
			envValue: "45s",
			test:     func(c *Config) bool { return c.Server.ReadTimeout == 45*time.Second },
		},
		{
			name:     "integer parsing",
			envVar:   "DISHEAP_API_PORT",
			envValue: "9090",
			test:     func(c *Config) bool { return c.Server.Port == 9090 },
		},
		{
			name:     "comma-separated list",
			envVar:   "DISHEAP_ENGINE_ENDPOINTS",
			envValue: "host1:9090,host2:9091,host3:9092",
			test: func(c *Config) bool {
				return len(c.Engine.Endpoints) == 3 &&
					c.Engine.Endpoints[0] == "host1:9090" &&
					c.Engine.Endpoints[1] == "host2:9091" &&
					c.Engine.Endpoints[2] == "host3:9092"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original value
			original := os.Getenv(tt.envVar)
			defer func() {
				if original == "" {
					os.Unsetenv(tt.envVar)
				} else {
					os.Setenv(tt.envVar, original)
				}
			}()

			// Set test value
			os.Setenv(tt.envVar, tt.envValue)

			// Load config and test
			config := Default()
			err := config.LoadFromEnvironment()
			require.NoError(t, err)

			assert.True(t, tt.test(config), "Test failed for %s=%s", tt.envVar, tt.envValue)
		})
	}
}
