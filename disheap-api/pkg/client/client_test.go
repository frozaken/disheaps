package client

import (
	"context"
	"testing"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.NotNil(t, config)
	assert.Equal(t, []string{"localhost:9090"}, config.Endpoints)
	assert.Equal(t, 5*time.Second, config.ConnectTimeout)
	assert.Equal(t, 30*time.Second, config.RequestTimeout)
	assert.Equal(t, 3, config.MaxRetries)
	assert.Equal(t, 100*time.Millisecond, config.RetryBackoffMin)
	assert.Equal(t, 1*time.Second, config.RetryBackoffMax)
	assert.Equal(t, 10, config.MaxConnections)
	assert.Equal(t, 5*time.Minute, config.IdleTimeout)
	assert.Nil(t, config.TLS)
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		expectErr bool
	}{
		{
			name:      "valid config",
			config:    DefaultConfig(),
			expectErr: false,
		},
		{
			name: "empty endpoints",
			config: &Config{
				Endpoints:      []string{},
				ConnectTimeout: 5 * time.Second,
				RequestTimeout: 30 * time.Second,
				MaxRetries:     3,
			},
			expectErr: true,
		},
		{
			name: "zero connect timeout",
			config: &Config{
				Endpoints:      []string{"localhost:9090"},
				ConnectTimeout: 0,
				RequestTimeout: 30 * time.Second,
				MaxRetries:     3,
			},
			expectErr: true,
		},
		{
			name: "zero request timeout",
			config: &Config{
				Endpoints:      []string{"localhost:9090"},
				ConnectTimeout: 5 * time.Second,
				RequestTimeout: 0,
				MaxRetries:     3,
			},
			expectErr: true,
		},
		{
			name: "negative max retries",
			config: &Config{
				Endpoints:      []string{"localhost:9090"},
				ConnectTimeout: 5 * time.Second,
				RequestTimeout: 30 * time.Second,
				MaxRetries:     -1,
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfig(tt.config)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewClient(t *testing.T) {
	t.Run("with valid config", func(t *testing.T) {
		// This test would require a running engine, so we'll skip the connection part
		config := &Config{
			Endpoints:      []string{"localhost:9999"}, // Use a port that likely won't be available
			ConnectTimeout: 1 * time.Millisecond,       // Very short timeout to fail fast
			RequestTimeout: 30 * time.Second,
			MaxRetries:     0,
		}

		// We expect this to fail since there's no server running, but the client should be created
		client, err := NewClient(config)

		// Since we can't connect, this should fail
		if err != nil {
			assert.Contains(t, err.Error(), "failed to establish any connections")
		} else {
			// If it somehow succeeds, clean up
			assert.NotNil(t, client)
			client.Close()
		}
	})

	t.Run("with nil config", func(t *testing.T) {
		client, err := NewClient(nil)

		// Should use default config, but still fail to connect
		if err != nil {
			assert.Contains(t, err.Error(), "failed to establish any connections")
		} else {
			// If it somehow succeeds, clean up
			assert.NotNil(t, client)
			client.Close()
		}
	})

	t.Run("with invalid config", func(t *testing.T) {
		config := &Config{
			Endpoints:      []string{}, // Invalid: no endpoints
			ConnectTimeout: 5 * time.Second,
			RequestTimeout: 30 * time.Second,
			MaxRetries:     3,
		}

		client, err := NewClient(config)
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "invalid config")
	})
}

func TestEngineClientInterface(t *testing.T) {
	// Test that our client struct implements the EngineClient interface
	config := DefaultConfig()

	// Create client (this will fail but we just want to test interface compliance)
	var _ EngineClient = &client{config: config}
}

// Mock helper functions for testing (these would be used with a test server)

func createMockHeapRequest() *disheapv1.MakeHeapReq {
	return &disheapv1.MakeHeapReq{
		Topic:             "test-topic",
		Mode:              disheapv1.Mode_MIN,
		Partitions:        3,
		ReplicationFactor: 2,
		TopKBound:         10,
	}
}

func createMockEnqueueRequest() *disheapv1.EnqueueReq {
	return &disheapv1.EnqueueReq{
		Topic:    "test-topic",
		Payload:  []byte("test message"),
		Priority: 100,
	}
}

// Integration test helpers (these would be used with a real engine server)

func TestClientOperations(t *testing.T) {
	t.Skip("Integration test - requires running engine server")

	// This is an example of how integration tests would look
	config := DefaultConfig()
	config.Endpoints = []string{"localhost:9090"}

	client, err := NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Test health check
	err = client.Healthy()
	assert.NoError(t, err)

	// Test heap creation
	makeReq := createMockHeapRequest()
	resp, err := client.MakeHeap(ctx, makeReq)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "test-topic", resp.Topic)

	// Test list heaps
	listReq := &disheapv1.ListHeapsReq{PageSize: 10}
	listResp, err := client.ListHeaps(ctx, listReq)
	assert.NoError(t, err)
	assert.NotNil(t, listResp)

	// Test enqueue
	enqReq := createMockEnqueueRequest()
	enqResp, err := client.Enqueue(ctx, enqReq)
	assert.NoError(t, err)
	assert.NotNil(t, enqResp)
	assert.NotEmpty(t, enqResp.MessageId)
}

func TestClientRetry(t *testing.T) {
	t.Skip("Integration test - requires controlled failure scenarios")

	// This would test retry behavior with a server that can simulate failures
}
