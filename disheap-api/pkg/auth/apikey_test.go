package auth

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateAPIKey(t *testing.T) {
	keyID, fullKey, err := GenerateAPIKey()
	require.NoError(t, err)

	// Check format: dh.<keyID>.<secret>
	assert.True(t, strings.HasPrefix(fullKey, "dh."))

	parts := strings.Split(strings.TrimPrefix(fullKey, "dh."), ".")
	require.Len(t, parts, 2)

	assert.Equal(t, keyID, parts[0])
	assert.NotEqual(t, keyID, parts[1]) // keyID and secret should be different

	// Generate another key to ensure uniqueness
	keyID2, fullKey2, err := GenerateAPIKey()
	require.NoError(t, err)

	assert.NotEqual(t, keyID, keyID2)
	assert.NotEqual(t, fullKey, fullKey2)
}

func TestParseAPIKey(t *testing.T) {
	tests := []struct {
		name      string
		apiKey    string
		expectErr bool
	}{
		{
			name:      "valid API key",
			apiKey:    "dh.YWJjZGVmZ2g.aWprbG1ub3BxcnN0dXZ3eHl6MTIzNA",
			expectErr: false,
		},
		{
			name:      "invalid prefix",
			apiKey:    "invalid.YWJjZGVmZ2g.aWprbG1ub3BxcnN0dXZ3eHl6MTIzNA",
			expectErr: true,
		},
		{
			name:      "missing parts",
			apiKey:    "dh.YWJjZGVmZ2g",
			expectErr: true,
		},
		{
			name:      "invalid base64 in keyID",
			apiKey:    "dh.invalid!@#.aWprbG1ub3BxcnN0dXZ3eHl6MTIzNA",
			expectErr: true,
		},
		{
			name:      "invalid base64 in secret",
			apiKey:    "dh.YWJjZGVmZ2g.invalid!@#",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyID, secret, err := ParseAPIKey(tt.apiKey)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Empty(t, keyID)
				assert.Empty(t, secret)
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, keyID)
				assert.NotEmpty(t, secret)
			}
		})
	}
}

func TestHashAndVerifyAPIKey(t *testing.T) {
	config := DefaultArgon2Config()
	apiKey := "dh.testkey.testsecret"

	// Hash the API key
	hashedKey, err := HashAPIKey(apiKey, config)
	require.NoError(t, err)

	// Verify correct API key
	valid, err := VerifyAPIKey(apiKey, hashedKey)
	require.NoError(t, err)
	assert.True(t, valid)

	// Verify incorrect API key
	valid, err = VerifyAPIKey("dh.wrong.key", hashedKey)
	require.NoError(t, err)
	assert.False(t, valid)

	// Test with empty API key
	valid, err = VerifyAPIKey("", hashedKey)
	require.NoError(t, err)
	assert.False(t, valid)
}

func TestAPIKeyManager(t *testing.T) {
	manager := NewAPIKeyManager()

	// Create an API key
	apiKey, fullKey, err := manager.CreateAPIKey("Test Key", "user123")
	require.NoError(t, err)

	assert.NotEmpty(t, apiKey.ID)
	assert.Equal(t, "Test Key", apiKey.Name)
	assert.NotEmpty(t, apiKey.HashedKey)
	assert.Equal(t, "user123", apiKey.CreatedBy)
	assert.False(t, apiKey.IsRevoked)
	assert.WithinDuration(t, time.Now(), apiKey.CreatedAt, time.Second)

	// Verify the API key
	valid, err := manager.VerifyAPIKey(fullKey, apiKey)
	require.NoError(t, err)
	assert.True(t, valid)

	// Test with revoked key
	manager.RevokeAPIKey(apiKey)
	valid, err = manager.VerifyAPIKey(fullKey, apiKey)
	require.NoError(t, err)
	assert.False(t, valid)
}

func TestAPIKeyToResponse(t *testing.T) {
	apiKey := &APIKey{
		ID:        "testkey123",
		Name:      "Test Key",
		HashedKey: "hashed",
		CreatedAt: time.Now(),
		CreatedBy: "user123",
		IsRevoked: false,
	}

	// Test with key included
	response := apiKey.ToResponse(true, "dh.testkey123.secret")
	assert.Equal(t, apiKey.ID, response.ID)
	assert.Equal(t, apiKey.Name, response.Name)
	assert.Equal(t, "dh.testkey123.secret", response.Key)
	assert.Equal(t, apiKey.CreatedBy, response.CreatedBy)

	// Test without key included
	response = apiKey.ToResponse(false, "dh.testkey123.secret")
	assert.Empty(t, response.Key)
}

func TestAPIKeyToListItem(t *testing.T) {
	lastUsed := time.Now().Add(-time.Hour)
	apiKey := &APIKey{
		ID:         "testkey123",
		Name:       "Test Key",
		CreatedAt:  time.Now(),
		LastUsedAt: &lastUsed,
		IsRevoked:  false,
		CreatedBy:  "user123",
	}

	listItem := apiKey.ToListItem()
	assert.Equal(t, apiKey.ID, listItem.ID)
	assert.Equal(t, apiKey.Name, listItem.Name)
	assert.Equal(t, apiKey.CreatedAt, listItem.CreatedAt)
	assert.Equal(t, apiKey.LastUsedAt, listItem.LastUsedAt)
	assert.Equal(t, apiKey.IsRevoked, listItem.IsRevoked)
	assert.Equal(t, apiKey.CreatedBy, listItem.CreatedBy)
}

func TestValidateAPIKeyFormat(t *testing.T) {
	tests := []struct {
		name      string
		apiKey    string
		expectErr bool
	}{
		{
			name:      "valid format",
			apiKey:    "dh.YWJjZGVmZ2g.aWprbG1ub3BxcnN0dXZ3eHl6MTIzNA",
			expectErr: false,
		},
		{
			name:      "invalid format",
			apiKey:    "invalid_format",
			expectErr: true,
		},
		{
			name:      "empty key",
			apiKey:    "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAPIKeyFormat(tt.apiKey)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDefaultArgon2Config(t *testing.T) {
	config := DefaultArgon2Config()

	assert.Equal(t, uint32(64*1024), config.Memory)
	assert.Equal(t, uint32(3), config.Iterations)
	assert.Equal(t, uint8(2), config.Parallelism)
	assert.Equal(t, uint32(16), config.SaltLength)
	assert.Equal(t, uint32(32), config.KeyLength)
}

func TestAPIKeyManagerUpdateLastUsed(t *testing.T) {
	manager := NewAPIKeyManager()

	apiKey := &APIKey{
		ID:         "test",
		Name:       "Test",
		HashedKey:  "hash",
		CreatedAt:  time.Now(),
		LastUsedAt: nil,
		IsRevoked:  false,
		CreatedBy:  "user",
	}

	// Initially no last used time
	assert.Nil(t, apiKey.LastUsedAt)

	// Update last used
	manager.UpdateLastUsed(apiKey)

	// Should now have a last used time
	assert.NotNil(t, apiKey.LastUsedAt)
	assert.WithinDuration(t, time.Now(), *apiKey.LastUsedAt, time.Second)
}

func BenchmarkGenerateAPIKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, err := GenerateAPIKey()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHashAPIKey(b *testing.B) {
	config := DefaultArgon2Config()
	apiKey := "dh.testkey.testsecret"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := HashAPIKey(apiKey, config)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVerifyAPIKey(b *testing.B) {
	config := DefaultArgon2Config()
	apiKey := "dh.testkey.testsecret"
	hashedKey, err := HashAPIKey(apiKey, config)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := VerifyAPIKey(apiKey, hashedKey)
		if err != nil {
			b.Fatal(err)
		}
	}
}
