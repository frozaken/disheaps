package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/argon2"
)

// APIKey represents an API key
type APIKey struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	HashedKey  string     `json:"hashed_key"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
	IsRevoked  bool       `json:"is_revoked"`
	CreatedBy  string     `json:"created_by"`
}

// APIKeyRequest represents a request to create an API key
type APIKeyRequest struct {
	Name string `json:"name" validate:"required,min=1,max=100"`
}

// APIKeyResponse represents an API key response (only returns the actual key once)
type APIKeyResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Key       string    `json:"key,omitempty"` // Only returned on creation
	CreatedAt time.Time `json:"created_at"`
	CreatedBy string    `json:"created_by"`
}

// APIKeyListItem represents an API key in a list (without sensitive data)
type APIKeyListItem struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
	IsRevoked  bool       `json:"is_revoked"`
	CreatedBy  string     `json:"created_by"`
}

// Argon2Config holds Argon2 hashing configuration
type Argon2Config struct {
	Memory      uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

// DefaultArgon2Config returns secure default Argon2 configuration
func DefaultArgon2Config() Argon2Config {
	return Argon2Config{
		Memory:      64 * 1024, // 64 MB
		Iterations:  3,         // 3 iterations
		Parallelism: 2,         // 2 threads
		SaltLength:  16,        // 16 bytes salt
		KeyLength:   32,        // 32 bytes key
	}
}

// generateSecureKey generates a cryptographically secure random key
func generateSecureKey(length int) ([]byte, error) {
	key := make([]byte, length)
	_, err := rand.Read(key)
	if err != nil {
		return nil, fmt.Errorf("failed to generate secure key: %w", err)
	}
	return key, nil
}

// GenerateAPIKey generates a new API key with the format: dh_<base64_key_id>_<base64_secret>
func GenerateAPIKey() (keyID string, fullKey string, err error) {
	// Generate key ID (8 bytes = ~11 chars base64)
	keyIDBytes, err := generateSecureKey(8)
	if err != nil {
		return "", "", err
	}

	// Generate secret (24 bytes = 32 chars base64)
	secretBytes, err := generateSecureKey(24)
	if err != nil {
		return "", "", err
	}

	// Encode to base64 (URL safe, no padding)
	keyID = base64.RawURLEncoding.EncodeToString(keyIDBytes)
	secret := base64.RawURLEncoding.EncodeToString(secretBytes)

	// Format according to INTERFACE_CONTRACTS.md: dh_<base64_key_id>_<base64_secret>
	fullKey = fmt.Sprintf("dh.%s.%s", keyID, secret)

	return keyID, fullKey, nil
}

// ParseAPIKey parses an API key string and returns the key ID and secret
func ParseAPIKey(apiKey string) (keyID string, secret string, err error) {
	// API key format: dh.<base64_key_id>.<base64_secret>
	if !strings.HasPrefix(apiKey, "dh.") {
		return "", "", fmt.Errorf("invalid API key format: missing dh. prefix")
	}

	// Remove prefix
	keyData := strings.TrimPrefix(apiKey, "dh.")

	// Split into parts
	parts := strings.Split(keyData, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid API key format: expected 2 parts after dh. prefix")
	}

	keyID = parts[0]
	secret = parts[1]

	// Validate base64 encoding
	if _, err := base64.RawURLEncoding.DecodeString(keyID); err != nil {
		return "", "", fmt.Errorf("invalid API key format: key ID is not valid base64")
	}

	if _, err := base64.RawURLEncoding.DecodeString(secret); err != nil {
		return "", "", fmt.Errorf("invalid API key format: secret is not valid base64")
	}

	return keyID, secret, nil
}

// HashAPIKey hashes an API key using Argon2id
func HashAPIKey(apiKey string, config Argon2Config) (string, error) {
	// Generate salt
	salt, err := generateSecureKey(int(config.SaltLength))
	if err != nil {
		return "", fmt.Errorf("failed to generate salt: %w", err)
	}

	// Hash the key using Argon2id
	hash := argon2.IDKey([]byte(apiKey), salt, config.Iterations, config.Memory, config.Parallelism, config.KeyLength)

	// Encode the salt and hash for storage
	// Format: $argon2id$v=19$m=65536,t=3,p=2$<base64_salt>$<base64_hash>
	encodedSalt := base64.RawStdEncoding.EncodeToString(salt)
	encodedHash := base64.RawStdEncoding.EncodeToString(hash)

	hashedKey := fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s",
		config.Memory, config.Iterations, config.Parallelism, encodedSalt, encodedHash)

	return hashedKey, nil
}

// VerifyAPIKey verifies an API key against its hash using Argon2id
func VerifyAPIKey(apiKey string, hashedKey string) (bool, error) {
	// Parse the hash format: $argon2id$v=19$m=65536,t=3,p=2$<base64_salt>$<base64_hash>
	parts := strings.Split(hashedKey, "$")
	if len(parts) != 6 {
		return false, fmt.Errorf("invalid hash format")
	}

	if parts[1] != "argon2id" || parts[2] != "v=19" {
		return false, fmt.Errorf("unsupported hash algorithm or version")
	}

	// Parse parameters
	var memory, iterations uint32
	var parallelism uint8
	_, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &iterations, &parallelism)
	if err != nil {
		return false, fmt.Errorf("failed to parse hash parameters: %w", err)
	}

	// Decode salt and hash
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, fmt.Errorf("failed to decode salt: %w", err)
	}

	expectedHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, fmt.Errorf("failed to decode hash: %w", err)
	}

	// Generate hash for the provided key using the same parameters
	actualHash := argon2.IDKey([]byte(apiKey), salt, iterations, memory, parallelism, uint32(len(expectedHash)))

	// Constant-time comparison to prevent timing attacks
	return subtle.ConstantTimeCompare(expectedHash, actualHash) == 1, nil
}

// ValidateAPIKeyFormat validates the format of an API key
func ValidateAPIKeyFormat(apiKey string) error {
	_, _, err := ParseAPIKey(apiKey)
	return err
}

// APIKeyManager provides API key management functionality
type APIKeyManager struct {
	config Argon2Config
}

// NewAPIKeyManager creates a new API key manager
func NewAPIKeyManager() *APIKeyManager {
	return &APIKeyManager{
		config: DefaultArgon2Config(),
	}
}

// NewAPIKeyManagerWithConfig creates a new API key manager with custom config
func NewAPIKeyManagerWithConfig(config Argon2Config) *APIKeyManager {
	return &APIKeyManager{
		config: config,
	}
}

// CreateAPIKey creates a new API key
func (m *APIKeyManager) CreateAPIKey(name, createdBy string) (*APIKey, string, error) {
	// Generate API key
	keyID, fullKey, err := GenerateAPIKey()
	if err != nil {
		return nil, "", fmt.Errorf("failed to generate API key: %w", err)
	}

	// Hash the API key for storage
	hashedKey, err := HashAPIKey(fullKey, m.config)
	if err != nil {
		return nil, "", fmt.Errorf("failed to hash API key: %w", err)
	}

	// Create API key record
	apiKey := &APIKey{
		ID:        keyID,
		Name:      name,
		HashedKey: hashedKey,
		CreatedAt: time.Now().UTC(),
		IsRevoked: false,
		CreatedBy: createdBy,
	}

	return apiKey, fullKey, nil
}

// VerifyAPIKey verifies an API key and returns the key ID if valid
func (m *APIKeyManager) VerifyAPIKey(apiKey string, storedKey *APIKey) (bool, error) {
	if storedKey.IsRevoked {
		return false, nil
	}

	return VerifyAPIKey(apiKey, storedKey.HashedKey)
}

// UpdateLastUsed updates the last used timestamp for an API key
func (m *APIKeyManager) UpdateLastUsed(apiKey *APIKey) {
	now := time.Now().UTC()
	apiKey.LastUsedAt = &now
}

// RevokeAPIKey revokes an API key
func (m *APIKeyManager) RevokeAPIKey(apiKey *APIKey) {
	apiKey.IsRevoked = true
}

// ToResponse converts an APIKey to an APIKeyResponse
func (k *APIKey) ToResponse(includeKey bool, fullKey string) APIKeyResponse {
	response := APIKeyResponse{
		ID:        k.ID,
		Name:      k.Name,
		CreatedAt: k.CreatedAt,
		CreatedBy: k.CreatedBy,
	}

	if includeKey {
		response.Key = fullKey
	}

	return response
}

// ToListItem converts an APIKey to an APIKeyListItem
func (k *APIKey) ToListItem() APIKeyListItem {
	return APIKeyListItem{
		ID:         k.ID,
		Name:       k.Name,
		CreatedAt:  k.CreatedAt,
		LastUsedAt: k.LastUsedAt,
		IsRevoked:  k.IsRevoked,
		CreatedBy:  k.CreatedBy,
	}
}
