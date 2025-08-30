package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// HashConfig holds configuration for password hashing
type HashConfig struct {
	// Argon2id parameters
	Memory      uint32 // Memory usage in KB
	Iterations  uint32 // Number of iterations
	Parallelism uint8  // Number of threads
	SaltLength  uint32 // Length of salt in bytes
	KeyLength   uint32 // Length of derived key in bytes
}

// DefaultHashConfig returns secure default hashing configuration
func DefaultHashConfig() HashConfig {
	return HashConfig{
		Memory:      64 * 1024, // 64 MB
		Iterations:  3,         // 3 iterations
		Parallelism: 2,         // 2 threads
		SaltLength:  16,        // 16 bytes salt
		KeyLength:   32,        // 32 bytes key
	}
}

// SecureHasher provides secure password and API key hashing
type SecureHasher struct {
	config HashConfig
}

// NewSecureHasher creates a new secure hasher with default config
func NewSecureHasher() *SecureHasher {
	return &SecureHasher{
		config: DefaultHashConfig(),
	}
}

// NewSecureHasherWithConfig creates a new secure hasher with custom config
func NewSecureHasherWithConfig(config HashConfig) *SecureHasher {
	return &SecureHasher{
		config: config,
	}
}

// generateSalt generates a cryptographically secure random salt
func (h *SecureHasher) generateSalt() ([]byte, error) {
	salt := make([]byte, h.config.SaltLength)
	_, err := rand.Read(salt)
	if err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}
	return salt, nil
}

// HashPassword hashes a password using Argon2id
func (h *SecureHasher) HashPassword(password string) (string, error) {
	return h.hashWithArgon2id(password)
}

// VerifyPassword verifies a password against its hash
func (h *SecureHasher) VerifyPassword(password, hashedPassword string) (bool, error) {
	return h.verifyArgon2id(password, hashedPassword)
}

// HashAPIKey hashes an API key using Argon2id
func (h *SecureHasher) HashAPIKey(apiKey string) (string, error) {
	return h.hashWithArgon2id(apiKey)
}

// VerifyAPIKey verifies an API key against its hash
func (h *SecureHasher) VerifyAPIKey(apiKey, hashedAPIKey string) (bool, error) {
	return h.verifyArgon2id(apiKey, hashedAPIKey)
}

// hashWithArgon2id performs the actual Argon2id hashing
func (h *SecureHasher) hashWithArgon2id(data string) (string, error) {
	// Generate salt
	salt, err := h.generateSalt()
	if err != nil {
		return "", err
	}

	// Hash using Argon2id
	hash := argon2.IDKey([]byte(data), salt, h.config.Iterations, h.config.Memory, h.config.Parallelism, h.config.KeyLength)

	// Encode for storage: $argon2id$v=19$m=memory,t=iterations,p=parallelism$salt$hash
	encodedSalt := base64.RawStdEncoding.EncodeToString(salt)
	encodedHash := base64.RawStdEncoding.EncodeToString(hash)

	hashedData := fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s",
		h.config.Memory, h.config.Iterations, h.config.Parallelism, encodedSalt, encodedHash)

	return hashedData, nil
}

// verifyArgon2id verifies data against an Argon2id hash
func (h *SecureHasher) verifyArgon2id(data, hashedData string) (bool, error) {
	// Parse hash format: $argon2id$v=19$m=memory,t=iterations,p=parallelism$salt$hash
	parts := strings.Split(hashedData, "$")
	if len(parts) != 6 {
		return false, fmt.Errorf("invalid hash format: expected 6 parts, got %d", len(parts))
	}

	// Validate algorithm and version
	if parts[1] != "argon2id" {
		return false, fmt.Errorf("unsupported hash algorithm: %s", parts[1])
	}
	if parts[2] != "v=19" {
		return false, fmt.Errorf("unsupported argon2 version: %s", parts[2])
	}

	// Parse parameters
	var memory, iterations uint32
	var parallelism uint8
	n, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &iterations, &parallelism)
	if err != nil || n != 3 {
		return false, fmt.Errorf("failed to parse hash parameters: %w", err)
	}

	// Decode salt
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, fmt.Errorf("failed to decode salt: %w", err)
	}

	// Decode expected hash
	expectedHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, fmt.Errorf("failed to decode hash: %w", err)
	}

	// Generate hash for the provided data using the same parameters
	actualHash := argon2.IDKey([]byte(data), salt, iterations, memory, parallelism, uint32(len(expectedHash)))

	// Constant-time comparison to prevent timing attacks
	return subtle.ConstantTimeCompare(expectedHash, actualHash) == 1, nil
}

// SimpleHasher provides basic SHA-256 hashing (for development/testing only)
type SimpleHasher struct{}

// NewSimpleHasher creates a simple hasher (NOT recommended for production)
func NewSimpleHasher() *SimpleHasher {
	return &SimpleHasher{}
}

// HashPassword hashes a password using SHA-256 (NOT secure for production)
func (h *SimpleHasher) HashPassword(password string) (string, error) {
	hash := sha256.Sum256([]byte(password))
	return base64.StdEncoding.EncodeToString(hash[:]), nil
}

// VerifyPassword verifies a password against a SHA-256 hash (NOT secure for production)
func (h *SimpleHasher) VerifyPassword(password, hashedPassword string) (bool, error) {
	expectedHash, err := base64.StdEncoding.DecodeString(hashedPassword)
	if err != nil {
		return false, err
	}

	actualHash := sha256.Sum256([]byte(password))
	return subtle.ConstantTimeCompare(expectedHash, actualHash[:]) == 1, nil
}

// HashAPIKey hashes an API key using SHA-256 (NOT secure for production)
func (h *SimpleHasher) HashAPIKey(apiKey string) (string, error) {
	return h.HashPassword(apiKey)
}

// VerifyAPIKey verifies an API key against a SHA-256 hash (NOT secure for production)
func (h *SimpleHasher) VerifyAPIKey(apiKey, hashedAPIKey string) (bool, error) {
	return h.VerifyPassword(apiKey, hashedAPIKey)
}

// Hasher interface for both secure and simple hashers
type Hasher interface {
	HashPassword(password string) (string, error)
	VerifyPassword(password, hashedPassword string) (bool, error)
	HashAPIKey(apiKey string) (string, error)
	VerifyAPIKey(apiKey, hashedAPIKey string) (bool, error)
}

// Ensure both hashers implement the interface
var (
	_ Hasher = (*SecureHasher)(nil)
	_ Hasher = (*SimpleHasher)(nil)
)
