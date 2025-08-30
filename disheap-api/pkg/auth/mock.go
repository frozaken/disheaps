package auth

import (
	"context"
	"errors"
	"time"
)

// MockAPIKeyStore is a simple in-memory implementation of APIKeyStore for testing
type MockAPIKeyStore struct {
	keys map[string]*APIKey
}

// NewMockAPIKeyStore creates a new mock API key store
func NewMockAPIKeyStore() *MockAPIKeyStore {
	return &MockAPIKeyStore{
		keys: make(map[string]*APIKey),
	}
}

// GetAPIKeyByID retrieves an API key by ID
func (m *MockAPIKeyStore) GetAPIKeyByID(ctx context.Context, keyID string) (*APIKey, error) {
	key, exists := m.keys[keyID]
	if !exists {
		return nil, errors.New("API key not found")
	}
	return key, nil
}

// UpdateAPIKeyLastUsed updates the last used timestamp
func (m *MockAPIKeyStore) UpdateAPIKeyLastUsed(ctx context.Context, keyID string, lastUsed time.Time) error {
	key, exists := m.keys[keyID]
	if !exists {
		return errors.New("API key not found")
	}
	key.LastUsedAt = &lastUsed
	return nil
}

// AddAPIKey adds an API key to the store (for testing)
func (m *MockAPIKeyStore) AddAPIKey(key *APIKey) {
	m.keys[key.ID] = key
}

// MockUserStore is a simple in-memory implementation of UserStore for testing
type MockUserStore struct {
	users map[string]*User
}

// NewMockUserStore creates a new mock user store
func NewMockUserStore() *MockUserStore {
	return &MockUserStore{
		users: make(map[string]*User),
	}
}

// GetUserByID retrieves a user by ID
func (m *MockUserStore) GetUserByID(ctx context.Context, userID string) (*User, error) {
	user, exists := m.users[userID]
	if !exists {
		return nil, errors.New("user not found")
	}
	return user, nil
}

// AddUser adds a user to the store (for testing)
func (m *MockUserStore) AddUser(user *User) {
	m.users[user.ID] = user
}

// NewMockAuthService creates a new auth service with mock stores for testing/development
func NewMockAuthService() *AuthService {
	apiKeyStore := NewMockAPIKeyStore()
	userStore := NewMockUserStore()

	// Create a test API key using the manager
	manager := NewAPIKeyManager()
	testKey, fullKey, _ := manager.CreateAPIKey("Development Test Key", "system")

	apiKeyStore.AddAPIKey(testKey)

	// Create a test user
	testUser := &User{
		ID:          "test-user",
		Name:        "Developer",
		Email:       "dev@example.com",
		IsActive:    true,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
		LastLoginAt: nil,
	}

	userStore.AddUser(testUser)

	// Log the test API key for development
	println("=== DEVELOPMENT API KEY ===")
	println("API Key:", fullKey)
	println("Usage: curl -H 'Authorization: Bearer", fullKey, "' http://localhost:8080/v1/stats")
	println("============================")

	return NewAuthService(apiKeyStore, userStore)
}
