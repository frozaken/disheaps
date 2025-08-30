package auth

import (
	"time"
)

// User represents a user in the system
type User struct {
	ID           string     `json:"id"`
	Email        string     `json:"email"`
	PasswordHash string     `json:"password_hash"`
	Name         string     `json:"name"`
	IsActive     bool       `json:"is_active"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	LastLoginAt  *time.Time `json:"last_login_at"`
}

// UserRequest represents a request to create or update a user
type UserRequest struct {
	Email    string `json:"email" validate:"required,email"`
	Password string `json:"password" validate:"required,min=8"`
	Name     string `json:"name" validate:"required,min=1,max=100"`
}

// UserResponse represents a user response (without sensitive data)
type UserResponse struct {
	ID          string     `json:"id"`
	Email       string     `json:"email"`
	Name        string     `json:"name"`
	IsActive    bool       `json:"is_active"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	LastLoginAt *time.Time `json:"last_login_at"`
}

// UserListItem represents a user in a list
type UserListItem struct {
	ID          string     `json:"id"`
	Email       string     `json:"email"`
	Name        string     `json:"name"`
	IsActive    bool       `json:"is_active"`
	CreatedAt   time.Time  `json:"created_at"`
	LastLoginAt *time.Time `json:"last_login_at"`
}

// LoginRequest represents a login request
type LoginRequest struct {
	Email    string `json:"email" validate:"required,email"`
	Password string `json:"password" validate:"required"`
}

// LoginResponse represents a login response
type LoginResponse struct {
	Token     string       `json:"token"`
	ExpiresAt time.Time    `json:"expires_at"`
	User      UserResponse `json:"user"`
}

// RefreshTokenRequest represents a refresh token request
type RefreshTokenRequest struct {
	Token string `json:"token" validate:"required"`
}

// ToResponse converts a User to a UserResponse
func (u *User) ToResponse() UserResponse {
	return UserResponse{
		ID:          u.ID,
		Email:       u.Email,
		Name:        u.Name,
		IsActive:    u.IsActive,
		CreatedAt:   u.CreatedAt,
		UpdatedAt:   u.UpdatedAt,
		LastLoginAt: u.LastLoginAt,
	}
}

// ToListItem converts a User to a UserListItem
func (u *User) ToListItem() UserListItem {
	return UserListItem{
		ID:          u.ID,
		Email:       u.Email,
		Name:        u.Name,
		IsActive:    u.IsActive,
		CreatedAt:   u.CreatedAt,
		LastLoginAt: u.LastLoginAt,
	}
}

// UserManager provides user management functionality
type UserManager struct {
	hasher Hasher
}

// NewUserManager creates a new user manager
func NewUserManager() *UserManager {
	return &UserManager{
		hasher: NewSecureHasher(),
	}
}

// NewUserManagerWithHasher creates a new user manager with custom hasher
func NewUserManagerWithHasher(hasher Hasher) *UserManager {
	return &UserManager{
		hasher: hasher,
	}
}

// CreateUser creates a new user with hashed password
func (m *UserManager) CreateUser(email, password, name string) (*User, error) {
	// Hash the password
	hashedPassword, err := m.hasher.HashPassword(password)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()

	user := &User{
		Email:        email,
		PasswordHash: hashedPassword,
		Name:         name,
		IsActive:     true,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	return user, nil
}

// VerifyPassword verifies a user's password
func (m *UserManager) VerifyPassword(user *User, password string) (bool, error) {
	return m.hasher.VerifyPassword(password, user.PasswordHash)
}

// UpdatePassword updates a user's password
func (m *UserManager) UpdatePassword(user *User, newPassword string) error {
	hashedPassword, err := m.hasher.HashPassword(newPassword)
	if err != nil {
		return err
	}

	user.PasswordHash = hashedPassword
	user.UpdatedAt = time.Now().UTC()

	return nil
}

// UpdateLastLogin updates the user's last login timestamp
func (m *UserManager) UpdateLastLogin(user *User) {
	now := time.Now().UTC()
	user.LastLoginAt = &now
	user.UpdatedAt = now
}

// DeactivateUser deactivates a user
func (m *UserManager) DeactivateUser(user *User) {
	user.IsActive = false
	user.UpdatedAt = time.Now().UTC()
}

// ActivateUser activates a user
func (m *UserManager) ActivateUser(user *User) {
	user.IsActive = true
	user.UpdatedAt = time.Now().UTC()
}
