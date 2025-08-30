package storage

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound indicates a resource was not found
	ErrNotFound = errors.New("not found")

	// ErrExists indicates a resource already exists
	ErrExists = errors.New("already exists")

	// ErrClosed indicates the storage is closed
	ErrClosed = errors.New("storage is closed")

	// ErrBatchTooLarge indicates a batch exceeds size limits
	ErrBatchTooLarge = errors.New("batch too large")

	// ErrTransactionConflict indicates a transaction conflict
	ErrTransactionConflict = errors.New("transaction conflict")

	// ErrCorrupted indicates data corruption
	ErrCorrupted = errors.New("data corrupted")

	// ErrReadOnly indicates storage is in read-only mode
	ErrReadOnly = errors.New("storage is read-only")
)

// ErrInvalidConfig represents a configuration validation error
type ErrInvalidConfig struct {
	Field string
	Issue string
}

func (e ErrInvalidConfig) Error() string {
	return fmt.Sprintf("invalid config field %s: %s", e.Field, e.Issue)
}

// ErrInvalidKey represents an invalid storage key error
type ErrInvalidKey struct {
	Key   string
	Issue string
}

func (e ErrInvalidKey) Error() string {
	return fmt.Sprintf("invalid key %s: %s", e.Key, e.Issue)
}

// ErrStorageOperation represents a storage operation error
type ErrStorageOperation struct {
	Operation string
	Key       string
	Cause     error
}

func (e ErrStorageOperation) Error() string {
	return fmt.Sprintf("storage operation %s failed for key %s: %v", e.Operation, e.Key, e.Cause)
}

func (e ErrStorageOperation) Unwrap() error {
	return e.Cause
}

// IsNotFound checks if an error is a not found error
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsExists checks if an error is an already exists error
func IsExists(err error) bool {
	return errors.Is(err, ErrExists)
}

// IsClosed checks if an error is a closed storage error
func IsClosed(err error) bool {
	return errors.Is(err, ErrClosed)
}

// IsCorrupted checks if an error is a corruption error
func IsCorrupted(err error) bool {
	return errors.Is(err, ErrCorrupted)
}
