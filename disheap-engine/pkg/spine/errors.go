package spine

import "fmt"

// ErrInvalidCandidate represents an invalid candidate error
type ErrInvalidCandidate struct {
	Reason string
}

func (e ErrInvalidCandidate) Error() string {
	return fmt.Sprintf("invalid candidate: %s", e.Reason)
}

// ErrSpineIndexFull indicates the spine index has reached capacity
type ErrSpineIndexFull struct {
	CurrentSize int
	MaxSize     int
}

func (e ErrSpineIndexFull) Error() string {
	return fmt.Sprintf("spine index full: %d/%d", e.CurrentSize, e.MaxSize)
}

// ErrPartitionNotFound indicates a requested partition is not found
type ErrPartitionNotFound struct {
	PartitionID uint32
}

func (e ErrPartitionNotFound) Error() string {
	return fmt.Sprintf("partition %d not found in spine index", e.PartitionID)
}

// ErrNoAvailableCandidates indicates no candidates are available for selection
type ErrNoAvailableCandidates struct {
	Reason string
}

func (e ErrNoAvailableCandidates) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("no available candidates: %s", e.Reason)
	}
	return "no available candidates"
}

// ErrInvalidSpineConfig represents invalid spine index configuration
type ErrInvalidSpineConfig struct {
	Field  string
	Reason string
}

func (e ErrInvalidSpineConfig) Error() string {
	return fmt.Sprintf("invalid spine config %s: %s", e.Field, e.Reason)
}

// ErrSpineClosed indicates the spine index is closed
var ErrSpineClosed = fmt.Errorf("spine index is closed")
