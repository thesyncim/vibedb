package raftmodel

import (
	"errors"
	"fmt"
)

var (
	// ErrWrongPhase identifies an operation attempted outside its Ready phase.
	ErrWrongPhase = errors.New("raftmodel: operation in wrong Ready phase")
	// ErrUnsupported identifies a deliberately excluded integration feature.
	ErrUnsupported = errors.New("raftmodel: unsupported integration feature")
	// ErrNotLeader identifies a ReadIndex request made without local leadership.
	ErrNotLeader = errors.New("raftmodel: local member is not leader")
	// ErrDuplicateReadContext identifies concurrent reuse of an opaque context.
	ErrDuplicateReadContext = errors.New("raftmodel: duplicate ReadIndex context")
	// ErrReadLeadershipLost marks a barrier whose issuing leadership is stale.
	ErrReadLeadershipLost = errors.New("raftmodel: ReadIndex leadership changed")
	// ErrAdmissionBound identifies input refused by a fixed memory or wire bound.
	ErrAdmissionBound = errors.New("raftmodel: bounded admission refused")
	// ErrConfChangePending identifies configuration admission while the core has
	// outstanding Ready/log work whose predecessor is not yet fully applied.
	ErrConfChangePending = errors.New("raftmodel: configuration change pending")
)

// Phase is the externally visible point in the synchronous Ready lifecycle.
type Phase uint8

const (
	PhaseIdle Phase = iota
	PhaseCaptured
	PhasePersisted
	PhaseMessagesDrained
	PhaseSnapshotInstalled
	PhaseEntriesApplied
	PhaseReadStatesRecorded
	PhaseFailed
)

func (p Phase) String() string {
	switch p {
	case PhaseIdle:
		return "idle"
	case PhaseCaptured:
		return "captured"
	case PhasePersisted:
		return "persisted"
	case PhaseMessagesDrained:
		return "messages-drained"
	case PhaseSnapshotInstalled:
		return "snapshot-installed"
	case PhaseEntriesApplied:
		return "entries-applied"
	case PhaseReadStatesRecorded:
		return "read-states-recorded"
	case PhaseFailed:
		return "failed"
	default:
		return fmt.Sprintf("phase(%d)", uint8(p))
	}
}

// PhaseError reports the exact lifecycle precondition that was violated.
type PhaseError struct {
	Operation string
	Have      Phase
	Want      Phase
}

func (e *PhaseError) Error() string {
	return fmt.Sprintf("raftmodel: %s requires phase %s, have %s", e.Operation, e.Want, e.Have)
}

func (e *PhaseError) Is(target error) bool { return target == ErrWrongPhase }

// PersistError preserves the storage cause while keeping the failed boundary
// machine-readable. The Node remains captured so the exact batch can be
// retried or the process can restart.
type PersistError struct {
	ReadyID uint64
	Err     error
}

func (e *PersistError) Error() string {
	return fmt.Sprintf("raftmodel: persist Ready %d: %v", e.ReadyID, e.Err)
}

func (e *PersistError) Unwrap() error { return e.Err }

// ApplyError is terminal for a Node instance because an apply error can be
// ambiguous with respect to reader publication. Recovery must construct a new
// Node from the stable log and the state machine's published cut.
type ApplyError struct {
	Stage Phase
	Index uint64
	Err   error
}

func (e *ApplyError) Error() string {
	if e.Index == 0 {
		return fmt.Sprintf("raftmodel: %s: %v", e.Stage, e.Err)
	}
	return fmt.Sprintf("raftmodel: %s at index %d: %v", e.Stage, e.Index, e.Err)
}

func (e *ApplyError) Unwrap() error { return e.Err }

// UnsupportedError names an intentionally unavailable protocol extension.
type UnsupportedError struct {
	Feature string
}

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("raftmodel: unsupported integration feature: %s", e.Feature)
}

func (e *UnsupportedError) Is(target error) bool { return target == ErrUnsupported }
