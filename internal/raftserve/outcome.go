package raftserve

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// ErrProposalAbandoned reports an admitted proposal whose group lost its local
// apply path before the registry observed a deterministic result.
var ErrProposalAbandoned = errors.New("raftserve: admitted proposal lost its local apply path")

// OutcomeCode is one closed terminal class. Deterministic apply classes may
// carry completion bytes. Infrastructure classes never claim an apply result.
type OutcomeCode uint8

const (
	OutcomePending OutcomeCode = iota
	OutcomeCompletion
	OutcomeRequestConflict
	OutcomeCompletionNotFound
	OutcomeRetryRetired
	OutcomeSessionEpoch
	OutcomeSessionSequence
	OutcomeSessionAck
	OutcomeSessionActive
	OutcomeSessionRetired
	OutcomeSessionReleased
	OutcomeSessionLeaseDeadline
	OutcomeStaleCommand
	OutcomeAdmissionBound
	OutcomeProposalRefused
	OutcomeNotLeader
	OutcomeProposalAbandoned
)

// Outcome is detached fixed-width result metadata for one exact proposal
// attempt. CompletionBytes reports the caller capacity needed by
// Waiter.TakeCompletionInto.
type Outcome struct {
	Code                      OutcomeCode
	AppliedIndex              uint64
	CompletionAppliedSequence uint64
	CompletionBytes           int
}

// Err returns the stable refusal represented by the outcome.
func (outcome Outcome) Err() error {
	switch outcome.Code {
	case OutcomeCompletion:
		return nil
	case OutcomeRequestConflict:
		return replicatedstate.ErrRequestConflict
	case OutcomeCompletionNotFound:
		return replicatedstate.ErrCompletionNotFound
	case OutcomeRetryRetired:
		return replicatedstate.ErrRetryRetired
	case OutcomeSessionEpoch:
		return replicatedstate.ErrSessionEpoch
	case OutcomeSessionSequence:
		return replicatedstate.ErrSessionSequence
	case OutcomeSessionAck:
		return replicatedstate.ErrSessionAck
	case OutcomeSessionActive:
		return replicatedstate.ErrSessionActive
	case OutcomeSessionRetired:
		return replicatedstate.ErrSessionRetired
	case OutcomeSessionReleased:
		return replicatedstate.ErrSessionReleased
	case OutcomeSessionLeaseDeadline:
		return replicatedstate.ErrSessionLeaseDeadline
	case OutcomeStaleCommand:
		return replicatedstate.ErrStaleCommand
	case OutcomeAdmissionBound:
		return replicatedstate.ErrAdmissionBound
	case OutcomeProposalRefused:
		return ErrProposalRefused
	case OutcomeNotLeader:
		return raftmodel.ErrNotLeader
	case OutcomeProposalAbandoned:
		return ErrProposalAbandoned
	default:
		return ErrWaiterPending
	}
}

func infrastructureOutcome(outcome OutcomeCode) bool {
	switch outcome {
	case OutcomeProposalRefused, OutcomeNotLeader, OutcomeProposalAbandoned:
		return true
	default:
		return false
	}
}

func outcomeCode(err error) (OutcomeCode, bool) {
	switch {
	case err == nil:
		return OutcomeCompletion, true
	case errors.Is(err, replicatedstate.ErrRequestConflict):
		return OutcomeRequestConflict, true
	case errors.Is(err, replicatedstate.ErrCompletionNotFound):
		return OutcomeCompletionNotFound, true
	case errors.Is(err, replicatedstate.ErrRetryRetired):
		return OutcomeRetryRetired, true
	case errors.Is(err, replicatedstate.ErrSessionEpoch):
		return OutcomeSessionEpoch, true
	case errors.Is(err, replicatedstate.ErrSessionSequence):
		return OutcomeSessionSequence, true
	case errors.Is(err, replicatedstate.ErrSessionAck):
		return OutcomeSessionAck, true
	case errors.Is(err, replicatedstate.ErrSessionActive):
		return OutcomeSessionActive, true
	case errors.Is(err, replicatedstate.ErrSessionRetired):
		return OutcomeSessionRetired, true
	case errors.Is(err, replicatedstate.ErrSessionReleased):
		return OutcomeSessionReleased, true
	case errors.Is(err, replicatedstate.ErrSessionLeaseDeadline):
		return OutcomeSessionLeaseDeadline, true
	case errors.Is(err, replicatedstate.ErrStaleCommand):
		return OutcomeStaleCommand, true
	case errors.Is(err, replicatedstate.ErrAdmissionBound):
		return OutcomeAdmissionBound, true
	default:
		return OutcomePending, false
	}
}
