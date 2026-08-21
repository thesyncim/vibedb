package raftmember

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftstore"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// ErrStaticCompletionCapacity reports that the fixed bootstrap WAL/apply pair
// cannot prove one retained completion slot for every entry it can hold.
var (
	ErrCompletionCapacity = errors.New(
		"raftmember: immutable-base WAL does not qualify completion capacity",
	)
	// ErrStaticCompletionCapacity is retained as an API alias for the original
	// index-one-only validator.
	ErrStaticCompletionCapacity = ErrCompletionCapacity
)

// ValidateImmutableBaseNoGCCompletionCapacity proves that the bounded suffix
// after this WAL's exact immutable base cannot create more retained completion
// records than the apply store can hold. It supports both index-one startup and
// newer certified learner bases.
func ValidateImmutableBaseNoGCCompletionCapacity(
	wal *raftstore.Store,
	apply *sqldriver.ReplicatedApply,
) error {
	return validateLiveNoGCCompletionCapacity(wal, apply, false)
}

// ValidateStaticNoGCCompletionCapacity proves the count-only completion bound
// for one exact live WAL/apply pair while the WAL has the immutable bootstrap
// base at index 1 and no runtime snapshot or compaction capability. The claim's
// exact authority profile is used to derive the live WAL binding, and every
// binding coordinate must match before its apply cut is considered. If C is
// the retained completion count, A the applied index, and L the last log index,
// the validated cut proves C <= A-1 and the static WAL proves
// L-1 <= MaxEntries. Every unapplied entry can add at most one completion, so
// C+(L-A) <= MaxCompletions. MaxEntries <= MaxCompletions additionally covers
// the complete future static suffix.
//
// This is an instantaneous predicate under caller-exclusive startup ownership,
// not a lease or token. It is not physical system/user file-byte reservation
// and grants no serving authority. It must be checked again for every reopened
// WAL/apply pair.
func ValidateStaticNoGCCompletionCapacity(
	wal *raftstore.Store,
	apply *sqldriver.ReplicatedApply,
) error {
	return validateLiveNoGCCompletionCapacity(wal, apply, true)
}

func validateLiveNoGCCompletionCapacity(
	wal *raftstore.Store,
	apply *sqldriver.ReplicatedApply,
	requireInitialBase bool,
) error {
	if wal == nil {
		return ErrWALUnavailable
	}
	profile, err := wal.CapacityProfile()
	if err != nil {
		return fmt.Errorf("%w: inspect capacity profile: %w", ErrWALUnavailable, err)
	}
	if wal.RecoveredTornCurrentSlot() {
		return ErrWALQuarantined
	}
	if apply == nil {
		return sqldriver.ErrReplicatedApplyClosed
	}
	applyProfile, err := apply.CapacityQualificationProfile()
	if err != nil {
		return err
	}
	liveBinding, err := BindingFromWAL(wal, applyProfile.Binding.Authority)
	if err != nil {
		return err
	}
	if liveBinding != applyProfile.Binding {
		return fmt.Errorf(
			"%w: replicated apply claim belongs to another WAL binding",
			ErrBindingMismatch,
		)
	}
	hardState, _, err := wal.InitialState()
	if err != nil {
		return fmt.Errorf("%w: inspect hard state: %w", ErrWALUnavailable, err)
	}
	lastIndex, err := wal.LastIndex()
	if err != nil {
		return fmt.Errorf("%w: inspect last index: %w", ErrWALUnavailable, err)
	}
	if requireInitialBase && profile.LogBaseIndex != 1 {
		return fmt.Errorf(
			"%w: require initial snapshot base 1, got %d",
			ErrStaticCompletionCapacity, profile.LogBaseIndex,
		)
	}
	return validateImmutableBaseNoGCCompletionCapacity(
		profile, applyProfile, hardState.GetCommit(), lastIndex,
	)
}

func validateStaticNoGCCompletionCapacity(
	profile raftstore.CapacityProfile,
	apply sqldriver.ReplicatedApplyCapacityProfile,
	commit uint64,
	last uint64,
) error {
	if profile.LogBaseIndex != 1 {
		return fmt.Errorf(
			"%w: require static snapshot base 1, got format=%d base=%d",
			ErrStaticCompletionCapacity,
			profile.Format,
			profile.LogBaseIndex,
		)
	}
	return validateImmutableBaseNoGCCompletionCapacity(profile, apply, commit, last)
}

func validateImmutableBaseNoGCCompletionCapacity(
	profile raftstore.CapacityProfile,
	apply sqldriver.ReplicatedApplyCapacityProfile,
	commit uint64,
	last uint64,
) error {
	if profile.Format != raftstore.CapacityFormatImmutableBase || profile.LogBaseIndex == 0 {
		return fmt.Errorf(
			"%w: unsupported immutable-base profile format=%d base=%d",
			ErrStaticCompletionCapacity, profile.Format, profile.LogBaseIndex,
		)
	}
	if apply.ApplyFormat != sqldriver.ReplicatedApplyFormat {
		return fmt.Errorf(
			"%w: unsupported apply completion format %d",
			ErrStaticCompletionCapacity, apply.ApplyFormat,
		)
	}
	if profile.MaxEntries == 0 || apply.MaxCompletions == 0 {
		return fmt.Errorf(
			"%w: WAL entries=%d completions=%d",
			ErrStaticCompletionCapacity,
			profile.MaxEntries,
			apply.MaxCompletions,
		)
	}
	base := profile.LogBaseIndex
	if !apply.Initialized || apply.Applied < base || commit < base || last < base ||
		apply.CompletionCount > apply.Applied-1 || apply.Applied > commit || commit > last {
		return fmt.Errorf(
			"%w: invalid apply/WAL cut initialized=%t C=%d A=%d commit=%d L=%d",
			ErrStaticCompletionCapacity, apply.Initialized, apply.CompletionCount,
			apply.Applied, commit, last,
		)
	}
	maxLast := base + profile.MaxEntries
	if maxLast < base {
		maxLast = ^uint64(0)
	}
	if last > maxLast {
		return fmt.Errorf(
			"%w: WAL last=%d exceeds sealed range [%d,%d]",
			ErrStaticCompletionCapacity, last, base, maxLast,
		)
	}
	if apply.CompletionCount > apply.MaxCompletions {
		return fmt.Errorf(
			"%w: completion count %d exceeds capacity %d",
			ErrStaticCompletionCapacity, apply.CompletionCount, apply.MaxCompletions,
		)
	}
	remainingCompletions := apply.MaxCompletions - apply.CompletionCount
	currentUnapplied := last - apply.Applied
	maximumFuture := maxLast - apply.Applied
	if currentUnapplied > remainingCompletions || maximumFuture > remainingCompletions {
		return fmt.Errorf(
			"%w: WAL entries=%d base=%d C=%d A=%d L=%d completions=%d",
			ErrStaticCompletionCapacity, profile.MaxEntries, base,
			apply.CompletionCount, apply.Applied, last, apply.MaxCompletions,
		)
	}
	return nil
}
