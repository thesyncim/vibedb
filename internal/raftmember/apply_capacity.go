package raftmember

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// ErrApplyCapacity reports that an immutable-base WAL and its bounded apply
// store do not satisfy their shared format, cut, range, or session bounds.
var ErrApplyCapacity = errors.New(
	"raftmember: immutable-base WAL does not qualify bounded apply capacity",
)

// ValidateImmutableBaseApplyCapacity proves that one exact live WAL/apply pair
// has matching authority, a coherent durable cut, a sealed immutable-base WAL
// range, and bounded session state. Applying suffix entries does not consume
// immutable apply capacity: each session owns a fixed retry ring.
func ValidateImmutableBaseApplyCapacity(
	wal *raftstore.Store,
	apply *sqldriver.ReplicatedApply,
) error {
	return validateLiveApplyCapacity(wal, apply, false)
}

func validateLiveApplyCapacity(
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
	if wal.RecoveredTornCurrentSlot() || wal.RecoveredTornFamilySlot() {
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
			ErrApplyCapacity, profile.LogBaseIndex,
		)
	}
	return validateImmutableBaseApplyCapacity(
		profile, applyProfile, max(hardState.GetCommit(), applyProfile.Applied), lastIndex,
	)
}

func validateImmutableBaseApplyCapacity(
	profile raftstore.CapacityProfile,
	apply sqldriver.ReplicatedApplyCapacityProfile,
	commit uint64,
	last uint64,
) error {
	if profile.Format != raftstore.CapacityFormatImmutableBase || profile.LogBaseIndex == 0 {
		return fmt.Errorf(
			"%w: unsupported immutable-base profile format=%d base=%d",
			ErrApplyCapacity, profile.Format, profile.LogBaseIndex,
		)
	}
	if apply.ApplyFormat != sqldriver.ReplicatedApplyFormat {
		return fmt.Errorf(
			"%w: unsupported apply format %d",
			ErrApplyCapacity, apply.ApplyFormat,
		)
	}
	if profile.MaxEntries == 0 || apply.MaxSessions == 0 ||
		apply.MaxSessions > replicatedstate.MaxRetainedSessions ||
		apply.RetryWindow == 0 || apply.RetryWindow > replicatedstate.MaxSessionRetryWindow {
		return fmt.Errorf(
			"%w: WAL entries=%d sessions=%d retry-window=%d",
			ErrApplyCapacity, profile.MaxEntries, apply.MaxSessions, apply.RetryWindow,
		)
	}
	base := profile.LogBaseIndex
	if !apply.Initialized || apply.Applied < base || commit < base || last < base ||
		apply.CheckpointApplied > apply.Applied ||
		apply.SessionCount > apply.Applied-1 || apply.SessionSlotCount > apply.Applied-1 ||
		apply.SessionEpochHighWater > apply.Applied ||
		apply.SessionCount != 0 && apply.SessionEpochHighWater == 0 ||
		apply.Applied > commit || commit > last {
		return fmt.Errorf(
			"%w: invalid apply/WAL cut initialized=%t sessions=%d slots=%d epoch-high=%d C=%d A=%d commit=%d L=%d",
			ErrApplyCapacity, apply.Initialized, apply.SessionCount,
			apply.SessionSlotCount, apply.SessionEpochHighWater,
			apply.CheckpointApplied, apply.Applied, commit, last,
		)
	}
	if apply.SessionCount > apply.MaxSessions ||
		apply.SessionSlotCount > apply.SessionCount*uint64(apply.RetryWindow) {
		return fmt.Errorf(
			"%w: session state sessions=%d/%d slots=%d retry-window=%d",
			ErrApplyCapacity, apply.SessionCount, apply.MaxSessions,
			apply.SessionSlotCount, apply.RetryWindow,
		)
	}
	maxLast := base + profile.MaxEntries
	if maxLast < base {
		maxLast = ^uint64(0)
	}
	if last > maxLast {
		return fmt.Errorf(
			"%w: WAL last=%d exceeds sealed range [%d,%d]",
			ErrApplyCapacity, last, base, maxLast,
		)
	}
	return nil
}
