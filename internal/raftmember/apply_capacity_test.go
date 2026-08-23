package raftmember

import (
	"errors"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestImmutableBaseApplyCapacityBoundary(t *testing.T) {
	base := raftstore.CapacityProfile{
		Format:       raftstore.CapacityFormatImmutableBase,
		LogBaseIndex: 1,
		MaxEntries:   8192,
	}
	baseApply := sqldriver.ReplicatedApplyCapacityProfile{
		ApplyFormat: sqldriver.ReplicatedApplyFormat,
		MaxSessions: 128,
		RetryWindow: 8,
		Initialized: true,
		Applied:     1,
	}
	tests := []struct {
		name      string
		profile   raftstore.CapacityProfile
		apply     sqldriver.ReplicatedApplyCapacityProfile
		commit    uint64
		last      uint64
		wantError bool
	}{
		{name: "empty bounded state", profile: base, apply: baseApply, commit: 1, last: 1},
		{name: "nontrivial unapplied suffix", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.Applied, result.SessionCount, result.SessionSlotCount = 3, 1, 2
			result.SessionEpochHighWater = 2
			return result
		}(), commit: 4, last: 5},
		{name: "full retry rings do not consume suffix capacity", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.MaxSessions, result.RetryWindow = 2, 2
			result.Applied, result.SessionCount, result.SessionSlotCount = 5, 2, 4
			result.SessionEpochHighWater = 4
			return result
		}(), commit: 6, last: 8193},
		{name: "maximum arithmetic", profile: raftstore.CapacityProfile{
			Format: raftstore.CapacityFormatImmutableBase, LogBaseIndex: 1,
			MaxEntries: math.MaxUint64,
		}, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.Applied = math.MaxUint64 - 1
			return result
		}(), commit: math.MaxUint64, last: math.MaxUint64},
		{name: "unknown WAL format", profile: raftstore.CapacityProfile{
			Format: 2, LogBaseIndex: 1, MaxEntries: 8192,
		}, apply: baseApply, commit: 1, last: 1, wantError: true},
		{name: "zero base", profile: raftstore.CapacityProfile{
			Format: raftstore.CapacityFormatImmutableBase, MaxEntries: 8192,
		}, apply: baseApply, wantError: true},
		{name: "unknown apply format", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.ApplyFormat++
			return result
		}(), commit: 1, last: 1, wantError: true},
		{name: "zero WAL entries", profile: raftstore.CapacityProfile{
			Format: raftstore.CapacityFormatImmutableBase, LogBaseIndex: 1,
		}, apply: baseApply, commit: 1, last: 1, wantError: true},
		{name: "zero sessions", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.MaxSessions = 0
			return result
		}(), commit: 1, last: 1, wantError: true},
		{name: "too many configured sessions", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.MaxSessions = replicatedstate.MaxRetainedSessions + 1
			return result
		}(), commit: 1, last: 1, wantError: true},
		{name: "zero retry window", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.RetryWindow = 0
			return result
		}(), commit: 1, last: 1, wantError: true},
		{name: "retry window too large", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.RetryWindow = replicatedstate.MaxSessionRetryWindow + 1
			return result
		}(), commit: 1, last: 1, wantError: true},
		{name: "uninitialized", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.Initialized = false
			return result
		}(), commit: 1, last: 1, wantError: true},
		{name: "apply below base", profile: raftstore.CapacityProfile{
			Format: raftstore.CapacityFormatImmutableBase, LogBaseIndex: 100, MaxEntries: 8192,
		}, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.Applied = 99
			return result
		}(), commit: 100, last: 100, wantError: true},
		{name: "session count ahead of apply", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.Applied, result.SessionCount = 2, 2
			result.SessionEpochHighWater = 1
			return result
		}(), commit: 2, last: 2, wantError: true},
		{name: "slot count ahead of apply", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.Applied, result.SessionCount, result.SessionSlotCount = 2, 1, 2
			result.SessionEpochHighWater = 1
			return result
		}(), commit: 2, last: 2, wantError: true},
		{name: "session epoch high-water ahead of apply", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.Applied, result.SessionEpochHighWater = 2, 3
			return result
		}(), commit: 2, last: 2, wantError: true},
		{name: "apply ahead of commit", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.Applied = 2
			return result
		}(), commit: 1, last: 2, wantError: true},
		{name: "commit ahead of last", profile: base, apply: baseApply, commit: 2, last: 1, wantError: true},
		{name: "session capacity exceeded", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.MaxSessions, result.Applied, result.SessionCount = 1, 3, 2
			result.SessionEpochHighWater = 2
			return result
		}(), commit: 3, last: 3, wantError: true},
		{name: "retry ring capacity exceeded", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.RetryWindow = 2
			result.Applied, result.SessionCount, result.SessionSlotCount = 4, 1, 3
			result.SessionEpochHighWater = 2
			return result
		}(), commit: 4, last: 4, wantError: true},
		{name: "last exceeds sealed suffix", profile: base, apply: baseApply,
			commit: 1, last: 8194, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateImmutableBaseApplyCapacity(
				test.profile, test.apply, test.commit, test.last,
			)
			if test.wantError && !errors.Is(err, ErrApplyCapacity) {
				t.Fatalf("validation error = %v, want ErrApplyCapacity", err)
			}
			if !test.wantError && err != nil {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
}

func TestImmutableBaseApplyCapacityAdvancedBase(t *testing.T) {
	profile := raftstore.CapacityProfile{
		Format: raftstore.CapacityFormatImmutableBase, LogBaseIndex: 100, MaxEntries: 4096,
	}
	apply := sqldriver.ReplicatedApplyCapacityProfile{
		ApplyFormat:           sqldriver.ReplicatedApplyFormat,
		MaxSessions:           128,
		RetryWindow:           8,
		Initialized:           true,
		Applied:               100,
		SessionCount:          12,
		SessionSlotCount:      64,
		SessionEpochHighWater: 100,
	}
	if err := validateImmutableBaseApplyCapacity(profile, apply, 100, 100); err != nil {
		t.Fatalf("exact newer-base capacity: %v", err)
	}
	if err := validateImmutableBaseApplyCapacity(profile, apply, 101, 101); err != nil {
		t.Fatalf("one unapplied newer-base entry: %v", err)
	}
}
