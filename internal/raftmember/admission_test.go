package raftmember

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestStaticNoGCCompletionCapacityBoundary(t *testing.T) {
	base := raftstore.CapacityProfile{
		Format:       raftstore.CapacityFormatStatic,
		LogBaseIndex: 1,
		MaxEntries:   8192,
	}
	baseApply := sqldriver.ReplicatedApplyCapacityProfile{
		ApplyFormat:    sqldriver.ReplicatedApplyFormat,
		MaxCompletions: 8192, Initialized: true, Applied: 1,
	}
	tests := []struct {
		name      string
		profile   raftstore.CapacityProfile
		apply     sqldriver.ReplicatedApplyCapacityProfile
		commit    uint64
		last      uint64
		wantError bool
	}{
		{name: "equality", profile: base, apply: baseApply, commit: 1, last: 1},
		{name: "nontrivial suffix", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.Applied, result.CompletionCount = 3, 1
			return result
		}(), commit: 4, last: 5},
		{name: "maximum arithmetic", profile: raftstore.CapacityProfile{
			Format:       raftstore.CapacityFormatStatic,
			LogBaseIndex: 1,
			MaxEntries:   math.MaxUint64,
		}, apply: sqldriver.ReplicatedApplyCapacityProfile{
			ApplyFormat: sqldriver.ReplicatedApplyFormat, MaxCompletions: math.MaxUint64,
			Initialized: true, Applied: math.MaxUint64 - 1, CompletionCount: math.MaxUint64 - 2,
		}, commit: math.MaxUint64, last: math.MaxUint64},
		{name: "maximum invalid count", profile: raftstore.CapacityProfile{
			Format:       raftstore.CapacityFormatStatic,
			LogBaseIndex: 1,
			MaxEntries:   math.MaxUint64,
		}, apply: sqldriver.ReplicatedApplyCapacityProfile{
			ApplyFormat: sqldriver.ReplicatedApplyFormat, MaxCompletions: math.MaxUint64,
			Initialized: true, Applied: math.MaxUint64, CompletionCount: math.MaxUint64,
		}, commit: math.MaxUint64, last: math.MaxUint64, wantError: true},
		{name: "maximum sealed suffix exceeded", profile: raftstore.CapacityProfile{
			Format:       raftstore.CapacityFormatStatic,
			LogBaseIndex: 1,
			MaxEntries:   math.MaxUint64 - 2,
		}, apply: sqldriver.ReplicatedApplyCapacityProfile{
			ApplyFormat: sqldriver.ReplicatedApplyFormat, MaxCompletions: math.MaxUint64,
			Initialized: true, Applied: math.MaxUint64 - 2,
		}, commit: math.MaxUint64 - 1, last: math.MaxUint64, wantError: true},
		{name: "unknown apply format", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.ApplyFormat = 2
			return result
		}(), commit: 1, last: 1, wantError: true},
		{name: "one short", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.MaxCompletions--
			return result
		}(), commit: 1, last: 1, wantError: true},
		{name: "zero WAL entries", profile: raftstore.CapacityProfile{
			Format: raftstore.CapacityFormatStatic, LogBaseIndex: 1,
		}, apply: baseApply, commit: 1, last: 1, wantError: true},
		{name: "zero completions", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.MaxCompletions = 0
			return result
		}(), commit: 1, last: 1, wantError: true},
		{name: "advanced base", profile: raftstore.CapacityProfile{
			Format: raftstore.CapacityFormatStatic, LogBaseIndex: 2, MaxEntries: 8192,
		}, apply: baseApply, commit: 1, last: 1, wantError: true},
		{name: "unknown capacity format", profile: raftstore.CapacityProfile{
			Format: raftstore.CapacityFormat(2), LogBaseIndex: 1, MaxEntries: 8192,
		}, apply: baseApply, commit: 1, last: 1, wantError: true},
		{name: "uninitialized", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.Initialized, result.Applied = false, 0
			return result
		}(), commit: 1, last: 1, wantError: true},
		{name: "completion count ahead", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.Applied, result.CompletionCount = 2, 2
			return result
		}(), commit: 2, last: 2, wantError: true},
		{name: "apply ahead of commit", profile: base, apply: func() sqldriver.ReplicatedApplyCapacityProfile {
			result := baseApply
			result.Applied = 2
			return result
		}(), commit: 1, last: 2, wantError: true},
		{name: "commit ahead of last", profile: base, apply: baseApply, commit: 2, last: 1, wantError: true},
		{name: "zero WAL cut", profile: base, apply: baseApply, wantError: true},
		{name: "last exceeds sealed suffix", profile: base, apply: baseApply,
			commit: 1, last: 8194, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateStaticNoGCCompletionCapacity(
				test.profile, test.apply, test.commit, test.last,
			)
			if test.wantError && !errors.Is(err, ErrStaticCompletionCapacity) {
				t.Fatalf("validation error = %v, want ErrStaticCompletionCapacity", err)
			}
			if !test.wantError && err != nil {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
}

func TestStaticNoGCCompletionCapacityArithmetic(t *testing.T) {
	// One previously unseen normal command creates one completion even when it
	// deterministically records a stale-fence or semantic refusal. Configuration
	// entries, empty normal entries, exact duplicates, and retained conflicts
	// create zero. Therefore one is the exact worst-case cost of every unapplied
	// log entry, independent of the entry mix.
	for maxEntries := uint64(1); maxEntries <= 128; maxEntries++ {
		for last := uint64(1); last-1 <= maxEntries; last++ {
			for applied := uint64(1); applied <= last; applied++ {
				for completions := uint64(0); completions <= applied-1; completions++ {
					unapplied := last - applied
					if completions+unapplied > maxEntries {
						t.Fatalf(
							"C=%d A=%d L=%d MaxEntries=%d violates C+(L-A)<=MaxEntries",
							completions, applied, last, maxEntries,
						)
					}
				}
			}
		}
	}
}

func TestValidateStaticNoGCCompletionCapacityAgainstLiveApply(t *testing.T) {
	tests := []struct {
		name           string
		maxCompletions uint64
		wantError      bool
	}{
		{name: "equality", maxCompletions: 8192},
		{name: "one short", maxCompletions: 8191, wantError: true},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			walIdentity := testWALIdentity(byte(170 + index))
			_, wal, _, _ := createWAL(t, walIdentity)
			_, database, _ := prepareSQLRoot(t, walIdentity, "capacity")
			authority := testAuthorityProfile()
			base, err := BindPreparedSQL(wal, database, authority, "docs")
			skipIfStrictAllocationUnsupported(t, "bind live capacity SQL root", err)
			if err != nil {
				t.Fatal(err)
			}
			options := testApplyOptions()
			options.MaxCompletions = test.maxCompletions
			claim, _, err := OpenPreparedApply(wal, database, authority, base, options)
			skipIfStrictAllocationUnsupported(t, "open live capacity apply", err)
			if err != nil {
				t.Fatal(err)
			}
			bootstrap, err := wal.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := claim.InstallSnapshot(bootstrap); err != nil {
				t.Fatal(err)
			}
			err = ValidateStaticNoGCCompletionCapacity(wal, claim)
			if test.wantError && !errors.Is(err, ErrStaticCompletionCapacity) {
				t.Fatalf("validation error = %v, want ErrStaticCompletionCapacity", err)
			}
			if !test.wantError && err != nil {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
}

func TestValidateStaticNoGCCompletionCapacityChecksActualCut(t *testing.T) {
	walIdentity := testWALIdentity(190)
	_, wal, _, _ := createWAL(t, walIdentity)
	_, database, _ := prepareSQLRoot(t, walIdentity, "capacity-cut")
	authority := testAuthorityProfile()
	base, err := BindPreparedSQL(wal, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind capacity-cut SQL root", err)
	if err != nil {
		t.Fatal(err)
	}
	options := testApplyOptions()
	options.MaxCompletions = uint64(testWALOptions().MaxEntries)
	claim, _, err := OpenPreparedApply(wal, database, authority, base, options)
	skipIfStrictAllocationUnsupported(t, "open capacity-cut apply", err)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateStaticNoGCCompletionCapacity(wal, claim); !errors.Is(
		err, ErrStaticCompletionCapacity,
	) {
		t.Fatalf("uninitialized qualification = %v, want ErrStaticCompletionCapacity", err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	if err := ValidateStaticNoGCCompletionCapacity(wal, claim); err != nil {
		t.Fatalf("bootstrap qualification: %v", err)
	}

	key, _ := orderedkey.AppendJSONString(nil, []byte(`"capacity"`), orderedkey.Ascending)
	document := []byte(`{"id":"capacity"}`)
	command := testApplyCommand(base, 1, key, document)
	meta := raftmodel.ApplyMeta{Index: 2, Term: 2, Type: pb.EntryNormal}
	if _, err := claim.ApplyNormal(meta, command); err != nil {
		t.Fatal(err)
	}
	profile, err := claim.CapacityQualificationProfile()
	if err != nil || !profile.Initialized || profile.Applied != 2 || profile.CompletionCount != 1 {
		t.Fatalf("applied capacity profile = %+v, %v", profile, err)
	}
	if err := ValidateStaticNoGCCompletionCapacity(wal, claim); !errors.Is(
		err, ErrStaticCompletionCapacity,
	) {
		t.Fatalf("ahead apply qualification = %v, want ErrStaticCompletionCapacity", err)
	}

	incarnation, err := wal.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	index, term, entryType := uint64(2), uint64(2), pb.EntryNormal
	if err := wal.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         1,
		HardState:       &pb.HardState{Term: &term, Commit: &index},
		Entries: []*pb.Entry{{
			Term: &term, Index: &index, Type: &entryType, Data: command,
		}},
		MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateStaticNoGCCompletionCapacity(wal, claim); err != nil {
		t.Fatalf("matched applied/WAL cut qualification: %v", err)
	}
	thirdIndex, fourthIndex := uint64(3), uint64(4)
	thirdTerm, thirdCommit := uint64(3), uint64(3)
	if err := wal.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         2,
		HardState:       &pb.HardState{Term: &thirdTerm, Commit: &thirdCommit},
		Entries: []*pb.Entry{
			{Term: &thirdTerm, Index: &thirdIndex, Type: &entryType},
			{Term: &thirdTerm, Index: &fourthIndex, Type: &entryType},
		},
		MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateStaticNoGCCompletionCapacity(wal, claim); err != nil {
		t.Fatalf("nontrivial unapplied suffix qualification: %v", err)
	}
	if _, err := claim.ApplyNormal(meta, nil); err == nil {
		t.Fatal("conflicting replay did not poison claim")
	}
	if err := ValidateStaticNoGCCompletionCapacity(wal, claim); !errors.Is(
		err, replicatedstate.ErrApplyPoisoned,
	) {
		t.Fatalf("poisoned qualification = %v, want ErrApplyPoisoned", err)
	}
}

func TestValidateStaticNoGCCompletionCapacityRejectsCrossBindingClaims(t *testing.T) {
	authority := testAuthorityProfile()
	firstIdentity := testWALIdentity(191)
	_, firstWAL, _, _ := createWAL(t, firstIdentity)
	_, firstDB, _ := prepareSQLRoot(t, firstIdentity, "capacity-first")
	firstBase, err := BindPreparedSQL(firstWAL, firstDB, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind first capacity SQL root", err)
	if err != nil {
		t.Fatal(err)
	}
	firstOptions := testApplyOptions()
	firstOptions.MaxCompletions = uint64(testWALOptions().MaxEntries)
	firstClaim, _, err := OpenPreparedApply(
		firstWAL, firstDB, authority, firstBase, firstOptions,
	)
	skipIfStrictAllocationUnsupported(t, "open first capacity apply", err)
	if err != nil {
		t.Fatal(err)
	}

	secondIdentity := testWALIdentity(192)
	_, secondWAL, _, _ := createWAL(t, secondIdentity)
	_, secondDB, _ := prepareSQLRoot(t, secondIdentity, "capacity-second")
	secondBase, err := BindPreparedSQL(secondWAL, secondDB, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind second capacity SQL root", err)
	if err != nil {
		t.Fatal(err)
	}
	secondOptions := testApplyOptions()
	secondOptions.MaxCompletions = uint64(testWALOptions().MaxEntries)
	secondClaim, _, err := OpenPreparedApply(
		secondWAL, secondDB, authority, secondBase, secondOptions,
	)
	skipIfStrictAllocationUnsupported(t, "open second capacity apply", err)
	if err != nil {
		t.Fatal(err)
	}

	for name, test := range map[string]struct {
		wal   *raftstore.Store
		claim *sqldriver.ReplicatedApply
	}{
		"first WAL with second claim": {wal: firstWAL, claim: secondClaim},
		"second WAL with first claim": {wal: secondWAL, claim: firstClaim},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateStaticNoGCCompletionCapacity(test.wal, test.claim); !errors.Is(
				err, ErrBindingMismatch,
			) {
				t.Fatalf("cross-binding qualification = %v, want ErrBindingMismatch", err)
			}
		})
	}
}

func TestStaticNoGCCompletionCapacityRequalifiesAfterRestart(t *testing.T) {
	walIdentity := testWALIdentity(201)
	walPath, wal, key, walOptions := createWAL(t, walIdentity)
	sqlPath, database, _ := prepareSQLRoot(t, walIdentity, "capacity-restart")
	authority := testAuthorityProfile()
	base, err := BindPreparedSQL(wal, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind restart capacity SQL root", err)
	if err != nil {
		t.Fatal(err)
	}
	options := testApplyOptions()
	options.MaxCompletions = uint64(walOptions.MaxEntries)
	claim, applyIdentity, err := OpenPreparedApply(wal, database, authority, base, options)
	skipIfStrictAllocationUnsupported(t, "open restart capacity apply", err)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	keyBytes, _ := orderedkey.AppendJSONString(nil, []byte(`"restart"`), orderedkey.Ascending)
	document := []byte(`{"id":"restart"}`)
	command := testApplyCommand(base, 1, keyBytes, document)
	if _, err := claim.ApplyNormal(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryNormal,
	}, command); err != nil {
		t.Fatal(err)
	}
	incarnation, err := wal.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	index, term, entryType := uint64(2), uint64(2), pb.EntryNormal
	if err := wal.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 1,
		HardState: &pb.HardState{Term: &term, Commit: &index},
		Entries: []*pb.Entry{{
			Term: &term, Index: &index, Type: &entryType, Data: command,
		}}, MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateStaticNoGCCompletionCapacity(wal, claim); err != nil {
		t.Fatalf("initial qualification: %v", err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedWAL, err := raftstore.Open(
		walPath, walIdentity, testTopologyRecoveryEpoch, key, walOptions,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedWAL.Close()
	reopenedDB, reopenedClaim, err := OpenBoundSQLWithApply(
		sqlPath, reopenedWAL, authority, base, applyIdentity,
	)
	skipIfStrictAllocationUnsupported(t, "reopen restart capacity apply", err)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = reopenedClaim.Close()
		_ = reopenedDB.Close()
	}()
	profile, err := reopenedClaim.CapacityQualificationProfile()
	if err != nil || profile.Applied != 2 || profile.CompletionCount != 1 || !profile.Initialized {
		t.Fatalf("reopened capacity profile = %+v, %v", profile, err)
	}
	if err := ValidateStaticNoGCCompletionCapacity(reopenedWAL, reopenedClaim); err != nil {
		t.Fatalf("reopened qualification: %v", err)
	}
	if filepath.Clean(walPath) == filepath.Clean(sqlPath) {
		t.Fatal("test accidentally aliased WAL and SQL paths")
	}
}

func TestValidateStaticNoGCCompletionCapacityRejectsUnavailableInputs(t *testing.T) {
	if err := ValidateStaticNoGCCompletionCapacity(nil, nil); !errors.Is(err, ErrWALUnavailable) {
		t.Fatalf("nil WAL error = %v, want ErrWALUnavailable", err)
	}
	walIdentity := testWALIdentity(220)
	_, wal, _, _ := createWAL(t, walIdentity)
	if err := ValidateStaticNoGCCompletionCapacity(wal, nil); !errors.Is(
		err, sqldriver.ErrReplicatedApplyClosed,
	) {
		t.Fatalf("nil apply error = %v, want ErrReplicatedApplyClosed", err)
	}
	_, database, _ := prepareSQLRoot(t, walIdentity, "capacity-closed")
	authority := testAuthorityProfile()
	base, err := BindPreparedSQL(wal, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind closed-input capacity SQL root", err)
	if err != nil {
		t.Fatal(err)
	}
	options := testApplyOptions()
	options.MaxCompletions = uint64(testWALOptions().MaxEntries)
	claim, _, err := OpenPreparedApply(wal, database, authority, base, options)
	skipIfStrictAllocationUnsupported(t, "open closed-input capacity apply", err)
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ValidateStaticNoGCCompletionCapacity(wal, claim); !errors.Is(
		err, sqldriver.ErrReplicatedApplyClosed,
	) {
		t.Fatalf("closed apply error = %v, want ErrReplicatedApplyClosed", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ValidateStaticNoGCCompletionCapacity(wal, claim); !errors.Is(
		err, ErrWALUnavailable,
	) {
		t.Fatalf("closed WAL error = %v, want ErrWALUnavailable", err)
	}
}

func TestValidateStaticNoGCCompletionCapacityRejectsTornWAL(t *testing.T) {
	walIdentity := testWALIdentity(230)
	walPath, wal, key, options := createWAL(t, walIdentity)
	if _, err := wal.BeginIncarnation(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(walPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	offset := int64(raftstore.StaticHeaderBytes + raftstore.CurrentSlotBytes + 128)
	one := []byte{0}
	if _, err = file.ReadAt(one, offset); err == nil {
		one[0] ^= 0xff
		_, err = file.WriteAt(one, offset)
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := raftstore.Open(
		walPath, walIdentity, testTopologyRecoveryEpoch, key, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !reopened.RecoveredTornCurrentSlot() {
		t.Fatal("test setup did not recover a torn current slot")
	}
	if err := ValidateStaticNoGCCompletionCapacity(reopened, nil); !errors.Is(
		err, ErrWALQuarantined,
	) {
		t.Fatalf("torn qualification error = %v, want ErrWALQuarantined", err)
	}
}

func TestValidateStaticNoGCCompletionCapacityRejectsNamespacePoisonedWAL(t *testing.T) {
	walIdentity := testWALIdentity(231)
	walPath, wal, _, _ := createWAL(t, walIdentity)
	incarnation, err := wal.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(walPath, walPath+".moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(walPath, []byte("foreign WAL leaf"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := wal.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 1,
	}); !errors.Is(err, raftstore.ErrNamespaceChanged) {
		t.Fatalf("namespace poison setup = %v, want ErrNamespaceChanged", err)
	}
	if err := ValidateStaticNoGCCompletionCapacity(wal, nil); !errors.Is(
		err, ErrWALUnavailable,
	) || !errors.Is(err, raftstore.ErrNamespaceChanged) {
		t.Fatalf("namespace-poisoned qualification = %v, want WAL unavailable/namespace changed", err)
	}
}
