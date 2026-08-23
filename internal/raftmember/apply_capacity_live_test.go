package raftmember

import (
	"errors"
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

func TestValidateImmutableBaseApplyCapacityChecksActualCut(t *testing.T) {
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
	options.MaxSessions = uint64(testWALOptions().MaxEntries)
	options.RetryWindow = 1
	claim, _, err := OpenPreparedApply(wal, database, authority, base, options)
	skipIfStrictAllocationUnsupported(t, "open capacity-cut apply", err)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateImmutableBaseApplyCapacity(wal, claim); !errors.Is(
		err, ErrApplyCapacity,
	) {
		t.Fatalf("uninitialized qualification = %v, want ErrApplyCapacity", err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	if err := ValidateImmutableBaseApplyCapacity(wal, claim); err != nil {
		t.Fatalf("bootstrap qualification: %v", err)
	}

	key, _ := orderedkey.AppendJSONString(nil, []byte(`"capacity"`), orderedkey.Ascending)
	document := []byte(`{"id":"capacity"}`)
	open, epoch := applyTestSessionOpen(t, claim, base, 2)
	command := testApplyCommand(base, epoch, 2, key, document)
	meta := raftmodel.ApplyMeta{Index: 3, Term: 2, Type: pb.EntryNormal}
	if _, err := claim.ApplyNormal(meta, command); err != nil {
		t.Fatal(err)
	}
	profile, err := claim.CapacityQualificationProfile()
	if err != nil || !profile.Initialized || profile.Applied != 3 ||
		profile.SessionCount != 1 || profile.SessionSlotCount != 1 ||
		profile.SessionEpochHighWater != epoch {
		t.Fatalf("applied capacity profile = %+v, %v", profile, err)
	}
	if err := ValidateImmutableBaseApplyCapacity(wal, claim); !errors.Is(
		err, ErrApplyCapacity,
	) {
		t.Fatalf("ahead apply qualification = %v, want ErrApplyCapacity", err)
	}

	incarnation, err := wal.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	openIndex, index, term, entryType := uint64(2), uint64(3), uint64(2), pb.EntryNormal
	if err := wal.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         1,
		HardState:       &pb.HardState{Term: &term, Commit: &index},
		Entries: []*pb.Entry{
			{Term: &term, Index: &openIndex, Type: &entryType, Data: open},
			{Term: &term, Index: &index, Type: &entryType, Data: command},
		},
		MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateImmutableBaseApplyCapacity(wal, claim); err != nil {
		t.Fatalf("matched applied/WAL cut qualification: %v", err)
	}
	fourthIndex, fifthIndex := uint64(4), uint64(5)
	fourthTerm, fourthCommit := uint64(3), uint64(4)
	if err := wal.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         2,
		HardState:       &pb.HardState{Term: &fourthTerm, Commit: &fourthCommit},
		Entries: []*pb.Entry{
			{Term: &fourthTerm, Index: &fourthIndex, Type: &entryType},
			{Term: &fourthTerm, Index: &fifthIndex, Type: &entryType},
		},
		MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateImmutableBaseApplyCapacity(wal, claim); err != nil {
		t.Fatalf("nontrivial unapplied suffix qualification: %v", err)
	}
	if _, err := claim.ApplyNormal(meta, nil); err == nil {
		t.Fatal("conflicting replay did not poison claim")
	}
	if err := ValidateImmutableBaseApplyCapacity(wal, claim); !errors.Is(
		err, replicatedstate.ErrApplyPoisoned,
	) {
		t.Fatalf("poisoned qualification = %v, want ErrApplyPoisoned", err)
	}
}

func TestValidateImmutableBaseApplyCapacityRejectsCrossBindingClaims(t *testing.T) {
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
	firstOptions.MaxSessions = uint64(testWALOptions().MaxEntries)
	firstOptions.RetryWindow = 1
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
	secondOptions.MaxSessions = uint64(testWALOptions().MaxEntries)
	secondOptions.RetryWindow = 1
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
			if err := ValidateImmutableBaseApplyCapacity(test.wal, test.claim); !errors.Is(
				err, ErrBindingMismatch,
			) {
				t.Fatalf("cross-binding qualification = %v, want ErrBindingMismatch", err)
			}
		})
	}
}

func TestImmutableBaseApplyCapacityRequalifiesAfterRestart(t *testing.T) {
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
	options.MaxSessions = uint64(walOptions.MaxEntries)
	options.RetryWindow = 1
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
	open, epoch := applyTestSessionOpen(t, claim, base, 2)
	command := testApplyCommand(base, epoch, 2, keyBytes, document)
	if _, err := claim.ApplyNormal(raftmodel.ApplyMeta{
		Index: 3, Term: 2, Type: pb.EntryNormal,
	}, command); err != nil {
		t.Fatal(err)
	}
	incarnation, err := wal.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	openIndex, index, term, entryType := uint64(2), uint64(3), uint64(2), pb.EntryNormal
	if err := wal.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 1,
		HardState: &pb.HardState{Term: &term, Commit: &index},
		Entries: []*pb.Entry{
			{Term: &term, Index: &openIndex, Type: &entryType, Data: open},
			{Term: &term, Index: &index, Type: &entryType, Data: command},
		}, MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateImmutableBaseApplyCapacity(wal, claim); err != nil {
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
	if err != nil || profile.Applied != 3 || profile.SessionCount != 1 ||
		profile.SessionSlotCount != 1 || profile.SessionEpochHighWater != epoch ||
		!profile.Initialized {
		t.Fatalf("reopened capacity profile = %+v, %v", profile, err)
	}
	if err := ValidateImmutableBaseApplyCapacity(reopenedWAL, reopenedClaim); err != nil {
		t.Fatalf("reopened qualification: %v", err)
	}
	if filepath.Clean(walPath) == filepath.Clean(sqlPath) {
		t.Fatal("test accidentally aliased WAL and SQL paths")
	}
}

func TestValidateImmutableBaseApplyCapacityRejectsUnavailableInputs(t *testing.T) {
	if err := ValidateImmutableBaseApplyCapacity(nil, nil); !errors.Is(err, ErrWALUnavailable) {
		t.Fatalf("nil WAL error = %v, want ErrWALUnavailable", err)
	}
	walIdentity := testWALIdentity(220)
	_, wal, _, _ := createWAL(t, walIdentity)
	if err := ValidateImmutableBaseApplyCapacity(wal, nil); !errors.Is(
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
	options.MaxSessions = uint64(testWALOptions().MaxEntries)
	options.RetryWindow = 1
	claim, _, err := OpenPreparedApply(wal, database, authority, base, options)
	skipIfStrictAllocationUnsupported(t, "open closed-input capacity apply", err)
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ValidateImmutableBaseApplyCapacity(wal, claim); !errors.Is(
		err, sqldriver.ErrReplicatedApplyClosed,
	) {
		t.Fatalf("closed apply error = %v, want ErrReplicatedApplyClosed", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ValidateImmutableBaseApplyCapacity(wal, claim); !errors.Is(
		err, ErrWALUnavailable,
	) {
		t.Fatalf("closed WAL error = %v, want ErrWALUnavailable", err)
	}
}

func TestValidateImmutableBaseApplyCapacityRejectsTornWAL(t *testing.T) {
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
	if err := ValidateImmutableBaseApplyCapacity(reopened, nil); !errors.Is(
		err, ErrWALQuarantined,
	) {
		t.Fatalf("torn qualification error = %v, want ErrWALQuarantined", err)
	}
}

func TestValidateImmutableBaseApplyCapacityRejectsNamespacePoisonedWAL(t *testing.T) {
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
	if err := ValidateImmutableBaseApplyCapacity(wal, nil); !errors.Is(
		err, ErrWALUnavailable,
	) || !errors.Is(err, raftstore.ErrNamespaceChanged) {
		t.Fatalf("namespace-poisoned qualification = %v, want WAL unavailable/namespace changed", err)
	}
}
