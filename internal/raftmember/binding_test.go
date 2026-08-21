package raftmember

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestBindingFromWALMapsCompleteIdentityWithoutMintingIncarnation(t *testing.T) {
	walIdentity := testWALIdentity(1)
	_, wal, _, _ := createWAL(t, walIdentity)
	authority := testAuthorityProfile()

	before := wal.CurrentIncarnation()
	got, err := BindingFromWAL(wal, authority)
	if err != nil {
		t.Fatalf("BindingFromWAL: %v", err)
	}
	want := sqldriver.ReplicatedShardStoreBinding{
		ClusterID:             walIdentity.ClusterID,
		ClusterIncarnation:    walIdentity.ClusterIncarnation,
		TopologyRecoveryEpoch: testTopologyRecoveryEpoch,
		Distribution:          walIdentity.Distribution,
		Shard:                 walIdentity.Shard,
		AllocationGeneration:  walIdentity.AllocationGeneration,
		ShardIncarnation:      walIdentity.ShardIncarnation,
		GroupID:               walIdentity.GroupID,
		MemberID:              walIdentity.MemberID,
		StoreID:               walIdentity.StoreID,
		Authority:             authority,
	}
	if got != want {
		t.Fatalf("BindingFromWAL = %+v, want %+v", got, want)
	}
	if after := wal.CurrentIncarnation(); after != before {
		t.Fatalf("BindingFromWAL minted incarnation: before=%d after=%d", before, after)
	}
	if _, err := BindPreparedSQL(wal, nil, authority, "docs"); !errors.Is(err, ErrInvalidDatabase) {
		t.Fatalf("BindPreparedSQL(nil database) = %v, want ErrInvalidDatabase", err)
	}
}

func TestBindingForNewWALMatchesTheEventuallyCreatedWAL(t *testing.T) {
	identity := testWALIdentity(12)
	authority := testAuthorityProfile()
	planned, err := BindingForNewWAL(identity, testTopologyRecoveryEpoch, authority)
	if err != nil {
		t.Fatal(err)
	}
	_, wal, _, _ := createWAL(t, identity)
	live, err := BindingFromWAL(wal, authority)
	if err != nil || live != planned || wal.CurrentIncarnation() != 0 {
		t.Fatalf("planned=%+v live=%+v incarnation=%d err=%v", planned, live, wal.CurrentIncarnation(), err)
	}
	invalid := identity
	invalid.StoreID = [16]byte{}
	if _, err := BindingForNewWAL(
		invalid, testTopologyRecoveryEpoch, authority,
	); !errors.Is(err, ErrWALUnavailable) {
		t.Fatalf("invalid planned identity error = %v", err)
	}
	if _, err := BindingForNewWAL(
		identity, 0, authority,
	); !errors.Is(err, ErrWALUnavailable) {
		t.Fatalf("zero topology epoch error = %v", err)
	}
	authority.RouteGeneration = 0
	if _, err := BindingForNewWAL(
		identity, testTopologyRecoveryEpoch, authority,
	); !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("zero authority generation error = %v", err)
	}
}

func TestBindPreparedSQLReturnsAndRequiresFullLocalIdentity(t *testing.T) {
	walIdentity := testWALIdentity(20)
	_, wal, _, _ := createWAL(t, walIdentity)
	authority := testAuthorityProfile()

	firstPath, firstDB, firstLocal := prepareSQLRoot(t, walIdentity, "first")
	first, err := BindPreparedSQL(wal, firstDB, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind first SQL root", err)
	if err != nil {
		t.Fatalf("BindPreparedSQL(first): %v", err)
	}
	if first.LogID != firstLocal.LogID {
		t.Fatalf("bound SQL LogID = %x, want retained local %x", first.LogID, firstLocal.LogID)
	}
	if first.Binding.StoreID != walIdentity.StoreID || first.Binding.GroupID != walIdentity.GroupID {
		t.Fatalf("bound WAL coordinates = %+v, want %+v", first.Binding, walIdentity)
	}
	if wal.CurrentIncarnation() != 0 {
		t.Fatalf("BindPreparedSQL minted WAL incarnation %d", wal.CurrentIncarnation())
	}
	if err := firstDB.Close(); err != nil {
		t.Fatalf("close first bound SQL root: %v", err)
	}

	// Simulate losing BindPreparedSQL's return after its catalog publication.
	// The independently retained pre-bind LogID plus the live WAL binding can
	// settle the full identity for durable caller retention.
	settledDB, settled, err := OpenBoundSQLForSettlement(
		firstPath, wal, authority, firstLocal.LogID, "docs",
	)
	skipIfStrictAllocationUnsupported(t, "settle first SQL root", err)
	if err != nil {
		t.Fatalf("OpenBoundSQLForSettlement(first): %v", err)
	}
	if settled != first {
		t.Fatalf("settled identity = %+v, want bound identity %+v", settled, first)
	}
	if err := settledDB.Close(); err != nil {
		t.Fatalf("close settled SQL root: %v", err)
	}
	if wal.CurrentIncarnation() != 0 {
		t.Fatalf("settlement minted WAL incarnation %d", wal.CurrentIncarnation())
	}

	reopened, err := OpenBoundSQL(firstPath, wal, authority, first)
	skipIfStrictAllocationUnsupported(t, "reopen first SQL root", err)
	if err != nil {
		t.Fatalf("OpenBoundSQL(first): %v", err)
	}
	if _, err := reopened.RequireReplicatedShardStore(first); err != nil {
		t.Fatalf("RequireReplicatedShardStore(first): %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened SQL root: %v", err)
	}

	// A separately prepared SQL root can carry the same WAL tuple, but its
	// independently minted LogID and storage layout are not interchangeable.
	// A byte-identical copy of the first SQL root would retain both and is
	// intentionally outside what this identity comparison can distinguish.
	secondPath, secondDB, secondLocal := prepareSQLRoot(t, walIdentity, "second")
	second, err := BindPreparedSQL(wal, secondDB, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind second SQL root", err)
	if err != nil {
		t.Fatalf("BindPreparedSQL(second): %v", err)
	}
	if second.LogID != secondLocal.LogID {
		t.Fatalf("second bound SQL LogID = %x, want %x", second.LogID, secondLocal.LogID)
	}
	if err := secondDB.Close(); err != nil {
		t.Fatalf("close second bound SQL root: %v", err)
	}
	wrongRootDB, _, err := OpenBoundSQLForSettlement(
		secondPath, wal, authority, firstLocal.LogID, "docs",
	)
	if wrongRootDB != nil {
		_ = wrongRootDB.Close()
		t.Fatal("settlement identity mismatch nevertheless returned an open database")
	}
	if !errors.Is(err, sqldriver.ErrReplicatedShardStoreIdentityMismatch) {
		t.Fatalf(
			"settle another SQL root = %v, want replicated identity mismatch", err,
		)
	}
	if _, err := OpenBoundSQL(secondPath, wal, authority, first); !errors.Is(
		err, sqldriver.ErrReplicatedShardStoreIdentityMismatch,
	) {
		t.Fatalf(
			"OpenBoundSQL another SQL root = %v, want replicated identity mismatch", err,
		)
	}
	secondReopened, err := OpenBoundSQL(secondPath, wal, authority, second)
	skipIfStrictAllocationUnsupported(t, "reopen second SQL root", err)
	if err != nil {
		t.Fatalf("OpenBoundSQL(second exact identity): %v", err)
	}
	if err := secondReopened.Close(); err != nil {
		t.Fatalf("close exact second open: %v", err)
	}

	missingLogID := first
	missingLogID.LogID = [16]byte{}
	if _, err := OpenBoundSQL(firstPath, wal, authority, missingLogID); !errors.Is(err, ErrExpectedSQLLogID) {
		t.Fatalf("OpenBoundSQL without retained LogID = %v, want ErrExpectedSQLLogID", err)
	}
	missingPath := filepath.Join(t.TempDir(), "must-not-settle.vdb")
	if _, _, err := OpenBoundSQLForSettlement(
		missingPath, wal, authority, [16]byte{}, "docs",
	); !errors.Is(err, ErrExpectedSQLLogID) {
		t.Fatalf(
			"OpenBoundSQLForSettlement without retained LogID = %v, want ErrExpectedSQLLogID", err,
		)
	}
	assertPathAbsent(t, missingPath)
	assertPathAbsent(t, missingPath+".lock")

	wrongAuthority := authority
	wrongAuthority.RouteGeneration++
	if _, err := OpenBoundSQL(firstPath, wal, wrongAuthority, first); !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("OpenBoundSQL wrong authority = %v, want ErrBindingMismatch", err)
	}
	wrongTableDB, _, err := OpenBoundSQLForSettlement(
		firstPath, wal, authority, firstLocal.LogID, "other",
	)
	if wrongTableDB != nil {
		_ = wrongTableDB.Close()
		t.Fatal("settlement table mismatch nevertheless returned an open database")
	}
	if !errors.Is(err, sqldriver.ErrReplicatedShardStoreIdentityMismatch) {
		t.Fatalf("settle wrong user table = %v, want replicated identity mismatch", err)
	}

	wrongWALIdentity := walIdentity
	wrongWALIdentity.StoreID[0] ^= 0xff
	_, wrongWAL, _, _ := createWAL(t, wrongWALIdentity)
	if _, err := OpenBoundSQL(firstPath, wrongWAL, authority, first); !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("OpenBoundSQL wrong live WAL = %v, want ErrBindingMismatch", err)
	}
	wrongWALDB, _, err := OpenBoundSQLForSettlement(
		firstPath, wrongWAL, authority, firstLocal.LogID, "docs",
	)
	if wrongWALDB != nil {
		_ = wrongWALDB.Close()
		t.Fatal("settlement WAL mismatch nevertheless returned an open database")
	}
	if !errors.Is(err, sqldriver.ErrReplicatedShardStoreIdentityMismatch) {
		t.Fatalf(
			"settle with wrong live WAL = %v, want replicated identity mismatch", err,
		)
	}
}

func TestClosedAndPoisonedWALRejectBeforeSQLBinding(t *testing.T) {
	authority := testAuthorityProfile()

	t.Run("nil", func(t *testing.T) {
		if _, err := BindingFromWAL(nil, authority); !errors.Is(err, ErrWALUnavailable) {
			t.Fatalf("BindingFromWAL(nil) = %v, want ErrWALUnavailable", err)
		}
		if _, err := BindPreparedSQL(nil, nil, authority, "docs"); !errors.Is(err, ErrWALUnavailable) {
			t.Fatalf("BindPreparedSQL(nil WAL) = %v, want ErrWALUnavailable", err)
		}
		if _, _, err := OpenBoundSQLForSettlement(
			"unused", nil, authority, [16]byte{1}, "docs",
		); !errors.Is(err, ErrWALUnavailable) {
			t.Fatalf("OpenBoundSQLForSettlement(nil WAL) = %v, want ErrWALUnavailable", err)
		}
	})

	t.Run("closed", func(t *testing.T) {
		walIdentity := testWALIdentity(50)
		_, wal, _, _ := createWAL(t, walIdentity)
		_, database, _ := prepareSQLRoot(t, walIdentity, "closed")
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := BindPreparedSQL(wal, database, authority, "docs"); !errors.Is(err, ErrWALUnavailable) {
			t.Fatalf("BindPreparedSQL(closed WAL) = %v, want ErrWALUnavailable", err)
		}
		assertSQLRootUnbound(t, database)

		missing := filepath.Join(t.TempDir(), "must-not-open.vdb")
		if _, err := OpenBoundSQL(missing, wal, authority, sqldriver.ReplicatedShardStoreIdentity{}); !errors.Is(err, ErrWALUnavailable) {
			t.Fatalf("OpenBoundSQL(closed WAL) = %v, want ErrWALUnavailable", err)
		}
		if _, _, err := OpenBoundSQLForSettlement(
			missing, wal, authority, [16]byte{1}, "docs",
		); !errors.Is(err, ErrWALUnavailable) {
			t.Fatalf("OpenBoundSQLForSettlement(closed WAL) = %v, want ErrWALUnavailable", err)
		}
		assertPathAbsent(t, missing)
		assertPathAbsent(t, missing+".lock")
	})

	t.Run("poisoned namespace", func(t *testing.T) {
		walIdentity := testWALIdentity(70)
		walPath, wal, _, _ := createWAL(t, walIdentity)
		_, database, _ := prepareSQLRoot(t, walIdentity, "poisoned")
		incarnation, err := wal.BeginIncarnation()
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(walPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(walPath, walPath+".replaced"); err != nil {
			t.Fatal(err)
		}
		replacement, err := os.OpenFile(walPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		truncateErr := replacement.Truncate(info.Size())
		closeErr := replacement.Close()
		if err := errors.Join(truncateErr, closeErr); err != nil {
			t.Fatal(err)
		}
		if err := wal.Persist(raftmodel.PersistBatch{
			NodeIncarnation: incarnation, ReadyID: 1,
		}); !errors.Is(err, raftstore.ErrNamespaceChanged) {
			t.Fatalf("poison WAL namespace = %v, want ErrNamespaceChanged", err)
		}
		if _, err := BindPreparedSQL(wal, database, authority, "docs"); !errors.Is(err, ErrWALUnavailable) {
			t.Fatalf("BindPreparedSQL(poisoned WAL) = %v, want ErrWALUnavailable", err)
		}
		assertSQLRootUnbound(t, database)

		missing := filepath.Join(t.TempDir(), "must-not-open.vdb")
		if _, err := OpenBoundSQL(
			missing, wal, authority, sqldriver.ReplicatedShardStoreIdentity{},
		); !errors.Is(err, ErrWALUnavailable) {
			t.Fatalf("OpenBoundSQL(poisoned WAL) = %v, want ErrWALUnavailable", err)
		}
		if _, _, err := OpenBoundSQLForSettlement(
			missing, wal, authority, [16]byte{1}, "docs",
		); !errors.Is(err, ErrWALUnavailable) {
			t.Fatalf("OpenBoundSQLForSettlement(poisoned WAL) = %v, want ErrWALUnavailable", err)
		}
		assertPathAbsent(t, missing)
		assertPathAbsent(t, missing+".lock")
	})
}

func TestRecoveredTornCurrentSlotQuarantinesBeforeSQLMutation(t *testing.T) {
	walIdentity := testWALIdentity(90)
	walPath, wal, key, options := createWAL(t, walIdentity)
	if _, err := wal.BeginIncarnation(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// BeginIncarnation selected slot 1 at generation 2. Damage its complete
	// sector checksum so recovery falls back to the authenticated generation-1
	// slot and marks the member as torn/quarantined.
	file, err := os.OpenFile(walPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	offset := int64(raftstore.StaticHeaderBytes + raftstore.CurrentSlotBytes + 128)
	one := []byte{0}
	if _, err := file.ReadAt(one, offset); err == nil {
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
		t.Fatalf("reopen torn WAL: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if !reopened.RecoveredTornCurrentSlot() {
		t.Fatal("test setup did not recover a torn current slot")
	}

	_, database, _ := prepareSQLRoot(t, walIdentity, "torn")
	if _, err := BindPreparedSQL(
		reopened, database, testAuthorityProfile(), "docs",
	); !errors.Is(err, ErrWALQuarantined) {
		t.Fatalf("BindPreparedSQL(torn WAL) = %v, want ErrWALQuarantined", err)
	}
	assertSQLRootUnbound(t, database)

	missing := filepath.Join(t.TempDir(), "must-not-open.vdb")
	if _, err := OpenBoundSQL(
		missing, reopened, testAuthorityProfile(), sqldriver.ReplicatedShardStoreIdentity{},
	); !errors.Is(err, ErrWALQuarantined) {
		t.Fatalf("OpenBoundSQL(torn WAL) = %v, want ErrWALQuarantined", err)
	}
	if _, _, err := OpenBoundSQLForSettlement(
		missing, reopened, testAuthorityProfile(), [16]byte{1}, "docs",
	); !errors.Is(err, ErrWALQuarantined) {
		t.Fatalf("OpenBoundSQLForSettlement(torn WAL) = %v, want ErrWALQuarantined", err)
	}
	assertPathAbsent(t, missing)
	assertPathAbsent(t, missing+".lock")
}

const testTopologyRecoveryEpoch uint64 = 29

func testAuthorityProfile() sqldriver.ReplicatedAuthorityProfile {
	return sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 31,
		ProtectionEpoch:        37,
		OwnershipEpoch:         41,
		SchemaGeneration:       43,
		RoutingVersion:         47,
		RouteGeneration:        53,
	}
}

func testWALIdentity(seed byte) raftstore.Identity {
	identity := raftstore.Identity{
		Distribution:         "orders",
		Shard:                "0000-7fff",
		AllocationGeneration: uint64(seed) + 1,
		MemberID:             uint64(seed) + 1,
	}
	fillID := func(id *[16]byte, offset byte) {
		for i := range id {
			id[i] = seed + offset + byte(i)
		}
	}
	fillID(&identity.ClusterID, 1)
	fillID(&identity.ClusterIncarnation, 19)
	fillID(&identity.ShardIncarnation, 37)
	fillID(&identity.GroupID, 55)
	fillID(&identity.StoreID, 73)
	return identity
}

func testWALKey() raftstore.Key {
	key := raftstore.Key{ID: "raftmember-test-key", Wrapped: []byte("opaque-test-wrapped-key")}
	for i := range key.Material {
		key.Material[i] = byte(i + 1)
	}
	return key
}

func testWALOptions() raftstore.Options {
	return raftstore.Options{
		MaxFileBytes:   160 << 20,
		MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
		MaxRecords:     1024,
		MaxEntries:     8192,
		MaxLiveBytes:   raftstore.MinimumReadyLiveBytes,
	}
}

func createWAL(
	t testing.TB,
	identity raftstore.Identity,
) (string, *raftstore.Store, raftstore.Key, raftstore.Options) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "member.wal")
	key := testWALKey()
	options := testWALOptions()
	index, term := uint64(1), uint64(1)
	store, err := raftstore.Create(path, identity, key, raftstore.Bootstrap{
		TopologyRecoveryEpoch: testTopologyRecoveryEpoch,
		Snapshot: &pb.Snapshot{
			Data: []byte("raftmember-static-bootstrap"),
			Metadata: &pb.SnapshotMetadata{
				Index: &index, Term: &term,
				ConfState: &pb.ConfState{Voters: []uint64{identity.MemberID}},
			},
		},
	}, options)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return path, store, key, options
}

func prepareSQLRoot(
	t testing.TB,
	identity raftstore.Identity,
	name string,
) (string, *sqldriver.Database, sqldriver.ShardStoreIdentity) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".vdb")
	database, err := sqldriver.InitializeShardStore(path, sqldriver.ShardStoreBinding{
		Distribution:         distribution.DistributionName(identity.Distribution),
		Shard:                distribution.ShardID(identity.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(identity.AllocationGeneration),
	})
	if err != nil {
		t.Fatalf("initialize SQL root: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	ctx := context.Background()
	session, err := database.NewSession(ctx)
	if err != nil {
		t.Fatalf("new SQL session: %v", err)
	}
	prepared, prepareErr := session.Prepare(ctx, `CREATE TABLE docs (PRIMARY KEY (id))`)
	if prepareErr == nil {
		_, prepareErr = prepared.Exec(ctx, nil)
	}
	if prepared != nil {
		prepareErr = errors.Join(prepareErr, prepared.Close())
	}
	err = errors.Join(prepareErr, session.Close())
	if err != nil {
		t.Fatalf("prepare empty SQL user table: %v", err)
	}
	local, err := database.ShardStoreIdentity()
	if err != nil {
		t.Fatalf("inspect local SQL identity: %v", err)
	}
	return path, database, local
}

func assertSQLRootUnbound(t testing.TB, database *sqldriver.Database) {
	t.Helper()
	if _, err := database.ReplicatedShardStoreIdentity(); err == nil {
		t.Fatal("failed WAL validation nevertheless published replicated SQL binding")
	}
}

func assertPathAbsent(t testing.TB, path string) {
	t.Helper()
	if _, err := os.Lstat(path); err == nil || !os.IsNotExist(err) {
		t.Fatalf("path %q exists or could not be classified after rejected open: %v", path, err)
	}
}

func skipIfStrictAllocationUnsupported(t testing.TB, operation string, err error) {
	t.Helper()
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		t.Skipf("%s requires strict physical allocation proof: %v", operation, err)
	}
}
