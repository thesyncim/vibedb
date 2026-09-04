package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestRF3RecoveryRejectsMissingPhysicalAuthority(t *testing.T) {
	for _, log := range []rf3RecoveryLog{nil, (*raftstore.Store)(nil), (*raftstore.GroupView)(nil)} {
		_, _, db, apply, err := openRF3RetainedApply("unopened", log, sqldriver.ReplicatedShardStoreIdentity{}, sqldriver.ReplicatedApplyIdentity{})
		if !errors.Is(err, raftmember.ErrWALUnavailable) || db != nil || apply != nil {
			t.Fatalf("missing authority accepted: %v", err)
		}
	}
}

// Both groups recover through the command's normal schema-selection boundary
// after the single physical node log is closed and reopened. No per-range WAL
// exists. A swapped group cannot bind or mutate the other group's SQL root.
func TestRF3RecoveryTwoGroupsShareNodeLog(t *testing.T) {
	fixture := newRF3NodeRecoveryFixture(t)
	nodeStore, boots, bases, applies, paths := fixture.store, fixture.boots, fixture.bases, fixture.applies, fixture.paths
	for i := range boots {
		group, _ := nodeStore.GroupByID(boots[i].Descriptor.GroupID)
		wrong, _ := nodeStore.GroupByID(boots[1-i].Descriptor.GroupID)
		_, _, db, claim, err := openRF3RetainedApply(paths[i], wrong, bases[i], applies[i])
		if !errors.Is(err, raftmember.ErrBindingMismatch) || db != nil || claim != nil {
			t.Fatalf("wrong group recovered: %v", err)
		}
		base, id, db, claim, err := openRF3RetainedApply(paths[i], group, bases[i], applies[i])
		if err != nil {
			t.Fatal(err)
		}
		if !base.Equal(bases[i]) || id != applies[i] || claim.Published().Applied != 1 {
			t.Fatal("recovery changed group identity/publication")
		}
		if err = errors.Join(claim.Close(), db.Close()); err != nil {
			t.Fatal(err)
		}

	}
}

type rf3NodeRecoveryFixture struct {
	store        *raftstore.NodeStore
	path         string
	node         raftstore.NodeIdentity
	key          raftstore.Key
	options      raftstore.NodeStoreOptions
	applyOptions sqldriver.ReplicatedApplyOptions
	boots        []raftstore.NodeBootstrap
	bases        [2]sqldriver.ReplicatedShardStoreIdentity
	applies      [2]sqldriver.ReplicatedApplyIdentity
	paths        [2]string
}

func newRF3NodeRecoveryFixture(t *testing.T) *rf3NodeRecoveryFixture {
	t.Helper()
	root := t.TempDir()
	node := raftstore.NodeIdentity{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, NodeID: [16]byte{3}}
	key := raftstore.Key{ID: "recovery-node-key", Wrapped: []byte("test-wrapped-key"), Material: [32]byte{4}}
	index, term := uint64(1), uint64(1)
	snapshot := &pb.Snapshot{Data: []byte("rf3-recovery-bootstrap"), Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}}
	boots := make([]raftstore.NodeBootstrap, 2)
	for i := range boots {
		boots[i] = raftstore.NodeBootstrap{Descriptor: raftstore.GroupDescriptor{
			TopologyRecoveryEpoch: 1, AllocationGeneration: 1, MemberID: 1,
			GroupID: [16]byte{byte(10 + i)}, ShardIncarnation: [16]byte{byte(20 + i)}, StoreID: [16]byte{byte(30 + i)},
			Distribution: "data", Shard: fmt.Sprint(i),
		}, Snapshot: snapshot}
	}
	options := raftstore.NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 256, RecentWaves: 64, MaxEntriesPerGroup: 64, ReaderSlots: 1, MaxGroups: 8}
	path := filepath.Join(root, "node")
	nodeStore, err := raftstore.CreateNodeStore(path, node, key, boots, options)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &rf3NodeRecoveryFixture{store: nodeStore, path: path, node: node, key: key, options: options}
	t.Cleanup(func() { _ = fixture.store.Close() })
	authority := sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1}
	var bases [2]sqldriver.ReplicatedShardStoreIdentity
	var applies [2]sqldriver.ReplicatedApplyIdentity
	var paths [2]string
	for i := range boots {
		group, ok := nodeStore.GroupByID(boots[i].Descriptor.GroupID)
		if !ok {
			t.Fatal("missing prepared group")
		}
		paths[i] = filepath.Join(root, fmt.Sprintf("group-%d.vdb", i))
		db, err := sqldriver.InitializeShardStore(paths[i], sqldriver.ShardStoreBinding{Distribution: "data", Shard: distribution.ShardID(fmt.Sprint(i)), AllocationGeneration: 1})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		session, err := db.NewSession(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		statement, err := session.Prepare(t.Context(), `CREATE TABLE docs (PRIMARY KEY (id))`)
		if err == nil {
			_, err = statement.Exec(t.Context(), nil)
		}
		if statement != nil {
			err = errors.Join(err, statement.Close())
		}
		err = errors.Join(err, session.Close())
		if err != nil {
			t.Fatal(err)
		}
		bases[i], err = raftmember.BindPreparedNodeSQL(group, db, authority, "docs")
		if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
			t.Skipf("strict physical allocation unavailable: %v", err)
		}
		if err != nil {
			t.Fatal(err)
		}
		applyOptions := sqldriver.ReplicatedApplyOptions{MaxSessions: 128, RetryWindow: 8,
			TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 256, MaxBytes: 384 << 20},
			Placement: sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "/id", TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}},
		}
		fixture.applyOptions = applyOptions
		claim, id, err := raftmember.OpenPreparedNodeApply(group, db, authority, bases[i], applyOptions)
		if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
			t.Skipf("strict physical allocation unavailable: %v", err)
		}
		if err != nil {
			t.Fatal(err)
		}
		_, err = claim.InstallSnapshot(snapshot)
		err = errors.Join(err, claim.Close(), db.Close())
		if err != nil {
			t.Fatal(err)
		}
		applies[i] = id
	}
	fixture.boots, fixture.bases, fixture.applies, fixture.paths = boots, bases, applies, paths
	fixture.reopen(t)
	return fixture
}

func (f *rf3NodeRecoveryFixture) reopen(t *testing.T) {
	t.Helper()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = raftstore.OpenNodeStore(f.path, f.node, f.key, f.options)
	if err != nil {
		t.Fatal(err)
	}
}
