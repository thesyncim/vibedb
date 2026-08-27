//go:build linux

package restoreservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestGroupInstallerBuildsAndRecoversThreeAuthorityFreeRoots(t *testing.T) {
	artifact, manifest, sourceGroup := restoreSourceArtifact(t)
	artifactHash := sha256.Sum256(artifact)
	cut, err := clusterbackup.GroupCutFromVerifiedArtifact(1, manifest, artifactHash, uint64(len(artifact)))
	if err != nil {
		t.Fatal(err)
	}
	authority := clusterbackup.CatalogCut{
		Generation: 1, Digest: filledDigest(4), PolicyGeneration: 7,
		Groups: []raftmember.GroupKey{sourceGroup},
	}
	certificate, err := clusterbackup.Certify(filledDigest(5), authority, []clusterbackup.GroupCut{cut})
	if err != nil {
		t.Fatal(err)
	}
	targetGroup := raftmember.GroupKey{
		ClusterID: filled16(30), ClusterIncarnation: filled16(31), TopologyRecoveryEpoch: 2,
		ShardIncarnation: filled16(32), GroupID: filled16(33),
	}
	evidence := []clusterbackup.ArtifactEvidence{{
		Group: cut.Group, SnapshotIndex: cut.SnapshotIndex, SnapshotTerm: cut.SnapshotTerm,
		Lineage: cut.Lineage, RelationManifestDigest: cut.RelationManifestDigest,
		ArtifactHash: cut.ArtifactHash, ArtifactBytes: cut.ArtifactBytes,
		ArtifactManifestDigest: cut.ArtifactManifestDigest,
	}}
	permit, err := clusterbackup.AdmitRestore(
		certificate, evidence, filledDigest(6), targetGroup.ClusterID, targetGroup.ClusterIncarnation,
	)
	if err != nil {
		t.Fatal(err)
	}
	target := clusterrestore.TargetGroup{Group: targetGroup}
	for i := range target.Replicas {
		target.Replicas[i] = clusterrestore.ReplicaIdentity{
			Member: uint64(i + 1), Node: rafttransport.NodeID(filled16(byte(40 + i))),
			Store: filled16(byte(50 + i)),
		}
	}
	operation, err := clusterrestore.NewOperation(
		permit, certificate, 0, 7, filledDigest(8), filledDigest(9), filledDigest(10),
		[]clusterrestore.TargetGroup{target},
	)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "installer")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	factory := &linuxReplicaFactory{t: t, root: filepath.Join(t.TempDir(), "replicas")}
	installer := GroupInstaller{Root: root, Factory: factory}
	witness, err := installer.Install(context.Background(), operation, 0, bytes.NewReader(artifact))
	if err != nil || witness.SanitizedImageDigest != manifest.ImageDigest ||
		witness.ArtifactManifest != manifest.Digest || witness.GenesisProof == ([32]byte{}) {
		t.Fatalf("witness=%+v err=%v", witness, err)
	}
	for i, digest := range witness.ReplicaRoots {
		if digest == ([32]byte{}) || i != 0 && digest == witness.ReplicaRoots[i-1] {
			t.Fatalf("replica roots=%x", witness.ReplicaRoots)
		}
	}
	if factory.commits != 3 {
		t.Fatalf("commits=%d", factory.commits)
	}
	replayed, err := installer.Install(context.Background(), operation, 0, bytes.NewReader(nil))
	if err != nil || replayed != witness || factory.commits != 3 || factory.recovers != 3 {
		t.Fatalf("replay=%+v commits=%d recovers=%d err=%v", replayed, factory.commits, factory.recovers, err)
	}
	for _, held := range factory.roots {
		if held.activation.Apply != nil {
			_ = held.activation.Apply.Close()
		}
		if held.database != nil {
			_ = held.database.Close()
		}
	}
}

type heldReplica struct {
	database   *sqldriver.Database
	identity   sqldriver.ReplicatedShardStoreIdentity
	options    sqldriver.ReplicatedApplyOptions
	bootstrap  *pb.Snapshot
	activation sqldriver.ReplicatedChildActivation
	digest     [32]byte
}

type linuxReplicaFactory struct {
	t        *testing.T
	root     string
	roots    [3]heldReplica
	commits  int
	recovers int
}

func (f *linuxReplicaFactory) OpenReplica(
	_ context.Context, operation clusterrestore.Operation, _ uint32, ordinal uint8,
	manifest replicatedstate.SnapshotArtifactManifest,
) (ReplicaRoot, error) {
	held := &f.roots[ordinal]
	if held.database == nil {
		target := operation.Targets[0]
		binding := sqldriver.ReplicatedShardStoreBinding{
			ClusterID: target.Group.ClusterID, ClusterIncarnation: target.Group.ClusterIncarnation,
			TopologyRecoveryEpoch: target.Group.TopologyRecoveryEpoch,
			Distribution:          manifest.State.Binding.Distribution, Shard: manifest.State.Binding.Shard,
			AllocationGeneration: 1, ShardIncarnation: target.Group.ShardIncarnation,
			GroupID: target.Group.GroupID, MemberID: target.Replicas[ordinal].Member,
			StoreID: target.Replicas[ordinal].Store,
			Authority: sqldriver.ReplicatedAuthorityProfile{
				ActivePolicyGeneration: operation.PolicyGeneration, ProtectionEpoch: 1,
				OwnershipEpoch: 1, SchemaGeneration: operation.Certificate.Groups[0].SchemaGeneration,
				RoutingVersion: 1, RouteGeneration: 1,
			},
		}
		database, err := newBoundRoot(filepath.Join(f.root, string(rune('a'+ordinal))), binding, true)
		if err != nil {
			return ReplicaRoot{}, err
		}
		held.database = database
		held.identity, err = bindBundle(database, binding)
		if err != nil {
			return ReplicaRoot{}, err
		}
		held.options = restoreApplyOptions()
		held.bootstrap = rf3Bootstrap()
	}
	return ReplicaRoot{
		Database: held.database, Identity: held.identity, ApplyOptions: held.options,
		Bootstrap: held.bootstrap,
		Recover: func(context.Context, replicatedstate.SnapshotArtifactManifest) (
			sqldriver.ReplicatedChildActivation, [32]byte, bool, error,
		) {
			if held.digest == ([32]byte{}) {
				return sqldriver.ReplicatedChildActivation{}, [32]byte{}, false, nil
			}
			f.recovers++
			return held.activation, held.digest, true, nil
		},
		Commit: func(_ context.Context, activation sqldriver.ReplicatedChildActivation) ([32]byte, error) {
			held.activation = activation
			held.digest = sha256.Sum256(append([]byte("replica-root"), byte(ordinal)))
			f.commits++
			return held.digest, nil
		},
	}, nil
}

func restoreSourceArtifact(t *testing.T) ([]byte, replicatedstate.SnapshotArtifactManifest, raftmember.GroupKey) {
	t.Helper()
	binding := sourceBinding()
	database, err := newBoundRoot(filepath.Join(t.TempDir(), "source"), binding, true)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	identity, err := bindBundle(database, binding)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := singletonBootstrap()
	apply, _, err := database.OpenReplicatedApply(identity, bootstrap, restoreApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer apply.Close()
	if _, err = apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	if _, err = apply.ApplyConfiguration(
		raftmodel.ApplyMeta{Index: 2, Term: 1, Type: pb.EntryConfChange}, bootstrap.Metadata.ConfState,
	); err != nil {
		t.Fatal(err)
	}
	cut, err := apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	manifest, writeErr := replicatedstate.WriteSnapshotArtifact(
		&artifact, cut, replicatedstate.SnapshotArtifactOptions{},
	)
	if err := errors.Join(writeErr, cut.Close()); err != nil {
		t.Fatal(err)
	}
	return artifact.Bytes(), manifest, raftmember.GroupKey{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		ShardIncarnation:      binding.ShardIncarnation, GroupID: binding.GroupID,
	}
}

func newBoundRoot(path string, binding sqldriver.ReplicatedShardStoreBinding, bundle bool) (*sqldriver.Database, error) {
	database, err := sqldriver.InitializeShardStore(path, sqldriver.ShardStoreBinding{
		Distribution:         distribution.DistributionName(binding.Distribution),
		Shard:                distribution.ShardID(binding.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(binding.AllocationGeneration),
	})
	if err != nil {
		return nil, err
	}
	session, err := database.NewSession(context.Background())
	if err == nil {
		statements := []string{`CREATE TABLE docs (PRIMARY KEY (id))`}
		if bundle {
			statements = append(statements, `CREATE TABLE email_claims (PRIMARY KEY (key))`)
		}
		for _, text := range statements {
			var statement *sqldriver.Prepared
			statement, err = session.Prepare(context.Background(), text)
			if err == nil {
				_, err = statement.Exec(context.Background(), nil)
			}
			if statement != nil {
				err = errors.Join(err, statement.Close())
			}
			if err != nil {
				break
			}
		}
		err = errors.Join(err, session.Close())
	}
	if err != nil {
		_ = database.Close()
		return nil, err
	}
	return database, nil
}

func bindBundle(database *sqldriver.Database, binding sqldriver.ReplicatedShardStoreBinding) (
	sqldriver.ReplicatedShardStoreIdentity, error,
) {
	return database.BindReplicatedShardStoreBundle(
		binding, "docs", []sqldriver.ReplicatedGlobalIndexRelation{{
			Relation: 2, Table: "email_claims", IndexID: 41, Incarnation: 1,
			LocatorCount: 1, Unique: true,
			KeyEncoding: sqldriver.ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			BucketBits:    distribution.DefaultVirtualBucketBits,
		}},
	)
}

func restoreApplyOptions() sqldriver.ReplicatedApplyOptions {
	return sqldriver.ReplicatedApplyOptions{
		MaxSessions: 128, RetryWindow: 8,
		TxnLimits: durable.TxnLimits{
			MaxCollections: 16, MaxDocuments: 4 * store.MaxChunkDocuments, MaxBytes: 384 << 20,
		},
		Placement: sqldriver.ReplicatedPlacementProfile{
			ShardKey: "/id", TupleVersion: distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
	}
}

func sourceBinding() sqldriver.ReplicatedShardStoreBinding {
	return sqldriver.ReplicatedShardStoreBinding{
		ClusterID: filled16(1), ClusterIncarnation: filled16(2), TopologyRecoveryEpoch: 1,
		Distribution: "restore", Shard: "source", AllocationGeneration: 1,
		ShardIncarnation: filled16(3), GroupID: filled16(4), MemberID: 1, StoreID: filled16(5),
		Authority: sqldriver.ReplicatedAuthorityProfile{
			ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1,
			SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1,
		},
	}
}

func singletonBootstrap() *pb.Snapshot { return bootstrap([]uint64{1}) }
func rf3Bootstrap() *pb.Snapshot       { return bootstrap([]uint64{1, 2, 3}) }
func bootstrap(voters []uint64) *pb.Snapshot {
	index, term := uint64(1), uint64(1)
	return &pb.Snapshot{Data: []byte("restore-test-bootstrap"), Metadata: &pb.SnapshotMetadata{
		Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: voters},
	}}
}
func filled16(value byte) (result [16]byte) {
	for i := range result {
		result[i] = value
	}
	return
}
func filledDigest(value byte) (result [32]byte) {
	for i := range result {
		result[i] = value
	}
	return
}
