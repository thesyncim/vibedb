package snapshottransfer

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

type snapshotAcceptAll struct{}

func (snapshotAcceptAll) ValidatePut([]byte, []byte) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}
func (snapshotAcceptAll) ValidateDelete([]byte, []byte, bool) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

func TestRepositoryAuthenticatesRealReplicatedStateArtifact(t *testing.T) {
	dir := t.TempDir()
	open := func(name string, options durable.Options) *durable.Collection {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })
		c, err := durable.Create(f, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	systemCollection := open("system.vdb", durable.Options{OpaqueValues: true})
	userCollection := open("user.vdb", durable.Options{})
	system := replicatedstate.CollectionTarget{Collection: systemCollection, Validation: replicatedstate.ValidationOpaqueBinary,
		Limits: replicatedstate.CollectionLimits{MaxKeyBytes: systemCollection.MaxKeyBytes(), MaxDocumentBytes: systemCollection.MaxDocumentBytes(), MaxDistinctMutations: systemCollection.MaxBatchDocuments(), MaxBatchBytes: systemCollection.MaxBatchBytes()}}
	validation := sha256.Sum256([]byte("snapshot-transfer-test"))
	user := replicatedstate.CollectionTarget{Collection: userCollection, Validation: replicatedstate.ValidationDeterministicMutation, ValidationDigest: validation, Validator: snapshotAcceptAll{},
		Limits: replicatedstate.CollectionLimits{MaxKeyBytes: userCollection.MaxKeyBytes(), MaxDocumentBytes: userCollection.MaxDocumentBytes(), MaxDistinctMutations: userCollection.MaxBatchDocuments(), MaxBatchBytes: userCollection.MaxBatchBytes()}}
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	id := func(seed byte) (out replication.ID128) {
		for i := range out {
			out[i] = seed + byte(i)
		}
		return
	}
	binding := replicatedstate.Binding{ClusterID: id(1), ClusterIncarnation: id(2), TopologyRecoveryEpoch: 3, Distribution: "d", Shard: "s", AllocationGeneration: 4, ShardIncarnation: id(5), GroupID: id(6), ActivePolicyGeneration: 7, ProtectionEpoch: 8, OwnershipEpoch: 9, SchemaGeneration: 10, RoutingVersion: 11, RouteGeneration: 12}
	index, term := uint64(1), uint64(1)
	bootstrap := &pb.Snapshot{Data: []byte("bootstrap"), Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}}
	machine, err := replicatedstate.Open(binding, bootstrap, system, replicatedstate.UserCollection{Name: "docs", Target: user}, log, replicatedstate.Options{TxnLimits: durable.TxnLimits{MaxCollections: 2, MaxDocuments: user.Limits.MaxDistinctMutations + 3, MaxBytes: 64 << 20}, MaxSessions: 8, RetryWindow: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = machine.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	cut, err := machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	defer cut.Close()
	var artifact bytes.Buffer
	manifest, err := replicatedstate.WriteSnapshotArtifact(&artifact, cut, replicatedstate.SnapshotArtifactOptions{TargetChunkBytes: MinChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	payload := artifact.Bytes()
	d := Descriptor{Group: raftmember.GroupKey{ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation, TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch, ShardIncarnation: binding.ShardIncarnation, GroupID: binding.GroupID}, SourceMember: 1, TargetMember: 2, TargetStore: id(20), TargetIncarnation: 21, SchemaGeneration: binding.SchemaGeneration, ReplicaSetVersion: manifest.State.ReplicaSetVersion, SnapshotIndex: manifest.State.Applied, SnapshotTerm: manifest.State.LastTerm, Lineage: manifest.State.LastEntryDigest, ArtifactHash: sha256.Sum256(payload), ArtifactBytes: uint64(len(payload)), ChunkBytes: MinChunkBytes}
	repository, err := OpenRepository(filepath.Join(t.TempDir(), "artifacts"), Limits{MaxArtifacts: 1, MaxArtifactBytes: 1 << 20, MaxDiskBytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	appendAll(t, repository, d, payload, 0)
	if _, complete, err := repository.Offset(d); err != nil || !complete {
		t.Fatalf("certified complete=%t err=%v", complete, err)
	}
}
