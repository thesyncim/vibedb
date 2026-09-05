package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

type rf3SnapshotFenceFixture struct {
	fence replicatedstate.SnapshotFence
	err   error
}

func (source *rf3SnapshotFenceFixture) SnapshotAuthorizationFence() (replicatedstate.SnapshotFence, error) {
	return source.fence, source.err
}

func TestRF3SnapshotDataAuthorizationUsesCurrentDurableFenceBeforeArtifactRead(t *testing.T) {
	group := rf3CommandGroup()
	identity := raftmember.RuntimeIdentity{Group: group, MemberID: 1}
	target := rf3ManifestEnrolledTarget{MemberID: 4, StoreID: [16]byte{7}, NodeIncarnation: 1}
	source := &rf3SnapshotFenceFixture{fence: replicatedstate.SnapshotFence{
		Binding: replicatedstate.Binding{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
			TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, ShardIncarnation: group.ShardIncarnation,
			GroupID: group.GroupID, SchemaGeneration: 1},
		ReplicaSetVersion: 1, Applied: 1, RelationManifestDigest: [32]byte{9},
	}}
	authorize := rf3SnapshotDataAuthorizer(source, identity, target)
	payload, artifactFence := rf3SnapshotAuthorizationArtifact(t, group)
	descriptor := snapshottransfer.Descriptor{Group: group, SourceMember: 1, TargetMember: target.MemberID,
		TargetStore: target.StoreID, TargetIncarnation: target.NodeIncarnation, SchemaGeneration: 1,
		ReplicaSetVersion: 2, SnapshotIndex: artifactFence.Applied, SnapshotTerm: artifactFence.LastTerm, Lineage: artifactFence.LastEntryDigest,
		ArtifactHash: sha256.Sum256(payload), ArtifactBytes: uint64(len(payload)), ChunkBytes: snapshottransfer.MinChunkBytes}
	if authorize(descriptor) {
		t.Fatal("future learner generation passed before durable membership apply")
	}
	// The authorizer was built at generation one, but the source has now
	// durably applied AddLearner. No restart or new closure may be necessary.
	source.fence.ReplicaSetVersion, source.fence.Applied = 2, 2
	if !authorize(descriptor) {
		t.Fatal("current learner generation was compared against startup metadata")
	}
	nodes := [2]rafttransport.NodeID{{1}, {4}}
	registry, err := rafttransport.NewStaticRegistry(nodes[0], []rafttransport.Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: nodes[0], Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 4, Node: nodes[1], Role: rafttransport.MemberEnrolled},
	}, rafttransport.Limits{MaxGroups: 1, MaxMembers: 2})
	if err != nil {
		t.Fatal(err)
	}
	repositoryPath := filepath.Join(t.TempDir(), "artifacts")
	repository, err := snapshottransfer.OpenRepository(repositoryPath, snapshottransfer.Limits{
		MaxArtifacts: 1, MaxArtifactBytes: uint64(len(payload)), MaxDiskBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	for offset := 0; offset < len(payload); {
		end := min(offset+int(descriptor.ChunkBytes), len(payload))
		chunk := payload[offset:end]
		if next, complete, err := repository.Append(descriptor, uint64(offset), chunk, sha256.Sum256(chunk)); err != nil || next != uint64(end) || complete != (end == len(payload)) {
			t.Fatalf("publish artifact: next=%d complete=%v err=%v", next, complete, err)
		}
		offset = end
	}
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	service, err := snapshottransfer.NewService(snapshottransfer.ServiceOptions{
		Repository: repository, Registry: registry, Authorize: authorize,
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConnections: 1,
		MaxChunkBytes: snapshottransfer.MinChunkBytes, MaxInflightBytes: snapshottransfer.MinChunkBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := func(d snapshottransfer.Descriptor) ([]byte, error) {
		discriminator := snapshottransfer.RequestDiscriminator()
		raw, err := snapshottransfer.AppendDescriptor(discriminator[:], d)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, make([]byte, 8)...)
		connection := &rf3SnapshotBufferConnection{input: bytes.NewReader(raw),
			peer: rafttransport.PeerIdentity{TrustDomain: registry.TrustDomain(), Node: nodes[1]}}
		err = service.Serve(context.Background(), connection)
		return connection.output.Bytes(), err
	}
	response, err := request(descriptor)
	if err != nil || !bytes.HasSuffix(response, payload[:min(len(payload), int(descriptor.ChunkBytes))]) {
		t.Fatalf("current learner artifact: bytes=%d err=%v", len(response), err)
	}
	if err := os.Truncate(filepath.Join(repositoryPath, "p-"+hex.EncodeToString(descriptor.ArtifactHash[:])), snapshottransfer.DescriptorBytes); err != nil {
		t.Fatal(err)
	}
	// Truncating this test artifact makes an attempted payload read return EOF.
	// Rejected requests must return the authorization error before that read.
	assertRejected := func(d snapshottransfer.Descriptor) {
		t.Helper()
		response, err := request(d)
		if len(response) != 0 || !errors.Is(err, snapshottransfer.ErrStaleFence) {
			t.Fatalf("rejected request reached artifact storage: bytes=%d err=%v", len(response), err)
		}
	}
	for _, mutate := range []func(*snapshottransfer.Descriptor){
		func(d *snapshottransfer.Descriptor) { d.ReplicaSetVersion = 1 },
		func(d *snapshottransfer.Descriptor) { d.SchemaGeneration++ },
		func(d *snapshottransfer.Descriptor) { d.TargetStore[0]++ },
		func(d *snapshottransfer.Descriptor) { d.TargetIncarnation++ },
		func(d *snapshottransfer.Descriptor) { d.Group.GroupID[0]++ },
	} {
		forged := descriptor
		mutate(&forged)
		assertRejected(forged)
	}
	source.fence.ReplicaSetVersion++
	assertRejected(descriptor)
	source.fence.ReplicaSetVersion--
	source.fence.Binding.SchemaGeneration++
	assertRejected(descriptor)
	source.fence.Binding.SchemaGeneration--
	source.fence.Binding.GroupID[0]++
	assertRejected(descriptor)
	source.fence.Binding.GroupID[0]--
	source.err = errors.New("source metadata unavailable")
	assertRejected(descriptor)
	source.err = nil
	if _, err := request(descriptor); !errors.Is(err, io.EOF) {
		t.Fatalf("positive control did not read truncated artifact: %v", err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if !authorize(descriptor) {
			panic("current source rejected")
		}
	}); allocs != 0 {
		t.Fatalf("per-chunk authorizer allocations: %v", allocs)
	}
}

func rf3SnapshotAuthorizationArtifact(t *testing.T, group raftmember.GroupKey) ([]byte, replicatedstate.SnapshotFence) {
	t.Helper()
	root := t.TempDir()
	open := func(name string, opaque bool) replicatedstate.CollectionTarget {
		file, err := os.OpenFile(filepath.Join(root, name), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		collection, err := durable.Create(file, durable.Options{OpaqueValues: opaque})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		target := replicatedstate.CollectionTarget{Collection: collection, Validation: replicatedstate.ValidationOpaqueBinary,
			Limits: replicatedstate.CollectionLimits{MaxKeyBytes: collection.MaxKeyBytes(), MaxDocumentBytes: collection.MaxDocumentBytes(),
				MaxDistinctMutations: collection.MaxBatchDocuments(), MaxBatchBytes: collection.MaxBatchBytes()}}
		if !opaque {
			target.Validation = replicatedstate.ValidationDeterministicMutation
			target.ValidationDigest = sha256.Sum256([]byte("snapshot-authorization-fixture"))
			target.Validator = rf3SnapshotFixtureValidator{}
		}
		return target
	}
	system, user := open("system", true), open("user", false)
	log, err := durable.NewTxnLog(root, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	binding := replicatedstate.Binding{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, ShardIncarnation: group.ShardIncarnation, GroupID: group.GroupID,
		Distribution: "snapshot", Shard: "all", AllocationGeneration: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
		OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1,
		OwnedRange: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}
	index, term := uint64(1), uint64(1)
	bootstrap := &pb.Snapshot{Data: []byte("snapshot-authorization"), Metadata: &pb.SnapshotMetadata{
		Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1}}}}
	machine, err := replicatedstate.Open(binding, bootstrap, system, replicatedstate.UserCollection{Name: "docs", Target: user}, log,
		replicatedstate.Options{TxnLimits: durable.TxnLimits{MaxCollections: 2, MaxDocuments: user.Limits.MaxDistinctMutations + 4, MaxBytes: 64 << 20}, MaxSessions: 8, RetryWindow: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := machine.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.ApplyConfiguration(raftmodel.ApplyMeta{Index: 2, Term: 1, Type: pb.EntryConfChange},
		&pb.ConfState{Voters: []uint64{1}, Learners: []uint64{4}}); err != nil {
		t.Fatal(err)
	}
	cut, err := machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer cut.Close()
	var artifact bytes.Buffer
	if _, err := replicatedstate.WriteSnapshotArtifact(&artifact, cut, replicatedstate.SnapshotArtifactOptions{}); err != nil {
		t.Fatal(err)
	}
	return artifact.Bytes(), cut.Fence()
}

type rf3SnapshotFixtureValidator struct{}

func (rf3SnapshotFixtureValidator) ValidatePut(_, _ []byte) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}
func (rf3SnapshotFixtureValidator) ValidateDelete(_, _ []byte, _ bool) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

type rf3SnapshotBufferConnection struct {
	net.Conn
	input  *bytes.Reader
	output bytes.Buffer
	peer   rafttransport.PeerIdentity
}

func (c *rf3SnapshotBufferConnection) Read(p []byte) (int, error)               { return c.input.Read(p) }
func (c *rf3SnapshotBufferConnection) Write(p []byte) (int, error)              { return c.output.Write(p) }
func (*rf3SnapshotBufferConnection) Close() error                               { return nil }
func (*rf3SnapshotBufferConnection) SetReadDeadline(time.Time) error            { return nil }
func (*rf3SnapshotBufferConnection) SetWriteDeadline(time.Time) error           { return nil }
func (c *rf3SnapshotBufferConnection) PeerIdentity() rafttransport.PeerIdentity { return c.peer }
func (*rf3SnapshotBufferConnection) PeerKeyDigest() [32]byte                    { return [32]byte{} }
func (*rf3SnapshotBufferConnection) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficSnapshot
}
