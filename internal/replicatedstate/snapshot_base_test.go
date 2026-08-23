package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestSnapshotBaseCertificateDeterministicStrictAndBounded(t *testing.T) {
	if MaxSnapshotBaseCertificateBytes > raftstore.MaxSnapshotBaseBytes {
		t.Fatalf("certificate bound %d exceeds WAL base bound %d",
			MaxSnapshotBaseCertificateBytes, raftstore.MaxSnapshotBaseBytes)
	}
	source, snapshot := snapshotArtifactFixture(t)
	_, manifest := writeSnapshotArtifactFixture(t, snapshot)
	first, err := BuildSnapshotBase(manifest, source.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildSnapshotBase(manifest, source.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(first)
	secondBytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(second)
	if !bytes.Equal(firstBytes, secondBytes) || len(first.GetData()) > MaxSnapshotBaseCertificateBytes {
		t.Fatalf("certificate deterministic=%t bytes=%d/%d",
			bytes.Equal(firstBytes, secondBytes), len(first.GetData()), MaxSnapshotBaseCertificateBytes)
	}
	opened, err := OpenSnapshotBase(first)
	if err != nil || !equalSnapshotArtifactManifest(opened.Manifest, manifest) ||
		opened.Digest == ([sha256.Size]byte{}) {
		t.Fatalf("OpenSnapshotBase = %+v, %v", opened, err)
	}

	corrupt := proto.Clone(first).(*raftpb.Snapshot)
	corrupt.Data[len(corrupt.Data)/2] ^= 1
	if _, err := OpenSnapshotBase(corrupt); !errors.Is(err, ErrSnapshotBase) {
		t.Fatalf("corrupt certificate error = %v", err)
	}
	wrongMetadata := proto.Clone(first).(*raftpb.Snapshot)
	wrongIndex := wrongMetadata.GetMetadata().GetIndex() + 1
	wrongMetadata.Metadata.Index = &wrongIndex
	if _, err := OpenSnapshotBase(wrongMetadata); !errors.Is(err, ErrSnapshotBase) {
		t.Fatalf("wrong metadata error = %v", err)
	}
	truncated := proto.Clone(first).(*raftpb.Snapshot)
	truncated.Data = truncated.Data[:len(truncated.Data)-1]
	if _, err := OpenSnapshotBase(truncated); !errors.Is(err, ErrSnapshotBase) {
		t.Fatalf("truncated certificate error = %v", err)
	}
	wrongBootstrap := proto.Clone(source.bootstrap).(*raftpb.Snapshot)
	wrongBootstrap.Data = append(bytes.Clone(wrongBootstrap.Data), 'x')
	if _, err := BuildSnapshotBase(manifest, wrongBootstrap); !errors.Is(err, ErrSnapshotBase) {
		t.Fatalf("wrong bootstrap error = %v", err)
	}
	for name, mutate := range map[string]func(*SnapshotArtifactManifest){
		"encoded bytes": func(manifest *SnapshotArtifactManifest) { manifest.EncodedBytes++ },
		"image digest":  func(manifest *SnapshotArtifactManifest) { manifest.ImageDigest[0] ^= 1 },
		"footer digest": func(manifest *SnapshotArtifactManifest) { manifest.Digest[0] ^= 1 },
	} {
		t.Run("impossible "+name, func(t *testing.T) {
			invalid := cloneSnapshotArtifactManifest(manifest)
			mutate(&invalid)
			if _, err := BuildSnapshotBase(invalid, source.bootstrap); !errors.Is(err, ErrSnapshotBase) {
				t.Fatalf("BuildSnapshotBase error = %v", err)
			}
		})
	}
}

func TestCertifiedLearnerBaseCatchesUpOnlyThroughAppendEntries(t *testing.T) {
	source := newMachineFixture(t)
	if _, err := source.machine.InstallSnapshot(source.bootstrap); err != nil {
		t.Fatal(err)
	}
	conf := &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	if _, err := source.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: raftpb.EntryConfChange,
	}, conf); err != nil {
		t.Fatal(err)
	}
	open := commandValue(source.binding, 1)
	open.ClientID = id128(70)
	open.ReplicaSetVersion = 2
	applySessionOpen(t, source.machine, 3, open)
	baseCommand := snapshotBaseTestCommand(source.binding, 1, 2, []byte("base"), []byte(`{"n":1}`))
	if _, err := source.machine.ApplyNormal(raftmodel.ApplyMeta{
		Index: 4, Term: 2, Type: raftpb.EntryNormal,
	}, baseCommand); err != nil {
		t.Fatal(err)
	}
	cut, err := source.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	manifest, err := WriteSnapshotArtifact(&artifact, cut, SnapshotArtifactOptions{})
	closeErr := cut.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("source artifact = %v, close=%v", err, closeErr)
	}

	dir := t.TempDir()
	system := createTargetAt(t, dir, "system", durable.Options{})
	system = systemTargetOf(system.Collection)
	user := createTargetAt(t, dir, "user", durable.Options{})
	stage, err := NewSnapshotArtifactStage(manifest, system, user, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stage.Receive(bytes.NewReader(artifact.Bytes()), func([]byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	candidate, err := stage.OpenCandidate(source.bootstrap, log, machineOptionsFor(user))
	if err != nil {
		t.Fatal(err)
	}
	base, err := BuildSnapshotBase(manifest, source.bootstrap)
	if err != nil {
		t.Fatal(err)
	}

	identity := raftstore.Identity{
		ClusterID:          [16]byte(source.binding.ClusterID),
		ClusterIncarnation: [16]byte(source.binding.ClusterIncarnation),
		Distribution:       source.binding.Distribution, Shard: source.binding.Shard,
		AllocationGeneration: source.binding.AllocationGeneration,
		ShardIncarnation:     [16]byte(source.binding.ShardIncarnation),
		GroupID:              [16]byte(source.binding.GroupID), MemberID: 2,
		StoreID: [16]byte(id128(90)),
	}
	key := raftstore.Key{ID: "snapshot-base-test", Wrapped: []byte("wrapped")}
	for index := range key.Material {
		key.Material[index] = byte(index + 1)
	}
	wal, err := raftstore.Create(
		filepath.Join(t.TempDir(), "learner.wal"), identity, key,
		raftstore.Bootstrap{TopologyRecoveryEpoch: source.binding.TopologyRecoveryEpoch, Snapshot: base},
		raftstore.Options{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	incarnation, err := wal.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	node, err := raftmodel.NewNode(2, incarnation, wal, candidate)
	if err != nil {
		t.Fatal(err)
	}
	tailIndex, tailTerm, entryType := manifest.State.Applied+1, uint64(3), raftpb.EntryNormal
	tailCommand := snapshotBaseTestCommand(
		source.binding, 2, manifest.State.ReplicaSetVersion,
		[]byte("tail"), []byte(`{"n":2}`),
	)
	messageType := raftpb.MsgApp
	leader := uint64(1)
	message := &raftpb.Message{
		Type: &messageType, From: &leader, To: &identity.MemberID, Term: &tailTerm,
		Index: &manifest.State.Applied, LogTerm: &manifest.State.LastTerm, Commit: &tailIndex,
		Entries: []*raftpb.Entry{{
			Term: &tailTerm, Index: &tailIndex, Type: &entryType, Data: tailCommand,
		}},
	}
	if err := node.Step(message); err != nil {
		t.Fatal(err)
	}
	driveReplicatedStateNode(t, node)
	publication := candidate.Published()
	if publication.Applied != tailIndex || publication.DataChainDigest == manifest.State.DataChainDigest {
		t.Fatalf("learner publication = %+v, base=%+v", publication, manifest.State)
	}
	last, err := wal.LastIndex()
	if err != nil || last != tailIndex {
		t.Fatalf("learner WAL last = %d, %v", last, err)
	}
}

func snapshotBaseTestCommand(
	binding Binding,
	sequence uint64,
	replicaSetVersion uint64,
	key, value []byte,
) []byte {
	fingerprint := sha256.Sum256([]byte{byte(sequence), 0x55})
	command, err := replication.AppendCommand(nil, replication.Command{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion:      replicaSetVersion,
		ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch:        binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
		SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration, Tenant: []byte("tenant"),
		// The configuration entry occupies index 2 and the explicit session open
		// mints epoch 3. Callers pass the zero-based user-request ordinal.
		ClientID: id128(70), ClientEpoch: 3, ClientSequence: sequence + 1,
		Fingerprint: fingerprint, Collection: "docs",
		Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: key, Value: value}},
	})
	if err != nil {
		panic(err)
	}
	return command
}

func FuzzOpenSnapshotBase(f *testing.F) {
	source, snapshot := snapshotArtifactFixture(f)
	_, manifest := writeSnapshotArtifactFixture(f, snapshot)
	base, err := BuildSnapshotBase(manifest, source.bootstrap)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(bytes.Clone(base.GetData()))
	metadata := proto.Clone(base.GetMetadata()).(*raftpb.SnapshotMetadata)
	f.Fuzz(func(t *testing.T, data []byte) {
		input := &raftpb.Snapshot{
			Data: bytes.Clone(data), Metadata: proto.Clone(metadata).(*raftpb.SnapshotMetadata),
		}
		opened, err := OpenSnapshotBase(input)
		if err != nil {
			return
		}
		rebuilt, err := BuildSnapshotBase(opened.Manifest, opened.StaticBootstrap)
		if err != nil || !proto.Equal(rebuilt, input) {
			t.Fatalf("accepted certificate is not canonical: rebuilt=%v error=%v", proto.Equal(rebuilt, input), err)
		}
	})
}
