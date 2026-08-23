package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestSourceCaptureAtomicallyFollowsApplyAndRecovers(t *testing.T) {
	partitioner, err := NewPartitioner(
		testSplitPlan(t, "node-b"), "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newSourceCaptureFixture(t, partitioner)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	fixture.clientEpoch = fixture.openSession(t, 2, []byte("tenant"), sourceCaptureID(20))
	capture, err := NewSourceCapture(partitioner, "split-capture", fixture.capture)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.machine.BeginTransitionCapture(capture); err != nil {
		t.Fatal(err)
	}

	cut, err := fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	options := ChildArtifactOptions{TargetChunkBytes: MinChildArtifactChunkBytes}
	options.Writers[1] = &artifact
	options.PayloadBuffers[1] = make([]byte, 0, MaxChildArtifactChunkBytes)
	var artifactWorkspace ChildArtifactWorkspace
	set, err := partitioner.WriteChildArtifacts(cut, options, &artifactWorkspace)
	closeErr := cut.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("artifact=%v close=%v", err, closeErr)
	}
	cursor, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}

	left := documentForChild(t, partitioner, 0)
	right := documentForChild(t, partitioner, 1)
	left, err = vibejson.AppendCanonicalize(nil, left)
	if err != nil {
		t.Fatal(err)
	}
	right, err = vibejson.AppendCanonicalize(nil, right)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(
		sourceCaptureMeta(3), fixture.command(2, replication.Mutation{
			Kind: replication.MutationPut, Key: []byte("row"), Value: left,
		}),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(
		sourceCaptureMeta(4), fixture.command(3, replication.Mutation{
			Kind: replication.MutationPut, Key: []byte("row"), Value: right,
		}),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(sourceCaptureMeta(5), nil); err != nil {
		t.Fatal(err)
	}
	if capture.Head() != 5 || fixture.capture.Len() != 4 {
		t.Fatalf("head=%d rows=%d", capture.Head(), fixture.capture.Len())
	}

	var readWorkspace SourceCaptureWorkspace
	entry, ok, err := capture.NextTailEntry(cursor, &readWorkspace)
	if err != nil || !ok || len(entry.Transitions) != 1 ||
		entry.Transitions[0].Before != nil ||
		!bytes.Equal(entry.Transitions[0].After, left) {
		t.Fatalf("insert entry=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor = translateCapturedEntry(t, partitioner, cursor, entry)
	entry, ok, err = capture.NextTailEntry(cursor, &readWorkspace)
	if err != nil || !ok || len(entry.Transitions) != 1 ||
		!bytes.Equal(entry.Transitions[0].Before, left) ||
		!bytes.Equal(entry.Transitions[0].After, right) {
		t.Fatalf("move entry=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor = translateCapturedEntry(t, partitioner, cursor, entry)
	entry, ok, err = capture.NextTailEntry(cursor, &readWorkspace)
	if err != nil || !ok || len(entry.Transitions) != 0 {
		t.Fatalf("empty entry=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor = translateCapturedEntry(t, partitioner, cursor, entry)
	if _, ok, err := capture.NextTailEntry(cursor, &readWorkspace); err != nil || ok {
		t.Fatalf("head read ok=%v err=%v", ok, err)
	}

	recovered, err := NewSourceCapture(partitioner, "split-capture", fixture.capture)
	if err != nil {
		t.Fatal(err)
	}
	reopenOptions := fixture.options
	reopenOptions.TransitionCapture = recovered
	reopened, err := replicatedstate.Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		replicatedstate.UserCollection{Name: "docs", Target: fixture.user},
		fixture.log, reopenOptions,
	)
	if err != nil || recovered.Head() != 5 {
		t.Fatalf("reopen=%v head=%d", err, recovered.Head())
	}
	if _, err := reopened.ApplyNormal(
		sourceCaptureMeta(6), fixture.command(4, replication.Mutation{
			Kind: replication.MutationDelete, Key: []byte("row"),
		}),
	); err != nil {
		t.Fatal(err)
	}
	entry, ok, err = recovered.NextTailEntry(cursor, &readWorkspace)
	if err != nil || !ok || len(entry.Transitions) != 1 ||
		!bytes.Equal(entry.Transitions[0].Before, right) || entry.Transitions[0].After != nil {
		t.Fatalf("delete entry=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor = translateCapturedEntry(t, partitioner, cursor, entry)
	if _, err := reopened.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 7, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	entry, ok, err = recovered.NextTailEntry(cursor, &readWorkspace)
	if err != nil || !ok || len(entry.Transitions) != 0 {
		t.Fatalf("configuration entry=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor = translateCapturedEntry(t, partitioner, cursor, entry)
	ownership, err := replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
		From: fixture.binding, ExpectedReplicaSetVersion: 7,
		SourceMember: 1, TargetMember: 2,
		ToOwnershipEpoch:  fixture.binding.OwnershipEpoch + 1,
		ToRoutingVersion:  fixture.binding.RoutingVersion + 1,
		ToRouteGeneration: fixture.binding.RouteGeneration + 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ApplyNormal(sourceCaptureMeta(8), ownership); err != nil {
		t.Fatal(err)
	}
	entry, ok, err = recovered.NextTailEntry(cursor, &readWorkspace)
	if err != nil || !ok || len(entry.Transitions) != 0 || !tailEntrySeals(entry) {
		t.Fatalf("seal entry=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor = translateCapturedEntry(t, partitioner, cursor, entry)
	if !cursor.Sealed() || recovered.Head() != 8 {
		t.Fatalf("sealed cursor=%+v head=%d", cursor, recovered.Head())
	}
}

func TestSourceCaptureRecoveryRejectsRecordCorruption(t *testing.T) {
	partitioner, err := NewPartitioner(
		testSplitPlan(t, "node-b"), "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newSourceCaptureFixture(t, partitioner)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	fixture.clientEpoch = fixture.openSession(t, 2, []byte("tenant"), sourceCaptureID(20))
	capture, _ := NewSourceCapture(partitioner, "split-capture", fixture.capture)
	if err := fixture.machine.BeginTransitionCapture(capture); err != nil {
		t.Fatal(err)
	}
	document := documentForChild(t, partitioner, 0)
	document, err = vibejson.AppendCanonicalize(nil, document)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(
		sourceCaptureMeta(3), fixture.command(2, replication.Mutation{
			Kind: replication.MutationPut, Key: []byte("row"), Value: document,
		}),
	); err != nil {
		t.Fatal(err)
	}
	var key [8]byte
	key[7] = 3
	raw, found, err := fixture.capture.AppendRaw(nil, key[:])
	if err != nil || !found {
		t.Fatal(err)
	}
	// Corrupt a fully significant base64 digit. The final digit contains
	// unused trailing bits for a 32-byte digest, so changing only those bits
	// can still decode to the original digest.
	raw[len(raw)-4] ^= 1
	if err := fixture.capture.Update(func(batch *durable.WriteBatch) error {
		return batch.Put(key[:], raw)
	}); err != nil {
		t.Fatal(err)
	}
	recovered, _ := NewSourceCapture(partitioner, "split-capture", fixture.capture)
	options := fixture.options
	options.TransitionCapture = recovered
	if _, err := replicatedstate.Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		replicatedstate.UserCollection{Name: "docs", Target: fixture.user},
		fixture.log, options,
	); !errors.Is(err, ErrSourceCapture) && !errors.Is(err, replicatedstate.ErrTransitionCapture) {
		t.Fatalf("corrupt recovery error=%v", err)
	}
}

func BenchmarkSourceCaptureNextTailEntry(b *testing.B) {
	capture, cursor, workspace, document := newSourceCaptureBenchmark(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(document)))
	b.ResetTimer()
	for range b.N {
		if _, ok, err := capture.NextTailEntry(cursor, workspace); err != nil || !ok {
			b.Fatalf("read ok=%v err=%v", ok, err)
		}
	}
}

func BenchmarkSourceCaptureLiveRead(b *testing.B) {
	capture, cursor, workspace, document := newSourceCaptureBenchmark(b)
	binary.BigEndian.PutUint64(workspace.key[:], cursor.applied+1)
	b.ReportAllocs()
	b.SetBytes(int64(len(document)))
	b.ResetTimer()
	for range b.N {
		raw, found, err := capture.target.Collection.AppendRaw(
			workspace.raw[:0], workspace.key[:],
		)
		if err != nil || !found {
			b.Fatalf("read found=%v err=%v", found, err)
		}
		workspace.raw = raw
	}
}

func BenchmarkSourceCaptureSnapshotRead(b *testing.B) {
	capture, cursor, workspace, document := newSourceCaptureBenchmark(b)
	binary.BigEndian.PutUint64(workspace.key[:], cursor.applied+1)
	var snapshot durable.Snapshot
	if err := capture.target.Collection.SnapshotInto(&snapshot); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := snapshot.Close(); err != nil {
			b.Error(err)
		}
	})
	b.ReportAllocs()
	b.SetBytes(int64(len(document)))
	b.ResetTimer()
	for range b.N {
		raw, found, err := snapshot.AppendRaw(workspace.raw[:0], workspace.key[:])
		if err != nil || !found {
			b.Fatalf("read found=%v err=%v", found, err)
		}
		workspace.raw = raw
	}
}

func BenchmarkSourceCaptureDecodeEntry(b *testing.B) {
	capture, cursor, workspace, document := newSourceCaptureBenchmark(b)
	binary.BigEndian.PutUint64(workspace.key[:], cursor.applied+1)
	raw, found, err := capture.target.Collection.AppendRaw(nil, workspace.key[:])
	if err != nil || !found {
		b.Fatalf("read found=%v err=%v", found, err)
	}
	if _, err := capture.decodeEntry(raw, workspace); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(document)))
	b.ResetTimer()
	for range b.N {
		if _, err := capture.decodeEntry(raw, workspace); err != nil {
			b.Fatal(err)
		}
	}
}

func newSourceCaptureBenchmark(
	b *testing.B,
) (*SourceCapture, TailCursor, *SourceCaptureWorkspace, []byte) {
	b.Helper()
	partitioner, err := NewPartitioner(
		testSplitPlan(b, "node-b"), "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		b.Fatal(err)
	}
	fixture := newSourceCaptureFixture(b, partitioner)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		b.Fatal(err)
	}
	fixture.clientEpoch = fixture.openSession(b, 2, []byte("tenant"), sourceCaptureID(20))
	capture, err := NewSourceCapture(partitioner, "split-capture", fixture.capture)
	if err != nil || fixture.machine.BeginTransitionCapture(capture) != nil {
		b.Fatal(err)
	}
	cut, err := fixture.machine.Snapshot("docs")
	if err != nil {
		b.Fatal(err)
	}
	var artifact bytes.Buffer
	options := ChildArtifactOptions{TargetChunkBytes: MinChildArtifactChunkBytes}
	options.Writers[1] = &artifact
	options.PayloadBuffers[1] = make([]byte, 0, MaxChildArtifactChunkBytes)
	var artifactWorkspace ChildArtifactWorkspace
	set, err := partitioner.WriteChildArtifacts(cut, options, &artifactWorkspace)
	if err != nil || cut.Close() != nil {
		b.Fatal(err)
	}
	cursor, err := partitioner.InitialTailCursor(set)
	if err != nil {
		b.Fatal(err)
	}
	document, err := vibejson.AppendCanonicalize(nil, documentForChild(b, partitioner, 1))
	if err != nil {
		b.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(
		sourceCaptureMeta(3), fixture.command(2, replication.Mutation{
			Kind: replication.MutationPut, Key: []byte("row"), Value: document,
		}),
	); err != nil {
		b.Fatal(err)
	}
	var workspace SourceCaptureWorkspace
	if _, ok, err := capture.NextTailEntry(cursor, &workspace); err != nil || !ok {
		b.Fatalf("warm read ok=%v err=%v", ok, err)
	}
	return capture, cursor, &workspace, document
}

type sourceCaptureFixture struct {
	machine            *replicatedstate.Machine
	binding            replicatedstate.Binding
	bootstrap          *pb.Snapshot
	system             replicatedstate.CollectionTarget
	user               replicatedstate.CollectionTarget
	capture            *durable.Collection
	log                *durable.TxnLog
	options            replicatedstate.Options
	clientEpoch        uint64
	retainedPruneEpoch uint64
}

const sourceCaptureRetryWindow = uint16(8)

type sourceCaptureValidator struct{}

func (sourceCaptureValidator) ValidatePut(_, _ []byte) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

func (sourceCaptureValidator) ValidateDelete(_, _ []byte, _ bool) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

func newSourceCaptureFixture(t testing.TB, partitioner *Partitioner) sourceCaptureFixture {
	t.Helper()
	dir := t.TempDir()
	create := func(name string, options durable.Options) *durable.Collection {
		file, err := os.OpenFile(
			filepath.Join(dir, name+".vdb"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if err != nil {
			t.Fatal(err)
		}
		collection, err := durable.Create(file, options)
		if err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close(); _ = file.Close() })
		return collection
	}
	systemCollection := create("system", durable.Options{OpaqueValues: true})
	userCollection := create("user", durable.Options{
		MaxDocumentBytes: 4096, MaxBatchDocuments: 4, MaxBatchBytes: 32 << 10,
	})
	captureCollection := create("capture", durable.Options{
		MaxDocumentBytes: 128 << 10, MaxBatchDocuments: 1, MaxBatchBytes: 256 << 10,
	})
	target := func(collection *durable.Collection) replicatedstate.CollectionTarget {
		return replicatedstate.CollectionTarget{
			Collection:       collection,
			Validation:       replicatedstate.ValidationDeterministicMutation,
			ValidationDigest: sha256.Sum256([]byte("range-split-source-capture-test")),
			Validator:        sourceCaptureValidator{},
			Limits: replicatedstate.CollectionLimits{
				MaxKeyBytes:          collection.MaxKeyBytes(),
				MaxDocumentBytes:     collection.MaxDocumentBytes(),
				MaxDistinctMutations: collection.MaxBatchDocuments(),
				MaxBatchBytes:        collection.MaxBatchBytes(),
			},
		}
	}
	system := target(systemCollection)
	system.Validation = replicatedstate.ValidationOpaqueBinary
	system.ValidationDigest = [32]byte{}
	system.Validator = nil
	user := target(userCollection)
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	binding := replicatedstate.Binding{
		ClusterID: sourceCaptureID(1), ClusterIncarnation: sourceCaptureID(2),
		TopologyRecoveryEpoch: 3,
		Distribution:          string(partitioner.source.Distribution), Shard: string(partitioner.source.Shard),
		AllocationGeneration: uint64(partitioner.source.AllocationGeneration),
		ShardIncarnation:     sourceCaptureID(4), GroupID: sourceCaptureID(5),
		ActivePolicyGeneration: 6, ProtectionEpoch: 7,
		OwnershipEpoch: uint64(partitioner.source.OwnershipEpoch), SchemaGeneration: 8,
		RoutingVersion: uint64(partitioner.source.RoutingVersion), RouteGeneration: 19,
	}
	index, term := uint64(1), uint64(1)
	bootstrap := &pb.Snapshot{
		Data: []byte("source-capture-bootstrap"),
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1}},
		},
	}
	options := replicatedstate.Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: 3,
			MaxDocuments: max(
				user.Limits.MaxDistinctMutations+4,
				int(sourceCaptureRetryWindow)+3,
			),
			MaxBytes: 64 << 20,
		},
		MaxSessions: 128,
		RetryWindow: sourceCaptureRetryWindow,
	}
	machine, err := replicatedstate.Open(
		binding, bootstrap, system,
		replicatedstate.UserCollection{Name: "docs", Target: user}, log, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	return sourceCaptureFixture{
		machine: machine, binding: binding, bootstrap: bootstrap,
		system: system, user: user, capture: captureCollection, log: log, options: options,
	}
}

func (f sourceCaptureFixture) command(
	sequence uint64,
	mutations ...replication.Mutation,
) []byte {
	fingerprint := sha256.Sum256([]byte{byte(sequence), 0x73})
	encoded, err := replication.AppendCommand(nil, replication.Command{
		ClusterID: f.binding.ClusterID, ClusterIncarnation: f.binding.ClusterIncarnation,
		TopologyRecoveryEpoch: f.binding.TopologyRecoveryEpoch,
		Distribution:          f.binding.Distribution, Shard: f.binding.Shard,
		AllocationGeneration: f.binding.AllocationGeneration,
		ShardIncarnation:     f.binding.ShardIncarnation, GroupID: f.binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: f.binding.ActivePolicyGeneration,
		ProtectionEpoch: f.binding.ProtectionEpoch, OwnershipEpoch: f.binding.OwnershipEpoch,
		SchemaGeneration: f.binding.SchemaGeneration, RoutingVersion: f.binding.RoutingVersion,
		RouteGeneration: f.binding.RouteGeneration, Tenant: []byte("tenant"),
		ClientID: sourceCaptureID(20), ClientEpoch: f.clientEpoch, ClientSequence: sequence,
		Fingerprint: fingerprint, Collection: "docs", Mutations: mutations,
	})
	if err != nil {
		panic(err)
	}
	return encoded
}

func (f sourceCaptureFixture) openSession(
	t testing.TB,
	index uint64,
	tenant []byte,
	clientID replication.ID128,
) uint64 {
	t.Helper()
	seed := make([]byte, 0, len("rangesplit/test-session-open/")+len(tenant)+len(clientID))
	seed = append(seed, "rangesplit/test-session-open/"...)
	seed = append(seed, tenant...)
	seed = append(seed, clientID[:]...)
	encoded, err := replication.AppendCommand(nil, replication.Command{
		Kind:                   replication.CommandSessionOpen,
		ClusterID:              f.binding.ClusterID,
		ClusterIncarnation:     f.binding.ClusterIncarnation,
		TopologyRecoveryEpoch:  f.binding.TopologyRecoveryEpoch,
		Distribution:           f.binding.Distribution,
		Shard:                  f.binding.Shard,
		AllocationGeneration:   f.binding.AllocationGeneration,
		ShardIncarnation:       f.binding.ShardIncarnation,
		GroupID:                f.binding.GroupID,
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: f.binding.ActivePolicyGeneration,
		ProtectionEpoch:        f.binding.ProtectionEpoch,
		OwnershipEpoch:         f.binding.OwnershipEpoch,
		SchemaGeneration:       f.binding.SchemaGeneration,
		RoutingVersion:         f.binding.RoutingVersion,
		RouteGeneration:        f.binding.RouteGeneration,
		Tenant:                 tenant,
		ClientID:               clientID,
		ClientSequence:         1,
		NextDeadlineUnixNano:   2_000_000_000_000_000_000,
		Fingerprint:            sha256.Sum256(seed),
		Collection:             "docs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.machine.AdmitCommand(encoded); err != nil {
		t.Fatalf("admit session open at %d: %v", index, err)
	}
	publication, err := f.machine.ApplyNormal(sourceCaptureMeta(index), encoded)
	if err != nil || publication.Applied != index {
		t.Fatalf("apply session open at %d = %+v, %v", index, publication, err)
	}
	lookup, err := f.machine.LookupCompletion(encoded)
	if err != nil {
		t.Fatalf("lookup session open at %d: %v", index, err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != replicatedstate.ResultSessionOpened ||
		completion.ClientEpoch != index || completion.ClientSequence != 1 ||
		completion.AppliedSequence != index {
		t.Fatalf("session open completion at %d = %+v, %v", index, completion, err)
	}
	return completion.ClientEpoch
}

func sourceCaptureID(seed byte) replication.ID128 {
	var id replication.ID128
	for index := range id {
		id[index] = seed + byte(index)
	}
	return id
}

func sourceCaptureMeta(index uint64) raftmodel.ApplyMeta {
	return raftmodel.ApplyMeta{Index: index, Term: 2, Type: pb.EntryNormal}
}

func translateCapturedEntry(
	t testing.TB,
	partitioner *Partitioner,
	cursor TailCursor,
	entry TailEntry,
) TailCursor {
	t.Helper()
	sinks := []TailSink{consumeTailBatch, consumeTailBatch}
	var workspace TailWorkspace
	next, _, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	return next
}
