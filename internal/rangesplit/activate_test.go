package rangesplit

import (
	"bytes"
	"crypto/sha256"
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
	"google.golang.org/protobuf/proto"
)

func TestInitializeReplicatedChildBuildsNoCopyRaftBase(t *testing.T) {
	partitioner, err := NewPartitioner(
		testSplitPlan(t, "node-b"), "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	source := newSourceCaptureFixture(t, partitioner)
	if _, err := source.machine.InstallSnapshot(source.bootstrap); err != nil {
		t.Fatal(err)
	}
	source.clientEpoch = source.openSession(t, 2, []byte("tenant"), sourceCaptureID(20))
	right, err := vibejson.AppendCanonicalize(nil, documentForChild(t, partitioner, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.machine.ApplyNormal(sourceCaptureMeta(3), source.command(2,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("right"), Value: right},
	)); err != nil {
		t.Fatal(err)
	}
	capture, err := NewSourceCapture(partitioner, "split-capture", source.capture)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.machine.BeginTransitionCapture(capture); err != nil {
		t.Fatal(err)
	}
	cut, err := source.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	artifactOptions := ChildArtifactOptions{TargetChunkBytes: MinChildArtifactChunkBytes}
	artifactOptions.Writers[1] = &artifact
	artifactOptions.PayloadBuffers[1] = make([]byte, 0, MaxChildArtifactChunkBytes)
	var artifactWorkspace ChildArtifactWorkspace
	set, err := partitioner.WriteChildArtifacts(cut, artifactOptions, &artifactWorkspace)
	closeErr := cut.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("artifact=%v close=%v", err, closeErr)
	}
	tail, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}

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
	userCollection := create("child-user", durable.Options{
		MaxDocumentBytes: 4096, MaxBatchDocuments: 4, MaxBatchBytes: 32 << 10,
	})
	systemCollection := create("child-system", durable.Options{OpaqueValues: true})
	captureCollection := create(replicatedstate.TransitionCaptureCollectionName, durable.Options{
		OpaqueValues: true, MaxKeyBytes: 8,
		MaxDocumentBytes:  replicatedstate.MaxTransitionCaptureRecordBytes,
		MaxBatchDocuments: 1,
		MaxBatchBytes:     replicatedstate.MaxTransitionCaptureRecordBytes + 8,
	})
	stage, err := NewChildStage(partitioner, set.Children[1], userCollection, nil)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	persist := func(raw []byte) error {
		persisted = append(persisted[:0], raw...)
		return nil
	}
	if _, err := stage.ReceiveArtifact(bytes.NewReader(artifact.Bytes()), persist); err != nil {
		t.Fatal(err)
	}
	if _, err := source.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 4, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	var captureWorkspace SourceCaptureWorkspace
	entry, ok, err := capture.NextTailEntry(tail, &captureWorkspace)
	if err != nil || !ok {
		t.Fatalf("configuration capture ok=%v err=%v", ok, err)
	}
	sinks := []TailSink{
		func(TailBatch) error { return nil },
		func(batch TailBatch) error { return stage.ApplyTailBatch(batch, persist) },
	}
	var tailWorkspace TailWorkspace
	tail, _, err = partitioner.TranslateTailEntry(tail, entry, sinks, &tailWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
		From: source.binding, ExpectedReplicaSetVersion: 4,
		SourceMember: 1, TargetMember: 2,
		ToOwnershipEpoch:  source.binding.OwnershipEpoch + 1,
		ToRoutingVersion:  source.binding.RoutingVersion + 1,
		ToRouteGeneration: source.binding.RouteGeneration + 1,
		ToOwnedRange:      partitioner.children[partitioner.retained].Range,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.machine.ApplyNormal(sourceCaptureMeta(5), ownership); err != nil {
		t.Fatal(err)
	}
	entry, ok, err = capture.NextTailEntry(tail, &captureWorkspace)
	if err != nil || !ok {
		t.Fatalf("seal capture ok=%v err=%v", ok, err)
	}
	tail, _, err = partitioner.TranslateTailEntry(tail, entry, sinks, &tailWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	stageCursor, ok := stage.Cursor()
	if !ok {
		t.Fatal("missing sealed stage cursor")
	}
	var cutoverWorkspace CutoverWorkspace
	certificate, err := partitioner.CertifyCutover(
		capture, tail, []ChildStageCursor{stageCursor}, &cutoverWorkspace,
	)
	if err != nil {
		t.Fatal(err)
	}

	targetOf := func(
		collection *durable.Collection,
		validation replicatedstate.ValidationProfile,
	) replicatedstate.CollectionTarget {
		target := replicatedstate.CollectionTarget{
			Collection: collection, Validation: validation,
			Limits: replicatedstate.CollectionLimits{
				MaxKeyBytes: collection.MaxKeyBytes(), MaxDocumentBytes: collection.MaxDocumentBytes(),
				MaxDistinctMutations: collection.MaxBatchDocuments(), MaxBatchBytes: collection.MaxBatchBytes(),
			},
		}
		if validation == replicatedstate.ValidationDeterministicMutation {
			target.ValidationDigest = sha256.Sum256([]byte("range-split-activated-child"))
			target.Validator = sourceCaptureValidator{}
		}
		return target
	}
	user := targetOf(userCollection, replicatedstate.ValidationDeterministicMutation)
	system := targetOf(systemCollection, replicatedstate.ValidationOpaqueBinary)
	txnLog, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = txnLog.Close() })
	child := partitioner.children[1]
	binding := source.binding
	binding.Shard = string(child.Shard)
	binding.AllocationGeneration = uint64(child.AllocationGeneration)
	binding.ShardIncarnation = sourceCaptureID(60)
	binding.GroupID = sourceCaptureID(80)
	binding.OwnershipEpoch = uint64(child.OwnershipEpoch)
	binding.RoutingVersion = uint64(partitioner.target)
	binding.RouteGeneration = certificate.SourceCoordinates().RouteGeneration
	binding.OwnedRange = child.Range
	index, term := uint64(1), uint64(1)
	bootstrap := &pb.Snapshot{
		Data: []byte("range-split-child-bootstrap"),
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{7}},
		},
	}
	maxDocuments, err := replicatedstate.RequiredBundleTransactionDocuments(
		user.Limits.MaxDistinctMutations,
		sourceCaptureRetryWindow,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := int(sourceCaptureRetryWindow) + 3; maxDocuments != want {
		t.Fatalf("activation transaction documents = %d, want %d", maxDocuments, want)
	}
	target := ChildActivationTarget{
		Binding: binding, StaticBootstrap: bootstrap, System: system,
		User:   replicatedstate.UserCollection{Name: "docs", Target: user},
		TxnLog: txnLog,
		MachineOptions: replicatedstate.Options{
			TxnLimits: durable.TxnLimits{
				MaxCollections: 3,
				MaxDocuments:   maxDocuments,
				MaxBytes:       512 << 20,
			},
			MaxSessions: 128,
			RetryWindow: sourceCaptureRetryWindow,
			TransitionCaptureTarget: replicatedstate.TransitionCaptureTarget{
				Name:       replicatedstate.TransitionCaptureCollectionName,
				Collection: captureCollection,
			},
		},
	}
	if err := stage.CheckActivationCoordinates(certificate, binding); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1_000, func() {
		if err := stage.CheckActivationCoordinates(certificate, binding); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("activation coordinate check allocations = %v, want 0", allocs)
	}
	afterSeal := documentForChild(t, partitioner, 1)
	if _, err := userCollection.Put([]byte("valid-after-seal"), afterSeal); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := stage.InitializeReplicatedChild(
		certificate, target,
	); !errors.Is(err, ErrChildStage) || systemCollection.Len() != 0 {
		t.Fatalf("post-seal insert activation err=%v systemRows=%d", err, systemCollection.Len())
	}
	if deleted, err := userCollection.Delete([]byte("valid-after-seal")); err != nil || !deleted {
		t.Fatalf("restore post-seal insert deleted=%v err=%v", deleted, err)
	}
	replacement := append([]byte(nil), right[:len(right)-1]...)
	replacement = append(replacement, []byte(`,"payload":"changed"}`)...)
	if created, err := userCollection.Put([]byte("right"), replacement); err != nil || created {
		t.Fatalf("post-seal replacement created=%v err=%v", created, err)
	}
	if _, _, _, err := stage.InitializeReplicatedChild(
		certificate, target,
	); !errors.Is(err, ErrChildStage) || systemCollection.Len() != 0 {
		t.Fatalf("post-seal replacement activation err=%v systemRows=%d", err, systemCollection.Len())
	}
	if created, err := userCollection.Put([]byte("right"), right); err != nil || created {
		t.Fatalf("restore post-seal replacement created=%v err=%v", created, err)
	}
	// Exact-generation fencing intentionally remains poisoned after a bytewise
	// restore. Crash recovery performs the one cold O(rows) audit and remints a
	// durable image identity; activation itself must not scan.
	beforeReopenScans := userCollection.Stats().SnapshotFullScanCalls
	stage, err = NewChildStage(partitioner, set.Children[1], userCollection, persisted)
	if err != nil {
		t.Fatalf("reopen sealed stage: %v", err)
	}
	afterReopenScans := userCollection.Stats().SnapshotFullScanCalls
	if afterReopenScans != beforeReopenScans+1 {
		t.Fatalf("reopen scans=%d, want exactly one", afterReopenScans-beforeReopenScans)
	}
	wrong := target
	wrong.Binding.AllocationGeneration++
	if _, _, _, err := stage.InitializeReplicatedChild(
		certificate, wrong,
	); !errors.Is(err, ErrChildStage) || systemCollection.Len() != 0 {
		t.Fatalf("wrong target err=%v systemRows=%d", err, systemCollection.Len())
	}
	beforeActivationScans := userCollection.Stats().SnapshotFullScanCalls
	machine, base, manifest, err := stage.InitializeReplicatedChild(certificate, target)
	if err != nil {
		t.Fatal(err)
	}
	retryMachine, retryBase, retryManifest, err := stage.InitializeReplicatedChild(certificate, target)
	if err != nil || retryMachine.Published().DataChainDigest != machine.Published().DataChainDigest ||
		retryManifest.Digest != manifest.Digest || !proto.Equal(base, retryBase) {
		t.Fatalf("retry manifest=%x baseEqual=%v err=%v", retryManifest.Digest, proto.Equal(base, retryBase), err)
	}
	if scans := userCollection.Stats().SnapshotFullScanCalls - beforeActivationScans; scans != 0 {
		t.Fatalf("activation scanned child image %d times", scans)
	}
	opened, err := replicatedstate.OpenSnapshotBase(base)
	if err != nil || opened.Manifest.State.Binding != binding || manifest.UserRows != 1 ||
		manifest.CaptureRows != 0 || manifest.CaptureImageDigest == ([sha256.Size]byte{}) {
		t.Fatalf("base state=%+v rows=%d err=%v", opened.Manifest.State, manifest.UserRows, err)
	}
	before, found, err := userCollection.AppendRaw(nil, []byte("right"))
	if err != nil || !found || !bytes.Equal(before, right) {
		t.Fatalf("staged row=%q found=%v err=%v", before, found, err)
	}
	if _, err := machine.InstallSnapshot(base); err != nil {
		t.Fatal(err)
	}
	beforeMachineReopenScans := userCollection.Stats().SnapshotFullScanCalls
	reopened, err := replicatedstate.Open(
		binding, bootstrap, system,
		replicatedstate.UserCollection{Name: "docs", Target: user},
		txnLog, target.MachineOptions,
	)
	if err != nil || reopened.Published().DataChainDigest != machine.Published().DataChainDigest {
		t.Fatalf("reopen activated child publication=%+v err=%v", reopened, err)
	}
	if scans := userCollection.Stats().SnapshotFullScanCalls - beforeMachineReopenScans; scans != 1 {
		t.Fatalf("activated child reopen scans=%d, want one validation pass", scans)
	}
}
