package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestRetainedPrunerResumesAcrossBothApplyCrashWindows(t *testing.T) {
	partitioner, fixture, capture, certificate := newRetainedPruneFixture(t)
	pruner, err := newTestRetainedPruner(t, partitioner, certificate, nil)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	persist := func(raw []byte) error {
		persisted = append(persisted[:0], raw...)
		return nil
	}
	limits := RetainedPruneLimits{MaxKeys: 1, MaxKeyBytes: 128, MaxScanRows: 2}
	var workspace RetainedPruneWorkspace
	snapshot, err := fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	persistFailure := errors.New("cursor store unavailable")
	if _, has, advanceErr := pruner.Advance(
		snapshot, capture, limits,
		func([]byte) error { return persistFailure }, &workspace,
	); !errors.Is(advanceErr, ErrRetainedPruneOutcomeUnknown) ||
		!errors.Is(advanceErr, persistFailure) || has ||
		pruner.Cursor().Phase() != RetainedPruneScan {
		t.Fatalf("persist failure has=%v err=%v phase=%v", has, advanceErr, pruner.Cursor().Phase())
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot, err = fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	batch, hasBatch, err := pruner.Advance(snapshot, capture, limits, persist, &workspace)
	if closeErr := snapshot.Close(); err != nil || closeErr != nil || !hasBatch || batch.Count != 1 {
		t.Fatalf("plan batch=%+v has=%v err=%v close=%v", batch, hasBatch, err, closeErr)
	}
	firstKey := bytes.Clone(firstRetainedPruneKey(batch))

	// Crash before proposal: the persisted awaiting cursor deterministically
	// replans the same exact batch from the unchanged source publication.
	pruner, err = newTestRetainedPruner(t, partitioner, certificate, persisted)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	retry, hasBatch, err := pruner.Advance(snapshot, capture, limits, persist, &workspace)
	if closeErr := snapshot.Close(); err != nil || closeErr != nil || !hasBatch ||
		retry.Digest != batch.Digest || !bytes.Equal(firstRetainedPruneKey(retry), firstKey) {
		t.Fatalf("retry batch=%+v has=%v err=%v close=%v", retry, hasBatch, err, closeErr)
	}

	binding := fixture.binding
	binding.OwnershipEpoch++
	binding.RoutingVersion++
	binding.RouteGeneration++
	// The prune controller owns an independent bounded session opened before
	// capture. Sequence one is its Open, so pruning starts at sequence two.
	sequence, applied := uint64(2), uint64(7)
	if _, err := fixture.machine.ApplyNormal(
		sourceCaptureMeta(applied), retainedPruneCommand(t, fixture, binding, 5, sequence, retry),
	); err != nil {
		t.Fatal(err)
	}

	// Crash after apply but before cursor confirmation: recovery consumes and
	// verifies the already durable capture entry, then advances the scan floor.
	pruner, err = newTestRetainedPruner(t, partitioner, certificate, persisted)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	if got, has, advanceErr := pruner.Advance(
		snapshot, capture, limits, persist, &workspace,
	); advanceErr != nil || has || got.Count != 0 {
		t.Fatalf("confirm batch=%+v has=%v err=%v", got, has, advanceErr)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}

	for pruner.Cursor().Phase() != RetainedPruneComplete {
		snapshot, err = fixture.machine.Snapshot("docs")
		if err != nil {
			t.Fatal(err)
		}
		batch, hasBatch, advanceErr := pruner.Advance(
			snapshot, capture, limits, persist, &workspace,
		)
		closeErr := snapshot.Close()
		if advanceErr != nil || closeErr != nil {
			t.Fatalf("advance=%v close=%v", advanceErr, closeErr)
		}
		if !hasBatch {
			continue
		}
		sequence++
		applied++
		if _, err := fixture.machine.ApplyNormal(
			sourceCaptureMeta(applied),
			retainedPruneCommand(t, fixture, binding, 5, sequence, batch),
		); err != nil {
			t.Fatal(err)
		}
	}
	cursor := pruner.Cursor()
	rows, _, generation, digest, ok := cursor.RetainedProof()
	if !ok || rows != 2 || generation == 0 || digest == ([sha256.Size]byte{}) ||
		fixture.user.Collection.Len() != 2 {
		t.Fatalf("cursor=%+v rows=%d generation=%d stored=%d", cursor, rows, generation, fixture.user.Collection.Len())
	}
	if reopened, err := newTestRetainedPruner(t, partitioner, certificate, persisted); err != nil ||
		reopened.Cursor().Phase() != RetainedPruneComplete {
		t.Fatalf("reopen=%v err=%v", reopened, err)
	}
	// A retained write committed immediately after terminal verification cannot
	// strand completion. One bounded turn authenticates and absorbs its capture
	// entry, and the advanced complete cursor remains restartable.
	sequence++
	applied++
	retained, err := vibejson.AppendCanonicalize(nil, documentForChild(t, partitioner, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(sourceCaptureMeta(applied), retainedMutationCommand(
		t, fixture, binding, 5, sequence,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("late-retained"), Value: retained},
	)); err != nil {
		t.Fatal(err)
	}
	snapshot, err = fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, has, advanceErr := pruner.Advance(snapshot, capture, limits, persist, &workspace); advanceErr != nil || has {
		t.Fatalf("late retained completion advance has=%v err=%v", has, advanceErr)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	advanced := pruner.Cursor()
	advancedRows, _, _, advancedDigest, proofOK := advanced.RetainedProof()
	if advanced.Phase() != RetainedPruneComplete || advanced.SourceCut().Applied != applied ||
		!proofOK || advancedRows != rows+1 || advancedDigest == cursor.retainedDigest {
		t.Fatalf("advanced complete cursor=%+v", advanced)
	}
	if reopened, err := newTestRetainedPruner(t, partitioner, certificate, persisted); err != nil ||
		reopened.Cursor().SourceCut() != advanced.SourceCut() {
		t.Fatalf("reopen late complete=%v err=%v", reopened, err)
	}
	opened, err := OpenRetainedPruneCursor(persisted)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := AppendRetainedPruneCursor(nil, opened)
	if err != nil || !bytes.Equal(reencoded, persisted) {
		t.Fatalf("cursor canonical replay equal=%t err=%v", bytes.Equal(reencoded, persisted), err)
	}
	if err := partitioner.VerifyRetainedPruneCompletion(
		certificate, cursor.OperationID(), cursor,
	); err != nil {
		t.Fatal(err)
	}
	wrongOperation := cursor.OperationID()
	wrongOperation[0] ^= 0xff
	if err := partitioner.VerifyRetainedPruneCompletion(
		certificate, wrongOperation, cursor,
	); !errors.Is(err, ErrRetainedPrune) {
		t.Fatalf("cross-operation completion err=%v", err)
	}
	currentManifest, err := distribution.NewManifest("orders", 11, []distribution.Shard{{
		ID: "source", AllocationGeneration: 7,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"node-a"}, Epoch: 5,
	}})
	if err != nil {
		t.Fatal(err)
	}
	nextManifest := testSplitPlan(t, "node-b").Manifest()
	if err := partitioner.ValidatePublicationTransition(
		currentManifest, nextManifest, 19, 20, certificate,
	); err != nil {
		t.Fatal(err)
	}
	if err := partitioner.ValidatePublicationTransition(
		currentManifest, nextManifest, 19, 21, certificate,
	); !errors.Is(err, ErrManifestTransition) {
		t.Fatalf("skipped catalog generation err=%v", err)
	}
	if err := partitioner.ValidatePublicationTransition(
		currentManifest, nextManifest, 18, 20, certificate,
	); !errors.Is(err, ErrManifestTransition) {
		t.Fatalf("certificate/catalog generation mismatch err=%v", err)
	}
	incomplete := cursor
	incomplete.phase = RetainedPruneScan
	incomplete.snapshotGeneration = 0
	incomplete.retainedDigest = [sha256.Size]byte{}
	if err := partitioner.VerifyRetainedPruneCompletion(
		certificate, cursor.OperationID(), incomplete,
	); !errors.Is(err, ErrRetainedPrune) {
		t.Fatalf("incomplete proof err=%v", err)
	}
	if err := partitioner.ValidatePublicationTransition(
		currentManifest, nextManifest, 19, 20, certificate,
	); err != nil {
		t.Fatalf("publication must precede prune: %v", err)
	}
	if err := partitioner.ValidateRetainedPruneAuthority(
		nextManifest, 20, certificate,
	); err != nil {
		t.Fatalf("published prune authority=%v", err)
	}
	if err := partitioner.ValidateRetainedPruneAuthority(
		currentManifest, 19, certificate,
	); !errors.Is(err, ErrRetainedPrune) {
		t.Fatalf("unpublished prune authority=%v", err)
	}
	if _, err := NewRetainedPruner(
		partitioner, certificate, nil, nil,
	); !errors.Is(err, ErrRetainedPrune) {
		t.Fatalf("pruner accepted unpublished source authority=%v", err)
	}
	if _, err := NewRetainedPruner(partitioner, certificate, forgedPruneAuthority{
		manifest: nextManifest, generation: 20, operation: [sha256.Size]byte{1},
		certificate: certificate.Digest(),
	}, nil); !errors.Is(err, ErrRetainedPrune) {
		t.Fatalf("pruner accepted structurally compatible forged authority=%v", err)
	}
	corrupt := bytes.Clone(persisted)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := newTestRetainedPruner(t, partitioner, certificate, corrupt); !errors.Is(err, ErrRetainedPrune) {
		t.Fatalf("corrupt cursor err=%v", err)
	}
}

type forgedPruneAuthority struct {
	manifest               *distribution.Manifest
	generation             uint64
	operation, certificate [sha256.Size]byte
}

func (a forgedPruneAuthority) Manifest() *distribution.Manifest { return a.manifest }
func (a forgedPruneAuthority) Generation() uint64               { return a.generation }
func (a forgedPruneAuthority) Operation() [sha256.Size]byte     { return a.operation }
func (a forgedPruneAuthority) Certificate() [sha256.Size]byte   { return a.certificate }

func BenchmarkRetainedPrunerPendingRetry(b *testing.B) {
	partitioner, fixture, capture, certificate := newRetainedPruneFixture(b)
	pruner, err := newTestRetainedPruner(b, partitioner, certificate, nil)
	if err != nil {
		b.Fatal(err)
	}
	snapshot, err := fixture.machine.Snapshot("docs")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = snapshot.Close() })
	limits := RetainedPruneLimits{MaxKeys: 1, MaxKeyBytes: 128, MaxScanRows: 2}
	var persisted []byte
	persist := func(raw []byte) error {
		persisted = append(persisted[:0], raw...)
		return nil
	}
	var workspace RetainedPruneWorkspace
	if _, has, err := pruner.Advance(
		snapshot, capture, limits, persist, &workspace,
	); err != nil || !has {
		b.Fatalf("initial plan has=%v err=%v", has, err)
	}
	if len(persisted) == 0 {
		b.Fatal("missing durable cursor")
	}
	if _, has, err := pruner.Advance(
		snapshot, capture, limits, persist, &workspace,
	); err != nil || !has {
		b.Fatalf("warm retry has=%v err=%v", has, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, has, err := pruner.Advance(
			snapshot, capture, limits, persist, &workspace,
		); err != nil || !has {
			b.Fatalf("retry has=%v err=%v", has, err)
		}
	}
}

func BenchmarkPublishBeforePruneAuthorityStages(b *testing.B) {
	partitioner, _, _, certificate := newRetainedPruneFixture(b)
	current, err := distribution.NewManifest("orders", 11, []distribution.Shard{{
		ID: "source", AllocationGeneration: 7,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"node-a"}, Epoch: 5,
	}})
	if err != nil {
		b.Fatal(err)
	}
	published := testSplitPlan(b, "node-b").Manifest()
	pruner, err := newTestRetainedPruner(b, partitioner, certificate, nil)
	if err != nil {
		b.Fatal(err)
	}
	cursor := pruner.Cursor()
	raw, err := AppendRetainedPruneCursor(nil, &cursor)
	if err != nil {
		b.Fatal(err)
	}
	var publishWorkspace, pruneWorkspace PublicationTransitionWorkspace
	for _, stage := range []struct {
		name string
		run  func() error
	}{
		{"publish_certificate", func() error {
			return partitioner.ValidatePublicationTransitionWithWorkspace(
				current, published, 19, 20, certificate, &publishWorkspace,
			)
		}},
		{"published_prune_authority", func() error {
			return partitioner.ValidateRetainedPruneAuthorityWithWorkspace(
				published, 20, certificate, &pruneWorkspace,
			)
		}},
	} {
		b.Run(stage.name, func(b *testing.B) {
			if err := stage.run(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := stage.run(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(raw)), "cursor-B")
			b.ReportMetric(float64(len(raw)-sha256.Size), "authenticated-meta-B")
		})
	}
}

func TestRetainedPruneLimitsRejectUnboundedScanWork(t *testing.T) {
	_, err := normalizeRetainedPruneLimits(RetainedPruneLimits{
		MaxScanRows: MaxRetainedPruneScanRows + 1,
	})
	if !errors.Is(err, ErrRetainedPrune) {
		t.Fatalf("err=%v, want %v", err, ErrRetainedPrune)
	}
}

func TestRetainedPrunerAdvancesRetainedWritesAndRejectsOutOfRangeWrites(t *testing.T) {
	partitioner, fixture, capture, certificate := newRetainedPruneFixture(t)
	pruner, err := newTestRetainedPruner(t, partitioner, certificate, nil)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	persist := func(raw []byte) error {
		persisted = append(persisted[:0], raw...)
		return nil
	}
	limits := RetainedPruneLimits{MaxKeys: 1, MaxKeyBytes: 128, MaxScanRows: 2}
	var workspace RetainedPruneWorkspace
	snapshot, err := fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, has, advanceErr := pruner.Advance(
		snapshot, capture, limits, persist, &workspace,
	); advanceErr != nil || !has {
		t.Fatalf("plan has=%v err=%v", has, advanceErr)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	binding := fixture.binding
	binding.OwnershipEpoch++
	binding.RoutingVersion++
	binding.RouteGeneration++
	retained, err := vibejson.AppendCanonicalize(nil, documentForChild(t, partitioner, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(sourceCaptureMeta(7), retainedMutationCommand(
		t, fixture, binding, 5, 2,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("e-retained"), Value: retained},
	)); err != nil {
		t.Fatal(err)
	}
	snapshot, err = fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	_, has, advanceErr := pruner.Advance(snapshot, capture, limits, persist, &workspace)
	closeErr := snapshot.Close()
	if advanceErr != nil || has || closeErr != nil ||
		pruner.Cursor().Phase() != RetainedPruneAwaitingApply {
		t.Fatalf("retained advance has=%v err=%v close=%v cursor=%+v",
			has, advanceErr, closeErr, pruner.Cursor())
	}
	pruner, err = newTestRetainedPruner(t, partitioner, certificate, persisted)
	if err != nil {
		t.Fatalf("restart after retained write: %v", err)
	}
	snapshot, err = fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	retry, has, advanceErr := pruner.Advance(snapshot, capture, limits, persist, &workspace)
	closeErr = snapshot.Close()
	if advanceErr != nil || !has || closeErr != nil || retry.Count != 1 {
		t.Fatalf("retry after retained write batch=%+v has=%v err=%v close=%v",
			retry, has, advanceErr, closeErr)
	}
	outOfRange, err := vibejson.AppendCanonicalize(nil, documentForChild(t, partitioner, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(sourceCaptureMeta(8), retainedMutationCommand(
		t, fixture, binding, 5, 3,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("f-out-of-range"), Value: outOfRange},
	)); err != nil {
		t.Fatal(err)
	}
	snapshot, err = fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	_, _, advanceErr = pruner.Advance(snapshot, capture, limits, persist, &workspace)
	closeErr = snapshot.Close()
	if !errors.Is(advanceErr, ErrRetainedPrune) || closeErr != nil {
		t.Fatalf("out-of-range source entry err=%v close=%v", advanceErr, closeErr)
	}
}

func newRetainedPruneFixture(
	t testing.TB,
) (*Partitioner, sourceCaptureFixture, *SourceCapture, CutoverCertificate) {
	t.Helper()
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
	left, err := vibejson.AppendCanonicalize(nil, documentForChild(t, partitioner, 0))
	if err != nil {
		t.Fatal(err)
	}
	right, err := vibejson.AppendCanonicalize(nil, documentForChild(t, partitioner, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(sourceCaptureMeta(3), fixture.command(2,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("a-left"), Value: left},
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("b-right"), Value: right},
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("c-left"), Value: left},
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("d-right"), Value: right},
	)); err != nil {
		t.Fatal(err)
	}
	fixture.retainedPruneEpoch = fixture.openSession(
		t, 4, []byte("split-controller"), sourceCaptureID(30),
	)
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
	database, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	child, err := database.CreateCollection("child", durable.Options{MaxBatchDocuments: 4})
	if err != nil {
		t.Fatal(err)
	}
	stage, err := NewChildStage(partitioner, set.Children[1], child, nil)
	if err != nil {
		t.Fatal(err)
	}
	var stageCursorRaw []byte
	persistStage := func(raw []byte) error {
		stageCursorRaw = append(stageCursorRaw[:0], raw...)
		return nil
	}
	if _, err := stage.ReceiveArtifact(bytes.NewReader(artifact.Bytes()), persistStage); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 5, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	var captureWorkspace SourceCaptureWorkspace
	entry, ok, err := capture.NextTailEntry(cursor, &captureWorkspace)
	if err != nil || !ok {
		t.Fatalf("config capture ok=%v err=%v", ok, err)
	}
	sinks := []TailSink{
		func(TailBatch) error { return nil },
		func(batch TailBatch) error { return stage.ApplyTailBatch(batch, persistStage) },
	}
	var tailWorkspace TailWorkspace
	cursor, _, err = partitioner.TranslateTailEntry(cursor, entry, sinks, &tailWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
		From: fixture.binding, ExpectedReplicaSetVersion: 5,
		SourceMember: 1, TargetMember: 2,
		ToOwnershipEpoch:  fixture.binding.OwnershipEpoch + 1,
		ToRoutingVersion:  fixture.binding.RoutingVersion + 1,
		ToRouteGeneration: fixture.binding.RouteGeneration + 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(sourceCaptureMeta(6), ownership); err != nil {
		t.Fatal(err)
	}
	entry, ok, err = capture.NextTailEntry(cursor, &captureWorkspace)
	if err != nil || !ok {
		t.Fatalf("seal capture ok=%v err=%v", ok, err)
	}
	cursor, _, err = partitioner.TranslateTailEntry(cursor, entry, sinks, &tailWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	stageCursor, _ := stage.Cursor()
	var cutoverWorkspace CutoverWorkspace
	certificate, err := partitioner.CertifyCutover(
		capture, cursor, []ChildStageCursor{stageCursor}, &cutoverWorkspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	return partitioner, fixture, capture, certificate
}

func newTestRetainedPruner(
	t testing.TB,
	partitioner *Partitioner,
	certificate CutoverCertificate,
	persisted []byte,
) (*RetainedPruner, error) {
	t.Helper()
	return newRetainedPruner(partitioner, certificate, [sha256.Size]byte{1}, persisted)
}

func retainedPruneCommand(
	t testing.TB,
	fixture sourceCaptureFixture,
	binding replicatedstate.Binding,
	replicaSetVersion uint64,
	sequence uint64,
	batch RetainedPruneBatch,
) []byte {
	t.Helper()
	mutations := make([]replication.Mutation, 0, batch.Count)
	iterator := batch.Iterator()
	for iterator.Next() {
		mutations = append(mutations, replication.Mutation{
			Kind: replication.MutationDelete, Key: bytes.Clone(iterator.Key()),
		})
	}
	return retainedMutationCommand(t, fixture, binding, replicaSetVersion, sequence, mutations...)
}

func retainedMutationCommand(
	t testing.TB,
	fixture sourceCaptureFixture,
	binding replicatedstate.Binding,
	replicaSetVersion uint64,
	sequence uint64,
	mutations ...replication.Mutation,
) []byte {
	t.Helper()
	fingerprint := sha256.Sum256([]byte{byte(sequence), 0x70})
	encoded, err := replication.AppendCommand(nil, replication.Command{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion:      replicaSetVersion,
		ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch:        binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
		SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration, Tenant: []byte("split-controller"),
		ClientID: sourceCaptureID(30), ClientEpoch: fixture.retainedPruneEpoch,
		ClientSequence: sequence,
		Fingerprint:    fingerprint,
		Batches:        []replication.RelationMutationBatch{{Relation: 1, Mutations: mutations}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func firstRetainedPruneKey(b RetainedPruneBatch) []byte {
	iterator := b.Iterator()
	if !iterator.Next() {
		return nil
	}
	return iterator.Key()
}
