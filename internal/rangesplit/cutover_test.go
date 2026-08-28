package rangesplit

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestCertifyCutoverRequiresExactCapturedSealAndDurableChildren(t *testing.T) {
	t.Run("local", func(t *testing.T) { testCapturedCutover(t, false) })
	t.Run("durable-lagged-fence", func(t *testing.T) { testCapturedCutover(t, true) })
}

func testCapturedCutover(t *testing.T, lagged bool) {
	partitioner, err := NewPartitioner(
		testSplitPlan(t, "node-b"), "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newSourceCaptureFixture(t, partitioner)
	var capture *SourceCapture
	configurationIndex := uint64(3)
	if lagged {
		partitioner, fixture, capture = activateLaggedCapture(t, partitioner, fixture)
		configurationIndex = 4
	} else {
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		fixture.clientEpoch = fixture.openSession(t, 2, []byte("tenant"), sourceCaptureID(20))
		capture, err = NewSourceCapture(partitioner, "split-capture", fixture.capture)
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.machine.BeginTransitionCapture(capture); err != nil {
			t.Fatal(err)
		}
	}

	cut, err := fixture.machine.Snapshot("docs")
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
	var persisted []byte
	persist := func(raw []byte) error {
		persisted = append(persisted[:0], raw...)
		return nil
	}
	if _, err := stage.ReceiveArtifact(bytes.NewReader(artifact.Bytes()), persist); err != nil {
		t.Fatal(err)
	}

	if _, err := fixture.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: configurationIndex, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	var captureWorkspace SourceCaptureWorkspace
	entry, ok, err := capture.NextTailEntry(cursor, &captureWorkspace)
	if err != nil || !ok {
		t.Fatalf("configuration capture ok=%v err=%v", ok, err)
	}
	sinks := []TailSink{
		func(TailBatch) error { return nil },
		func(batch TailBatch) error { return stage.ApplyTailBatch(batch, persist) },
	}
	var tailWorkspace TailWorkspace
	cursor, _, err = partitioner.TranslateTailEntry(cursor, entry, sinks, &tailWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	seal := partitioner.sealCoordinates(partitioner.initialCoordinates(fixture.binding.RouteGeneration))
	transition := replicatedstate.OwnershipTransition{
		From: fixture.binding, ExpectedReplicaSetVersion: configurationIndex,
		SourceMember: 1, TargetMember: 2,
		ToOwnershipEpoch:  seal.OwnershipEpoch,
		ToRoutingVersion:  seal.RoutingVersion,
		ToRouteGeneration: seal.RouteGeneration,
		ToOwnedRange:      partitioner.children[partitioner.retained].Range,
	}
	if lagged {
		for _, mutate := range []func(*replicatedstate.OwnershipTransition){
			func(v *replicatedstate.OwnershipTransition) { v.ToRoutingVersion++ },
			func(v *replicatedstate.OwnershipTransition) { v.ToRouteGeneration++ },
			func(v *replicatedstate.OwnershipTransition) { v.ToOwnedRange = v.From.OwnedRange },
		} {
			wrong := transition
			mutate(&wrong)
			raw, encodeErr := replicatedstate.AppendOwnershipTransition(nil, wrong)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			if _, applyErr := fixture.machine.ApplyNormal(sourceCaptureMeta(configurationIndex+1), raw); !errors.Is(applyErr, replicatedstate.ErrOwnershipTransition) {
				t.Fatalf("unauthorized jump accepted: %v", applyErr)
			}
			capture = reopenLaggedCapture(t, &fixture)
		}
	}
	ownership, err := replicatedstate.AppendOwnershipTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(sourceCaptureMeta(configurationIndex+1), ownership); err != nil {
		t.Fatal(err)
	}
	if lagged {
		capture = reopenLaggedCapture(t, &fixture)
	}
	entry, ok, err = capture.NextTailEntry(cursor, &captureWorkspace)
	if err != nil || !ok || !tailEntrySeals(entry) {
		t.Fatalf("seal capture=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor, _, err = partitioner.TranslateTailEntry(cursor, entry, sinks, &tailWorkspace)
	if err != nil || !cursor.Sealed() {
		t.Fatalf("seal cursor=%+v err=%v", cursor, err)
	}
	stageCursor, ok := stage.Cursor()
	if !ok || stageCursor.Phase() != ChildStageSealed {
		t.Fatalf("stage cursor=%+v ok=%v", stageCursor, ok)
	}
	if _, err := NewChildStage(partitioner, set.Children[1], child, persisted); err != nil {
		t.Fatalf("reopen sealed stage: %v", err)
	}

	var cutoverWorkspace CutoverWorkspace
	certificate, err := partitioner.CertifyCutover(
		capture, cursor, []ChildStageCursor{stageCursor}, &cutoverWorkspace,
	)
	if err != nil || certificate.Digest() == ([32]byte{}) ||
		certificate.SourceCut() != cursor.SourceCut() ||
		certificate.SourceCoordinates() != cursor.SourceCoordinates() {
		t.Fatalf("certificate=%+v err=%v", certificate, err)
	}
	if err := partitioner.VerifyCutoverCertificate(certificate); err != nil {
		t.Fatal(err)
	}
	if lagged {
		wrong := certificate
		wrong.coordinates.RouteGeneration++
		wrong.digest = cutoverDigest(&wrong, &CutoverVerifyWorkspace{})
		if err := partitioner.VerifyCutoverCertificate(wrong); !errors.Is(err, ErrCutoverCertificate) {
			t.Fatalf("resealed unauthorized target generation accepted: %v", err)
		}
	}
	imageDigest, imageOK := certificate.ChildImageDigest(1)
	_, _, wantImageDigest, wantImageOK := stageCursor.ImageProof()
	if !imageOK || !wantImageOK || imageDigest != wantImageDigest {
		t.Fatalf("certificate image=%x/%v stage=%x/%v", imageDigest, imageOK, wantImageDigest, wantImageOK)
	}
	if _, ok := certificate.ChildImageDigest(0); ok {
		t.Fatal("retained child unexpectedly has a pre-prune image proof")
	}
	raw, err := AppendCutoverCertificate(nil, &certificate)
	if err != nil || len(raw) != cutoverCertificateBytes {
		t.Fatalf("encode bytes=%d err=%v", len(raw), err)
	}
	opened, err := OpenCutoverCertificate(raw)
	if err != nil || *opened != certificate {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	buffer := make([]byte, 0, cutoverCertificateBytes)
	var encodeWorkspace CutoverWorkspace
	if _, err := AppendCutoverCertificateWithWorkspace(
		buffer[:0], &certificate, &encodeWorkspace,
	); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1_000, func() {
		var appendErr error
		buffer, appendErr = AppendCutoverCertificateWithWorkspace(
			buffer[:0], &certificate, &encodeWorkspace,
		)
		if appendErr != nil {
			panic(appendErr)
		}
	}); allocations != 0 {
		t.Fatalf("warm certificate encode allocations=%v", allocations)
	}
	corrupt := bytes.Clone(raw)
	corrupt[200] ^= 1
	if _, err := OpenCutoverCertificate(corrupt); !errors.Is(err, ErrCutoverCertificate) {
		t.Fatalf("corrupt error=%v", err)
	}

	if _, err := fixture.machine.ApplyNormal(sourceCaptureMeta(configurationIndex+2), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := partitioner.CertifyCutover(
		capture, cursor, []ChildStageCursor{stageCursor}, &cutoverWorkspace,
	); !errors.Is(err, ErrCutoverCertificate) {
		t.Fatalf("advanced source certificate error=%v", err)
	}
}
