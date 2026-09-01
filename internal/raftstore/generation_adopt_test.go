package raftstore

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestGenerationLiveAdoptionPreservesOwnerReadyAndIncarnation(t *testing.T) {
	path, source, options, builder := prepareGenerationSource(t)
	defer source.Close()
	if err := source.AdoptSelectedGeneration(); !errors.Is(err, ErrGenerationActivationPending) {
		t.Fatalf("unselected adoption: %v", err)
	}
	incarnation, ready, digest := source.CurrentIncarnation(), source.observedReadyID, source.observedReadyDigest
	for generation := uint64(1); generation <= 2; generation++ {
		candidate, err := builder.Build()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := source.PublishGenerationSelection(builder); err != nil {
			t.Fatal(err)
		}
		unadopted := &testGenerationActivationSettler{}
		if err := source.CommitGenerationSelection(unadopted); err == nil || unadopted.calls != 0 {
			t.Fatalf("source committed without adoption: %v", err)
		}
		if err := source.AdoptSelectedGeneration(); err != nil {
			t.Fatal(err)
		}
		if !source.begun || source.CurrentIncarnation() != incarnation || source.observedReadyID != ready || source.observedReadyDigest != digest {
			t.Fatal("live owner incarnation or Ready cursor reset")
		}
		activation, err := source.PendingGenerationActivation()
		if err != nil || activation.Info.BindingDigest != candidate.Info.BindingDigest || activation.Info.Generation != generation {
			t.Fatalf("adopted different candidate: %+v %v", activation.Info, err)
		}
		if err := source.AdoptSelectedGeneration(); err != nil {
			t.Fatalf("idempotent adoption: %v", err)
		}
		if _, _, err := source.InitialState(); !errors.Is(err, ErrGenerationActivationPending) {
			t.Fatalf("adoption released serving fence: %v", err)
		}
		failed := errors.New("injected SQL settlement")
		settler := &testGenerationActivationSettler{fail: failed}
		if err := source.CommitGenerationSelection(settler); !errors.Is(err, failed) {
			t.Fatalf("settlement error lost: %v", err)
		}
		if _, err := os.Stat(candidate.Path); err != nil {
			t.Fatalf("unsettled candidate removed: %v", err)
		}
		settler.fail = nil
		if err := source.CommitGenerationSelection(settler); err != nil {
			t.Fatal(err)
		}
		for _, staleReady := range []uint64{0, ready - 1} {
			if err := source.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: staleReady}); err == nil {
				t.Fatalf("generation %d accepted stale Ready %d", generation, staleReady)
			}
		}
		index := uint64(4) + generation
		batch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: ready + 1,
			HardState: hard(4, index), Entries: []*pb.Entry{entry(index, 4, "after-live-adoption")}, MustSync: true}
		if err := source.Persist(batch); err != nil {
			t.Fatalf("next Ready after generation %d: %v", generation, err)
		}
		if err := source.Persist(batch); err != nil {
			t.Fatalf("exact Ready retry after generation %d: %v", generation, err)
		}
		ready, digest = source.observedReadyID, source.observedReadyDigest
		if generation == 1 {
			builder, err = source.PrepareGeneration(testGenerationInput(testGenerationSnapshot(index, 4, "next-base")), testKey())
			if err != nil {
				t.Fatal(err)
			}
			defer builder.Close()
		}
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	info, err := reopened.GenerationInfo()
	if err != nil || info.Generation != 2 {
		t.Fatalf("reopen live generations: %+v %v", info, err)
	}
	hardState, _, err := reopened.InitialState()
	if err != nil || hardState.GetCommit() != 6 {
		t.Fatalf("reopen lost post-adoption Ready: %v %v", hardState, err)
	}
}

func TestGenerationLiveAdoptionCarriesPublicationBackedCommitFloor(t *testing.T) {
	_, source, _ := createTestStore(t)
	defer source.Close()
	incarnation, err := source.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 1),
		Entries: []*pb.Entry{entry(2, 2, "durable")}, MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err = source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 2),
	}); err != nil {
		t.Fatal(err)
	}
	if source.current.hard.GetCommit() != 1 || source.image.hard.GetCommit() != 2 {
		t.Fatalf("source commit coordinates current=%d image=%d, want 1/2",
			source.current.hard.GetCommit(), source.image.hard.GetCommit())
	}
	builder, err := source.PrepareGeneration(testGenerationInput(
		testGenerationSnapshot(2, 2, "publication-backed"),
	), testKey())
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	candidate, err := builder.Build()
	if err != nil || candidate.Info.HardCommit != 2 || builder.seal.hard.GetCommit() != 2 {
		t.Fatalf("candidate commit=%d seal=%d err=%v, want 2",
			candidate.Info.HardCommit, builder.seal.hard.GetCommit(), err)
	}
	if _, err = source.PublishGenerationSelection(builder); err != nil {
		t.Fatal(err)
	}
	if err = source.AdoptSelectedGeneration(); err != nil {
		t.Fatal(err)
	}
	if err = source.CommitGenerationSelection(&testGenerationActivationSettler{}); err != nil {
		t.Fatal(err)
	}
	hardState, _, err := source.InitialState()
	if err != nil || hardState.GetCommit() != 2 {
		t.Fatalf("adopted commit=%d err=%v, want 2", hardState.GetCommit(), err)
	}
}

func TestPrepareGenerationAcceptsExactPublicationCommitAfterRestart(t *testing.T) {
	path, source, options := createTestStore(t)
	incarnation, err := source.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 1),
		Entries: []*pb.Entry{entry(2, 2, "durable")}, MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err = reopened.BeginIncarnation(); err != nil {
		t.Fatal(err)
	}
	input := testGenerationInput(testGenerationSnapshot(2, 2, "publication-backed-restart"))
	input.PublicationCommit = 2
	builder, err := reopened.PrepareGeneration(input, testKey())
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	if _, err = builder.Build(); err != nil {
		t.Fatal(err)
	}
	if _, err = reopened.PublishGenerationSelection(builder); err != nil {
		t.Fatal(err)
	}
	if err = reopened.AdoptSelectedGeneration(); err != nil {
		t.Fatal(err)
	}
	if err = reopened.CommitGenerationSelection(&testGenerationActivationSettler{}); err != nil {
		t.Fatal(err)
	}
	hardState, _, err := reopened.InitialState()
	if err != nil || hardState.GetCommit() != 2 {
		t.Fatalf("adopted publication commit=%d err=%v, want 2", hardState.GetCommit(), err)
	}
}

func TestGenerationSourceReadyCursorIsAuthenticated(t *testing.T) {
	_, source, _, builder := prepareGenerationSource(t)
	defer source.Close()
	if _, err := builder.Build(); err != nil {
		t.Fatal(err)
	}
	seal := builder.seal
	if seal.sourceReadyID != 2 || seal.sourceReadyID != source.current.retry.readyID {
		t.Fatalf("missing exact source cursor: %d", seal.sourceReadyID)
	}
	for _, wrong := range []uint64{0, 1, 3} {
		foreign := seal
		foreign.sourceReadyID = wrong
		foreign.bindingDigest = generationBindingDigest(foreign)
		if builder.candidateSealMatches(foreign, builder.input.Snapshot) {
			t.Fatalf("accepted foreign source Ready cursor %d", wrong)
		}
	}
	for _, candidate := range []struct {
		incarnation, ready uint64
		valid              bool
	}{
		{seal.sourceCurrentIncarnation - 1, 100, false},
		{seal.sourceCurrentIncarnation, 0, false},
		{seal.sourceCurrentIncarnation, 1, false},
		{seal.sourceCurrentIncarnation, 2, false},
		{seal.sourceCurrentIncarnation, 3, true},
		{seal.sourceCurrentIncarnation + 1, 0, false},
		{seal.sourceCurrentIncarnation + 1, 1, true},
	} {
		if got := generationReadyAfterSource(seal, candidate.incarnation, candidate.ready); got != candidate.valid {
			t.Fatalf("source continuation (%d,%d)=%t", candidate.incarnation, candidate.ready, got)
		}
	}
	current := currentState{currentIncarnation: seal.sourceCurrentIncarnation}
	if got := generationReadyFloor(current, generationRecovery{present: true, seal: seal}); got != seal.sourceReadyID {
		t.Fatalf("idle compaction dropped inherited Ready floor: %d", got)
	}
	current.currentIncarnation++
	if got := generationReadyFloor(current, generationRecovery{present: true, seal: seal}); got != 0 {
		t.Fatalf("new incarnation inherited stale Ready floor: %d", got)
	}
}

func TestGenerationLiveAdoptionSettlesUnknownSelectionAndMatchesReopen(t *testing.T) {
	path, source, options, builder := prepareGenerationSource(t)
	defer source.Close()
	if _, err := builder.Build(); err != nil {
		t.Fatal(err)
	}
	originalSync := source.options.ops.sync
	injected := errors.New("selection Sync returned unknown")
	source.options.ops.sync = func(file *os.File) error {
		err := originalSync(file)
		if err == nil && file == source.family.file {
			return injected
		}
		return err
	}
	identity, err := source.PublishGenerationSelection(builder)
	if !errors.Is(err, injected) || !identity.Valid() {
		t.Fatalf("selection fault not reached: %+v %v", identity, err)
	}
	source.options.ops.sync = originalSync
	if err := source.AdoptSelectedGeneration(); err != nil {
		t.Fatal(err)
	}
	live, err := source.PendingGenerationActivation()
	if err != nil || !identity.Matches(live.Info) {
		t.Fatalf("unknown selection changed identity: %+v %v", live.Info, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := reopened.PendingGenerationActivation()
	if err != nil || recovered.Info != live.Info {
		t.Fatalf("cold/live activation mismatch: live=%+v recovered=%+v err=%v", live.Info, recovered.Info, err)
	}
	if err := reopened.CommitGenerationSelection(&testGenerationActivationSettler{}); err != nil {
		t.Fatal(err)
	}
}

func TestGenerationLiveAdoptionRejectsChangedCandidateBeforeTransplant(t *testing.T) {
	_, source, _, builder := prepareGenerationSource(t)
	defer source.Close()
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.PublishGenerationSelection(builder); err != nil {
		t.Fatal(err)
	}
	originalFile, originalHeader := source.file, source.header.headerDigest
	file, err := os.OpenFile(candidate.Path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0}, 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.AdoptSelectedGeneration(); err == nil {
		t.Fatal("adopted corrupted candidate")
	}
	if source.file != originalFile || source.header.headerDigest != originalHeader || !source.activationPending {
		t.Fatal("failed candidate validation changed or unfenced source")
	}
}

func TestGenerationLiveAdoptionRetriesUnknownActivePublicationWithoutResettling(t *testing.T) {
	_, source, _, builder := prepareGenerationSource(t)
	defer source.Close()
	if _, err := builder.Build(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PublishGenerationSelection(builder); err != nil {
		t.Fatal(err)
	}
	if err := source.AdoptSelectedGeneration(); err != nil {
		t.Fatal(err)
	}
	originalSync := source.options.ops.sync
	injected := errors.New("active publication returned unknown")
	source.options.ops.sync = func(file *os.File) error {
		err := originalSync(file)
		if err == nil && file == source.family.file {
			return injected
		}
		return err
	}
	settler := &testGenerationActivationSettler{}
	if err := source.CommitGenerationSelection(settler); !errors.Is(err, injected) || settler.calls != 1 || settler.completes != 0 {
		t.Fatalf("missing active publication barrier: calls=%d completes=%d err=%v", settler.calls, settler.completes, err)
	}
	source.options.ops.sync = originalSync
	if err := source.AdoptSelectedGeneration(); err != nil {
		t.Fatal(err)
	}
	if err := source.CommitGenerationSelection(settler); err != nil || settler.calls != 1 || settler.completes != 1 {
		t.Fatalf("replayed SQL settlement after active publication: calls=%d completes=%d err=%v", settler.calls, settler.completes, err)
	}
}
