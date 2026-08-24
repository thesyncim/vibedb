package raftstore

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func testGenerationSnapshot(index, term uint64, data string) *pb.Snapshot {
	return &pb.Snapshot{
		Data: []byte(data),
		Metadata: &pb.SnapshotMetadata{
			Index: &index,
			Term:  &term,
			ConfState: &pb.ConfState{
				Voters: []uint64{testIdentity().MemberID},
			},
		},
	}
}

func testRetentionCommitment() [sha256.Size]byte {
	return sha256.Sum256([]byte("checkpoint-retention-commitment"))
}

func prepareGenerationSource(t testing.TB) (string, *Store, Options, *GenerationBuilder) {
	t.Helper()
	path, store, options := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         1,
		HardState:       hard(2, 3),
		Entries: []*pb.Entry{
			entry(2, 2, "two"),
			entry(3, 2, "three"),
		},
		MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         2,
		HardState:       hard(3, 4),
		Entries:         []*pb.Entry{entry(4, 3, "four")},
		MustSync:        true,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := testGenerationSnapshot(3, 2, "checkpoint-three")
	key := testKey()
	builder, err := store.PrepareGeneration(GenerationInput{
		Snapshot:            snapshot,
		RetentionCommitment: testRetentionCommitment(),
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	// The builder owns its complete cut and key locator after return.
	snapshot.Data[0] ^= 0xff
	*snapshot.Metadata.Index = 2
	key.Wrapped[0] ^= 0xff
	key.Material[0] ^= 0xff
	t.Cleanup(func() { _ = builder.Close() })
	return path, store, options, builder
}

func TestGenerationCandidateCapturesExactCutAndLeavesSourceAuthoritative(t *testing.T) {
	path, source, options, builder := prepareGenerationSource(t)
	incarnation := source.CurrentIncarnation()
	peerBuilder, err := source.PrepareGeneration(GenerationInput{
		Snapshot:            cloneSnapshot(builder.input.Snapshot),
		RetentionCommitment: builder.input.RetentionCommitment,
	}, testKey())
	if err != nil {
		t.Fatal(err)
	}
	defer peerBuilder.Close()

	// Appending after Prepare mutates the live source but not the immutable
	// descriptor/current-slot cut owned by the builder.
	if err := source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         3,
		HardState:       hard(4, 5),
		Entries:         []*pb.Entry{entry(5, 4, "source-five")},
		MustSync:        true,
	}); err != nil {
		t.Fatal(err)
	}

	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Path != builder.CandidatePath() || candidate.Info.Path != candidate.Path ||
		candidate.Info.Generation != FirstWALGeneration || candidate.Info.BaseIndex != 3 ||
		candidate.Info.BaseTerm != 2 || candidate.Info.LastIndex != 4 ||
		candidate.Info.HardTerm != 3 || candidate.Info.HardCommit != 4 ||
		candidate.Info.SourceCurrentIncarnation != incarnation ||
		candidate.Info.RetainedEntries != 1 || candidate.Info.RetainedBytes == 0 ||
		candidate.Info.RetentionCommitment != testRetentionCommitment() ||
		candidate.Info.SourceCutDigest != builder.current.chainDigest {
		t.Fatalf("candidate info = %+v", candidate.Info)
	}
	if again, retryErr := builder.Build(); retryErr != nil || again != candidate {
		t.Fatalf("idempotent Build = %+v, %v, want %+v", again, retryErr, candidate)
	}
	if peer, peerErr := peerBuilder.Build(); peerErr != nil || peer != candidate ||
		peerBuilder.source != nil {
		t.Fatalf("independent idempotent Build = %+v, %v source=%v, want %+v",
			peer, peerErr, peerBuilder.source, candidate)
	}

	generated, err := openUnselectedGenerationForTest(
		candidate.Path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	info, err := generated.GenerationInfo()
	if err != nil || info != candidate.Info {
		t.Fatalf("GenerationInfo = %+v, %v, want %+v", info, err, candidate.Info)
	}
	snapshot, err := generated.Snapshot()
	if err != nil || snapshot.GetMetadata().GetIndex() != 3 ||
		snapshot.GetMetadata().GetTerm() != 2 || string(snapshot.GetData()) != "checkpoint-three" {
		t.Fatalf("candidate Snapshot = %+v, %v", snapshot, err)
	}
	retained, err := generated.Entries(4, 5, ^uint64(0))
	if err != nil || len(retained) != 1 || string(retained[0].GetData()) != "four" {
		t.Fatalf("candidate retained suffix = %+v, %v", retained, err)
	}
	if generated.CurrentIncarnation() != incarnation {
		t.Fatalf("candidate incarnation = %d, want %d", generated.CurrentIncarnation(), incarnation)
	}
	nextIncarnation, err := generated.BeginIncarnation()
	if err != nil || nextIncarnation != incarnation+1 {
		t.Fatalf("candidate BeginIncarnation = %d, %v", nextIncarnation, err)
	}
	if err := generated.Persist(raftmodel.PersistBatch{
		NodeIncarnation: nextIncarnation,
		ReadyID:         1,
		HardState:       hard(4, 5),
		Entries:         []*pb.Entry{entry(5, 4, "candidate-five")},
		MustSync:        true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := generated.Close(); err != nil {
		t.Fatal(err)
	}

	// The sibling is explicitly openable but never changes Open(path)'s
	// authority. Both paths advance independently after the captured cut.
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedSource, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedSource.Close()
	sourceFive, err := reopenedSource.Entries(5, 6, ^uint64(0))
	if err != nil || len(sourceFive) != 1 || string(sourceFive[0].GetData()) != "source-five" {
		t.Fatalf("source suffix after candidate build = %+v, %v", sourceFive, err)
	}
	reopenedCandidate, err := openUnselectedGenerationForTest(
		candidate.Path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedCandidate.Close()
	candidateFive, err := reopenedCandidate.Entries(5, 6, ^uint64(0))
	if err != nil || len(candidateFive) != 1 || string(candidateFive[0].GetData()) != "candidate-five" {
		t.Fatalf("candidate suffix after restart = %+v, %v", candidateFive, err)
	}
	if _, err := reopenedSource.GenerationInfo(); !errors.Is(err, ErrGenerationCandidate) {
		t.Fatalf("initial source GenerationInfo = %v", err)
	}
}

func TestGenerationCandidateSupportsFullyCheckpointedSuffix(t *testing.T) {
	_, source, options := createTestStore(t)
	incarnation, err := source.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         1,
		HardState:       hard(2, 3),
		Entries: []*pb.Entry{
			entry(2, 2, "two"),
			entry(3, 2, "three"),
		},
		MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	builder, err := source.PrepareGeneration(GenerationInput{
		Snapshot:            testGenerationSnapshot(3, 2, "checkpoint-through-three"),
		RetentionCommitment: testRetentionCommitment(),
	}, testKey())
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Info.BaseIndex != 3 || candidate.Info.LastIndex != 3 ||
		candidate.Info.RetainedEntries != 0 || candidate.Info.RetainedBytes != 0 {
		t.Fatalf("fully checkpointed generation info = %+v", candidate.Info)
	}
	opened, err := openUnselectedGenerationForTest(
		candidate.Path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	first, firstErr := opened.FirstIndex()
	last, lastErr := opened.LastIndex()
	state, _, stateErr := opened.InitialState()
	if firstErr != nil || lastErr != nil || stateErr != nil || first != 4 || last != 3 ||
		state.GetTerm() != 2 || state.GetCommit() != 3 {
		t.Fatalf("fully checkpointed image = first %d/%v last %d/%v state %+v/%v",
			first, firstErr, last, lastErr, state, stateErr)
	}
}

func TestGenerationCandidateConflictNeverOverwritesOccupant(t *testing.T) {
	_, _, _, builder := prepareGenerationSource(t)
	want := []byte("foreign-candidate-occupant")
	if err := os.WriteFile(builder.CandidatePath(), want, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Build(); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("Build with occupied candidate = %v, want ErrGenerationConflict", err)
	}
	got, err := os.ReadFile(builder.CandidatePath())
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("occupied candidate changed = %q, %v", got, err)
	}
}

func TestGenerationBuildFailureReclaimsConstructionNamesAndRetries(t *testing.T) {
	_, _, _, builder := prepareGenerationSource(t)
	injected := errors.New("injected generation preallocation failure")
	builder.options.ops.preallocate = func(*os.File, int64) error { return injected }
	if _, err := builder.Build(); !errors.Is(err, injected) ||
		!errors.Is(err, ErrPersistenceDefinite) {
		t.Fatalf("failed generation Build = %v", err)
	}
	requireNoGenerationConstructionNames(t, builder.parentPath)
	if _, err := os.Lstat(builder.CandidatePath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate after failed Build = %v", err)
	}

	builder.options.ops.preallocate = func(file *os.File, size int64) error {
		return file.Truncate(size)
	}
	if _, err := builder.Build(); err != nil {
		t.Fatalf("retry Build: %v", err)
	}
	requireNoGenerationConstructionNames(t, builder.parentPath)
}

func TestGenerationRetainedWriteFailureReclaimsStageAndRetries(t *testing.T) {
	_, _, _, builder := prepareGenerationSource(t)
	original := builder.options.ops.writeAt
	injected := errors.New("injected retained generation write failure")
	writes := 0
	faulted := false
	builder.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		writes++
		if !faulted && offset > HeaderBytes {
			faulted = true
			if len(data) == 0 {
				return 0, injected
			}
			n, _ := file.WriteAt(data[:1], offset)
			return n, errors.Join(injected, io.ErrShortWrite)
		}
		return original(file, data, offset)
	}
	if _, err := builder.Build(); !errors.Is(err, injected) ||
		!errors.Is(err, ErrPersistenceDefinite) {
		t.Fatalf("retained-write Build = %v", err)
	}
	if !faulted || writes < 4 {
		t.Fatalf("retained-write faulted=%v after %d writes", faulted, writes)
	}
	requireNoGenerationConstructionNames(t, builder.parentPath)
	if _, err := os.Lstat(builder.CandidatePath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate after retained-write failure = %v", err)
	}

	builder.options.ops.writeAt = original
	if _, err := builder.Build(); err != nil {
		t.Fatalf("retry after retained-write failure: %v", err)
	}
	requireNoGenerationConstructionNames(t, builder.parentPath)
}

func TestGenerationDurabilityBarrierFailuresReclaimStageAndRetry(t *testing.T) {
	tests := []struct {
		name        string
		failSync    int
		wantUnknown bool
	}{
		{name: "records", failSync: 1},
		{name: "current-slot", failSync: 2, wantUnknown: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, builder := prepareGenerationSource(t)
			originalSync := builder.options.ops.sync
			injected := errors.New("injected generation durability barrier failure")
			syncs := 0
			builder.options.ops.sync = func(file *os.File) error {
				syncs++
				if syncs == test.failSync {
					return injected
				}
				return originalSync(file)
			}
			if _, err := builder.Build(); !errors.Is(err, injected) ||
				errors.Is(err, ErrPersistenceUnknown) != test.wantUnknown ||
				errors.Is(err, ErrPersistenceDefinite) == test.wantUnknown {
				t.Fatalf("Build with sync %d failure = %v", test.failSync, err)
			}
			if syncs != test.failSync {
				t.Fatalf("generation sync calls = %d, want %d", syncs, test.failSync)
			}
			requireNoGenerationConstructionNames(t, builder.parentPath)
			if _, err := os.Lstat(builder.CandidatePath()); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("candidate after sync %d failure = %v", test.failSync, err)
			}
			builder.options.ops.sync = originalSync
			if _, err := builder.Build(); err != nil {
				t.Fatalf("retry after sync %d failure: %v", test.failSync, err)
			}
			requireNoGenerationConstructionNames(t, builder.parentPath)
		})
	}
}

func TestGenerationPublicationRefusesChangedSourceNamespace(t *testing.T) {
	path, _, _, builder := prepareGenerationSource(t)
	moved := path + ".moved"
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Build(); !errors.Is(err, ErrNamespaceChanged) {
		t.Fatalf("Build after source rename = %v, want ErrNamespaceChanged", err)
	}
	requireNoGenerationConstructionNames(t, builder.parentPath)
	if _, err := os.Lstat(builder.CandidatePath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate after source rename = %v", err)
	}
	if err := os.Rename(moved, path); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Build(); err != nil {
		t.Fatalf("Build after exact source restoration: %v", err)
	}
}

func TestGenerationReplayCompactsOverwrittenSourceSuffix(t *testing.T) {
	_, source, options := createTestStore(t)
	incarnation, err := source.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         1,
		HardState:       hard(2, 2),
		Entries: []*pb.Entry{
			entry(2, 2, "two"), entry(3, 2, "three"),
			entry(4, 2, "old-four"), entry(5, 2, "old-five"),
		},
		MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         2,
		HardState:       hard(3, 6),
		Entries: []*pb.Entry{
			entry(4, 3, "new-four"), entry(5, 3, "new-five"),
			entry(6, 3, "six"),
		},
		MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	builder, err := source.PrepareGeneration(GenerationInput{
		Snapshot:            testGenerationSnapshot(4, 3, "checkpoint-four"),
		RetentionCommitment: testRetentionCommitment(),
	}, testKey())
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Info.BaseIndex != 4 || candidate.Info.LastIndex != 6 ||
		candidate.Info.RetainedEntries != 2 {
		t.Fatalf("overwritten generation info = %+v", candidate.Info)
	}
	opened, err := openUnselectedGenerationForTest(
		candidate.Path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	entries, err := opened.Entries(5, 7, ^uint64(0))
	if err != nil || len(entries) != 2 ||
		string(entries[0].GetData()) != "new-five" || string(entries[1].GetData()) != "six" {
		t.Fatalf("compacted overwritten suffix = %+v, %v", entries, err)
	}
}

func TestGenerationSuffixProjectionFindsFutureBaseAfterEarlyTermChanges(t *testing.T) {
	_, source, options := createTestStore(t)
	incarnation, err := source.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	readys := []raftmodel.PersistBatch{
		{
			NodeIncarnation: incarnation, ReadyID: 1,
			HardState: hard(2, 1),
			Entries: []*pb.Entry{
				entry(2, 2, "old-two"), entry(3, 2, "old-three"),
			},
		},
		{
			NodeIncarnation: incarnation, ReadyID: 2,
			HardState: hard(3, 1),
			Entries: []*pb.Entry{
				entry(2, 3, "new-two"), entry(3, 3, "new-three"),
				entry(4, 3, "four"),
			},
		},
		{
			NodeIncarnation: incarnation, ReadyID: 3,
			HardState: hard(4, 6),
			Entries: []*pb.Entry{
				entry(5, 3, "five"), entry(6, 4, "checkpoint-six"),
			},
		},
		{
			NodeIncarnation: incarnation, ReadyID: 4,
			HardState: hard(4, 6),
			Entries:   []*pb.Entry{entry(7, 4, "retained-seven")},
			MustSync:  true,
		},
	}
	for position, ready := range readys {
		if err := source.Persist(ready); err != nil {
			t.Fatalf("Persist Ready %d: %v", position+1, err)
		}
	}
	requireGenerationCandidateMatchesSourceCut(
		t, source, options,
		testGenerationSnapshot(6, 4, "future-checkpoint-six"),
	)
}

func TestGenerationSuffixProjectionResetsWhenLastFallsBelowFutureBase(t *testing.T) {
	_, source, options := createTestStore(t)
	incarnation, err := source.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	initial := make([]*pb.Entry, 7)
	for position := range initial {
		initial[position] = entry(uint64(position+2), 2, "old")
	}
	readys := []raftmodel.PersistBatch{
		{
			NodeIncarnation: incarnation, ReadyID: 1,
			HardState: hard(2, 1), Entries: initial,
		},
		{
			NodeIncarnation: incarnation, ReadyID: 2,
			HardState: hard(3, 1),
			Entries: []*pb.Entry{
				entry(3, 3, "replacement-three"),
				entry(4, 3, "replacement-four"),
			},
		},
		{
			NodeIncarnation: incarnation, ReadyID: 3,
			HardState: hard(4, 6),
			Entries: []*pb.Entry{
				entry(5, 3, "replacement-five"),
				entry(6, 4, "replacement-checkpoint-six"),
				entry(7, 4, "replacement-seven"),
			},
			MustSync: true,
		},
	}
	for position, ready := range readys {
		if err := source.Persist(ready); err != nil {
			t.Fatalf("Persist Ready %d: %v", position+1, err)
		}
	}
	requireGenerationCandidateMatchesSourceCut(
		t, source, options,
		testGenerationSnapshot(6, 4, "regrown-checkpoint-six"),
	)
}

func TestGenerationSourceRejectsCommittedOverwriteCrossingCheckpointBase(t *testing.T) {
	_, source, options := createTestStore(t)
	incarnation, err := source.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 1,
		HardState: hard(2, 6),
		Entries: []*pb.Entry{
			entry(2, 2, "two"), entry(3, 2, "three"), entry(4, 2, "four"),
			entry(5, 2, "five"), entry(6, 2, "checkpoint-six"), entry(7, 2, "old-seven"),
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 2,
		HardState: hard(3, 6),
		Entries: []*pb.Entry{
			entry(5, 2, "five"),
			entry(6, 3, "changed-committed-checkpoint-six"),
			entry(7, 3, "new-seven"),
		},
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("committed overwrite crossing checkpoint base = %v, want ErrInvalid", err)
	}
	// The rejected Ready does not consume its identity. Reusing it with an
	// exact committed prefix and changing only the uncommitted suffix is legal.
	if err := source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 2,
		HardState: hard(3, 6),
		Entries: []*pb.Entry{
			entry(5, 2, "five"), entry(6, 2, "checkpoint-six"),
			entry(7, 3, "new-seven"),
		},
		MustSync: true,
	}); err != nil {
		t.Fatalf("legal overwrite crossing checkpoint base: %v", err)
	}
	requireGenerationCandidateMatchesSourceCut(
		t, source, options,
		testGenerationSnapshot(6, 2, "committed-checkpoint-six"),
	)
}

func TestGenerationSuffixProjectionAppliesHardStateOnlyReadysBelowFutureBase(t *testing.T) {
	_, source, options := createTestStore(t)
	incarnation, err := source.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 1,
		HardState: hard(2, 1),
		Entries: []*pb.Entry{
			entry(2, 2, "two"), entry(3, 2, "three"), entry(4, 2, "four"),
			entry(5, 2, "five"), entry(6, 2, "checkpoint-six"), entry(7, 2, "seven"),
		},
	}); err != nil {
		t.Fatal(err)
	}
	for offset, state := range []*pb.HardState{
		hard(3, 2), hard(4, 4), hard(5, 6),
	} {
		if err := source.Persist(raftmodel.PersistBatch{
			NodeIncarnation: incarnation,
			ReadyID:         uint64(offset + 2),
			HardState:       state,
			MustSync:        true,
		}); err != nil {
			t.Fatalf("Persist HardState-only Ready %d: %v", offset+2, err)
		}
	}
	requireGenerationCandidateMatchesSourceCut(
		t, source, options,
		testGenerationSnapshot(6, 2, "hard-state-checkpoint-six"),
	)
}

func TestGenerationReplayOverwritesAcrossFlushedAndPendingChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	options := testOptions()
	options.MaxRecords = uint64(MaxReadyEntries) + 16
	source, err := Create(path, testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	incarnation, err := source.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	first := make([]*pb.Entry, MaxReadyEntries)
	for position := range first {
		index := uint64(position + 2)
		first[position] = entry(index, 2, "old")
	}
	if err := source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 1,
		HardState: hard(2, 1), Entries: first,
	}); err != nil {
		t.Fatal(err)
	}
	lastOld := uint64(MaxReadyEntries) + 2
	if err := source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 2,
		HardState: hard(2, 1), Entries: []*pb.Entry{entry(lastOld, 2, "tail")},
	}); err != nil {
		t.Fatal(err)
	}
	overwriteFirst := uint64(MaxReadyEntries) - 6
	overwrite := make([]*pb.Entry, 13)
	for position := range overwrite {
		overwrite[position] = entry(overwriteFirst+uint64(position), 3, "new")
	}
	overwriteLast := overwrite[len(overwrite)-1].GetIndex()
	if err := source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 3,
		HardState: hard(3, overwriteLast), Entries: overwrite,
		MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	builder, err := source.PrepareGeneration(GenerationInput{
		Snapshot:            testGenerationSnapshot(1, 1, "checkpoint-one"),
		RetentionCommitment: testRetentionCommitment(),
	}, testKey())
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	generated, err := openUnselectedGenerationForTest(
		candidate.Path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer generated.Close()
	wantEntries, err := source.Entries(2, overwriteLast+1, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	gotEntries, err := generated.Entries(2, overwriteLast+1, ^uint64(0))
	if err != nil || len(gotEntries) != len(wantEntries) {
		t.Fatalf("generated entries = %d, %v, want %d", len(gotEntries), err, len(wantEntries))
	}
	for position := range wantEntries {
		if !entriesSemanticallyEqual(gotEntries[position], wantEntries[position]) {
			t.Fatalf("generated entry %d = %+v, want %+v",
				position, gotEntries[position], wantEntries[position])
		}
	}
	wantHard, _, err := source.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	gotHard, _, err := generated.InitialState()
	if err != nil || !reflect.DeepEqual(gotHard, wantHard) {
		t.Fatalf("generated HardState = %+v, %v, want %+v", gotHard, err, wantHard)
	}
	if generated.current.recordSequence != 4 ||
		candidate.Info.RetainedEntries != uint64(len(wantEntries)) {
		t.Fatalf("cross-chunk generation records/entries = %d/%d, want 4/%d",
			generated.current.recordSequence, candidate.Info.RetainedEntries, len(wantEntries))
	}
}

func TestGenerationReplayWritesOnlyProjectedSuffix(t *testing.T) {
	// One maximum-size entry batch makes prefix amplification observable.
	path := filepath.Join(t.TempDir(), "projected-suffix.wal")
	options := testOptions()
	projectedSource, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer projectedSource.Close()
	incarnation, err := projectedSource.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]*pb.Entry, MaxReadyEntries)
	for position := range entries {
		entries[position] = entry(uint64(position+2), 2, "prefix")
	}
	last := entries[len(entries)-1].GetIndex()
	if err := projectedSource.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 1,
		HardState: hard(2, last), Entries: entries, MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	baseIndex := last - 7
	builder, err := projectedSource.PrepareGeneration(GenerationInput{
		Snapshot:            testGenerationSnapshot(baseIndex, 2, "near-tail-checkpoint"),
		RetentionCommitment: testRetentionCommitment(),
	}, testKey())
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	originalWrite := builder.options.ops.writeAt
	writes := 0
	writtenBytes := 0
	builder.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		writes++
		writtenBytes += len(data)
		return originalWrite(file, data, offset)
	}
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Info.RetainedEntries != 7 || writes != 6 ||
		writtenBytes != 6*recordDamageGranule {
		t.Fatalf("projected suffix entries/writes/bytes = %d/%d/%d, want 7/6/%d",
			candidate.Info.RetainedEntries, writes, writtenBytes, 6*recordDamageGranule)
	}
}

func TestGenerationReplayCoalescesSequentialReadyRecords(t *testing.T) {
	_, source, options := createTestStore(t)
	incarnation, err := source.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	const readyCount = 128
	for readyID := uint64(1); readyID <= readyCount; readyID++ {
		index := readyID + 1
		if err := source.Persist(raftmodel.PersistBatch{
			NodeIncarnation: incarnation,
			ReadyID:         readyID,
			HardState:       hard(2, index),
			Entries:         []*pb.Entry{entry(index, 2, "x")},
			MustSync:        true,
		}); err != nil {
			t.Fatalf("Persist Ready %d: %v", readyID, err)
		}
	}
	builder, err := source.PrepareGeneration(GenerationInput{
		Snapshot:            testGenerationSnapshot(1, 1, "checkpoint-one"),
		RetentionCommitment: testRetentionCommitment(),
	}, testKey())
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	originalWrite := builder.options.ops.writeAt
	writes := 0
	builder.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		writes++
		return originalWrite(file, data, offset)
	}
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	opened, err := openUnselectedGenerationForTest(
		candidate.Path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if opened.current.recordSequence != 3 || candidate.Info.RetainedEntries != readyCount {
		t.Fatalf("coalesced generation records/entries = %d/%d, want 3/%d",
			opened.current.recordSequence, candidate.Info.RetainedEntries, readyCount)
	}
	// Construction writes the three initial regions, one final coalesced suffix
	// chunk, the seal, and the authoritative current slot.
	// In particular, tiny Ready records must not rewrite a growing chunk.
	if writes != 6 {
		t.Fatalf("coalesced generation writes = %d, want 6", writes)
	}
}

func TestGenerationBuildReclaimsOneDeterministicAbandonedStage(t *testing.T) {
	_, _, _, builder := prepareGenerationSource(t)
	lease, err := builder.acquireBuildLease()
	if err != nil {
		t.Fatal(err)
	}
	stage, err := builder.createStage()
	if err != nil {
		_ = lease.Close()
		t.Fatal(err)
	}
	stagePath := filepath.Join(builder.parentPath, builder.stageBase())
	if err := stage.Close(); err != nil {
		_ = lease.Close()
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(stagePath); err != nil || info.Size() != builder.options.maxFileBytes {
		t.Fatalf("abandoned stage = %+v, %v", info, err)
	}
	if _, err := builder.Build(); err != nil {
		t.Fatalf("Build after abandoned stage: %v", err)
	}
	if _, err := os.Lstat(stagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stage after settled Build = %v", err)
	}
	lockInfo, err := os.Stat(filepath.Join(builder.parentPath, builder.buildLockBase()))
	if err != nil || lockInfo.Size() != 0 {
		t.Fatalf("persistent generation build lock = %+v, %v", lockInfo, err)
	}
}

func TestGenerationBuildReclaimsPublishedStageLinkBeforeIdempotentValidation(t *testing.T) {
	_, _, _, builder := prepareGenerationSource(t)
	if err := builder.acquireSource(); err != nil {
		t.Fatal(err)
	}
	lease, err := builder.acquireBuildLease()
	if err != nil {
		_ = builder.releaseSource()
		t.Fatal(err)
	}
	stage, err := builder.createStage()
	if err != nil {
		_ = lease.Close()
		t.Fatal(err)
	}
	scratch, sourceHeader, err := builder.replaySourceIntoGeneration(stage)
	if err == nil {
		err = builder.finishGenerationScratch(stage, scratch, sourceHeader)
	}
	if err == nil {
		err = builder.publishStage(stage)
	}
	if closeErr := stage.Close(); err == nil {
		err = closeErr
	}
	if closeErr := lease.Close(); err == nil {
		err = closeErr
	}
	if closeErr := builder.releaseSource(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	stagePath := filepath.Join(builder.parentPath, builder.stageBase())
	stageInfo, err := os.Lstat(stagePath)
	if err != nil {
		t.Fatal(err)
	}
	candidateInfo, err := os.Lstat(builder.CandidatePath())
	if err != nil || !os.SameFile(stageInfo, candidateInfo) {
		t.Fatalf("published crash names do not share inode: stage=%+v candidate=%+v err=%v",
			stageInfo, candidateInfo, err)
	}
	candidate, err := builder.Build()
	if err != nil || candidate.Path != builder.CandidatePath() {
		t.Fatalf("idempotent Build after published-stage crash = %+v, %v", candidate, err)
	}
	if _, err := os.Lstat(stagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("published stage link after retry = %v", err)
	}
}

func TestGenerationBuildReclaimsDistinctStageBeforeReportingCandidateConflict(t *testing.T) {
	_, _, _, builder := prepareGenerationSource(t)
	lease, err := builder.acquireBuildLease()
	if err != nil {
		t.Fatal(err)
	}
	stage, err := builder.createStage()
	if err != nil {
		_ = lease.Close()
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		_ = lease.Close()
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	want := []byte("foreign-candidate-after-abandoned-stage")
	if err := os.WriteFile(builder.CandidatePath(), want, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Build(); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("Build with distinct stage and occupied candidate = %v", err)
	}
	stagePath := filepath.Join(builder.parentPath, builder.stageBase())
	if _, err := os.Lstat(stagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("distinct abandoned stage after conflict = %v", err)
	}
	got, err := os.ReadFile(builder.CandidatePath())
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("occupied candidate after stage reclaim = %q, %v", got, err)
	}
}

func TestGenerationBuildLeaseExcludesConcurrentBuilder(t *testing.T) {
	_, _, _, builder := prepareGenerationSource(t)
	lease, err := builder.acquireBuildLease()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Build(); !errors.Is(err, ErrLocked) {
		_ = lease.Close()
		t.Fatalf("Build behind live generation lease = %v, want ErrLocked", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Build(); err != nil {
		t.Fatalf("Build after generation lease release: %v", err)
	}
}

func TestGenerationPublicationSettlesLandedLinkError(t *testing.T) {
	_, _, _, builder := prepareGenerationSource(t)
	injected := errors.New("injected landed generation link error")
	builder.link = func(root *os.Root, oldName, newName string) error {
		if err := root.Link(oldName, newName); err != nil {
			return err
		}
		return injected
	}
	if _, err := builder.Build(); err != nil {
		t.Fatalf("Build with landed Link error: %v", err)
	}
	requireNoGenerationConstructionNames(t, builder.parentPath)
}

func TestGenerationPublicationClassifiesProvenAbsentLinkAsDefinite(t *testing.T) {
	_, _, _, builder := prepareGenerationSource(t)
	injected := errors.New("injected absent generation link error")
	builder.link = func(*os.Root, string, string) error { return injected }
	if _, err := builder.Build(); !errors.Is(err, injected) ||
		!errors.Is(err, ErrPersistenceDefinite) || errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("Build with absent Link = %v", err)
	}
	builder.link = linkGenerationName
	if _, err := os.Lstat(builder.CandidatePath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate after absent Link = %v", err)
	}
	requireNoGenerationConstructionNames(t, builder.parentPath)
	if _, err := builder.Build(); err != nil {
		t.Fatalf("retry after absent Link: %v", err)
	}
}

func TestGenerationValidationRejectsAdvancedCandidate(t *testing.T) {
	_, _, options, builder := prepareGenerationSource(t)
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	opened, err := openUnselectedGenerationForTest(
		candidate.Path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	incarnation, err := opened.BeginIncarnation()
	if err != nil {
		_ = opened.Close()
		t.Fatal(err)
	}
	if err := opened.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         1,
		HardState:       hard(4, 5),
		Entries:         []*pb.Entry{entry(5, 4, "advanced-five")},
		MustSync:        true,
	}); err != nil {
		_ = opened.Close()
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.ValidateCandidate(); !errors.Is(err, ErrGenerationCandidate) ||
		!errors.Is(err, ErrCorrupt) {
		t.Fatalf("ValidateCandidate after independent advance = %v", err)
	}
}

func TestGenerationBuildReclaimsStaleUnselectedCandidate(t *testing.T) {
	_, source, _, first := prepareGenerationSource(t)
	stale, err := first.Build()
	if err != nil {
		t.Fatal(err)
	}
	incarnation := source.CurrentIncarnation()
	if err := source.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         3,
		HardState:       hard(4, 5),
		Entries:         []*pb.Entry{entry(5, 4, "five")},
		MustSync:        true,
	}); err != nil {
		t.Fatal(err)
	}
	fresh, err := source.PrepareGeneration(GenerationInput{
		Snapshot:            testGenerationSnapshot(3, 2, "checkpoint-three"),
		RetentionCommitment: testRetentionCommitment(),
	}, testKey())
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	rebuilt, err := fresh.Build()
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.Path != stale.Path || rebuilt.Info.LastIndex != 5 ||
		rebuilt.Info.SourceCutDigest == stale.Info.SourceCutDigest {
		t.Fatalf("stale/rebuilt candidate = %+v / %+v", stale.Info, rebuilt.Info)
	}
	if validated, err := fresh.ValidateCandidate(); err != nil || validated != rebuilt {
		t.Fatalf("rebuilt candidate validation = %+v, %v", validated, err)
	}
}

func TestGenerationCandidateStrictReopenRejectsCiphertextDamage(t *testing.T) {
	_, _, options, builder := prepareGenerationSource(t)
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	opened, err := openUnselectedGenerationForTest(
		candidate.Path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	header := cloneGenerationHeader(opened.header)
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(candidate.Path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, recordPrefixBytes)
	if _, err := file.ReadAt(prefix, HeaderBytes); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	first, err := inspectRecordPrefix(prefix, header, builder.options)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	damageOffset := int64(HeaderBytes + first.total + recordPrefixBytes + len(header.keyID))
	var damaged [1]byte
	if _, err := file.ReadAt(damaged[:], damageOffset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	damaged[0] ^= 0x80
	if _, err := file.WriteAt(damaged[:], damageOffset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.ValidateCandidate(); !errors.Is(err, ErrGenerationCandidate) ||
		!errors.Is(err, ErrCorrupt) {
		t.Fatalf("ValidateCandidate after damage = %v", err)
	}
}

func TestPrepareGenerationRejectsUncertifiedOrInexactSources(t *testing.T) {
	_, unbegun, _ := createTestStore(t)
	valid := GenerationInput{
		Snapshot:            testGenerationSnapshot(1, 1, "base"),
		RetentionCommitment: testRetentionCommitment(),
	}
	if _, err := unbegun.PrepareGeneration(valid, testKey()); !errors.Is(err, ErrGenerationSource) {
		t.Fatalf("unbegun PrepareGeneration = %v", err)
	}

	_, source, _, _ := prepareGenerationSource(t)
	invalid := []struct {
		name  string
		input GenerationInput
		key   Key
		want  error
	}{
		{name: "zero-commitment", input: GenerationInput{Snapshot: valid.Snapshot}, key: testKey(), want: ErrGenerationSource},
		{name: "future-base", input: GenerationInput{
			Snapshot: testGenerationSnapshot(5, 3, "future"), RetentionCommitment: testRetentionCommitment(),
		}, key: testKey(), want: ErrGenerationSource},
		{name: "term-mismatch", input: GenerationInput{
			Snapshot: testGenerationSnapshot(3, 3, "wrong-term"), RetentionCommitment: testRetentionCommitment(),
		}, key: testKey(), want: ErrGenerationSource},
	}
	wrongKey := testKey()
	wrongKey.Material[0] ^= 0xff
	invalid = append(invalid, struct {
		name  string
		input GenerationInput
		key   Key
		want  error
	}{name: "wrong-key", input: valid, key: wrongKey, want: ErrKeyMismatch})
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if builder, err := source.PrepareGeneration(test.input, test.key); err == nil {
				_ = builder.Close()
				t.Fatal("PrepareGeneration accepted invalid input")
			} else if !errors.Is(err, test.want) {
				t.Fatalf("PrepareGeneration = %v, want %v", err, test.want)
			}
		})
	}
}

func TestGenerationSealCodecBindsEveryByte(t *testing.T) {
	_, _, _, builder := prepareGenerationSource(t)
	if _, err := builder.Build(); err != nil {
		t.Fatal(err)
	}
	encoded, err := marshalGenerationSeal(builder.seal)
	if err != nil {
		t.Fatal(err)
	}
	wantGolden := [sha256.Size]byte{
		0x97, 0xf0, 0x58, 0xc9, 0x4f, 0x00, 0xf4, 0xf5,
		0x3a, 0x22, 0xaf, 0xb0, 0x9e, 0x77, 0xc5, 0xa6,
		0xce, 0xf8, 0xfc, 0x43, 0xff, 0x85, 0x55, 0xa9,
		0x9a, 0x92, 0x36, 0xa0, 0x26, 0xab, 0x16, 0x30,
	}
	if got := sha256.Sum256(encoded); got != wantGolden {
		t.Fatalf("generation seal golden SHA-256 = %x, want %x", got, wantGolden)
	}
	decoded, err := unmarshalGenerationSeal(encoded)
	if err != nil || !equalGenerationSeal(decoded, builder.seal) {
		t.Fatalf("generation seal round trip = %+v, %v", decoded, err)
	}
	for offset := range encoded {
		damaged := append([]byte(nil), encoded...)
		damaged[offset] ^= 0x80
		if _, err := unmarshalGenerationSeal(damaged); err == nil {
			t.Fatalf("generation seal accepted damage at byte %d", offset)
		}
	}
}

func TestGenerationBuilderStreamsWorkspaceAndClearsSecrets(t *testing.T) {
	_, _, _, builder := prepareGenerationSource(t)
	if _, err := builder.Build(); err != nil {
		t.Fatal(err)
	}
	if builder.source != nil || !builder.loaded || builder.seal.suffixCount != 1 {
		t.Fatalf("streamed builder state = source %v loaded %v suffix %d",
			builder.source, builder.loaded, builder.seal.suffixCount)
	}
	if builder.header.dataKey == ([32]byte{}) || builder.header.nonceKey == ([32]byte{}) ||
		builder.key.Material == ([32]byte{}) {
		t.Fatal("builder secret witness is unexpectedly zero before Close")
	}
	if err := builder.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(builder.header, headerState{}) ||
		!reflect.DeepEqual(builder.current, currentState{}) ||
		builder.input.Snapshot != nil || builder.input.RetentionCommitment != ([32]byte{}) ||
		!reflect.DeepEqual(builder.seal, generationSeal{}) ||
		!reflect.DeepEqual(builder.key, Key{}) {
		t.Fatal("Builder.Close retained cut, workspace, or key material")
	}
	if err := builder.Close(); err != nil {
		t.Fatalf("idempotent Builder.Close = %v", err)
	}
}

func TestRetainedEntryCodecRoundTripIsDetached(t *testing.T) {
	entries := []*pb.Entry{
		entry(8, 4, "alpha"),
		entry(9, 5, "beta"),
	}
	encoded, err := marshalRetainedEntries(entries)
	if err != nil {
		t.Fatal(err)
	}
	wantGolden := [sha256.Size]byte{
		0x82, 0xdf, 0x72, 0x13, 0x32, 0x5c, 0x8f, 0xf0,
		0xb2, 0x97, 0x9c, 0xaa, 0x31, 0x17, 0x02, 0x07,
		0x98, 0x0b, 0xb4, 0x96, 0x41, 0x5a, 0x83, 0xb8,
		0x84, 0xd0, 0x6d, 0x34, 0x3f, 0x91, 0x0c, 0x0f,
	}
	if got := sha256.Sum256(encoded); got != wantGolden {
		t.Fatalf("retained entries golden SHA-256 = %x, want %x", got, wantGolden)
	}
	options, err := normalizeOptions(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := unmarshalRetainedEntries(encoded, options)
	if err != nil || len(decoded) != len(entries) {
		t.Fatalf("retained decode = %+v, %v", decoded, err)
	}
	for index := range entries {
		if !entriesSemanticallyEqual(decoded[index], entries[index]) {
			t.Fatalf("retained entry %d = %+v, want %+v", index, decoded[index], entries[index])
		}
	}
	decoded[0].Data[0] ^= 0xff
	if bytes.Equal(decoded[0].GetData(), entries[0].GetData()) {
		t.Fatal("retained decode aliases caller entry data")
	}
	if _, err := unmarshalRetainedEntries(encoded[:len(encoded)-1], options); err == nil {
		t.Fatal("retained decoder accepted truncation")
	}
}

func requireGenerationCandidateMatchesSourceCut(
	t *testing.T,
	source *Store,
	options Options,
	snapshot *pb.Snapshot,
) GenerationCandidate {
	t.Helper()
	baseIndex := snapshot.GetMetadata().GetIndex()
	baseTerm := snapshot.GetMetadata().GetTerm()
	sourceHard, _, err := source.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	sourceFirst, err := source.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	sourceLast, err := source.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if baseIndex < sourceFirst-1 || baseIndex > sourceLast {
		t.Fatalf("oracle base %d outside source [%d,%d]", baseIndex, sourceFirst-1, sourceLast)
	}
	baseObservedTerm, err := source.Term(baseIndex)
	if err != nil || baseObservedTerm != baseTerm {
		t.Fatalf("source base term = %d, %v, want %d", baseObservedTerm, err, baseTerm)
	}
	var retained []*pb.Entry
	if baseIndex < sourceLast {
		retained, err = source.Entries(baseIndex+1, sourceLast+1, ^uint64(0))
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(retained) > MaxReadyEntries || entriesFootprint(retained) > DefaultGenerationRetainedChunkBytes {
		t.Fatal("small-suffix generation oracle requires one retained record")
	}
	source.mu.RLock()
	captured := cloneGenerationCurrent(source.current)
	source.mu.RUnlock()

	wantSnapshot := cloneSnapshot(snapshot)
	builder, err := source.PrepareGeneration(GenerationInput{
		Snapshot:            snapshot,
		RetentionCommitment: testRetentionCommitment(),
	}, testKey())
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	wantRetainedBytes := uint64(entriesFootprint(retained))
	wantHash := newRetainedSuffixHash(baseIndex, baseTerm)
	for _, retainedEntry := range retained {
		wantHash.add(retainedEntry)
	}
	seal := builder.seal
	if seal.sourceCurrentGeneration != captured.generation ||
		seal.sourceWALEnd != uint64(captured.walEnd) ||
		seal.sourceRecordSequence != captured.recordSequence ||
		seal.sourceChainDigest != captured.chainDigest ||
		seal.sourceCurrentIncarnation != captured.currentIncarnation ||
		seal.sourceFirst != captured.first || seal.sourceLast != captured.last ||
		seal.baseIndex != baseIndex || seal.baseTerm != baseTerm ||
		seal.suffixFirst != baseIndex+1 || seal.suffixLast != sourceLast ||
		seal.suffixCount != uint64(len(retained)) || seal.suffixBytes != wantRetainedBytes ||
		seal.suffixDigest != wantHash.finish() ||
		!proto.Equal(seal.hard, sourceHard) {
		t.Fatalf("projected generation seal = %+v, captured current = %+v", seal, captured)
	}
	if candidate.Info != generationInfo(
		candidate.Path, seal, builder.candidateFileID, builder.candidateHeaderDigest,
	) ||
		candidate.Info.BaseIndex != baseIndex || candidate.Info.BaseTerm != baseTerm ||
		candidate.Info.LastIndex != sourceLast ||
		candidate.Info.RetainedEntries != uint64(len(retained)) ||
		candidate.Info.RetainedBytes != wantRetainedBytes ||
		candidate.Info.HardTerm != sourceHard.GetTerm() ||
		candidate.Info.HardVote != sourceHard.GetVote() ||
		candidate.Info.HardCommit != sourceHard.GetCommit() {
		t.Fatalf("projected candidate info = %+v", candidate.Info)
	}

	generated, err := openUnselectedGenerationForTest(
		candidate.Path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	generatedSnapshot, snapshotErr := generated.Snapshot()
	generatedHard, generatedConf, stateErr := generated.InitialState()
	generatedFirst, firstErr := generated.FirstIndex()
	generatedLast, lastErr := generated.LastIndex()
	generatedBaseTerm, termErr := generated.Term(baseIndex)
	var generatedRetained []*pb.Entry
	if baseIndex < sourceLast {
		generatedRetained, err = generated.Entries(baseIndex+1, sourceLast+1, ^uint64(0))
	}
	wantRecords := uint64(2)
	if len(retained) != 0 {
		wantRecords++
	}
	generatedRecords := generated.current.recordSequence
	generatedEnd := generated.current.walEnd
	closeErr := generated.Close()
	if snapshotErr != nil || stateErr != nil || firstErr != nil || lastErr != nil ||
		termErr != nil || err != nil || closeErr != nil {
		t.Fatalf("open projected candidate errors: snapshot=%v state=%v first=%v last=%v term=%v entries=%v close=%v",
			snapshotErr, stateErr, firstErr, lastErr, termErr, err, closeErr)
	}
	if !proto.Equal(generatedSnapshot, wantSnapshot) ||
		!proto.Equal(generatedHard, sourceHard) ||
		!proto.Equal(generatedConf, wantSnapshot.GetMetadata().GetConfState()) ||
		generatedFirst != baseIndex+1 || generatedLast != sourceLast ||
		generatedBaseTerm != baseTerm || generatedRecords != wantRecords ||
		len(generatedRetained) != len(retained) {
		t.Fatalf("projected candidate geometry: snapshot=%+v hard=%+v conf=%+v first=%d last=%d baseTerm=%d records=%d retained=%d",
			generatedSnapshot, generatedHard, generatedConf, generatedFirst, generatedLast,
			generatedBaseTerm, generatedRecords, len(generatedRetained))
	}
	for position := range retained {
		if !entriesSemanticallyEqual(generatedRetained[position], retained[position]) {
			t.Fatalf("projected retained entry %d = %+v, want %+v",
				position, generatedRetained[position], retained[position])
		}
	}

	before := make([]byte, generatedEnd)
	file, err := os.Open(candidate.Path)
	if err != nil {
		t.Fatalf("open exact candidate prefix: %v", err)
	}
	_, err = file.ReadAt(before, 0)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("read exact candidate prefix: %v", err)
	}
	validated, err := builder.ValidateCandidate()
	if err != nil || validated != candidate {
		t.Fatalf("ValidateCandidate = %+v, %v, want %+v", validated, err, candidate)
	}
	again, err := builder.Build()
	if err != nil || again != candidate {
		t.Fatalf("idempotent Build = %+v, %v, want %+v", again, err, candidate)
	}
	after := make([]byte, generatedEnd)
	file, err = os.Open(candidate.Path)
	if err != nil {
		t.Fatalf("reopen exact candidate prefix: %v", err)
	}
	_, err = file.ReadAt(after, 0)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("idempotent candidate prefix changed: equal=%v err=%v", bytes.Equal(after, before), err)
	}
	return candidate
}

func BenchmarkGenerationReplay4096TinyReadyRecords(b *testing.B) {
	root := b.TempDir()
	b.ReportAllocs()
	b.ReportMetric(MaxReadyEntries, "ready/op")
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		path := filepath.Join(root, "raft-"+strconv.Itoa(iteration)+".wal")
		options := testOptions()
		options.MaxRecords = uint64(MaxReadyEntries) + 16
		options.ops.sync = func(*os.File) error { return nil }
		source, err := Create(path, testIdentity(), testKey(), testBootstrap(), options)
		if err != nil {
			b.Fatal(err)
		}
		incarnation, err := source.BeginIncarnation()
		if err != nil {
			_ = source.Close()
			b.Fatal(err)
		}
		for readyID := uint64(1); readyID <= MaxReadyEntries; readyID++ {
			index := readyID + 1
			if err := source.Persist(raftmodel.PersistBatch{
				NodeIncarnation: incarnation, ReadyID: readyID,
				HardState: hard(2, index), Entries: []*pb.Entry{entry(index, 2, "x")},
			}); err != nil {
				_ = source.Close()
				b.Fatal(err)
			}
		}
		builder, err := source.PrepareGeneration(GenerationInput{
			Snapshot:            testGenerationSnapshot(1, 1, "checkpoint-one"),
			RetentionCommitment: testRetentionCommitment(),
		}, testKey())
		if err != nil {
			_ = source.Close()
			b.Fatal(err)
		}
		b.SetBytes(builder.current.walEnd - HeaderBytes)
		b.StartTimer()
		_, buildErr := builder.Build()
		b.StopTimer()
		closeErr := errors.Join(builder.Close(), source.Close())
		if buildErr != nil || closeErr != nil {
			b.Fatalf("Build/Close = %v/%v", buildErr, closeErr)
		}
	}
}

func TestGenerationRecordCryptoWorkspaceEquivalentAndAllocationBounded(t *testing.T) {
	_, store, _ := createTestStore(t)
	payload, err := marshalReadyPayload(raftmodel.PersistBatch{
		NodeIncarnation: 7,
		ReadyID:         11,
		HardState:       hard(9, 2),
		Entries:         []*pb.Entry{entry(2, 9, "workspace-equivalence")},
		MustSync:        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	envelopeInput := recordEnvelope{
		kind: recordKindReady, flags: 1,
		sequence: 19, incarnation: 7, readyID: 11,
		previous: sha256.Sum256([]byte("previous-record")),
		fileID:   store.header.fileID,
	}
	record, _, _, err := marshalRecord(
		envelopeInput.kind, envelopeInput.flags, envelopeInput.sequence,
		envelopeInput.incarnation, envelopeInput.readyID, envelopeInput.previous,
		payload, store.header, store.options,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantEnvelope, err := inspectRecordPrefix(
		record[:recordPrefixBytes], store.header, store.options,
	)
	if err != nil {
		t.Fatal(err)
	}
	workspace := newRecordDecodeWorkspace(store.header)
	gotEnvelope, err := inspectRecordPrefixWithWorkspace(
		record[:recordPrefixBytes], store.header, store.options, &workspace,
	)
	if err != nil || gotEnvelope != wantEnvelope {
		t.Fatalf("workspace envelope = %+v, %v, want %+v", gotEnvelope, err, wantEnvelope)
	}

	wantContext := recordTagContext(wantEnvelope)
	gotContext := putRecordTagContext(&workspace.tagContext, wantEnvelope)
	if !bytes.Equal(gotContext, wantContext) {
		t.Fatalf("workspace tag context = %x, want %x", gotContext, wantContext)
	}
	wantKey := deriveObjectKey(
		store.header.dataKey, "wal-record", wantEnvelope.sequence,
		wantEnvelope.payloadDigest,
	)
	gotKey := workspace.crypto.deriveObjectKey(
		"wal-record", wantEnvelope.sequence, wantEnvelope.payloadDigest,
	)
	wantNonce := deriveObjectNonce(
		store.header.nonceKey, "wal-record", wantEnvelope.sequence,
		wantEnvelope.payloadDigest,
	)
	gotNonce := workspace.crypto.deriveObjectNonce(
		"wal-record", wantEnvelope.sequence, wantEnvelope.payloadDigest,
	)
	wantTag := makeObjectTag(
		store.header.nonceKey, "wal-record", wantEnvelope.sequence,
		wantContext, payload,
	)
	gotTag := workspace.crypto.makeObjectTag(
		"wal-record", wantEnvelope.sequence, gotContext, payload,
	)
	if gotKey != wantKey || gotNonce != wantNonce || gotTag != wantTag {
		t.Fatalf("workspace crypto differs: key=%v nonce=%v tag=%v",
			gotKey == wantKey, gotNonce == wantNonce, gotTag == wantTag)
	}

	wantRecord, err := unmarshalInspectedRecord(
		record, wantEnvelope, store.header, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	gotRecord, err := unmarshalInspectedRecord(
		record, gotEnvelope, store.header, &workspace,
	)
	if err != nil || gotRecord.envelope != wantRecord.envelope ||
		gotRecord.digest != wantRecord.digest || !bytes.Equal(gotRecord.payload, wantRecord.payload) {
		t.Fatalf("workspace record differs: envelope=%v digest=%v payload=%v err=%v",
			gotRecord.envelope == wantRecord.envelope,
			gotRecord.digest == wantRecord.digest,
			bytes.Equal(gotRecord.payload, wantRecord.payload), err)
	}

	baselinePrefixAllocations := testing.AllocsPerRun(1000, func() {
		envelope, inspectErr := inspectRecordPrefix(
			record[:recordPrefixBytes], store.header, store.options,
		)
		if inspectErr != nil || envelope != wantEnvelope {
			panic("baseline prefix inspection failed")
		}
	})
	workspacePrefixAllocations := testing.AllocsPerRun(1000, func() {
		envelope, inspectErr := inspectRecordPrefixWithWorkspace(
			record[:recordPrefixBytes], store.header, store.options, &workspace,
		)
		if inspectErr != nil || envelope != wantEnvelope {
			panic("workspace prefix inspection failed")
		}
	})
	if workspacePrefixAllocations >= baselinePrefixAllocations {
		t.Fatalf("workspace prefix inspection allocations = %.2f, baseline %.2f",
			workspacePrefixAllocations, baselinePrefixAllocations)
	}
	baselineAllocations := testing.AllocsPerRun(1000, func() {
		if _, decodeErr := unmarshalInspectedRecord(
			record, wantEnvelope, store.header, nil,
		); decodeErr != nil {
			panic("baseline record decode failed")
		}
	})
	workspaceAllocations := testing.AllocsPerRun(1000, func() {
		if _, decodeErr := unmarshalInspectedRecord(
			record, wantEnvelope, store.header, &workspace,
		); decodeErr != nil {
			panic("workspace record decode failed")
		}
	})
	if workspaceAllocations >= baselineAllocations {
		t.Fatalf("workspace record decode allocations = %.2f, baseline %.2f",
			workspaceAllocations, baselineAllocations)
	}
	t.Logf("record decode allocations: workspace %.2f, baseline %.2f",
		workspaceAllocations, baselineAllocations)
	t.Logf("record prefix allocations: workspace %.2f, baseline %.2f",
		workspacePrefixAllocations, baselinePrefixAllocations)
}

func requireNoGenerationConstructionNames(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if filepath.Ext(name) == ".tmp" || filepath.Ext(name) == ".stage" ||
			bytes.Contains([]byte(name), []byte(".stage-")) {
			t.Fatalf("leaked generation construction name %q", name)
		}
	}
}
