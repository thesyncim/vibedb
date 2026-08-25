package distributedtxn

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func journalID(seed byte) ID {
	var id ID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func journalDigest(seed byte) Digest {
	var digest Digest
	for i := range digest {
		digest[i] = seed + byte(i)
	}
	return digest
}

func journalParticipant(t testing.TB, id ID, mutation []byte) []byte {
	t.Helper()
	raw, err := AppendParticipant(nil, ParticipantRecord{
		ID: id, State: ParticipantStaged, Revision: 1, RoutingVersion: 9,
		AllocationGeneration: 4, OwnershipEpoch: 7,
		CoordinatorDistribution: []byte("docs"), CoordinatorShard: []byte("-40"), CoordinatorAllocation: 4,
		CoordinatorRoutingVersion: 9, CoordinatorOwnershipEpoch: 7,
		MutationDigest: journalDigest(id[0] + 40), Mutation: mutation,
	})
	if err != nil {
		t.Fatalf("AppendParticipant: %v", err)
	}
	return raw
}

func journalCoordinator(t testing.TB, id ID) []byte {
	t.Helper()
	raw, err := AppendCoordinator(nil, CoordinatorRecord{
		ID: id, State: CoordinatorStaging, Revision: 1, CatalogGeneration: 9,
		RecoveryDeadline: 1234,
		Participants: []ParticipantRef{
			{Distribution: []byte("docs"), Shard: []byte("-40"), RoutingVersion: 9, AllocationGeneration: 4, OwnershipEpoch: 7,
				MutationDigest: journalDigest(40), State: ParticipantStaged},
			{Distribution: []byte("docs"), Shard: []byte("40-"), RoutingVersion: 9, AllocationGeneration: 5, OwnershipEpoch: 8,
				MutationDigest: journalDigest(80), State: ParticipantStaged},
		},
	})
	if err != nil {
		t.Fatalf("AppendCoordinator: %v", err)
	}
	return raw
}

func TestJournalIdempotentStageTransitionAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	id := journalID(1)
	raw := journalParticipant(t, id, []byte("mutation-body-without-json"))
	staged, err := j.StageParticipant(raw)
	if err != nil {
		t.Fatalf("StageParticipant: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	stagedBytes := info.Size()
	duplicate, err := j.StageParticipant(raw)
	if err != nil || duplicate != staged {
		t.Fatalf("duplicate stage = %+v, %v; want %+v", duplicate, err, staged)
	}
	info, _ = os.Stat(path)
	if info.Size() != stagedBytes {
		t.Fatalf("duplicate stage grew journal from %d to %d", stagedBytes, info.Size())
	}
	prepared, err := j.TransitionParticipant(id, 1, ParticipantPrepared)
	if err != nil || prepared.Revision != 2 || prepared.ParticipantState != ParticipantPrepared {
		t.Fatalf("prepare = %+v, %v", prepared, err)
	}
	applied, err := j.TransitionParticipant(id, 2, ParticipantApplied)
	if err != nil || applied.Revision != 3 || applied.ParticipantState != ParticipantApplied {
		t.Fatalf("apply = %+v, %v", applied, err)
	}
	if again, err := j.TransitionParticipant(id, 2, ParticipantApplied); err != nil || again != applied {
		t.Fatalf("duplicate apply = %+v, %v; want %+v", again, err, applied)
	}
	if _, err := j.TransitionParticipant(id, 2, ParticipantAborted); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("conflicting transition = %v, want ErrJournalConflict", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	j, err = OpenJournal(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer j.Close()
	status, ok := j.Status(id)
	if !ok || status != applied {
		t.Fatalf("recovered status = %+v,%v, want %+v,true", status, ok, applied)
	}
	record, err := j.Participant(id)
	if err != nil || !bytes.Equal(record.Mutation, []byte("mutation-body-without-json")) {
		t.Fatalf("recovered participant = %+v, %v", record, err)
	}
}

func TestJournalCoordinatorAndIdentityConflict(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "transactions.vtj"))
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	defer j.Close()
	id := journalID(7)
	raw := journalCoordinator(t, id)
	if _, err := j.StageCoordinator(raw); err != nil {
		t.Fatalf("StageCoordinator: %v", err)
	}
	if _, err := j.StageParticipant(journalParticipant(t, id, []byte("coordinator-local-mutation"))); err != nil {
		t.Fatalf("same ID coordinator participant: %v", err)
	}
	if coordinator, ok := j.CoordinatorStatus(id); !ok || coordinator.Role != RoleCoordinator {
		t.Fatalf("coordinator role = %+v,%v", coordinator, ok)
	}
	if participant, ok := j.ParticipantStatus(id); !ok || participant.Role != RoleParticipant {
		t.Fatalf("participant role = %+v,%v", participant, ok)
	}
	committed, err := j.TransitionCoordinator(id, 1, CoordinatorCommitted)
	if err != nil || committed.CoordinatorState != CoordinatorCommitted {
		t.Fatalf("commit = %+v, %v", committed, err)
	}
	retired, err := j.TransitionCoordinator(id, 2, CoordinatorRetired)
	if err != nil || retired.Revision != 3 || retired.CoordinatorState != CoordinatorRetired {
		t.Fatalf("retire = %+v, %v", retired, err)
	}
}

func TestJournalSegmentedManifestSealPagedRecovery(t *testing.T) {
	descriptor, pages := buildManifest(t, 100_000)
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	id := journalID(101)
	participants := make([]ParticipantRef, MaxManifestPageParticipants)
	identities := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)
	for i, raw := range pages {
		segment, err := j.StageManifestSegment(id, raw, participants, identities)
		if err != nil || segment.Index != uint32(i) {
			t.Fatalf("stage page %d = %+v, %v", i, segment, err)
		}
		if _, err := j.StageManifestSegment(id, raw, participants, identities); err != nil {
			t.Fatalf("idempotent page %d: %v", i, err)
		}
	}
	coordinatorRaw, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: id, State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 1234, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.StageManifestCoordinator(coordinatorRaw); err != nil {
		t.Fatal(err)
	}
	if _, err := j.TransitionCoordinator(id, 1, CoordinatorCommitted); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("unbound ordinary decision = %v", err)
	}
	committed, err := j.SealManifestCoordinator(id, 1, CoordinatorCommitted)
	if err != nil || committed.CoordinatorState != CoordinatorCommitted || committed.Revision != 2 {
		t.Fatalf("sealed commit = %+v, %v", committed, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	status, ok := j.CoordinatorStatus(id)
	if !ok || status != committed {
		t.Fatalf("recovered status = %+v,%v", status, ok)
	}
	coordinator, err := j.ManifestCoordinator(id)
	if err != nil || coordinator.Manifest != descriptor {
		t.Fatalf("recovered coordinator = %+v, %v", coordinator, err)
	}
	reader, err := NewManifestReader(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint32(0); index < descriptor.SegmentCount; index++ {
		page, err := j.ManifestPage(id, index, participants, identities)
		if err != nil {
			t.Fatalf("recover page %d: %v", index, err)
		}
		if _, err := reader.OpenNext(page.Segment.Raw, participants, identities); err != nil {
			t.Fatalf("verify page %d: %v", index, err)
		}
	}
	if err := reader.Seal(); err != nil {
		t.Fatal(err)
	}
}

func TestJournalSegmentedManifestRefusesMissingAndTrailingPages(t *testing.T) {
	descriptor, pages := buildManifest(t, 4097)
	j, err := OpenJournal(filepath.Join(t.TempDir(), "transactions.vtj"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	id := journalID(111)
	participants := make([]ParticipantRef, MaxManifestPageParticipants)
	identities := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)
	if _, err := j.StageManifestSegment(id, pages[1], participants, identities); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("sparse first page = %v", err)
	}
	for _, raw := range pages[:len(pages)-1] {
		if _, err := j.StageManifestSegment(id, raw, participants, identities); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: id, State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.StageManifestCoordinator(raw); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("missing final page = %v", err)
	}
}

func TestJournalRefusesConcurrentShardWideParticipants(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "transactions.vtj"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	first := journalID(31)
	if _, err := j.StageParticipant(journalParticipant(t, first, []byte("first"))); err != nil {
		t.Fatalf("stage first: %v", err)
	}
	second := journalID(32)
	if _, err := j.StageParticipant(journalParticipant(t, second, []byte("second"))); !errors.Is(err, ErrJournalBusy) {
		t.Fatalf("stage overlapping participant = %v, want ErrJournalBusy", err)
	}
	if _, err := j.TransitionParticipant(first, 1, ParticipantAborted); err != nil {
		t.Fatalf("abort first: %v", err)
	}
	if _, err := j.StageParticipant(journalParticipant(t, second, []byte("second"))); err != nil {
		t.Fatalf("stage after release: %v", err)
	}
}

func TestJournalMissingParticipantAbortFencesLateStage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	id := journalID(61)
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	aborted, err := j.AbortParticipant(id, 1)
	if err != nil || aborted.ParticipantState != ParticipantAborted || aborted.Revision != 2 {
		t.Fatalf("abort tombstone = %+v, %v", aborted, err)
	}
	late := journalParticipant(t, id, []byte("late"))
	if _, err := j.StageParticipant(late); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("late stage = %v, want ErrJournalConflict", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if status, ok := j.ParticipantStatus(id); !ok || status.ParticipantState != ParticipantAborted {
		t.Fatalf("recovered fence = %+v,%v", status, ok)
	}
	if _, err := j.StageParticipant(late); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("late stage after restart = %v, want ErrJournalConflict", err)
	}
}

func TestJournalScopesAllowDisjointParticipantsAndTraffic(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "transactions.vtj"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	stage := func(id ID, scope IntentScope) error {
		mutation := []byte("scoped")
		raw, err := AppendParticipant(nil, ParticipantRecord{
			ID: id, State: ParticipantStaged, Revision: 1, RoutingVersion: 9,
			AllocationGeneration: 4, OwnershipEpoch: 7,
			CoordinatorDistribution: []byte("docs"), CoordinatorShard: []byte("-40"),
			CoordinatorAllocation: 4, CoordinatorRoutingVersion: 9, CoordinatorOwnershipEpoch: 7,
			BucketBits: 8, IntentScopes: []IntentScope{scope},
			MutationDigest: journalDigest(id[0] + 40), Mutation: mutation,
		})
		if err != nil {
			return err
		}
		_, err = j.StageParticipant(raw)
		return err
	}
	first, second, overlapping := journalID(81), journalID(82), journalID(83)
	if err := stage(first, IntentScope{Start: 10, End: 12}); err != nil {
		t.Fatal(err)
	}
	if err := stage(second, IntentScope{Start: 20, End: 22}); err != nil {
		t.Fatalf("disjoint participant: %v", err)
	}
	if err := stage(overlapping, IntentScope{Start: 11, End: 13}); !errors.Is(err, ErrJournalBusy) {
		t.Fatalf("overlapping participant = %v, want ErrJournalBusy", err)
	}
	if err := j.WaitNoParticipantBarrier(
		context.Background(), 8, []IntentScope{{Start: 30, End: 31}},
	); err != nil {
		t.Fatalf("disjoint traffic: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- j.WaitNoParticipantBarrier(
			waitCtx, 8, []IntentScope{{Start: 10, End: 11}},
		)
	}()
	select {
	case err := <-done:
		t.Fatalf("overlapping traffic returned before release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := j.AbortParticipant(first, 1); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("released traffic: %v", err)
	}
	if _, err := j.AbortParticipant(second, 1); err != nil {
		t.Fatal(err)
	}
}

func TestJournalDropsTornFinalEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	id := journalID(9)
	if _, err := j.StageParticipant(journalParticipant(t, id, []byte("safe"))); err != nil {
		t.Fatalf("StageParticipant: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := file.Write([]byte{'V', 'T', 'J'}); err != nil {
		t.Fatalf("append torn entry: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close torn file: %v", err)
	}
	j, err = OpenJournal(path)
	if err != nil {
		t.Fatalf("recover torn tail: %v", err)
	}
	defer j.Close()
	if status, ok := j.Status(id); !ok || status.ParticipantState != ParticipantStaged {
		t.Fatalf("status after torn recovery = %+v,%v", status, ok)
	}
}

func BenchmarkJournalStatus(b *testing.B) {
	path := filepath.Join(b.TempDir(), "transactions.vtj")
	j, err := OpenJournal(path)
	if err != nil {
		b.Fatal(err)
	}
	defer j.Close()
	id := journalID(3)
	if _, err := j.StageParticipant(journalParticipant(b, id, []byte("mutation"))); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, ok := j.Status(id); !ok {
			b.Fatal("missing status")
		}
	}
}
