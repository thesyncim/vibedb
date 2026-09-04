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

func journalTarget(t testing.TB, id ID, mutation []byte) []byte {
	t.Helper()
	raw, err := AppendTarget(nil, TargetRecord{
		ID: id, State: TargetStaged, Revision: 1, RoutingVersion: 9,
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
		RecoveryDeadline: 3,
		Targets: []TransactionTargetRef{
			{Distribution: []byte("docs"), Shard: []byte("-40"), RoutingVersion: 9, AllocationGeneration: 4, OwnershipEpoch: 7,
				MutationDigest: journalDigest(40), State: TargetStaged},
			{Distribution: []byte("docs"), Shard: []byte("40-"), RoutingVersion: 9, AllocationGeneration: 5, OwnershipEpoch: 8,
				MutationDigest: journalDigest(80), State: TargetStaged},
		},
	})
	if err != nil {
		t.Fatalf("AppendCoordinator: %v", err)
	}
	return raw
}

func TestJournalCoordinatorRecoveryPulseIsDurableBoundedAndRevisionNeutral(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	id := journalID(91)
	if _, err = j.StageCoordinator(journalCoordinator(t, id)); err != nil {
		t.Fatal(err)
	}
	initialReserve := j.Usage().ControlReserveBytes
	for pulse := uint8(1); pulse <= 3; pulse++ {
		status, pulseErr := j.PulseCoordinator(id, 1, pulse)
		if pulseErr != nil || status.Revision != 1 || status.RecoveryPulse != pulse ||
			status.CoordinatorState != CoordinatorStaging {
			t.Fatalf("pulse %d status=%+v err=%v", pulse, status, pulseErr)
		}
		duplicate, duplicateErr := j.PulseCoordinator(id, 1, pulse)
		if duplicateErr != nil || duplicate != status {
			t.Fatalf("duplicate pulse %d status=%+v err=%v", pulse, duplicate, duplicateErr)
		}
		wantReserve := initialReserve - uint64(pulse)*uint64(journalEntryHeaderBytes+4)
		if got := j.Usage().ControlReserveBytes; got != wantReserve {
			t.Fatalf("pulse %d reserve=%d want=%d", pulse, got, wantReserve)
		}
	}
	if _, err = j.PulseCoordinator(id, 1, 4); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("pulse beyond immutable limit err=%v", err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	status, found := j.CoordinatorStatus(id)
	if !found || status.Revision != 1 || status.RecoveryPulse != 3 ||
		status.CoordinatorState != CoordinatorStaging {
		t.Fatalf("reopened pulse status=%+v found=%v", status, found)
	}
}

func TestJournalIdempotentStageTransitionAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	id := journalID(1)
	raw := journalTarget(t, id, []byte("mutation-body-without-json"))
	staged, err := j.StageTarget(raw)
	if err != nil {
		t.Fatalf("StageParticipant: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	stagedBytes := info.Size()
	duplicate, err := j.StageTarget(raw)
	if err != nil || duplicate != staged {
		t.Fatalf("duplicate stage = %+v, %v; want %+v", duplicate, err, staged)
	}
	info, _ = os.Stat(path)
	if info.Size() != stagedBytes {
		t.Fatalf("duplicate stage grew journal from %d to %d", stagedBytes, info.Size())
	}
	prepared, err := j.TransitionTarget(id, 1, TargetPrepared)
	if err != nil || prepared.Revision != 2 || prepared.TargetState != TargetPrepared {
		t.Fatalf("prepare = %+v, %v", prepared, err)
	}
	applied, err := j.TransitionTarget(id, 2, TargetApplied)
	if err != nil || applied.Revision != 3 || applied.TargetState != TargetApplied {
		t.Fatalf("apply = %+v, %v", applied, err)
	}
	if again, err := j.TransitionTarget(id, 2, TargetApplied); err != nil || again != applied {
		t.Fatalf("duplicate apply = %+v, %v; want %+v", again, err, applied)
	}
	if _, err := j.TransitionTarget(id, 2, TargetAborted); !errors.Is(err, ErrJournalConflict) {
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
	record, err := j.Target(id)
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
	if _, err := j.StageTarget(journalTarget(t, id, []byte("coordinator-local-mutation"))); err != nil {
		t.Fatalf("same ID coordinator participant: %v", err)
	}
	if coordinator, ok := j.CoordinatorStatus(id); !ok || coordinator.Role != RoleCoordinator {
		t.Fatalf("coordinator role = %+v,%v", coordinator, ok)
	}
	if target, ok := j.TargetStatus(id); !ok || target.Role != RoleTarget {
		t.Fatalf("participant role = %+v,%v", target, ok)
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
	targets := make([]TransactionTargetRef, MaxManifestPageTargets)
	identities := make([]byte, MaxManifestPageTargets*MaxShardIdentityBytes*2)
	coordinatorRaw, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: id, State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 3, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.StageManifestCoordinator(coordinatorRaw); err != nil {
		t.Fatal(err)
	}
	for i, raw := range pages {
		segment, err := j.StageManifestSegment(id, raw, targets, identities)
		if err != nil || segment.Index != uint32(i) {
			t.Fatalf("stage page %d = %+v, %v", i, segment, err)
		}
		if _, err := j.StageManifestSegment(id, raw, targets, identities); err != nil {
			t.Fatalf("idempotent page %d: %v", i, err)
		}
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
		page, err := j.ManifestPage(id, index, targets, identities)
		if err != nil {
			t.Fatalf("recover page %d: %v", index, err)
		}
		if _, err := reader.OpenNext(page.Segment.Raw, targets, identities); err != nil {
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
	targets := make([]TransactionTargetRef, MaxManifestPageTargets)
	identities := make([]byte, MaxManifestPageTargets*MaxShardIdentityBytes*2)
	if _, err := j.StageManifestSegment(id, pages[0], targets, identities); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("page before coordinator = %v", err)
	}
	raw, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: id, State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.StageManifestCoordinator(raw); err != nil {
		t.Fatal(err)
	}
	if _, err := j.StageManifestSegment(id, pages[1], targets, identities); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("sparse first page = %v", err)
	}
	for _, raw := range pages[:len(pages)-1] {
		if _, err := j.StageManifestSegment(id, raw, targets, identities); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := j.SealManifestCoordinator(id, 1, CoordinatorCommitted); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("commit with missing final page = %v", err)
	}
	aborted, err := j.SealManifestCoordinator(id, 1, CoordinatorAborted)
	if err != nil || aborted.CoordinatorState != CoordinatorAborted {
		t.Fatalf("abort incomplete manifest = %+v, %v", aborted, err)
	}
	if _, err := j.StageManifestSegment(id, pages[len(pages)-1], targets, identities); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("page after decision = %v", err)
	}
}

func TestJournalSegmentedManifestIncompleteBeginRecoversAndAborts(t *testing.T) {
	descriptor, pages := buildManifest(t, 4097)
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	id := journalID(121)
	coordinatorRaw, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: id, State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 3, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	targets := make([]TransactionTargetRef, MaxManifestPageTargets)
	identities := make([]byte, MaxManifestPageTargets*MaxShardIdentityBytes*2)
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.StageManifestCoordinator(coordinatorRaw); err != nil {
		t.Fatal(err)
	}
	if _, err := j.StageManifestSegment(id, pages[0], targets, identities); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if status, ok := j.CoordinatorStatus(id); !ok ||
		status.CoordinatorState != CoordinatorStaging {
		t.Fatalf("recovered incomplete status = %+v,%v", status, ok)
	}
	if _, err := j.SealManifestCoordinator(id, 1, CoordinatorCommitted); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("commit recovered incomplete manifest = %v", err)
	}
	aborted, err := j.SealManifestCoordinator(id, 1, CoordinatorAborted)
	if err != nil || aborted.CoordinatorState != CoordinatorAborted {
		t.Fatalf("abort recovered incomplete manifest = %+v, %v", aborted, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if status, ok := j.CoordinatorStatus(id); !ok || status != aborted {
		t.Fatalf("recovered abort = %+v,%v", status, ok)
	}
	if _, err := j.StageManifestSegment(id, pages[1], targets, identities); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("late page after recovered abort = %v", err)
	}
}

func TestJournalRefusesConcurrentShardWideTargets(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "transactions.vtj"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	first := journalID(31)
	if _, err := j.StageTarget(journalTarget(t, first, []byte("first"))); err != nil {
		t.Fatalf("stage first: %v", err)
	}
	second := journalID(32)
	if _, err := j.StageTarget(journalTarget(t, second, []byte("second"))); !errors.Is(err, ErrJournalBusy) {
		t.Fatalf("stage overlapping participant = %v, want ErrJournalBusy", err)
	}
	if _, err := j.TransitionTarget(first, 1, TargetAborted); err != nil {
		t.Fatalf("abort first: %v", err)
	}
	if _, err := j.StageTarget(journalTarget(t, second, []byte("second"))); err != nil {
		t.Fatalf("stage after release: %v", err)
	}
}

func TestJournalMissingTargetAbortFencesLateStage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	id := journalID(61)
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	aborted, err := j.AbortTarget(id, 1)
	if err != nil || aborted.TargetState != TargetAborted || aborted.Revision != 2 {
		t.Fatalf("abort tombstone = %+v, %v", aborted, err)
	}
	late := journalTarget(t, id, []byte("late"))
	if _, err := j.StageTarget(late); !errors.Is(err, ErrJournalConflict) {
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
	if status, ok := j.TargetStatus(id); !ok || status.TargetState != TargetAborted {
		t.Fatalf("recovered fence = %+v,%v", status, ok)
	}
	if _, err := j.StageTarget(late); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("late stage after restart = %v, want ErrJournalConflict", err)
	}
}

func TestJournalScopesAllowDisjointTargetsAndTraffic(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "transactions.vtj"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	stage := func(id ID, scope IntentScope) error {
		mutation := []byte("scoped")
		raw, err := AppendTarget(nil, TargetRecord{
			ID: id, State: TargetStaged, Revision: 1, RoutingVersion: 9,
			AllocationGeneration: 4, OwnershipEpoch: 7,
			CoordinatorDistribution: []byte("docs"), CoordinatorShard: []byte("-40"),
			CoordinatorAllocation: 4, CoordinatorRoutingVersion: 9, CoordinatorOwnershipEpoch: 7,
			BucketBits: 8, IntentScopes: []IntentScope{scope},
			MutationDigest: journalDigest(id[0] + 40), Mutation: mutation,
		})
		if err != nil {
			return err
		}
		_, err = j.StageTarget(raw)
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
	if err := j.WaitNoTargetBarrier(
		context.Background(), 8, []IntentScope{{Start: 30, End: 31}},
	); err != nil {
		t.Fatalf("disjoint traffic: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- j.WaitNoTargetBarrier(
			waitCtx, 8, []IntentScope{{Start: 10, End: 11}},
		)
	}()
	select {
	case err := <-done:
		t.Fatalf("overlapping traffic returned before release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := j.AbortTarget(first, 1); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("released traffic: %v", err)
	}
	if _, err := j.AbortTarget(second, 1); err != nil {
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
	if _, err := j.StageTarget(journalTarget(t, id, []byte("safe"))); err != nil {
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
	if status, ok := j.Status(id); !ok || status.TargetState != TargetStaged {
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
	if _, err := j.StageTarget(journalTarget(b, id, []byte("mutation"))); err != nil {
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
