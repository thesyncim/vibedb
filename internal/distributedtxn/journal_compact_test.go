package distributedtxn

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestJournalCompactReclaimsRetiredManifestAndReopensExact(t *testing.T) {
	descriptor, pages := buildManifest(t, 50_000)
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	coordinatorID := journalID(171)
	coordinatorRaw, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: coordinatorID, State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 11, RecoveryDeadline: 99, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.StageManifestCoordinator(coordinatorRaw); err != nil {
		t.Fatal(err)
	}
	participants := make([]ParticipantRef, MaxManifestPageParticipants)
	identities := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)
	for _, page := range pages {
		if _, err = j.StageManifestSegment(coordinatorID, page, participants, identities); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = j.SealManifestCoordinator(coordinatorID, 1, CoordinatorCommitted); err != nil {
		t.Fatal(err)
	}
	retired, err := j.TransitionCoordinator(coordinatorID, 2, CoordinatorRetired)
	if err != nil {
		t.Fatal(err)
	}

	participantID := journalID(191)
	participantRaw := journalParticipant(t, participantID, bytes.Repeat([]byte("m"), 8<<10))
	if _, err = j.StageParticipant(participantRaw); err != nil {
		t.Fatal(err)
	}
	if _, err = j.TransitionParticipant(participantID, 1, ParticipantAborted); err != nil {
		t.Fatal(err)
	}
	released, err := j.TransitionParticipant(participantID, 2, ParticipantReleased)
	if err != nil {
		t.Fatal(err)
	}
	fenceID := journalID(211)
	fenced, err := j.AbortParticipant(fenceID, 1)
	if err != nil {
		t.Fatal(err)
	}

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	opportunity := j.CompactionOpportunity()
	if !opportunity.Recommended || opportunity.RetainedBytes != uint64(before.Size()) ||
		opportunity.CompactedBytes == 0 || opportunity.ReclaimableBytes < MinimumJournalCompactionReclaimBytes {
		t.Fatalf("compaction opportunity = %+v", opportunity)
	}
	if err = j.Compact(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	usage := j.Usage()
	if after.Size() != int64(usage.RetainedBytes) {
		t.Fatalf("compacted file=%d accounting=%d", after.Size(), usage.RetainedBytes)
	}
	if after.Size()*32 >= before.Size() {
		t.Fatalf("compaction ratio = %d/%d, want <1/32", after.Size(), before.Size())
	}
	if usage.ActiveManifestBytes != 0 || usage.ControlReserveBytes != 0 {
		t.Fatalf("terminal compacted usage = %+v", usage)
	}
	if opportunity = j.CompactionOpportunity(); opportunity.Recommended ||
		opportunity.ReclaimableBytes != 0 || opportunity.CompactedBytes != usage.RetainedBytes {
		t.Fatalf("post-compact opportunity = %+v usage=%+v", opportunity, usage)
	}
	if got, err := j.StageManifestCoordinator(coordinatorRaw); err != nil || got != retired {
		t.Fatalf("retired coordinator retry = %+v, %v", got, err)
	}
	if got, err := j.StageParticipant(participantRaw); err != nil || got != released {
		t.Fatalf("released participant retry = %+v, %v", got, err)
	}
	if got, err := j.ParticipantStage(participantID); err != nil || !bytes.Equal(got, participantRaw) {
		t.Fatalf("terminal participant stage len=%d err=%v", len(got), err)
	}
	if got, err := j.ParticipantStage(fenceID); err != nil || got != nil {
		t.Fatalf("fence stage = %x, %v", got, err)
	}
	if _, err = j.ManifestPage(coordinatorID, 0, participants, identities); !errors.Is(err, ErrJournalNotFound) {
		t.Fatalf("retired manifest page = %v", err)
	}
	firstSize := after.Size()
	if err = j.Compact(); err != nil {
		t.Fatal(err)
	}
	second, err := os.Stat(path)
	if err != nil || second.Size() != firstSize {
		t.Fatalf("idempotent compact size=%d want=%d err=%v", second.Size(), firstSize, err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path+".compact", bytes.Repeat([]byte("orphan"), 1024), 0o600); err != nil {
		t.Fatal(err)
	}

	j, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if _, err = os.Stat(path + ".compact"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale compaction workspace survived reopen: %v", err)
	}
	if got, ok := j.CoordinatorStatus(coordinatorID); !ok || got != retired {
		t.Fatalf("reopened coordinator = %+v,%v", got, ok)
	}
	if got, ok := j.ParticipantStatus(participantID); !ok || got != released {
		t.Fatalf("reopened participant = %+v,%v", got, ok)
	}
	if got, ok := j.ParticipantStatus(fenceID); !ok || got != fenced {
		t.Fatalf("reopened fence = %+v,%v", got, ok)
	}
	if got, err := j.ParticipantStage(participantID); err != nil || !bytes.Equal(got, participantRaw) {
		t.Fatalf("reopened participant stage len=%d err=%v", len(got), err)
	}
	if got, err := j.ManifestCoordinator(coordinatorID); err != nil || got.Manifest != descriptor {
		t.Fatalf("reopened compact coordinator = %+v, %v", got, err)
	}
}

func TestJournalCompactPreservesActiveManifestContinuation(t *testing.T) {
	descriptor, pages := buildManifest(t, 4097)
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	id := journalID(231)
	raw, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: id, State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 17, RecoveryDeadline: 123, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	participants := make([]ParticipantRef, MaxManifestPageParticipants)
	identities := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.StageManifestCoordinator(raw); err != nil {
		t.Fatal(err)
	}
	if _, err = j.StageManifestSegment(id, pages[0], participants, identities); err != nil {
		t.Fatal(err)
	}
	if err = j.Compact(); err != nil {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}

	j, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range pages[1:] {
		if _, err = j.StageManifestSegment(id, page, participants, identities); err != nil {
			t.Fatal(err)
		}
	}
	committed, err := j.SealManifestCoordinator(id, 1, CoordinatorCommitted)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Compact(); err != nil {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}

	j, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if got, ok := j.CoordinatorStatus(id); !ok || got != committed {
		t.Fatalf("committed status = %+v,%v", got, ok)
	}
	reader, err := NewManifestReader(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint32(0); index < descriptor.SegmentCount; index++ {
		page, pageErr := j.ManifestPage(id, index, participants, identities)
		if pageErr != nil {
			t.Fatal(pageErr)
		}
		if _, pageErr = reader.OpenNext(page.Segment.Raw, participants, identities); pageErr != nil {
			t.Fatal(pageErr)
		}
	}
	if err = reader.Seal(); err != nil {
		t.Fatal(err)
	}
}

func TestJournalCompactPreservesCoordinatorRecoveryPulse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	id := journalID(241)
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.StageCoordinator(journalCoordinator(t, id)); err != nil {
		t.Fatal(err)
	}
	for pulse := uint8(1); pulse <= 2; pulse++ {
		if _, err = j.PulseCoordinator(id, 1, pulse); err != nil {
			t.Fatalf("pulse %d: %v", pulse, err)
		}
	}
	wantUsage := j.Usage()
	if opportunity := j.CompactionOpportunity(); opportunity.CompactedBytes == 0 {
		t.Fatalf("compaction opportunity = %+v", opportunity)
	}
	if err = j.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := j.Usage(); got != wantUsage {
		t.Fatalf("compacted usage = %+v, want %+v", got, wantUsage)
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
	if !found || status.Revision != 1 || status.CoordinatorState != CoordinatorStaging ||
		status.RecoveryPulse != 2 {
		t.Fatalf("reopened status = %+v, found=%v", status, found)
	}
	if got := j.Usage(); got != wantUsage {
		t.Fatalf("reopened usage = %+v, want %+v", got, wantUsage)
	}
	if status, err = j.PulseCoordinator(id, 1, 3); err != nil || status.RecoveryPulse != 3 {
		t.Fatalf("continued pulse = %+v, %v", status, err)
	}
}
