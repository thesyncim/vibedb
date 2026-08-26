package distributedtxn

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func manifestPageIndex(t *testing.T, raw []byte, index uint32) []byte {
	t.Helper()
	mutated := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint32(mutated[8:12], index)
	binary.LittleEndian.PutUint32(
		mutated[len(mutated)-4:],
		crc32.Checksum(mutated[:len(mutated)-4], castagnoli),
	)
	return mutated
}

func stageManifestForHardening(
	t *testing.T,
	j *Journal,
	id ID,
	descriptor ManifestDescriptor,
) {
	t.Helper()
	raw, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: id, State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 1, RecoveryDeadline: 1, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.StageManifestCoordinator(raw); err != nil {
		t.Fatal(err)
	}
}

func TestJournalManifestLargePageIndexesFailClosed(t *testing.T) {
	descriptor, pages := buildManifest(t, 1)
	participants := make([]ParticipantRef, MaxManifestPageParticipants)
	identities := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)

	for _, index := range []uint32{1 << 31, math.MaxUint32} {
		t.Run(stringIndex(index), func(t *testing.T) {
			j, err := OpenJournal(filepath.Join(t.TempDir(), "transactions.vtj"))
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			id := journalID(byte(index >> 24))
			stageManifestForHardening(t, j, id, descriptor)
			mutated := manifestPageIndex(t, pages[0], index)
			if _, err := j.StageManifestSegment(id, mutated, participants, identities); !errors.Is(err, ErrJournalConflict) {
				t.Fatalf("StageManifestSegment index %#x = %v", index, err)
			}

			page, err := OpenManifestSegment(mutated, participants, identities)
			if err != nil {
				t.Fatal(err)
			}
			if err := j.replayManifestSegment(id, mutated, page); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("replayManifestSegment index %#x = %v", index, err)
			}

			j.manifests[id].segments = append(j.manifests[id].segments, pages[0])
			if _, err := j.ManifestPage(id, index, participants, identities); !errors.Is(err, ErrJournalNotFound) {
				t.Fatalf("ManifestPage index %#x = %v", index, err)
			}
		})
	}
}

func stringIndex(index uint32) string {
	var out [8]byte
	const hex = "0123456789abcdef"
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = hex[index&15]
		index >>= 4
	}
	return string(out[:])
}

func TestJournalRetiredManifestReclaimsPagesAcrossReplay(t *testing.T) {
	descriptor, pages := buildManifest(t, 4097)
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	id := journalID(201)
	participants := make([]ParticipantRef, MaxManifestPageParticipants)
	identities := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)

	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	stageManifestForHardening(t, j, id, descriptor)
	var retained uint64
	for _, raw := range pages {
		if _, err := j.StageManifestSegment(id, raw, participants, identities); err != nil {
			t.Fatal(err)
		}
		retained += uint64(len(raw))
	}
	if j.manifestBytes != retained {
		t.Fatalf("manifest bytes = %d, want %d", j.manifestBytes, retained)
	}
	if _, err := j.StageManifestSegment(id, pages[len(pages)-1], participants, identities); err != nil {
		t.Fatalf("idempotent final page: %v", err)
	}
	if j.manifestBytes != retained {
		t.Fatalf("duplicate changed manifest bytes to %d", j.manifestBytes)
	}
	if _, err := j.SealManifestCoordinator(id, 1, CoordinatorCommitted); err != nil {
		t.Fatal(err)
	}
	retired, err := j.TransitionCoordinator(id, 2, CoordinatorRetired)
	if err != nil {
		t.Fatal(err)
	}
	if j.manifestBytes != 0 || j.manifests[id] != nil {
		t.Fatalf("retired pages retained: bytes=%d manifest=%v", j.manifestBytes, j.manifests[id] != nil)
	}
	if status, ok := j.CoordinatorStatus(id); !ok || status != retired {
		t.Fatalf("retired tombstone = %+v,%v", status, ok)
	}
	if _, err := j.ManifestPage(id, 0, participants, identities); !errors.Is(err, ErrJournalNotFound) {
		t.Fatalf("retired ManifestPage = %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if j.manifestBytes != 0 || j.manifests[id] != nil {
		t.Fatalf("replay retained retired pages: bytes=%d manifest=%v", j.manifestBytes, j.manifests[id] != nil)
	}
	if status, ok := j.CoordinatorStatus(id); !ok || status != retired {
		t.Fatalf("replayed tombstone = %+v,%v", status, ok)
	}
	if again, err := j.TransitionCoordinator(id, 2, CoordinatorRetired); err != nil || again != retired {
		t.Fatalf("idempotent retire = %+v, %v", again, err)
	}
}

func TestJournalByteAdmissionsAreFiniteAndNonMutating(t *testing.T) {
	descriptor, pages := buildManifest(t, 1)
	path := filepath.Join(t.TempDir(), "transactions.vtj")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	id := journalID(211)
	stageManifestForHardening(t, j, id, descriptor)
	participants := make([]ParticipantRef, MaxManifestPageParticipants)
	identities := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)
	j.manifestBytes = MaxActiveManifestBytes
	if _, err := j.StageManifestSegment(id, pages[0], participants, identities); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("active manifest admission = %v", err)
	}
	if len(j.manifests[id].segments) != 0 || j.manifestBytes != MaxActiveManifestBytes {
		t.Fatal("failed manifest admission mutated resident accounting")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	j.retainedBytes = MaxRetainedJournalBytes - journalEntryHeaderBytes - 3
	if err := j.writeEntryLocked(journalCoordinatorTransition, 0, 1, id, nil); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("retained journal admission = %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != info.Size() {
		t.Fatalf("failed disk admission grew journal from %d to %d", info.Size(), after.Size())
	}
}

func TestJournalControlReserveCompletesCleanupAtAdmissionEdge(t *testing.T) {
	descriptor, pages := buildManifest(t, 4097)
	j, err := OpenJournal(filepath.Join(t.TempDir(), "transactions.vtj"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	id := journalID(221)
	stageManifestForHardening(t, j, id, descriptor)
	participants := make([]ParticipantRef, MaxManifestPageParticipants)
	identities := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)
	if _, err := j.StageManifestSegment(id, pages[0], participants, identities); err != nil {
		t.Fatal(err)
	}
	usage := j.Usage()
	wantReserve := coordinatorControlReserve(CoordinatorStaging, true)
	if usage.ControlReserveBytes != wantReserve || usage.ActiveManifestBytes != uint64(len(pages[0])) {
		t.Fatalf("staging usage = %+v, reserve want %d", usage, wantReserve)
	}

	// Put data admission exactly at the boundary after preserving all bytes
	// needed for abort plus retirement. Another page must be refused without
	// consuming that control reserve.
	j.retainedBytes = MaxRetainedJournalBytes - j.controlReserve
	if _, err := j.StageManifestSegment(id, pages[1], participants, identities); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("page at reserved edge = %v", err)
	}
	if j.controlReserve != wantReserve || j.manifestBytes != uint64(len(pages[0])) {
		t.Fatalf("failed edge admission changed accounting: %+v", j.Usage())
	}
	aborted, err := j.SealManifestCoordinator(id, 1, CoordinatorAborted)
	if err != nil {
		t.Fatalf("reserved abort = %v", err)
	}
	if aborted.Revision != 2 || j.controlReserve != coordinatorControlReserve(CoordinatorAborted, true) {
		t.Fatalf("abort accounting = %+v usage=%+v", aborted, j.Usage())
	}
	retired, err := j.TransitionCoordinator(id, 2, CoordinatorRetired)
	if err != nil {
		t.Fatalf("reserved retire = %v", err)
	}
	if retired.Revision != 3 || j.retainedBytes != MaxRetainedJournalBytes ||
		j.controlReserve != 0 || j.manifestBytes != 0 || j.manifests[id] != nil {
		t.Fatalf("retired edge accounting = %+v usage=%+v", retired, j.Usage())
	}
}
