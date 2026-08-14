package durable

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// journalBatchAtCapacity builds an admitted 64-entry batch whose ordinary
// kind-3 envelope ends exactly at capacity. The conditional kind-4 envelope for
// the same entries is therefore one sector larger solely because of its fixed
// conditional header.
func journalBatchAtCapacity(
	t testing.TB, sectorSize uint32, capacity uint64,
) []storeio.RecoveryBatchEntry {
	t.Helper()
	const entryCount = 64
	if capacity > uint64(^uint(0)>>1) {
		t.Fatalf("journal capacity %d exceeds native int", capacity)
	}
	fixed := storeio.RecoveryJournalRecordPrefixSize +
		storeio.RecoveryJournalRecordTrailerSize +
		entryCount*storeio.RecoveryBatchEntryHeaderSize + entryCount
	valueBytes := int(capacity) - fixed
	if valueBytes < entryCount {
		t.Fatalf("journal capacity %d is too small for boundary batch", capacity)
	}
	entries := make([]storeio.RecoveryBatchEntry, entryCount)
	perEntry, extra := valueBytes/entryCount, valueBytes%entryCount
	for i := range entries {
		size := perEntry
		if i < extra {
			size++
		}
		if size < len(`{"v":""}`) {
			t.Fatalf("entry value size %d cannot hold a JSON document", size)
		}
		value := make([]byte, size)
		copy(value, `{"v":"`)
		for at := len(`{"v":"`); at < size-2; at++ {
			value[at] = 'x'
		}
		copy(value[size-2:], `"}`)
		entries[i] = storeio.RecoveryBatchEntry{
			Kind:  storeio.RecoveryRecordKindPut,
			Key:   []byte{byte(i + 1)},
			Value: value,
		}
	}
	if got := storeio.RecoveryBatchRecordPaddedSize(
		sectorSize, entries,
	); got != int(capacity) {
		t.Fatalf("ordinary batch bytes=%d, want capacity %d", got, capacity)
	}
	if got, want := storeio.RecoveryConditionalBatchRecordPaddedSize(
		sectorSize, entries,
	), int(capacity)+int(sectorSize); got != want {
		t.Fatalf("conditional batch bytes=%d, want one sector beyond capacity %d", got, want)
	}
	return entries
}

func TestConditionalJournalRoomGrowsAcknowledgementLanes(t *testing.T) {
	tests := []struct {
		name     string
		options  func() Options
		wantSync bool
		wantAck  bool
	}{
		{
			name:     "sync",
			options:  syncPrimaryJournalTestOptions,
			wantSync: true,
		},
		{
			name: "buffered-journal-ack",
			options: func() Options {
				return journalTestOptions(CheckpointPowerSafe)
			},
			wantAck: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := test.options()
			// Keep initial reservation at the bounded inline cadence while making
			// every entry in the exact-capacity batch valid under the collection's
			// document and batch admission profile.
			options.InlineValueBytes = 64
			options.MaxDocumentBytes = 1 << 20
			options.MaxBatchDocuments = 64
			options.MaxBatchBytes = 4 << 20
			coll, file, _ := openPrimaryBatchStore(t, options)
			defer file.Close()
			defer coll.Close()

			coll.writer.Lock()
			defer coll.writer.Unlock()
			if got := coll.syncJournalLane(); got != test.wantSync {
				t.Fatalf("sync journal lane=%v, want %v", got, test.wantSync)
			}
			if got := coll.bufferedJournalAckLane(); got != test.wantAck {
				t.Fatalf("buffered journal ack lane=%v, want %v", got, test.wantAck)
			}
			header := coll.journal.Header()
			if header.SealedCapacity {
				t.Fatal("ordinary acknowledgement journal is unexpectedly sealed")
			}
			if coll.journal.Cursor() != 0 {
				t.Fatalf("initial journal cursor=%d, want 0", coll.journal.Cursor())
			}
			entries := journalBatchAtCapacity(
				t, header.SectorSize, header.Capacity,
			)
			ordinary, err := coll.journal.PrepareBatch(entries)
			if err != nil {
				t.Fatalf("prepare ordinary batch: %v", err)
			}
			conditional, err := coll.journal.PrepareConditionalBatch(entries)
			if err != nil {
				t.Fatalf("prepare conditional batch: %v", err)
			}
			if !coll.journal.PreparedBatchFits(ordinary) {
				t.Fatal("ordinary kind-3 boundary batch does not fit")
			}
			if coll.journal.PreparedBatchFits(conditional) {
				t.Fatal("conditional kind-4 boundary batch unexpectedly fits before growth")
			}
			batch := coll.fileWriteBatch()
			defer coll.releaseFileWriteBatch(batch)
			for i := range entries {
				if err := batch.Put(entries[i].Key, entries[i].Value); err != nil {
					t.Fatalf("batch put %d: %v", i, err)
				}
			}
			beforeCheckpoints := coll.automaticCheckpoints.Load()
			staged, err := coll.stagePrimaryBatchConditionalLocked(batch)
			if err != nil {
				t.Fatalf("stage conditional batch: %v", err)
			}
			if !staged.live {
				t.Fatal("conditional batch staged no live mutations")
			}
			defer coll.unwindStagedPrimaryBatch(&staged)
			stagedPlan, err := coll.journal.PrepareConditionalBatch(
				coll.batchJournalEntries,
			)
			if err != nil {
				t.Fatalf("prepare staged conditional batch: %v", err)
			}
			grown := coll.journal.Header()
			if grown.Capacity <= header.Capacity {
				t.Fatalf("conditional room left capacity=%d, want growth past %d",
					grown.Capacity, header.Capacity)
			}
			if !coll.journal.PreparedBatchFits(conditional) {
				t.Fatalf("conditional batch still does not fit grown capacity %d",
					grown.Capacity)
			}
			if !coll.journal.PreparedBatchFits(stagedPlan) {
				t.Fatalf("staged conditional batch does not fit grown capacity %d",
					grown.Capacity)
			}
			if got := coll.automaticCheckpoints.Load(); got != beforeCheckpoints {
				t.Fatalf("empty conditional growth used checkpoint: %d -> %d",
					beforeCheckpoints, got)
			}
			if coll.journal.Cursor() != 0 {
				t.Fatalf("conditional growth moved cursor to %d", coll.journal.Cursor())
			}
		})
	}
}

func TestOrdinaryBufferedAckBatchKeepsPhysicalFallback(t *testing.T) {
	options := journalTestOptions(CheckpointPowerSafe)
	options.InlineValueBytes = 64
	options.MaxDocumentBytes = 1 << 20
	options.MaxBatchDocuments = 64
	options.MaxBatchBytes = 4 << 20
	coll, file, _ := openPrimaryBatchStore(t, options)
	defer file.Close()
	defer coll.Close()

	if !coll.bufferedJournalAckLane() {
		t.Fatal("expected buffered journal acknowledgement lane")
	}
	header := coll.journal.Header()
	entries := journalBatchAtCapacity(
		t, header.SectorSize, header.Capacity+uint64(header.SectorSize),
	)
	if coll.journal.FitsBatch(entries) {
		t.Fatal("oversized ordinary kind-3 batch unexpectedly fits")
	}
	beforeGeneration := coll.Generation()
	if err := coll.Update(func(batch *WriteBatch) error {
		for i := range entries {
			if err := batch.Put(entries[i].Key, entries[i].Value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("ordinary buffered oversized Update: %v", err)
	}
	if got := coll.Generation(); got <= beforeGeneration {
		t.Fatalf("generation=%d, want newer than %d", got, beforeGeneration)
	}
	if got, want := coll.committer.DurableGeneration(), coll.Generation(); got != want {
		t.Fatalf("physical fallback durable generation=%d, want %d", got, want)
	}
	if got := coll.journal.Header().Capacity; got != header.Capacity {
		t.Fatalf("ordinary buffered journal grew from %d to %d", header.Capacity, got)
	}
	if got := coll.journal.Cursor(); got != 0 {
		t.Fatalf("ordinary buffered physical fallback left cursor=%d", got)
	}
}

func TestOrdinaryBufferedDeltaFullBatchFallsBackWithoutGrowth(t *testing.T) {
	options := journalDeltaTestOptions()
	coll := buildTemplateHeavyOverlayCollection(t, t.TempDir(), 128, options)
	rootLazyBufferedJournal(t, coll)
	if !coll.bufferedJournalDeltaLane() {
		t.Fatal("expected ordinary buffered delta lane")
	}
	beforePhysical := coll.committer.DurableGeneration()

	coll.writer.Lock()
	header := coll.journal.Header()
	if header.SealedCapacity {
		coll.writer.Unlock()
		t.Fatal("ordinary buffered delta journal is unexpectedly sealed")
	}
	if coll.journal.Cursor() != 0 {
		coll.writer.Unlock()
		t.Fatalf("rooted journal cursor=%d, want 0", coll.journal.Cursor())
	}
	entries := journalBatchAtCapacity(t, header.SectorSize, header.Capacity)
	plan, err := coll.journal.PrepareBatch(entries)
	if err != nil {
		coll.writer.Unlock()
		t.Fatalf("prepare full ordinary batch: %v", err)
	}
	// A current-format live window must start exactly one generation beyond
	// its rooted base. Fill the bounded region with that next valid atomic
	// record; the following collection mutation deliberately reuses the same
	// prospective generation before the physical fallback recycles the window.
	if got, want := coll.journal.BaseGeneration(), coll.Generation(); got != want {
		coll.writer.Unlock()
		t.Fatalf("journal base generation=%d, want collection generation %d", got, want)
	}
	if _, err := coll.journal.AppendPreparedBatch(
		coll.Generation()+1, entries, plan,
	); err != nil {
		coll.writer.Unlock()
		t.Fatalf("append full ordinary batch: %v", err)
	}
	if coll.journal.Cursor() != header.Capacity {
		coll.writer.Unlock()
		t.Fatalf("full ordinary batch cursor=%d, want capacity %d",
			coll.journal.Cursor(), header.Capacity)
	}
	if err := coll.growJournalForRecordLocked(
		int(header.Capacity) + int(header.SectorSize),
	); err != nil {
		coll.writer.Unlock()
		t.Fatalf("ordinary buffered growth gate: %v", err)
	}
	if got := coll.journal.Header().Capacity; got != header.Capacity {
		coll.writer.Unlock()
		t.Fatalf("ordinary buffered journal grew from %d to %d",
			header.Capacity, got)
	}
	coll.writer.Unlock()

	key := []byte(templateHeavyOverlayKey(0))
	if created, err := coll.Put(key, journalDeltaGroupDoc(0, 73)); err != nil || created {
		t.Fatalf("buffered replacement=%v,%v, want existing-key success", created, err)
	}
	target := coll.Generation()
	if target <= beforePhysical {
		t.Fatalf("target generation=%d, want newer than physical %d",
			target, beforePhysical)
	}

	coll.writer.Lock()
	after := coll.journalDeltaAppendedGeneration.Load()
	if !coll.bufferedJournalDeltaStateEligible(after) {
		coll.writer.Unlock()
		t.Fatal("ordinary buffered suffix is not eligible for journal planning")
	}
	_, _, complete, err := coll.prepareBufferedJournalDeltaLocked(after, target)
	coll.writer.Unlock()
	if err != nil {
		t.Fatalf("prepare buffered journal delta: %v", err)
	}
	if complete {
		t.Fatal("full journal reported the next ordinary kind-3 delta complete")
	}

	before := coll.Stats()
	if err := coll.Flush(); err != nil {
		t.Fatalf("physical fallback Flush: %v", err)
	}
	afterStats := coll.Stats()
	if got := coll.committer.DurableGeneration(); got != target {
		t.Fatalf("physical durable generation=%d, want fallback target %d", got, target)
	}
	if afterStats.JournalDeltaCheckpoints != before.JournalDeltaCheckpoints {
		t.Fatalf("full journal used a delta checkpoint: %d -> %d",
			before.JournalDeltaCheckpoints, afterStats.JournalDeltaCheckpoints)
	}
	if got := coll.journal.Header().Capacity; got != header.Capacity {
		t.Fatalf("physical fallback grew journal from %d to %d", header.Capacity, got)
	}
	if coll.journal.Cursor() != 0 {
		t.Fatalf("physical fallback left journal cursor=%d, want recycled 0",
			coll.journal.Cursor())
	}
}
