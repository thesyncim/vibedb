package competitive

import (
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

// TestVibeDurableOrdinarySyncJournalsThroughPrimary proves the adapter now
// measures the ordered primary graph: loadBulk builds through CreateFromPrimary,
// so every Put routes through the primary mutation path, and ordinary-sync's
// journalAckLocked -- which fires ONLY from that path -- records one
// acknowledgement per mutation. journalAcks being non-zero and accounting for
// the whole window (with chainAcks) is the primary-routing proof: a chunk-layout
// store would journal nothing.
func TestVibeDurableOrdinarySyncJournalsThroughPrimary(t *testing.T) {
	factory, ok := FactoryNamed("vibejson-durable")
	if !ok {
		t.Fatal("vibejson-durable factory missing")
	}
	e, _, cleanup := newLoaded(t, factory, Config{Durability: DurabilityOrdinarySync})
	defer cleanup()
	v := e.(*vibeDurable)

	base := v.coll.Stats()
	const puts = 200
	scratch := make([]byte, 0, 512)
	for i := 0; i < puts; i++ {
		idx := i % len(docs)
		scratch = AppendSameSizeUpdatedJSON(scratch[:0], docs, idx)
		if err := e.Put(docs[idx].Key, scratch); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	stats := v.coll.Stats()
	journal := stats.JournalAcks - base.JournalAcks
	chain := stats.ChainAcks - base.ChainAcks
	if journal == 0 {
		t.Fatalf("ordinary-sync recorded no journal acknowledgements (journal=%d chain=%d)",
			stats.JournalAcks, stats.ChainAcks)
	}
	// Every mutation is acknowledged through exactly one lane: the per-mutation
	// journal append, or -- when the journal fills and forces a checkpoint -- the
	// chain. Together they must account for the whole window, and the journal lane
	// must dominate for a store with a comfortably sized journal.
	if journal+chain != uint64(puts) {
		t.Fatalf("acknowledgements journal=%d + chain=%d != %d mutations", journal, chain, puts)
	}
	if journal <= chain {
		t.Fatalf("journal lane did not dominate: journal=%d chain=%d", journal, chain)
	}
}

// TestVibeDurablePowerSafeJournalsEveryAcknowledgement holds the power-safe row
// to the same engagement proof as ordinary-sync. This table was burned once by
// a lane that silently disengaged its durability mechanism and published a win;
// any journal-backed comparison row must therefore prove per-mutation journal
// acknowledgements, not assume them from configuration. Fewer mutations than
// the ordinary-sync variant because each acknowledgement here pays a real
// drive-cache drain (~4 ms on this platform's F_FULLFSYNC class).
func TestVibeDurablePowerSafeJournalsEveryAcknowledgement(t *testing.T) {
	if testing.Short() {
		t.Skip("each acknowledgement pays a full power-safe barrier")
	}
	factory, ok := FactoryNamed("vibejson-durable")
	if !ok {
		t.Fatal("vibejson-durable factory missing")
	}
	e, _, cleanup := newLoaded(t, factory, Config{Durability: DurabilityPowerSafe})
	defer cleanup()
	v := e.(*vibeDurable)

	base := v.coll.Stats()
	const puts = 50
	scratch := make([]byte, 0, 512)
	for i := 0; i < puts; i++ {
		idx := i % len(docs)
		scratch = AppendSameSizeUpdatedJSON(scratch[:0], docs, idx)
		if err := e.Put(docs[idx].Key, scratch); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	stats := v.coll.Stats()
	journal := stats.JournalAcks - base.JournalAcks
	chain := stats.ChainAcks - base.ChainAcks
	if journal == 0 {
		t.Fatalf("power-safe recorded no journal acknowledgements (journal=%d chain=%d)",
			stats.JournalAcks, stats.ChainAcks)
	}
	if journal+chain != uint64(puts) {
		t.Fatalf("acknowledgements journal=%d + chain=%d != %d mutations", journal, chain, puts)
	}
	if journal <= chain {
		t.Fatalf("journal lane did not dominate: journal=%d chain=%d", journal, chain)
	}
}

// TestVibeDurableBufferedVisibleRoutesDeletesThroughPrimary proves the buffered
// lane is also on the primary graph: MergeReclassEvaluations increments only in
// the primary delete path (deletePrimaryWithMerge), so a non-zero count after
// deletes is proof the chunk layout is not being measured.
func TestVibeDurableBufferedVisibleRoutesDeletesThroughPrimary(t *testing.T) {
	factory, ok := FactoryNamed("vibejson-durable")
	if !ok {
		t.Fatal("vibejson-durable factory missing")
	}
	e, _, cleanup := newLoaded(t, factory, Config{Durability: DurabilityBufferedVisible})
	defer cleanup()
	v := e.(*vibeDurable)

	base := v.coll.Stats().MergeReclassEvaluations
	for i := 0; i < 64 && i < len(docs); i++ {
		if err := e.Delete(docs[i].Key); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	if got := v.coll.Stats().MergeReclassEvaluations - base; got == 0 {
		t.Fatal("buffered-visible deletes did not run the primary merge/reclass evaluation (chunk layout still measured?)")
	}
}

// TestVibeDurableUnindexedFilterRunsOnPrimarySnapshot confirms the query
// FromFile filter path opens the primary-layout snapshot the unindexed instance
// now loads through CreateFromPrimary: a full scan-and-filter must return the
// corpus's ~1% FilterValue population, not error on the layout.
func TestVibeDurableUnindexedFilterRunsOnPrimarySnapshot(t *testing.T) {
	factory, ok := FactoryNamed("vibejson-durable")
	if !ok {
		t.Fatal("vibejson-durable factory missing")
	}
	e, _, cleanup := newLoaded(t, factory, Config{Durability: DurabilityBufferedVisible})
	defer cleanup()
	n, err := e.FilterCount(FilterValue)
	if err != nil {
		t.Fatalf("FilterCount on primary snapshot: %v", err)
	}
	if n <= 0 {
		t.Fatalf("FilterCount(%q) = %d on primary snapshot, want > 0", FilterValue, n)
	}
}

func TestVibeDurableBufferedVisibleUsesFilesystemCheckpointLane(t *testing.T) {
	engine, err := newVibeDurable(Config{
		Durability: DurabilityBufferedVisible,
		CacheBytes: DefaultCacheBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	buffered := engine.(*vibeDurable)
	options := buffered.options()
	if options.Durability != durable.DurabilityBufferedVisible {
		t.Fatalf("store durability = %d, want buffered-visible", options.Durability)
	}
	if options.CheckpointStrength != durable.CheckpointFilesystem {
		t.Fatalf(
			"checkpoint strength = %d, want ordinary filesystem",
			options.CheckpointStrength,
		)
	}
	if options.Backend != durable.BackendPortable {
		t.Fatalf("backend = %d, want portable", options.Backend)
	}
	if options.MaxDocumentBytes != 1<<10 {
		t.Fatalf(
			"maximum document bytes = %d, want benchmark corpus bound",
			options.MaxDocumentBytes,
		)
	}

	powerSafeEngine, err := newVibeDurable(Config{
		Durability: DurabilityPowerSafe,
		CacheBytes: DefaultCacheBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	powerSafe := powerSafeEngine.(*vibeDurable).options()
	// The power-safe row rides the journal group commit: buffered-visible
	// with a per-mutation redo record synced at the platform's strongest
	// power-loss boundary, not the strict DurabilitySync lane (whose
	// acknowledgements cannot group). See the DurabilityPowerSafe mapping.
	if powerSafe.Durability != durable.DurabilityBufferedVisible {
		t.Fatalf(
			"power-safe store durability = %d, want buffered-visible",
			powerSafe.Durability,
		)
	}
	if !powerSafe.RecoveryJournal {
		t.Fatal("power-safe row must acknowledge through the recovery journal")
	}
	if powerSafe.CheckpointStrength != durable.CheckpointPowerSafe {
		t.Fatalf(
			"power-safe checkpoint strength = %d, want power-safe",
			powerSafe.CheckpointStrength,
		)
	}
}
