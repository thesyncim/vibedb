package competitive

import (
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

// TestVibeDBOrdinarySyncJournalsThroughPrimary proves the adapter now
// measures the ordered primary graph: loadBulk builds through CreateFromPrimary,
// so every Put routes through the primary mutation path, and ordinary-sync's
// journalAckLocked -- which fires ONLY from that path -- records one
// acknowledgement per mutation. journalAcks being non-zero and accounting for
// the whole window (with chainAcks) is the primary-routing proof: a chunk-layout
// store would journal nothing.
func TestVibeDBOrdinarySyncJournalsThroughPrimary(t *testing.T) {
	factory, ok := FactoryNamed("vibedb")
	if !ok {
		t.Fatal("vibedb factory missing")
	}
	e, _, cleanup := newLoaded(t, factory, Config{Durability: DurabilityOrdinarySync})
	defer cleanup()
	v := e.(*vibeDBEngine)

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

// TestVibeDBPowerSafeJournalsEveryAcknowledgement holds the power-safe row
// to the same engagement proof as ordinary-sync. This table was burned once by
// a lane that silently disengaged its durability mechanism and published a win;
// any journal-backed comparison row must therefore prove per-mutation journal
// acknowledgements, not assume them from configuration. Fewer mutations than
// the ordinary-sync variant because each acknowledgement here pays a real
// drive-cache drain (~4 ms on this platform's F_FULLFSYNC class).
func TestVibeDBPowerSafeJournalsEveryAcknowledgement(t *testing.T) {
	if testing.Short() {
		t.Skip("each acknowledgement pays a full power-safe barrier")
	}
	factory, ok := FactoryNamed("vibedb")
	if !ok {
		t.Fatal("vibedb factory missing")
	}
	e, _, cleanup := newLoaded(t, factory, Config{Durability: DurabilityPowerSafe})
	defer cleanup()
	v := e.(*vibeDBEngine)

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

// TestVibeDBBufferedVisibleRoutesDeletesThroughPrimary pins the unified
// primary delete fast path. Dense leaves do not run empty-leaf reclamation;
// structural hygiene runs only after a delete makes a routed leaf empty.
func TestVibeDBBufferedVisibleRoutesDeletesThroughPrimary(t *testing.T) {
	factory, ok := FactoryNamed("vibedb")
	if !ok {
		t.Fatal("vibedb factory missing")
	}
	e, _, cleanup := newLoaded(t, factory, Config{Durability: DurabilityBufferedVisible})
	defer cleanup()
	v := e.(*vibeDBEngine)

	base := v.coll.Stats().PrimaryEmptyReclaims
	for i := 0; i < 64 && i < len(docs); i++ {
		if err := e.Delete(docs[i].Key); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	if got := v.coll.Stats().PrimaryEmptyReclaims - base; got != 0 {
		t.Fatalf(
			"buffered-visible dense primary deletes ran %d empty-leaf reclaims, want 0",
			got,
		)
	}
}

// TestVibeDBUnindexedFilterRunsOnPrimarySnapshot confirms the query
// FromFile filter path opens the primary-layout snapshot the unindexed instance
// now loads through CreateFromPrimary: a full scan-and-filter must return the
// corpus's ~1% FilterValue population, not error on the layout.
func TestVibeDBUnindexedFilterRunsOnPrimarySnapshot(t *testing.T) {
	factory, ok := FactoryNamed("vibedb")
	if !ok {
		t.Fatal("vibedb factory missing")
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

func TestVibeDBBufferedVisibleUsesFilesystemCheckpointLane(t *testing.T) {
	engine, err := newVibeDB(Config{
		Durability: DurabilityBufferedVisible,
		CacheBytes: DefaultCacheBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	buffered := engine.(*vibeDBEngine)
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

	powerSafeEngine, err := newVibeDB(Config{
		Durability: DurabilityPowerSafe,
		CacheBytes: DefaultCacheBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	powerSafe := powerSafeEngine.(*vibeDBEngine).options()
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

func TestVibeDBScanAllBytesWarmedAllocatesNothing(t *testing.T) {
	factory, ok := FactoryNamed("vibedb")
	if !ok {
		t.Fatal("vibedb factory missing")
	}
	corpus := Corpus(2_048)
	engine, _, cleanup := newLoadedCorpus(t, factory, Config{
		Durability: DurabilityBufferedVisible,
	}, corpus)
	defer cleanup()

	session := engine.Session(0)
	for _, test := range []struct {
		name string
		scan func() (int, error)
	}{
		{name: "engine", scan: engine.ScanAllBytes},
		{name: "session", scan: session.ScanAllBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows, err := test.scan()
			if err != nil {
				t.Fatalf("warm ScanAllBytes: %v", err)
			}
			if rows != len(corpus) {
				t.Fatalf("warm ScanAllBytes visited %d rows, want %d", rows, len(corpus))
			}

			var scanErr error
			allocs := testing.AllocsPerRun(20, func() {
				rows, scanErr = test.scan()
			})
			if scanErr != nil {
				t.Fatalf("warmed ScanAllBytes: %v", scanErr)
			}
			if rows != len(corpus) {
				t.Fatalf("warmed ScanAllBytes visited %d rows, want %d", rows, len(corpus))
			}
			if allocs != 0 {
				t.Fatalf("warmed ScanAllBytes allocated %.2f times, want 0", allocs)
			}
		})
	}
}

// TestVibeDBPointMutationWarmedAllocations keeps the benchmark adapter
// honest: its string key conversion lives in caller-owned retained storage, so
// the only steady allocation is durable's immutable state publication. Both
// the single-client handle and a concurrent-harness session own their scratch.
func TestVibeDBPointMutationWarmedAllocations(t *testing.T) {
	factory, ok := FactoryNamed("vibedb")
	if !ok {
		t.Fatal("vibedb factory missing")
	}
	engine, _, cleanup := newLoaded(t, factory, Config{
		Durability: DurabilityBufferedVisible,
	})
	defer cleanup()

	for _, test := range []struct {
		name   string
		handle EngineSession
		key    string
		value  []byte
	}{
		{
			name: "engine", handle: engine,
			key: docs[100].Key, value: docs[100].JSON,
		},
		{
			name: "session", handle: engine.Session(1),
			key: docs[101].Key, value: docs[101].JSON,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.handle.Put(test.key, test.value); err != nil {
				t.Fatalf("warm Put: %v", err)
			}
			readBuf := make([]byte, 0, len(test.value)+64)
			if _, err := test.handle.Get(readBuf[:0], test.key); err != nil {
				t.Fatalf("warm Get: %v", err)
			}
			getAllocs := testing.AllocsPerRun(50, func() {
				if _, err := test.handle.Get(readBuf[:0], test.key); err != nil {
					panic(err)
				}
			})
			if getAllocs != 0 {
				t.Fatalf("warmed Get allocated %.2f times, want 0", getAllocs)
			}
			putAllocs := testing.AllocsPerRun(50, func() {
				if err := test.handle.Put(test.key, test.value); err != nil {
					panic(err)
				}
			})
			if putAllocs != 1 {
				t.Fatalf(
					"warmed Put allocated %.2f times, want 1 published state",
					putAllocs,
				)
			}
			deleteRestoreAllocs := testing.AllocsPerRun(50, func() {
				if err := test.handle.Delete(test.key); err != nil {
					panic(err)
				}
				if err := test.handle.Upsert(test.key, test.value); err != nil {
					panic(err)
				}
			})
			if deleteRestoreAllocs != 2 {
				t.Fatalf(
					"warmed delete+restore allocated %.2f times, want 2 published states",
					deleteRestoreAllocs,
				)
			}
		})
	}
}
