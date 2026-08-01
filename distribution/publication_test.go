package distribution

// ManifestHolder promises lock-free reads that never observe a torn or mixed
// generation. The basic test pins the atomic swap semantics; the concurrent
// test runs many readers against a writer publishing a stream of distinct,
// immutable generations and asserts every read sees one whole generation — its
// version matching its own shard geometry, its resolved shard actually
// containing the probed point. Run under -race it also proves no data race on
// the shared pointer. Each generation is self-describing (version g has a
// deterministic shard count), so a reader that ever combined two generations
// would fail the consistency check, not merely the race detector.

import (
	"sync"
	"testing"
)

// concurrentGenShardCount is the deterministic shard count for generation g,
// so a reader can cross-check a manifest's version against its own geometry.
func concurrentGenShardCount(g int) int { return 1 + g%7 }

// concurrentGen builds generation g: version g and concurrentGenShardCount(g)
// evenly spaced shards.
func concurrentGen(tb testing.TB, g int) *Manifest {
	tb.Helper()
	shards := shardsFromBounds(evenlySpacedBounds(concurrentGenShardCount(g))...)
	m, err := NewManifest("dist", RoutingVersion(g), shards)
	if err != nil {
		tb.Fatalf("concurrentGen(%d): %v", g, err)
	}
	return m
}

func TestManifestHolderPublishAndLoad(t *testing.T) {
	empty := NewManifestHolder(nil)
	if empty.Current() != nil {
		t.Fatal("Current() on a nil-seeded holder should be nil")
	}

	m1 := manifestFromBounds(t, hb(0x80))
	m2 := manifestFromBounds(t, hb(0x40), hb(0xc0))

	h := NewManifestHolder(m1)
	if h.Current() != m1 {
		t.Fatal("Current() did not return the seeded manifest")
	}
	h.Publish(m2)
	if h.Current() != m2 {
		t.Fatal("Current() did not return the most recently published manifest")
	}
	h.Publish(m1)
	if h.Current() != m1 {
		t.Fatal("Publish did not install the newer generation")
	}
}

// TestManifestHolderConcurrentPublication is the -race gate: concurrent readers
// resolve against whatever generation is current while a writer swaps in a
// stream of new immutable manifests. Every read must be internally consistent.
func TestManifestHolderConcurrentPublication(t *testing.T) {
	const generations = 400
	gens := make([]*Manifest, generations)
	for g := range gens {
		gens[g] = concurrentGen(t, g)
	}

	h := NewManifestHolder(gens[0])
	stop := make(chan struct{})
	var wg sync.WaitGroup

	const readers = 8
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				m := h.Current()
				if m == nil {
					t.Error("Current() returned nil after seeding")
					return
				}
				v := int(m.Version())
				// A whole generation: version and geometry agree.
				if m.ShardCount() != concurrentGenShardCount(v) {
					t.Errorf("mixed generation: version %d reports %d shards, want %d",
						v, m.ShardCount(), concurrentGenShardCount(v))
					return
				}
				// The resolved shard belongs to this same generation and
				// actually contains the probe point.
				p := pt(uint64(v) * 0x1000)
				id, ok := m.ResolvePoint(p)
				if !ok {
					t.Errorf("version %d: ResolvePoint missed on a complete manifest", v)
					return
				}
				found := false
				for i := 0; i < m.ShardCount(); i++ {
					s, _ := m.ShardInfo(i)
					if s.ID == id {
						found = true
						if !s.Range.Contains(p) {
							t.Errorf("version %d: shard %q does not contain resolved point", v, id)
						}
						break
					}
				}
				if !found {
					t.Errorf("version %d: resolved shard %q not in this generation", v, id)
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for g := 0; g < generations; g++ {
			h.Publish(gens[g])
		}
		close(stop)
	}()

	wg.Wait()
	if got := int(h.Current().Version()); got != generations-1 {
		t.Fatalf("final published version = %d, want %d", got, generations-1)
	}
}
