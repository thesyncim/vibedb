package competitive

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

type pebbleEngine struct {
	cfg   Config
	db    *pebble.DB
	cache *pebble.Cache
	wopts *pebble.WriteOptions
}

func newPebble(cfg Config) (Engine, error) {
	return openPebbleMode(cfg, true)
}

func openPebble(cfg Config) (Engine, error) {
	if _, err := os.Stat(filepath.Join(cfg.Dir, "CURRENT")); err != nil {
		return nil, err
	}
	return openPebbleMode(cfg, false)
}

func openPebbleMode(cfg Config, _ bool) (Engine, error) {
	if err := validateEngineExactIndexes("pebble", cfg.ExactIndexes); err != nil {
		return nil, err
	}
	mode, err := ResolveDurabilityMode("pebble", cfg.Durability)
	if err != nil {
		return nil, err
	}
	cfg.Durability = mode
	profile, err := ResolveStorageProfile("pebble", cfg.StorageProfile)
	if err != nil {
		return nil, err
	}
	cfg.StorageProfile = profile.Profile
	// Pebble's default block cache is 8 MiB. Every other engine here was given
	// 64 MiB, so leaving Pebble on the default would make it lose a cache
	// fight it was never entered into.
	cache := pebble.NewCache(cfg.CacheBytes)
	opts := &pebble.Options{
		Cache: cache,
		// The intrinsic profile forces uncompressed SST blocks. The production
		// profile explicitly restores Pebble v1.1.5's default, Snappy. A
		// single LevelOptions entry is inherited by every higher level.
		Levels: []pebble.LevelOptions{{Compression: pebbleStorageCompression(profile)}},
		// 64 MiB memtables let the whole ~25 MiB corpus land in one or two
		// flushes rather than a dozen, which is what a bulk load would be
		// configured for in practice.
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       4,
		MaxConcurrentCompactions:    func() int { return 2 },
	}
	opts.EnsureDefaults()
	db, err := pebble.Open(cfg.Dir, opts)
	if err != nil {
		cache.Unref()
		return nil, err
	}
	wopts := pebble.NoSync
	if cfg.Durability == DurabilityOrdinarySync {
		wopts = pebble.Sync
	}
	return &pebbleEngine{cfg: cfg, db: db, cache: cache, wopts: wopts}, nil
}

func pebbleStorageCompression(profile StorageProfileResolution) pebble.Compression {
	if profile.compression == storageCompressionSnappy {
		return pebble.SnappyCompression
	}
	return pebble.NoCompression
}

func (p *pebbleEngine) Name() string { return "pebble" }

func (p *pebbleEngine) DurabilityMode() DurabilityMode { return p.cfg.Durability }

func (p *pebbleEngine) Durability() string {
	if p.cfg.Durability == DurabilityOrdinarySync {
		return "pebble.Sync (WAL fsynced before return; on darwin this does not drain the drive cache)"
	}
	return "pebble.NoSync (visible before stable storage; recent WAL bytes may remain buffered inside Pebble and can be lost on process or machine crash)"
}

func (p *pebbleEngine) Tuning() string {
	storage := "Compression=None on all SST levels for the intrinsic uncompressed comparison; "
	if p.cfg.StorageProfile == StorageProfileProduction {
		storage = "Compression=Snappy on all SST levels, Pebble v1.1.5's default; WAL and metadata bytes remain uncompressed and included; "
	}
	return "Cache=64 MiB instead of the 8 MiB default, matching every other engine's read-cache budget; " +
		storage +
		"MemTableSize=64 MiB so the bulk load is not chopped into a dozen flushes; " +
		"MaxConcurrentCompactions=2. " +
		"Checkpoint uses LogData(nil, pebble.Sync), Pebble's own documented WAL sequence fence, rather than " +
		"forcing the memtable to an SST; DiskBytes flushes separately after timed checkpoint accounting so the " +
		"footprint is materialized without charging that stronger maintenance operation to checkpoint latency. " +
		"CHECKED AND REJECTED: a bloom filter on every level (FilterPolicy=bloom.FilterPolicy(10)) measured 1441 ns " +
		"against 1284 ns without one, i.e. it is a small loss, because every key this harness probes exists — a bloom " +
		"filter can only save the read of a table that does not contain the key, and there are none. Do not re-add it " +
		"as an obvious win. " +
		"NOT tuned away, and it is a stated asymmetry against Pebble: about half of Pebble's disk figure is a live WAL " +
		"holding a second copy of records that DiskBytes' Flush has already written into SSTs, and Pebble offers no " +
		"call to retire it. Those bytes are fully allocated, not sparse — the allocated column charges every one of " +
		"them. SQLite, by contrast, is given PRAGMA wal_checkpoint(TRUNCATE) and its log leaves its figure entirely. " +
		"Run cmd/footprint -engine=pebble -files to see the split before comparing Pebble's total with anyone else's"
}

func (p *pebbleEngine) Load(docs []Doc) error {
	batch := p.db.NewBatch()
	defer batch.Close()
	for i := range docs {
		if err := batch.Set([]byte(docs[i].Key), docs[i].JSON, nil); err != nil {
			return err
		}
	}
	return batch.Commit(p.wopts)
}

func (p *pebbleEngine) Get(dst []byte, key string) ([]byte, error) {
	v, closer, err := p.db.Get([]byte(key))
	if err != nil {
		return dst, err
	}
	dst = append(dst, v...)
	return dst, closer.Close()
}

func (p *pebbleEngine) Put(key string, doc []byte) error {
	rawKey := []byte(key)
	if err := p.requireKey(rawKey, key); err != nil {
		return err
	}
	return p.db.Set(rawKey, doc, p.wopts)
}

func (p *pebbleEngine) Upsert(key string, doc []byte) error {
	return p.db.Set([]byte(key), doc, p.wopts)
}

func (p *pebbleEngine) Delete(key string) error {
	rawKey := []byte(key)
	if err := p.requireKey(rawKey, key); err != nil {
		return err
	}
	return p.db.Delete(rawKey, p.wopts)
}

func (p *pebbleEngine) requireKey(rawKey []byte, key string) error {
	_, closer, err := p.db.Get(rawKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return fmt.Errorf("missing key %q", key)
		}
		return err
	}
	return closer.Close()
}

func (p *pebbleEngine) Scan() (int, error) {
	it, err := p.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	n := 0
	var sink byte
	for it.First(); it.Valid(); it.Next() {
		v, err := it.ValueAndErr()
		if err != nil {
			return n, err
		}
		if len(v) > 0 {
			sink ^= v[0]
		}
		n++
	}
	foldScanSink(sink)
	return n, it.Error()
}

func (p *pebbleEngine) ScanAllBytes() (int, error) {
	it, err := p.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	n := 0
	var sink byte
	for it.First(); it.Valid(); it.Next() {
		v, err := it.ValueAndErr()
		if err != nil {
			return n, err
		}
		sink ^= touchAll(v)
		n++
	}
	foldScanSink(sink)
	return n, it.Error()
}

func (p *pebbleEngine) Visit(fn func(key string, value []byte) error) error {
	it, err := p.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		v, err := it.ValueAndErr()
		if err != nil {
			return err
		}
		if err := fn(string(it.Key()), v); err != nil {
			return err
		}
	}
	return it.Error()
}

func (p *pebbleEngine) FilterCount(value string) (int, error) {
	needle := jsonScalarNeedle(value)
	it, err := p.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	n := 0
	for it.First(); it.Valid(); it.Next() {
		v, err := it.ValueAndErr()
		if err != nil {
			return n, err
		}
		ok, err := matchesCountry(v, needle)
		if err != nil {
			return n, err
		}
		if ok {
			n++
		}
	}
	return n, it.Error()
}

func (p *pebbleEngine) IndexedCount(string) (int, error) { return 0, ErrNoIndex }

func (p *pebbleEngine) ProbeExactIndex(uint8, string) (ExactIndexProbe, error) {
	return ExactIndexProbe{}, ErrNoIndex
}

func (p *pebbleEngine) DiskBytes() (int64, error) {
	if err := p.db.Flush(); err != nil {
		return 0, err
	}
	return dirBytes(p.cfg.Dir)
}

func (p *pebbleEngine) Checkpoint() error {
	// Pebble's own DB.Checkpoint implementation uses this exact operation to
	// guarantee that every earlier sequence number is recoverable. A memtable
	// flush would add unnecessary LSM maintenance to the logical durability
	// fence and make this lane stronger than its peers.
	return p.db.LogData(nil, pebble.Sync)
}

func (p *pebbleEngine) MaintenanceFloor() error {
	return p.db.Compact([]byte("doc:"), []byte("doc;"), true)
}

func (p *pebbleEngine) MaintenanceFloorDescription() string {
	return `Compact(["doc:", "doc;"), parallelize=true)`
}

func (p *pebbleEngine) Close() error {
	err := p.db.Close()
	p.cache.Unref()
	return err
}

// Session returns the engine itself: *pebble.DB is documented safe for
// concurrent Get/Set/Delete/NewIter, and this adapter keeps no per-caller
// scratch (Get closes its value handle inline, the write path holds no cached
// read handle), so N clients share one handle with nothing to race on.
func (p *pebbleEngine) Session(int) EngineSession { return p }
