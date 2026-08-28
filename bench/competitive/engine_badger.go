package competitive

import (
	"errors"
	"os"
	"path/filepath"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

type badgerEngine struct {
	cfg Config
	db  *badger.DB
	// self is the engine's own single-client session. The Engine-interface
	// point methods delegate to it so there is exactly one implementation of
	// the cached-read-transaction logic, shared with the sessions Session vends.
	self *badgerSession
}

// badgerSession is one client's view onto the shared *badger.DB. The database
// handle is documented safe for concurrent transactions; what is not safe to
// share is the cached read transaction the point-read path holds, because a
// *badger.Txn is a single-reader object and Put/Delete discard it out from under
// a concurrent Get. Each session therefore owns its own read transaction.
type badgerSession struct {
	cfg  Config
	db   *badger.DB
	read *badger.Txn
}

// badgerScanOptions is the iterator configuration every full-corpus walk here
// uses.
//
// PrefetchValues defaults to true, and leaving it on costs Badger a large
// factor on this corpus. Badger's prefetcher hands each item to a worker pool
// to have its value resolved ahead of the cursor, which pays when values are
// large and live in the value log, and does not pay when they are ~250 bytes
// and already inline in the LSM tables. Leaving it on the default published
// Badger as the slowest engine in the scan row when it is not.
// BenchmarkTuning/badger measures the pair; do not quote the effect from here.
func badgerScanOptions(cfg Config) badger.IteratorOptions {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = cfg.Untuned
	if opt.PrefetchValues {
		opt.PrefetchSize = 256
	}
	return opt
}

func newBadger(cfg Config) (Engine, error) {
	return openBadgerMode(cfg, true)
}

func openBadger(cfg Config) (Engine, error) {
	if _, err := os.Stat(filepath.Join(cfg.Dir, "MANIFEST")); err != nil {
		return nil, err
	}
	return openBadgerMode(cfg, false)
}

func openBadgerMode(cfg Config, _ bool) (Engine, error) {
	if err := validateEngineExactIndexes("badger", cfg.ExactIndexes); err != nil {
		return nil, err
	}
	mode, err := ResolveDurabilityMode("badger", cfg.Durability)
	if err != nil {
		return nil, err
	}
	cfg.Durability = mode
	profile, err := ResolveStorageProfile("badger", cfg.StorageProfile)
	if err != nil {
		return nil, err
	}
	cfg.StorageProfile = profile.Profile
	compression, blockCacheBytes, indexCacheBytes := badgerStorageOptions(profile, cfg.CacheBytes)
	opt := badger.DefaultOptions(cfg.Dir).
		// Badger logs to stderr at INFO by default, which would pollute
		// benchmark output and cost real time during a 100k-document load.
		WithLogger(nil).
		// The intrinsic profile forces None for the apples-to-apples format
		// lane. The production profile restores Badger's pinned recommended
		// default, Snappy. Only SST blocks are compressed; the value log and
		// other files still count in full.
		WithCompression(compression).
		// The default value-log file size is 1 GiB, which Badger mmaps. With
		// ~250-byte values that reserves three orders of magnitude more
		// address space than the data needs and distorts RSS badly. 64 MiB
		// still amortises rotation across the whole corpus.
		WithValueLogFileSize(64 << 20).
		// Give Badger the same 64 MiB read-cache budget as everyone else.
		// Badger's own guidance is a zero block cache when compression is off,
		// and a block cache when it is on. Keep the total fixed while moving
		// the budget between index and block caches with the profile.
		WithBlockCacheSize(blockCacheBytes).
		WithIndexCacheSize(indexCacheBytes).
		// The harness never runs concurrent conflicting transactions, so
		// conflict detection is pure overhead here.
		WithDetectConflicts(false).
		WithNumVersionsToKeep(1).
		WithSyncWrites(cfg.Durability == DurabilityOrdinarySync)
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}
	e := &badgerEngine{cfg: cfg, db: db}
	e.self = &badgerSession{cfg: cfg, db: db}
	return e, nil
}

func badgerStorageOptions(
	profile StorageProfileResolution,
	cacheBytes int64,
) (options.CompressionType, int64, int64) {
	if profile.compression == storageCompressionSnappy {
		return options.Snappy, cacheBytes, 0
	}
	return options.None, 0, cacheBytes
}

func (b *badgerEngine) Name() string { return "badger" }

func (b *badgerEngine) DurabilityMode() DurabilityMode { return b.cfg.Durability }

func (b *badgerEngine) Durability() string {
	if b.cfg.Durability == DurabilityOrdinarySync {
		// NOT matched with the other engines on darwin, and the report must
		// say so. Badger's log files are mmapped and its Sync is
		// unix.Msync(MS_SYNC) (ristretto/z/mmap_unix.go), which pushes dirty
		// pages to the filesystem but does not force the drive to flush its
		// write cache. bbolt and Pebble issue plain fsync and have the same
		// limitation. VibeDB explicitly issues F_FULLFSYNC, and SQLite is
		// given PRAGMA fullfsync to match it. Badger exposes no option to
		// request F_FULLFSYNC, so its sync=true row is not comparable with
		// those two crash-safe rows.
		return "SyncWrites=true, but msync(MS_SYNC) only — NOT power-loss comparable with F_FULLFSYNC on darwin"
	}
	return "SyncWrites=false, Badger's default (mmap-backed writes are visible without msync; documented for process-crash, not hard-reboot survival)"
}

func (b *badgerEngine) Tuning() string {
	storage := "Compression=None for the intrinsic uncompressed comparison; BlockCacheSize=0 with IndexCacheSize=64 MiB, Badger's recommendation when compression is off; "
	if b.cfg.StorageProfile == StorageProfileProduction {
		storage = "Compression=Snappy for SST blocks, Badger v4.9.5's recommended default; BlockCacheSize=64 MiB with IndexCacheSize=0, keeping the common cache budget while following Badger's requirement/recommendation to cache compressed blocks; "
	}
	return "Logger=nil; " + storage +
		"ValueLogFileSize=64 MiB instead of the 1 GiB default, which would otherwise mmap a gigabyte for a 25 MiB corpus; " +
		"DetectConflicts=false (no concurrent transactions here); NumVersionsToKeep=1; " +
		"IteratorOptions.PrefetchValues=false on every full-corpus walk, against Badger's default of true, because the " +
		"prefetch worker pool only pays for large value-log-resident values and these are ~250 bytes inline in the LSM " +
		"tables; point reads run inside one long-lived read transaction rather than opening a badger.Txn per Get, " +
		"matching the transaction-free read that store/durable's AppendRaw gets (the write path drops it, as a writer " +
		"must). Both are reverted by Config.Untuned and both effects are measured by BenchmarkTuning/badger, so neither " +
		"is a claim you have to take on trust"
}

func (b *badgerEngine) Load(docs []Doc) error {
	wb := b.db.NewWriteBatch()
	defer wb.Cancel()
	for i := range docs {
		if err := wb.Set([]byte(docs[i].Key), docs[i].JSON); err != nil {
			return err
		}
	}
	return wb.Flush()
}

// readTxn lazily opens, and then caches, one read transaction for point reads.
//
// It mirrors what bbolt needed and why: store/durable's AppendRaw takes no
// transaction at all, so charging the competitors a transaction begin/discard
// per Get measured a difference the API shapes do not actually have.
//
// Put drops it, and that is correctness rather than hygiene: a Badger read
// transaction pins a read timestamp, so a Get issued through one opened before
// a Put would return the pre-Put value. The read-only workloads never write, so
// they never observe a stale snapshot; the write workloads never hold one.
func (s *badgerSession) readTxn() *badger.Txn {
	if s.read == nil {
		s.read = s.db.NewTransaction(false)
	}
	return s.read
}

func (s *badgerSession) dropReadTxn() {
	if s.read != nil {
		s.read.Discard()
		s.read = nil
	}
}

func (s *badgerSession) releaseReads() { s.dropReadTxn() }

func (s *badgerSession) Get(dst []byte, key string) ([]byte, error) {
	if s.cfg.Untuned {
		// Badger's own idiom: a transaction per read.
		err := s.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}
			dst, err = item.ValueCopy(dst)
			return err
		})
		return dst, err
	}
	item, err := s.readTxn().Get([]byte(key))
	if err != nil {
		return dst, err
	}
	return item.ValueCopy(dst)
}

func (s *badgerSession) Put(key string, doc []byte) error {
	s.dropReadTxn()
	return s.db.Update(func(txn *badger.Txn) error {
		rawKey := []byte(key)
		if _, err := txn.Get(rawKey); err != nil {
			return err
		}
		return txn.Set(rawKey, doc)
	})
}

func (s *badgerSession) Upsert(key string, doc []byte) error {
	s.dropReadTxn()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), doc)
	})
}

func (s *badgerSession) Delete(key string) error {
	s.dropReadTxn()
	return s.db.Update(func(txn *badger.Txn) error {
		rawKey := []byte(key)
		if _, err := txn.Get(rawKey); err != nil {
			return err
		}
		return txn.Delete(rawKey)
	})
}

func (s *badgerSession) Scan() (int, error) {
	n := 0
	var sink byte
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerScanOptions(s.cfg))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := it.Item().Value(func(v []byte) error {
				if len(v) > 0 {
					sink ^= v[0]
				}
				return nil
			}); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	foldScanSink(sink)
	return n, err
}

func (s *badgerSession) ScanAllBytes() (int, error) {
	n := 0
	var sink byte
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerScanOptions(s.cfg))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := it.Item().Value(func(v []byte) error {
				sink ^= touchAll(v)
				return nil
			}); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	foldScanSink(sink)
	return n, err
}

func (b *badgerEngine) Get(dst []byte, key string) ([]byte, error) { return b.self.Get(dst, key) }
func (b *badgerEngine) Put(key string, doc []byte) error           { return b.self.Put(key, doc) }
func (b *badgerEngine) Upsert(key string, doc []byte) error        { return b.self.Upsert(key, doc) }
func (b *badgerEngine) Delete(key string) error                    { return b.self.Delete(key) }
func (b *badgerEngine) Scan() (int, error)                         { return b.self.Scan() }
func (b *badgerEngine) ScanAllBytes() (int, error)                 { return b.self.ScanAllBytes() }

// Session vends a fresh per-client session sharing the *badger.DB. Each carries
// its own read transaction, so N clients never race on one *badger.Txn while the
// database handle serializes their writes internally.
func (b *badgerEngine) Session(int) EngineSession {
	return &badgerSession{cfg: b.cfg, db: b.db}
}

func (b *badgerEngine) Visit(fn func(key string, value []byte) error) error {
	return b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerScanOptions(b.cfg))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			if err := item.Value(func(v []byte) error { return fn(key, v) }); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *badgerEngine) FilterCount(value string) (int, error) {
	needle := jsonScalarNeedle(value)
	n := 0
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerScanOptions(b.cfg))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := it.Item().Value(func(v []byte) error {
				ok, err := matchesCountry(v, needle)
				if err != nil {
					return err
				}
				if ok {
					n++
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return n, err
}

func (b *badgerEngine) IndexedCount(string) (int, error) { return 0, ErrNoIndex }

func (b *badgerEngine) ProbeExactIndex(uint8, string) (ExactIndexProbe, error) {
	return ExactIndexProbe{}, ErrNoIndex
}

func (b *badgerEngine) DiskBytes() (int64, error) {
	if err := b.Checkpoint(); err != nil {
		return 0, err
	}
	return dirBytes(b.cfg.Dir)
}

func (b *badgerEngine) Checkpoint() error { return b.db.Sync() }

func (b *badgerEngine) MaintenanceFloor() error {
	b.self.dropReadTxn()
	if err := b.db.Flatten(2); err != nil {
		return err
	}
	for {
		err := b.db.RunValueLogGC(0.5)
		if errors.Is(err, badger.ErrNoRewrite) {
			return b.db.Sync()
		}
		if err != nil {
			return err
		}
	}
}

func (b *badgerEngine) MaintenanceFloorDescription() string {
	return "Flatten(2), then RunValueLogGC(0.5) until ErrNoRewrite, then Sync"
}

func (b *badgerEngine) Close() error {
	b.self.dropReadTxn()
	return b.db.Close()
}
