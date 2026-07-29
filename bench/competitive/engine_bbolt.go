package competitive

import (
	"fmt"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

var boltBucket = []byte("docs")

type bboltEngine struct {
	cfg Config
	db  *bolt.DB
	// self is the engine's own single-client session; the Engine-interface
	// point methods delegate to it so the cached-read-transaction logic has one
	// implementation, shared with the sessions Session vends.
	self *bboltSession
}

// bboltSession is one client's view onto the shared *bolt.DB. bbolt is MVCC:
// many read-only transactions and one read-write transaction run concurrently,
// and db.Update serializes writers internally, so the handle is safe to share.
// What is not safe to share is the cached read transaction and bucket handle the
// point-read path holds — a *bolt.Tx is single-goroutine, and a write drops it
// out from under a concurrent Get — so each session owns its own.
type bboltSession struct {
	cfg    Config
	db     *bolt.DB
	read   *bolt.Tx
	bucket *bolt.Bucket
}

func newBbolt(cfg Config) (Engine, error) {
	mode, err := ResolveDurabilityMode("bbolt", cfg.Durability)
	if err != nil {
		return nil, err
	}
	cfg.Durability = mode
	path := filepath.Join(cfg.Dir, "bolt.db")
	opts := &bolt.Options{
		// bbolt's default is to fsync the freelist alongside every commit.
		// Skipping it is the standard production setting: the freelist is
		// rebuilt from the page tree on open, so the only cost is a slower
		// open after a crash, not a correctness loss.
		NoFreelistSync: true,
		FreelistType:   bolt.FreelistMapType,
		// Pre-size the mmap so the bulk load does not remap repeatedly.
		InitialMmapSize: 256 << 20,
		// bbolt is not a page-cache-bounded engine: it mmaps the whole file
		// and relies on the OS. There is no knob to give it the same 64 MiB
		// budget the other engines got, which is itself a finding — see
		// Tuning.
		NoSync: cfg.Durability == DurabilityBufferedVisible,
	}
	db, err := bolt.Open(path, 0o600, opts)
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucket)
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}
	e := &bboltEngine{cfg: cfg, db: db}
	e.self = &bboltSession{cfg: cfg, db: db}
	return e, nil
}

func (b *bboltEngine) Name() string { return "bbolt" }

func (b *bboltEngine) DurabilityMode() DurabilityMode { return b.cfg.Durability }

func (b *bboltEngine) Durability() string {
	if b.cfg.Durability == DurabilityOrdinarySync {
		return "default (fsync per read-write transaction on darwin; does not drain the drive cache)"
	}
	return "NoSync=true (data and meta pages are written without fdatasync; no stable-storage guarantee at acknowledgement)"
}

func (b *bboltEngine) Tuning() string {
	return "NoFreelistSync=true and FreelistType=map (both standard production settings); " +
		"InitialMmapSize=256 MiB to avoid remap churn during the load; " +
		"Bucket.FillPercent=0.9 for the append-ordered keys the corpus uses; " +
		"point reads run inside one long-lived read transaction, with the bucket handle resolved once, instead of a " +
		"db.View plus a Bucket lookup per Get. That is not a micro-optimisation, it is a correction: the per-Get " +
		"transaction begin/rollback is a cost store/durable's transaction-free AppendRaw never pays, so charging bbolt " +
		"for it measured a difference the two APIs do not have. Reverted by Config.Untuned and measured by " +
		"BenchmarkTuning/bbolt. The write path drops the transaction first, as a bbolt writer must. " +
		"bbolt has no read-cache bound — it mmaps the file and the OS page cache is the cache — " +
		"so it could not be given the same 64 MiB budget as the others; it is effectively allowed more"
}

func (b *bboltEngine) Load(docs []Doc) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(boltBucket)
		// Keys are inserted in ascending order, so a near-full leaf fill is
		// optimal: it is bbolt's documented setting for append-only patterns.
		bkt.FillPercent = 0.9
		for i := range docs {
			if err := bkt.Put([]byte(docs[i].Key), docs[i].JSON); err != nil {
				return err
			}
		}
		return nil
	})
}

// readBucket lazily opens, and then caches, one read transaction and its
// bucket handle for point reads. See Tuning: a db.View per Get charged bbolt an
// overhead that the engine it is being compared against does not pay.
//
// A held read transaction is not free in bbolt either — it pins the pages its
// snapshot reaches, so a writer cannot reuse them and the file grows, and it
// reads the pre-write state until it is rolled back. That is why Put drops it,
// and it is the same hazard store/durable's snapshot lease has. Neither
// engine's read path is charged for opening one per operation here, and neither
// engine's write path holds one.
func (s *bboltSession) readBucket() (*bolt.Bucket, error) {
	if s.bucket != nil {
		return s.bucket, nil
	}
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	s.read = tx
	s.bucket = tx.Bucket(boltBucket)
	return s.bucket, nil
}

func (s *bboltSession) dropReadTx() {
	if s.read != nil {
		_ = s.read.Rollback()
		s.read = nil
		s.bucket = nil
	}
}

func (s *bboltSession) releaseReads() { s.dropReadTx() }

func (s *bboltSession) Get(dst []byte, key string) ([]byte, error) {
	if s.cfg.Untuned {
		// bbolt's own idiom: a read transaction per read.
		err := s.db.View(func(tx *bolt.Tx) error {
			v := tx.Bucket(boltBucket).Get([]byte(key))
			if v == nil {
				return fmt.Errorf("missing key %q", key)
			}
			dst = append(dst, v...)
			return nil
		})
		return dst, err
	}
	bkt, err := s.readBucket()
	if err != nil {
		return dst, err
	}
	v := bkt.Get([]byte(key))
	if v == nil {
		return dst, fmt.Errorf("missing key %q", key)
	}
	return append(dst, v...), nil
}

func (s *bboltSession) Put(key string, doc []byte) error {
	s.dropReadTx()
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(boltBucket)
		rawKey := []byte(key)
		if bucket.Get(rawKey) == nil {
			return fmt.Errorf("missing key %q", key)
		}
		return bucket.Put(rawKey, doc)
	})
}

func (s *bboltSession) Upsert(key string, doc []byte) error {
	s.dropReadTx()
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucket).Put([]byte(key), doc)
	})
}

func (s *bboltSession) Delete(key string) error {
	s.dropReadTx()
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(boltBucket)
		rawKey := []byte(key)
		if bucket.Get(rawKey) == nil {
			return fmt.Errorf("missing key %q", key)
		}
		return bucket.Delete(rawKey)
	})
}

func (s *bboltSession) ScanAllBytes() (int, error) {
	n := 0
	var sink byte
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucket).ForEach(func(k, v []byte) error {
			sink ^= touchAll(v)
			n++
			return nil
		})
	})
	foldScanSink(sink)
	return n, err
}

func (b *bboltEngine) Get(dst []byte, key string) ([]byte, error) { return b.self.Get(dst, key) }
func (b *bboltEngine) Put(key string, doc []byte) error          { return b.self.Put(key, doc) }
func (b *bboltEngine) Upsert(key string, doc []byte) error       { return b.self.Upsert(key, doc) }
func (b *bboltEngine) Delete(key string) error                   { return b.self.Delete(key) }
func (b *bboltEngine) ScanAllBytes() (int, error)                { return b.self.ScanAllBytes() }

// Session vends a fresh per-client session sharing the *bolt.DB. Each carries
// its own read transaction and bucket handle; bbolt's MVCC lets those readers
// run alongside the single writer db.Update serializes internally.
func (b *bboltEngine) Session(int) EngineSession {
	return &bboltSession{cfg: b.cfg, db: b.db}
}

func (b *bboltEngine) Scan() (int, error) {
	n := 0
	var sink byte
	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucket).ForEach(func(k, v []byte) error {
			if len(v) > 0 {
				sink ^= v[0]
			}
			n++
			return nil
		})
	})
	foldScanSink(sink)
	return n, err
}

func (b *bboltEngine) Visit(fn func(key string, value []byte) error) error {
	return b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucket).ForEach(func(k, v []byte) error {
			return fn(string(k), v)
		})
	})
}

func (b *bboltEngine) FilterCount(value string) (int, error) {
	needle := jsonScalarNeedle(value)
	n := 0
	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucket).ForEach(func(k, v []byte) error {
			ok, err := matchesCountry(v, needle)
			if err != nil {
				return err
			}
			if ok {
				n++
			}
			return nil
		})
	})
	return n, err
}

func (b *bboltEngine) IndexedCount(string) (int, error) { return 0, ErrNoIndex }

func (b *bboltEngine) DiskBytes() (int64, error) {
	if err := b.Checkpoint(); err != nil {
		return 0, err
	}
	return dirBytes(b.cfg.Dir)
}

func (b *bboltEngine) Checkpoint() error { return b.db.Sync() }

func (b *bboltEngine) MaintenanceFloor() error { return nil }

func (b *bboltEngine) MaintenanceFloorDescription() string {
	return "none (bbolt exposes no online compaction)"
}

func (b *bboltEngine) Close() error {
	b.self.dropReadTx()
	return b.db.Close()
}
