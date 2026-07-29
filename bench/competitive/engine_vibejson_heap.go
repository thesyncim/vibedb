package competitive

import (
	"fmt"
	"sync/atomic"

	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
)

// heapCollection is the subset of the heap collection type this harness uses,
// captured as a local interface rather than a named concrete type. The store
// package's concrete type name is being renamed in a change that is in flight
// alongside this harness; naming only the methods keeps the benchmark building
// through the rename.
type heapCollection interface {
	Put(key string, src []byte) (bool, error)
	Delete(key string) (bool, error)
	GetRaw(key string) (vibejson.RawValue, bool)
	AppendRaw(dst []byte, key string) ([]byte, bool)
	Snapshot() (store.Snapshot, error)
	Len() uint64
}

type vibeHeap struct {
	cfg             Config
	coll            heapCollection
	snap            store.Snapshot
	exec            query.Exec
	filter          *query.Query
	loaded          bool
	snapshotCurrent bool
}

func newVibeHeap(cfg Config) (Engine, error) {
	mode, err := ResolveDurabilityMode("vibejson-heap", cfg.Durability)
	if err != nil {
		return nil, err
	}
	cfg.Durability = mode
	return &vibeHeap{
		cfg: cfg,
		filter: query.Select(query.Count()).
			Where(query.Cmp(FilterField, query.Eq, FilterValue)),
	}, nil
}

func (v *vibeHeap) Name() string { return "vibejson-heap" }

func (v *vibeHeap) DurabilityMode() DurabilityMode { return v.cfg.Durability }

func (v *vibeHeap) Durability() string {
	return "none (process memory only; not a durable store)"
}

func (v *vibeHeap) Tuning() string {
	return "ShapeTapes=false (the corpus has a nested object and an array, so no document conforms to a shape tape); " +
		"default ChunkDocuments=64; bulk path is store.Builder, the documented equivalent of the other engines' batch APIs"
}

// storeOptions is the heap configuration. ShapeTapes is deliberately left off:
// its conformance rule excludes any document with a nested object or array,
// and every corpus document has both, so enabling it would cost the
// conformance check on every insert and store nothing compactly.
func (v *vibeHeap) storeOptions() store.Options {
	return store.Options{}
}

func (v *vibeHeap) Load(docs []Doc) error {
	builder, err := store.NewBuilder(v.storeOptions())
	if err != nil {
		return err
	}
	if v.cfg.Indexed {
		if err := builder.CreateIndex(store.IndexDefinition{
			Name:  FilterField,
			Paths: []string{FilterPath},
		}); err != nil {
			return err
		}
	}
	for i := range docs {
		if err := builder.Append(docs[i].Key, docs[i].JSON); err != nil {
			return err
		}
	}
	built, err := builder.Build()
	if err != nil {
		return err
	}
	v.coll = built
	snap, err := built.Snapshot()
	if err != nil {
		return err
	}
	v.snap = snap
	v.loaded = true
	v.snapshotCurrent = true
	return nil
}

func (v *vibeHeap) Get(dst []byte, key string) ([]byte, error) {
	out, ok := v.coll.AppendRaw(dst, key)
	if !ok {
		return dst, fmt.Errorf("missing key %q", key)
	}
	return out, nil
}

func (v *vibeHeap) Put(key string, doc []byte) error {
	_, err := v.coll.Put(key, doc)
	if err == nil {
		v.snapshotCurrent = false
	}
	return err
}

func (v *vibeHeap) Upsert(key string, doc []byte) error { return v.Put(key, doc) }

func (v *vibeHeap) Delete(key string) error {
	deleted, err := v.coll.Delete(key)
	if err == nil && deleted {
		v.snapshotCurrent = false
	}
	if err == nil && !deleted {
		return fmt.Errorf("missing key %q", key)
	}
	return err
}

func (v *vibeHeap) refreshSnapshot() error {
	if v.snapshotCurrent {
		return nil
	}
	snap, err := v.coll.Snapshot()
	if err != nil {
		return err
	}
	v.snap = snap
	v.snapshotCurrent = true
	return nil
}

// Scan walks the snapshot published at load time. The harness never mutates
// and scans the same instance — the write workloads get their own fixture —
// so there is no stale-snapshot question to answer here.
func (v *vibeHeap) Scan() (int, error) {
	if err := v.refreshSnapshot(); err != nil {
		return 0, err
	}
	n := 0
	var sink byte
	v.snap.Range(func(key string, value vibejson.RawValue) bool {
		b := value.Bytes()
		if len(b) > 0 {
			sink ^= b[0]
		}
		n++
		return true
	})
	foldScanSink(sink)
	return n, nil
}

func (v *vibeHeap) ScanAllBytes() (int, error) {
	if err := v.refreshSnapshot(); err != nil {
		return 0, err
	}
	n := 0
	var sink byte
	v.snap.Range(func(key string, value vibejson.RawValue) bool {
		sink ^= touchAll(value.Bytes())
		n++
		return true
	})
	foldScanSink(sink)
	return n, nil
}

func (v *vibeHeap) Visit(fn func(key string, value []byte) error) error {
	if err := v.refreshSnapshot(); err != nil {
		return err
	}
	var ferr error
	v.snap.Range(func(key string, value vibejson.RawValue) bool {
		if err := fn(key, value.Bytes()); err != nil {
			ferr = err
			return false
		}
		return true
	})
	return ferr
}

// scanSink defeats dead-store elimination on the scan workloads. Each scan folds
// its whole per-call XOR accumulator into it exactly once (not once per
// document), so the atomic is off the per-value path. It must be atomic because
// a scan workload under -clients=N folds into it from several goroutines at
// once; its value is never asserted, so a plain read-modify-write would be only
// a benign accumulator race, but a benign race still trips the detector and the
// concurrent lanes must run clean.
var scanSink atomic.Uint64

// foldScanSink folds one scan's accumulator into scanSink race-free.
func foldScanSink(sink byte) { scanSink.Add(uint64(sink)) }

func (v *vibeHeap) FilterCount(value string) (int, error) {
	if v.cfg.Indexed {
		return 0, fmt.Errorf("FilterCount must run against an unindexed instance")
	}
	return v.runFilter(value)
}

func (v *vibeHeap) IndexedCount(value string) (int, error) {
	if !v.cfg.Indexed {
		return 0, ErrNoIndex
	}
	return v.runFilter(value)
}

func (v *vibeHeap) runFilter(value string) (int, error) {
	if err := v.refreshSnapshot(); err != nil {
		return 0, err
	}
	q := v.filter
	if value != FilterValue {
		q = query.Select(query.Count()).Where(query.Cmp(FilterField, query.Eq, value))
	}
	if err := q.RunInto(&v.exec, query.FromSnapshot(v.snap)); err != nil {
		return 0, err
	}
	col, ok := v.exec.Result.Column("count(*)")
	if !ok || len(col.Cells) == 0 {
		return 0, fmt.Errorf("no count column in result")
	}
	n, ok := col.Cells[0].Int64()
	if !ok {
		return 0, fmt.Errorf("count cell is not an integer")
	}
	return int(n), nil
}

func (v *vibeHeap) Checkpoint() error         { return nil }
func (v *vibeHeap) DiskBytes() (int64, error) { return 0, nil }

func (v *vibeHeap) Close() error {
	v.exec.Release()
	v.coll = nil
	v.snap = store.Snapshot{}
	v.snapshotCurrent = false
	return nil
}

// Session returns the engine itself. vibejson-heap is the volatile, non-durable
// label engine and shares a cached snapshot plus a query executor across calls,
// so it is only safe under a single client; concurrent mutation of the in-memory
// collection is a store-level property this harness does not assert. No required
// concurrent lane runs it (the concurrency diagnostics are durable, SQLite, and
// Badger), and at clients=1 the shared self-session is exactly the engine's own
// behaviour, preserving single-client parity.
func (v *vibeHeap) Session(int) EngineSession { return v }
