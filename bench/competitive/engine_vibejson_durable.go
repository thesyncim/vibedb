package competitive

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// durableCollection is the subset of store/durable's collection type this
// harness uses. As with heapCollection, it is spelled as a local interface so
// the in-flight rename of the concrete type cannot break the benchmark.
type durableCollection interface {
	Put(key string, src []byte) (bool, error)
	Delete(key string) (bool, error)
	AppendRaw(dst []byte, key string) ([]byte, bool, error)
	Snapshot() (*durable.Snapshot, error)
	Len() uint64
	Stats() durable.Stats
	Flush() error
	Close() error
}

type vibeDurable struct {
	cfg     Config
	path    string
	file    *os.File
	coll    durableCollection
	snap    *durable.Snapshot
	exec    query.Exec
	scratch []byte
}

func newVibeDurable(cfg Config) (Engine, error) {
	if cfg.Compact && cfg.PutLoop {
		return nil, fmt.Errorf("vibejson-durable: compact format requires the bulk path")
	}
	mode, err := ResolveDurabilityMode("vibejson-durable", cfg.Durability)
	if err != nil {
		return nil, err
	}
	cfg.Durability = mode
	return &vibeDurable{cfg: cfg}, nil
}

func (v *vibeDurable) Name() string { return "vibejson-durable" }

func (v *vibeDurable) DurabilityMode() DurabilityMode { return v.cfg.Durability }

func (v *vibeDurable) Durability() string {
	switch v.cfg.Durability {
	case DurabilityPowerSafe:
		return "DurabilitySync (each generation fenced to stable storage before Put returns or becomes visible)"
	case DurabilityBufferedVisible:
		return "DurabilityBufferedVisible + CheckpointFilesystem over the ordered primary graph (routed splits/merges, resident router; exact secondary-index posting tiles are maintained in the same publish as each mutation, so a reader's index is never stale; ordinary admission stages bounded reader-visible COW pages without waking the device worker; staging pressure may checkpoint early; scheduled checkpoints fold the deferred mutations and their posting pages into a durable root with ordinary two-phase fsync)"
	case DurabilityOrdinarySync:
		return "DurabilityBufferedVisible + CheckpointFilesystem + RecoveryJournal over the ordered primary graph (every Put/Delete routes through the primary mutation path, maintains the exact-index posting tiles, and appends one redo record to the paired journal, synced at ordinary filesystem strength before returning; on recovery the record replays through the same path and rebuilds an identical posting index; a checkpoint folds the deferred mutations and posting pages into a durable root and recycles the journal)"
	default:
		return "DurabilityAsyncVisible (accepted into a private queue and immediately visible; may be lost before a process-crash kernel write; background worker uses the normal stable-storage fences)"
	}
}

func (v *vibeDurable) Tuning() string {
	if v.cfg.Untuned {
		return "defaults only, for comparison against the tuned row"
	}
	format := "DocumentFormatVerbatim"
	if v.cfg.Compact {
		format = "DocumentFormatCompact"
	}
	mode := ""
	if v.cfg.Durability == DurabilityBufferedVisible {
		mode = "Buffered-visible ordinarily keeps the persistence worker asleep until Checkpoint, uses bounded fresh-COW staging, groups the captured cut under one alternate root, and explicitly selects the ordinary two-phase filesystem-sync checkpoint used by this comparison; staging pressure can force an earlier checkpoint and comparative runs must verify the selected interval stays below that bound; "
	}
	layout := "the ordered primary graph (CreateFromPrimary bulk / the primary mutation path for Put and Delete) is the only engine measured, including indexed rows: exact secondary-index posting tiles are maintained on the graph and updated in the same publish as each Put/Delete, so an indexed filter reads the graph's posting index; verbatim and compact rows are both leaf classes on the graph; "
	return format + "; " + mode + layout +
		"ResidentBytes=64 MiB (the default, and the read-cache budget every other engine was matched to); " +
		"PageSize=4 KiB default; buffered read and write modes (O_DIRECT is Linux-only); " +
		"MaxBatchDocuments=1 and MaxDocumentBytes=1 KiB because this harness exposes only point mutations over a corpus whose largest document is below that bound; the restriction cuts worst-case staging reservation without changing any measured value; " +
		"BufferCount=1024, QueueSlots=1024, GroupLimit=64 (buffered-visible normalizes the physical checkpoint group to QueueSlots). The default BufferCount is sized for the collection's " +
		"worst-case transaction geometry; the explicit pool keeps this workload's staging capacity stable. " +
		"This tuning was originally justified by a " +
		"25-35x faster Put and that figure does not currently reproduce — BenchmarkPointWriteDurableDefaults measures " +
		"the pair and RESULTS.md reports what it is worth today, which is far less. " +
		"CommitCoalesce=0, i.e. no acknowledged-latency-for-throughput trade. " +
		"CreateFromPrimary defaults to verbatim; compact is a separate explicit row because it materially trades read speed " +
		"for space. Put replay always emits verbatim pages. Never publish one representation as the engine's only footprint"
}

func (v *vibeDurable) options() durable.Options {
	opts := durable.Options{
		ResidentBytes: v.cfg.CacheBytes,
	}
	switch v.cfg.Durability {
	case DurabilityBufferedVisible:
		opts.Durability = durable.DurabilityBufferedVisible
		opts.Backend = durable.BackendPortable
		opts.CheckpointStrength = durable.CheckpointFilesystem
	case DurabilityOrdinarySync:
		// The recovery journal gives buffered-visible a per-mutation durable
		// acknowledgement at ordinary filesystem-sync strength: one redo record
		// appended and synced before Put returns, recycled at each checkpoint.
		// That is the same guarantee class as bbolt/badger/pebble's sync modes
		// on this platform, which also stop at the ordinary fsync fence.
		opts.Durability = durable.DurabilityBufferedVisible
		opts.Backend = durable.BackendPortable
		opts.CheckpointStrength = durable.CheckpointFilesystem
		opts.RecoveryJournal = true
	case DurabilityAsyncStableInFlight:
		opts.Durability = durable.DurabilityAsyncVisible
	}
	if v.cfg.Compact {
		opts.DocumentFormat = durable.DocumentFormatCompact
	}
	if !v.cfg.Untuned {
		// This adapter cannot express Collection.Update, so reserving for the
		// collection default's multi-document transaction would spend staging
		// memory on an operation the competitive interface cannot issue.
		opts.MaxBatchDocuments = 1
		// The benchmark corpus and its same-size replacements are all below
		// 1 KiB. Reserving overflow buffers for the production 4 MiB default
		// would shrink buffered checkpoint depth for values this harness can
		// never submit, measuring unused API range rather than the workload.
		opts.MaxDocumentBytes = 1 << 10
		opts.BufferCount = 1024
		opts.QueueSlots = 1024
		opts.GroupLimit = 64
	}
	if v.cfg.Indexed {
		opts.Indexes = []store.IndexDefinition{{
			Name:  FilterField,
			Paths: []string{FilterPath},
		}}
	}
	return opts
}

func (v *vibeDurable) Load(docs []Doc) error {
	v.path = filepath.Join(v.cfg.Dir, "vibejson.db")
	f, err := os.OpenFile(v.path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	v.file = f
	// A secondary index loads through the primary mutation path, which maintains
	// the exact secondary index incrementally, one publish per Put -- the same
	// path the indexed mutation workload exercises. Both this path and the bulk
	// CreateFromPrimary cutover share one ceiling: a physical index's whole
	// posting set serialises into a single term leaf (65,535 postings / 64 KiB),
	// so a low-cardinality corpus spread across enough documents overruns it on
	// either path and surfaces as ErrPrimaryCutoverUnsupported rather than a
	// silent fallback. Until multi-leaf term indexes land (loadBulk's future-work
	// note), the indexed configuration loads through Put and callers size the
	// indexed corpus below that ceiling.
	if v.cfg.PutLoop || v.cfg.Indexed {
		return v.loadByPut(f, docs)
	}
	return v.loadBulk(f, docs)
}

// loadBulk is store/durable's bulk path: build the collection in memory, then
// write it as one durable generation. It is the fair counterpart of bbolt's
// single write transaction, Badger's WriteBatch, Pebble's batch, and SQLite's
// single transaction — none of those replay individual mutations either. The
// in-memory build is included in the measurement, because a caller who only
// wants the file still has to pay it.
func (v *vibeDurable) loadBulk(f *os.File, docs []Doc) error {
	opts := v.options()
	// The heap options are left at their zero value, matching the durable
	// Options' own embedded collection defaults, so the intermediate
	// collection is shaped exactly like the one durable would have built.
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		return err
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
	// The ordered primary graph is the only engine now (routed splits/merges, the
	// resident router, journal-wired acknowledgements, and on-graph exact
	// secondary-index posting tiles). Every corpus builds through it.
	//
	// The exact-index format holds one physical index's postings in a single term
	// leaf (bounded to 65,535 postings / 64 KiB). A large low-cardinality corpus
	// spreads one term across enough posting tiles to exceed that, which the bulk
	// build reports as ErrPrimaryCutoverUnsupported. Multi-leaf term indexes are
	// future work; until they land the harness sizes its indexed configurations
	// below the single-leaf posting ceiling, and an oversized index surfaces here
	// as that hard error rather than a silent chunk-layout fallback.
	if _, err := durable.CreateFromPrimary(built, f, v.primaryBulkOptions()); err != nil {
		return err
	}
	db, err := durable.Open(f, opts)
	if err != nil {
		return err
	}
	v.coll = db
	return nil
}

// loadByPut is the mutation-replay path, measured separately because the gap
// between it and loadBulk is one of the more useful numbers in the report. It
// creates an empty ordered-primary collection and replays every document through
// the primary mutation path — and, for ordinary-sync, its wired recovery
// journal — exactly like the measured workload.
func (v *vibeDurable) loadByPut(f *os.File, docs []Doc) error {
	opts := v.options()
	db, err := durable.Create(f, opts)
	if err != nil {
		return err
	}
	v.coll = db
	for i := range docs {
		if _, err := db.Put(docs[i].Key, docs[i].JSON); err != nil {
			return err
		}
	}
	return db.Flush()
}

// primaryBulkOptions is the tuned options with BufferCount cleared. The single
// CreateFromPrimary transaction stages the whole graph at once and needs at
// least pageCount+1 commit buffers, which for a large corpus exceeds the small
// BufferCount the mutation workload is tuned to; zeroing it lets the bulk write
// auto-size its own pool. Open still uses the tuned BufferCount, so the measured
// mutation staging depth -- the thing the tuning fixes -- is unchanged.
func (v *vibeDurable) primaryBulkOptions() durable.Options {
	opts := v.options()
	opts.BufferCount = 0
	opts.QueueSlots = 0
	return opts
}

// snapshot lazily opens, and then caches, the read snapshot the scan and
// filter workloads run against.
//
// It is lazy, and Put drops it, for a reason worth recording: an open durable
// snapshot holds a lease that pins retired extents, and a store written to
// while a snapshot stays open exhausts Options.MaxRetiredExtents and fails
// with "retired extent capacity exhausted". Holding one open across a
// long-running write loop — which an earlier version of this harness did —
// takes roughly thirteen thousand replacements to hit at the default bound.
// That is a genuine operational hazard for a reader that keeps a snapshot for
// the lifetime of a request handler, but it is not what the point-write
// benchmark is supposed to be measuring, so the write path holds no snapshot.
func (v *vibeDurable) snapshot() (*durable.Snapshot, error) {
	if v.snap != nil {
		return v.snap, nil
	}
	snap, err := v.coll.Snapshot()
	if err != nil {
		return nil, err
	}
	v.snap = snap
	return snap, nil
}

func (v *vibeDurable) releaseSnapshot() {
	if v.snap != nil {
		_ = v.snap.Close()
		v.snap = nil
	}
}

func (v *vibeDurable) Get(dst []byte, key string) ([]byte, error) {
	out, ok, err := v.coll.AppendRaw(dst, key)
	if err != nil {
		return dst, err
	}
	if !ok {
		return dst, fmt.Errorf("missing key %q", key)
	}
	return out, nil
}

func (v *vibeDurable) Put(key string, doc []byte) error {
	// See snapshot: a write path must not hold a snapshot lease open.
	v.releaseSnapshot()
	_, err := v.coll.Put(key, doc)
	return err
}

func (v *vibeDurable) Upsert(key string, doc []byte) error { return v.Put(key, doc) }

func (v *vibeDurable) Delete(key string) error {
	v.releaseSnapshot()
	deleted, err := v.coll.Delete(key)
	if err == nil && !deleted {
		return fmt.Errorf("missing key %q", key)
	}
	return err
}

func (v *vibeDurable) Scan() (int, error) {
	snap, err := v.snapshot()
	if err != nil {
		return 0, err
	}
	n := 0
	var sink byte
	scratch, err := snap.RangeRawBuffer(v.scratch, func(key, value []byte) error {
		if len(value) > 0 {
			sink ^= value[0]
		}
		n++
		return nil
	})
	v.scratch = scratch
	foldScanSink(sink)
	return n, err
}

func (v *vibeDurable) ScanAllBytes() (int, error) {
	snap, err := v.snapshot()
	if err != nil {
		return 0, err
	}
	n := 0
	var sink byte
	scratch, err := snap.RangeRawBuffer(v.scratch, func(key, value []byte) error {
		sink ^= touchAll(value)
		n++
		return nil
	})
	v.scratch = scratch
	foldScanSink(sink)
	return n, err
}

func (v *vibeDurable) Visit(fn func(key string, value []byte) error) error {
	snap, err := v.snapshot()
	if err != nil {
		return err
	}
	scratch, err := snap.RangeRawBuffer(v.scratch, func(key, value []byte) error {
		return fn(string(key), value)
	})
	v.scratch = scratch
	return err
}

func (v *vibeDurable) FilterCount(value string) (int, error) {
	if v.cfg.Indexed {
		return 0, fmt.Errorf("FilterCount must run against an unindexed instance")
	}
	return v.runFilter(value)
}

func (v *vibeDurable) IndexedCount(value string) (int, error) {
	if !v.cfg.Indexed {
		return 0, ErrNoIndex
	}
	return v.runFilter(value)
}

func (v *vibeDurable) runFilter(value string) (int, error) {
	snap, err := v.snapshot()
	if err != nil {
		return 0, err
	}
	q := query.Select(query.Count()).Where(query.Cmp(FilterField, query.Eq, value))
	if err := q.RunInto(&v.exec, query.FromFile(snap)); err != nil {
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

func (v *vibeDurable) DiskBytes() (int64, error) {
	if err := v.Checkpoint(); err != nil {
		return 0, err
	}
	return dirBytes(v.cfg.Dir)
}

func (v *vibeDurable) Checkpoint() error {
	if v.coll == nil {
		return nil
	}
	return v.coll.Flush()
}

func (v *vibeDurable) MaintenanceFloor() error { return v.Checkpoint() }

func (v *vibeDurable) MaintenanceFloorDescription() string {
	return "Flush (the ordinary mutation representation is unchanged)"
}

// AutomaticCheckpoints reports persistence boundaries forced by bounded
// staging pressure rather than requested through the benchmark's schedule.
// The mixed harness samples it outside the timed interval.
func (v *vibeDurable) AutomaticCheckpoints() uint64 {
	if v.coll == nil {
		return 0
	}
	return v.coll.Stats().AutomaticCheckpoints
}

func (v *vibeDurable) Close() error {
	v.exec.Release()
	v.releaseSnapshot()
	if v.coll != nil {
		if err := v.coll.Close(); err != nil {
			return err
		}
		v.coll = nil
	}
	if v.file != nil {
		_ = v.file.Close()
		v.file = nil
	}
	return nil
}

// vibeDurableSession is one client's private view onto the shared durable
// collection. The collection handle (v.coll) is shared — its public mutation
// path is serialized under one writer mutex and its readers are lock-free behind
// the reader-epoch fence, so concurrent Put/Delete/AppendRaw/Snapshot on it are
// safe by the store's own design. What is NOT safe to share, and is the whole
// reason this type exists, is the adapter's per-caller scratch: the cached read
// snapshot (opened lazily, dropped on every write to avoid pinning retired
// extents) and the decode buffer reused across scans. Both move here so N
// clients never race on v.snap or v.scratch.
type vibeDurableSession struct {
	coll    durableCollection
	snap    *durable.Snapshot
	scratch []byte
}

// snapshot mirrors vibeDurable.snapshot on the session's own slot; see the
// commentary there for why the write path must not hold one open.
func (s *vibeDurableSession) snapshot() (*durable.Snapshot, error) {
	if s.snap != nil {
		return s.snap, nil
	}
	snap, err := s.coll.Snapshot()
	if err != nil {
		return nil, err
	}
	s.snap = snap
	return snap, nil
}

func (s *vibeDurableSession) releaseReads() {
	if s.snap != nil {
		_ = s.snap.Close()
		s.snap = nil
	}
}

func (s *vibeDurableSession) Get(dst []byte, key string) ([]byte, error) {
	out, ok, err := s.coll.AppendRaw(dst, key)
	if err != nil {
		return dst, err
	}
	if !ok {
		return dst, fmt.Errorf("missing key %q", key)
	}
	return out, nil
}

func (s *vibeDurableSession) Put(key string, doc []byte) error {
	s.releaseReads()
	_, err := s.coll.Put(key, doc)
	return err
}

func (s *vibeDurableSession) Upsert(key string, doc []byte) error { return s.Put(key, doc) }

func (s *vibeDurableSession) Delete(key string) error {
	s.releaseReads()
	deleted, err := s.coll.Delete(key)
	if err == nil && !deleted {
		return fmt.Errorf("missing key %q", key)
	}
	return err
}

func (s *vibeDurableSession) ScanAllBytes() (int, error) {
	snap, err := s.snapshot()
	if err != nil {
		return 0, err
	}
	n := 0
	var sink byte
	scratch, err := snap.RangeRawBuffer(s.scratch, func(key, value []byte) error {
		sink ^= touchAll(value)
		n++
		return nil
	})
	s.scratch = scratch
	foldScanSink(sink)
	return n, err
}

// Session vends a per-client session over the shared collection. Every session
// carries its own snapshot slot and scan buffer; the collection itself is
// shared, which is the point — the concurrent lane measures the store's own
// writer mutex and reader-epoch fence under N callers.
func (v *vibeDurable) Session(int) EngineSession {
	return &vibeDurableSession{coll: v.coll}
}
