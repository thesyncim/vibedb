package competitive

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// The durable ordered-primary format accepts keys up to 256 bytes. Keep each
// benchmark client one fixed buffer of exactly that bound: valid operations
// never allocate for string conversion and invalid input cannot grow retained
// adapter memory. AppendRaw, Put, and Delete borrow the key only for their
// call, so the client may overwrite this buffer at its next operation.
const vibeDBKeyBytes = 256

type vibeDBEngine struct {
	cfg         Config
	path        string
	file        *os.File
	coll        *durable.Collection
	snap        *durable.Snapshot
	exec        query.Exec
	filterQuery *query.Query
	filterValue string
	scratch     []byte
	keyBuf      [vibeDBKeyBytes]byte
	scan        *vibeDBScanState
}

type vibeDBWriteCounters struct {
	patchAttempts uint64
	patches       uint64
	folds         uint64
	replacements  uint64
}

func vibeDBWriteCountersOf(engine Engine) (vibeDBWriteCounters, bool) {
	v, ok := engine.(*vibeDBEngine)
	if !ok || v.coll == nil {
		return vibeDBWriteCounters{}, false
	}
	stats := v.coll.Stats()
	return vibeDBWriteCounters{
		patchAttempts: stats.ConcurrentPrimaryScalarPatchAttempts,
		patches:       stats.ConcurrentPrimaryScalarPatches,
		folds:         stats.PrimaryOverlayFolds,
		replacements:  stats.ConcurrentPrimaryReplaces,
	}, true
}

func vibeDBPointKey(
	buf *[vibeDBKeyBytes]byte,
	key string,
) ([]byte, error) {
	if len(key) > len(buf) {
		return nil, durable.ErrKeyTooLarge
	}
	copy(buf[:], key)
	return buf[:len(key)], nil
}

// vibeDBScanState owns the reconstruction buffer and callbacks for one
// benchmark client. The callback method values are bound once when the session
// is created, rather than rebuilt as escaping closures on every scan.
type vibeDBScanState struct {
	scratch []byte
	rows    int
	sink    byte
	first   func(key, value []byte) error
	all     func(key, value []byte) error
}

func newVibeDBScanState() *vibeDBScanState {
	state := new(vibeDBScanState)
	state.first = state.consumeFirstByte
	state.all = state.consumeAllBytes
	return state
}

func (s *vibeDBScanState) consumeFirstByte(_ []byte, value []byte) error {
	if len(value) > 0 {
		s.sink ^= value[0]
	}
	s.rows++
	return nil
}

func (s *vibeDBScanState) consumeAllBytes(_ []byte, value []byte) error {
	s.sink ^= touchAll(value)
	s.rows++
	return nil
}

// rangeVibeDBCurrent is the adapter's single dependency on the durable
// current-scan API. Keeping that boundary in one place makes scan API changes
// mechanical without reintroducing per-call adapter closures.
func rangeVibeDBCurrent(
	coll *durable.Collection,
	scratch []byte,
	visit func(key, value []byte) error,
) ([]byte, error) {
	return coll.RangeRawCurrentBuffer(scratch, visit)
}

func (s *vibeDBScanState) run(
	coll *durable.Collection,
	visit func(key, value []byte) error,
) (int, error) {
	s.rows = 0
	s.sink = 0
	scratch, err := rangeVibeDBCurrent(coll, s.scratch, visit)
	s.scratch = scratch
	foldScanSink(s.sink)
	return s.rows, err
}

func newVibeDB(cfg Config) (Engine, error) {
	mode, err := ResolveDurabilityMode("vibedb", cfg.Durability)
	if err != nil {
		return nil, err
	}
	cfg.Durability = mode
	profile, err := ResolveStorageProfile("vibedb", cfg.StorageProfile)
	if err != nil {
		return nil, err
	}
	cfg.StorageProfile = profile.Profile
	return &vibeDBEngine{cfg: cfg, scan: newVibeDBScanState()}, nil
}

func (v *vibeDBEngine) Name() string { return "vibedb" }

func (v *vibeDBEngine) DurabilityMode() DurabilityMode { return v.cfg.Durability }

func (v *vibeDBEngine) Durability() string {
	switch v.cfg.Durability {
	case DurabilityPowerSafe:
		return "DurabilityBufferedVisible + CheckpointPowerSafe + RecoveryJournal over the ordered primary graph (every Put/Delete appends one redo record to the paired journal and syncs it at the platform's strongest power-loss boundary — F_FULLFSYNC class — before returning, so each acknowledged mutation survives sudden power loss; concurrent acknowledgements share one barrier through the journal's group commit; checkpoints fold at the same strength; the strict visibility-follows-durability lane is DurabilitySync, which pays the same single journal fence but does not group)"
	case DurabilityBufferedVisible:
		return "DurabilityBufferedVisible + CheckpointFilesystem over the ordered primary graph (routed splits/merges, resident router; exact secondary-index posting tiles are maintained in the same publish as each mutation, so a reader's index is never stale; ordinary admission stages bounded reader-visible COW pages without waking the device worker; staging pressure may checkpoint early; scheduled checkpoints fold the deferred mutations and their posting pages into a durable root with ordinary two-phase fsync)"
	case DurabilityOrdinarySync:
		return "DurabilityBufferedVisible + CheckpointFilesystem + RecoveryJournal over the ordered primary graph (every Put/Delete routes through the primary mutation path, maintains the exact-index posting tiles, and appends one redo record to the paired journal, synced at ordinary filesystem strength before returning; on recovery the record replays through the same path and rebuilds an identical posting index; a checkpoint folds the deferred mutations and posting pages into a durable root and recycles the journal)"
	default:
		return "DurabilityAsyncVisible (accepted into a private queue and immediately visible; may be lost before a process-crash kernel write; background worker uses the normal stable-storage fences)"
	}
}

func (v *vibeDBEngine) Tuning() string {
	if v.cfg.Untuned {
		return "defaults only, for comparison against the tuned row"
	}
	mode := ""
	if v.cfg.Durability == DurabilityBufferedVisible {
		mode = "Buffered-visible ordinarily keeps the persistence worker asleep until Checkpoint, uses bounded fresh-COW staging, groups the captured cut under one alternate root, and explicitly selects the ordinary two-phase filesystem-sync checkpoint used by this comparison; staging pressure can force an earlier checkpoint and comparative runs must verify the selected interval stays below that bound; "
	}
	layout := "the ordered primary graph (compact-stripe bulk / the primary mutation path for Put and Delete) is the only engine measured, including indexed rows: exact secondary-index posting tiles are maintained on the graph and updated in the same publish as each Put/Delete, so an indexed filter reads the graph's posting index; "
	return mode + layout +
		"ResidentBytes=64 MiB (the default, and the read-cache budget every other engine was matched to); " +
		"PageSize=4 KiB default; buffered read and write modes (O_DIRECT is Linux-only); " +
		"MaxBatchDocuments=1 and MaxDocumentBytes=1 KiB because this harness exposes only point mutations over a corpus whose largest document is below that bound; the restriction cuts worst-case staging reservation without changing any measured value; " +
		"BufferCount=8192, QueueSlots=128, GroupLimit=64. The wider descriptor pool lets the 64 MiB resident budget select its bounded 1,024-dirty-leaf overlay directory; runtime admission charges certified replacements their actual routed leaf extent plus parent scratch, while shape-changing mutations reserve the full leaf extent. 128 queue slots keep the committer's fixed descriptor arena below its one-million-entry ceiling. Buffered-visible normalizes the physical checkpoint group to QueueSlots. The default BufferCount is sized for the collection's " +
		"worst-case transaction geometry; the explicit pool keeps this workload's staging capacity stable. " +
		"BenchmarkPointWriteDurableDefaults measures the tuned/default pair directly. " +
		"CommitCoalesce=0, i.e. no acknowledged-latency-for-throughput trade. " +
		"CreateFromRecords emits the sole canonical compact-stripe representation directly from the borrowed bulk batch"
}

func (v *vibeDBEngine) options() durable.Options {
	opts := durable.Options{
		ResidentBytes: v.cfg.CacheBytes,
	}
	switch v.cfg.Durability {
	case DurabilityBufferedVisible:
		opts.Durability = durable.DurabilityBufferedVisible
		opts.Backend = durable.BackendPortable
		opts.CheckpointStrength = durable.CheckpointFilesystem
	case DurabilityPowerSafe:
		// Symmetric to the ordinary-sync mapping below: buffered-visible plus
		// the recovery journal, with the pre-return record sync at the
		// platform's strongest boundary (F_FULLFSYNC class) instead of the
		// ordinary fence. The lane's promise attaches to the acknowledgement —
		// every returned mutation has crossed the drive-cache drain — and
		// concurrent acknowledgements share one barrier through the journal's
		// group commit. Visibility precedes the durable acknowledgement exactly
		// as on the ordinary-sync row. The strict visibility-follows-durability
		// configuration is DurabilitySync; it pays the same single journal
		// fence per mutation but its acknowledgements cannot group yet, so it
		// is not the comparison row.
		opts.Durability = durable.DurabilityBufferedVisible
		opts.Backend = durable.BackendPortable
		opts.CheckpointStrength = durable.CheckpointPowerSafe
		opts.RecoveryJournal = true
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
		opts.BufferCount = 8192
		opts.QueueSlots = 128
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

func (v *vibeDBEngine) Load(docs []Doc) error {
	v.path = filepath.Join(v.cfg.Dir, "vibedb.db")
	f, err := os.OpenFile(v.path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	v.file = f
	// PutLoop deliberately exercises one acknowledged mutation per document.
	// Ordinary indexed loads use CreateFromRecords: the durable exact index now
	// cuts giant low-cardinality terms into spanned leaves, so routing a large
	// indexed corpus through the old replay workaround would hide the scalable
	// bulk representation and make the read benchmark artificially unavailable.
	if v.cfg.PutLoop {
		return v.loadByPut(f, docs)
	}
	return v.loadBulk(f, docs)
}

// loadBulk is store/durable's native borrowed-record bulk path. It is the fair
// counterpart of bbolt's single write transaction, Badger's WriteBatch,
// Pebble's batch, and SQLite's single transaction — none of those replay
// individual mutations or require a second in-memory database either.
func (v *vibeDBEngine) loadBulk(f *os.File, docs []Doc) error {
	opts := v.options()
	records := make([]durable.PrimaryBulkRecord, len(docs))
	for i := range docs {
		records[i] = durable.PrimaryBulkRecord{
			Key: docs[i].Key, Value: docs[i].JSON,
		}
	}
	// The ordered primary graph is the only engine now (routed splits/merges, the
	// resident router, journal-wired acknowledgements, and on-graph exact
	// secondary-index posting tiles). Every corpus builds through it.
	//
	// The exact-index format cuts large low-cardinality terms into deterministic
	// spanned leaves. Keeping this bulk path here is important: replaying one Put
	// per document would measure mutation durability and conceal the scalable
	// multi-leaf representation that read-heavy log workloads depend on.
	if _, err := durable.CreateFromRecords(records, f, v.primaryBulkOptions()); err != nil {
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
// the primary mutation path — the synchronous lane acknowledging through its
// recovery journal — exactly like the measured workload.
func (v *vibeDBEngine) loadByPut(f *os.File, docs []Doc) error {
	opts := v.options()
	db, err := durable.Create(f, opts)
	if err != nil {
		return err
	}
	v.coll = db
	for i := range docs {
		if _, err := db.Put([]byte(docs[i].Key), docs[i].JSON); err != nil {
			return err
		}
	}
	return db.Flush()
}

// primaryBulkOptions is the tuned options with BufferCount cleared. The single
// CreateFromRecords transaction stages the whole graph at once and needs at
// least pageCount+1 commit buffers, which for a large corpus exceeds the small
// BufferCount the mutation workload is tuned to; zeroing it lets the bulk write
// auto-size its own pool. Open still uses the tuned BufferCount, so the measured
// mutation staging depth -- the thing the tuning fixes -- is unchanged.
func (v *vibeDBEngine) primaryBulkOptions() durable.Options {
	opts := v.options()
	opts.BufferCount = 0
	opts.QueueSlots = 0
	return opts
}

// snapshot lazily opens and caches the read snapshot used by filter workloads.
// Full scans use RangeRawCurrentBuffer's ephemeral generation pin instead.
//
// It is lazy, and Put drops it, because an open durable
// snapshot holds a lease that pins retired extents, and a store written to
// while a snapshot stays open exhausts Options.MaxRetiredExtents and fails
// with "retired extent capacity exhausted". Holding one open across a
// long-running write loop can exhaust the configured bound. The point-write
// benchmark therefore holds no snapshot across a write.
func (v *vibeDBEngine) snapshot() (*durable.Snapshot, error) {
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

func (v *vibeDBEngine) releaseSnapshot() {
	if v.snap != nil {
		_ = v.snap.Close()
		v.snap = nil
	}
}

func (v *vibeDBEngine) Get(dst []byte, key string) ([]byte, error) {
	keyBytes, err := vibeDBPointKey(&v.keyBuf, key)
	if err != nil {
		return dst, err
	}
	out, ok, err := v.coll.AppendRaw(dst, keyBytes)
	if err != nil {
		return dst, err
	}
	if !ok {
		return dst, fmt.Errorf("missing key %q", key)
	}
	return out, nil
}

func (v *vibeDBEngine) Put(key string, doc []byte) error {
	// See snapshot: a write path must not hold a snapshot lease open.
	v.releaseSnapshot()
	keyBytes, err := vibeDBPointKey(&v.keyBuf, key)
	if err != nil {
		return err
	}
	_, err = v.coll.Put(keyBytes, doc)
	return err
}

func (v *vibeDBEngine) Upsert(key string, doc []byte) error { return v.Put(key, doc) }

func (v *vibeDBEngine) Delete(key string) error {
	v.releaseSnapshot()
	keyBytes, err := vibeDBPointKey(&v.keyBuf, key)
	if err != nil {
		return err
	}
	deleted, err := v.coll.Delete(keyBytes)
	if err == nil && !deleted {
		return fmt.Errorf("missing key %q", key)
	}
	return err
}

func (v *vibeDBEngine) Scan() (int, error) {
	return v.scan.run(v.coll, v.scan.first)
}

func (v *vibeDBEngine) ScanAllBytes() (int, error) {
	return v.scan.run(v.coll, v.scan.all)
}

func (v *vibeDBEngine) Visit(fn func(key string, value []byte) error) error {
	scratch, err := rangeVibeDBCurrent(v.coll, v.scratch, func(key, value []byte) error {
		return fn(string(key), value)
	})
	v.scratch = scratch
	return err
}

func (v *vibeDBEngine) FilterCount(value string) (int, error) {
	if v.cfg.Indexed {
		return 0, fmt.Errorf("FilterCount must run against an unindexed instance")
	}
	return v.runFilter(value)
}

func (v *vibeDBEngine) IndexedCount(value string) (int, error) {
	if !v.cfg.Indexed {
		return 0, ErrNoIndex
	}
	return v.runFilter(value)
}

func (v *vibeDBEngine) runFilter(value string) (int, error) {
	snap, err := v.snapshot()
	if err != nil {
		return 0, err
	}
	if v.filterQuery == nil || v.filterValue != value {
		v.filterQuery = query.Select(query.Count()).Where(
			query.Cmp(FilterField, query.Eq, value),
		)
		v.filterValue = value
	}
	if err := v.filterQuery.RunInto(&v.exec, query.FromFile(snap)); err != nil {
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

func (v *vibeDBEngine) DiskBytes() (int64, error) {
	if err := v.Checkpoint(); err != nil {
		return 0, err
	}
	return dirBytes(v.cfg.Dir)
}

func (v *vibeDBEngine) Checkpoint() error {
	if v.coll == nil {
		return nil
	}
	return v.coll.Flush()
}

func (v *vibeDBEngine) MaintenanceFloor() error {
	if v.coll == nil || v.file == nil {
		return nil
	}
	// Repack is deliberately offline: close the live collection so its newest
	// journal-visible generation is folded into a quiescent source before the
	// snapshot scan. The benchmark keeps both the pre-floor COW high-water and
	// the post-floor row, so this stronger rewrite is never hidden inside timed
	// mutation throughput.
	if err := v.Close(); err != nil {
		return err
	}
	sourcePath := v.path
	source, err := os.OpenFile(sourcePath, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	outputPath := sourcePath + ".repack"
	output, err := os.OpenFile(
		outputPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		_ = source.Close()
		return err
	}
	cleanupOutput := true
	defer func() {
		_ = source.Close()
		_ = output.Close()
		if cleanupOutput {
			_ = os.Remove(outputPath)
			_ = os.Remove(durable.RecoveryJournalPath(outputPath))
		}
	}()
	if _, err := durable.Repack(
		source, output, v.primaryBulkOptions(),
	); err != nil {
		return err
	}
	if err := source.Close(); err != nil {
		return err
	}
	if err := output.Close(); err != nil {
		return err
	}
	// This harness consumes the compact output in place of the source; it does
	// not claim a crash-atomic production cutover protocol. Repack itself is a
	// vacuum-into primitive, leaving deployment to publish the completed pair
	// with its own manifest/rename protocol.
	if err := os.Remove(sourcePath); err != nil {
		return err
	}
	sourceJournal := durable.RecoveryJournalPath(sourcePath)
	if err := os.Remove(sourceJournal); err != nil && !os.IsNotExist(err) {
		return err
	}
	v.path = outputPath
	cleanupOutput = false
	return nil
}

func (v *vibeDBEngine) MaintenanceFloorDescription() string {
	return "offline out-of-place durable.Repack (vacuum-into), then remove the benchmark source pair; cutover protocol excluded"
}

// AutomaticCheckpoints reports persistence boundaries forced by bounded
// staging pressure rather than requested through the benchmark's schedule.
// The mixed harness samples it outside the timed interval.
func (v *vibeDBEngine) AutomaticCheckpoints() uint64 {
	if v.coll == nil {
		return 0
	}
	return v.coll.Stats().AutomaticCheckpoints
}

// DurableStats exposes value-only internal counters to opt-in diagnostic
// harnesses. Published benchmark tables do not depend on it.
func (v *vibeDBEngine) DurableStats() durable.Stats {
	if v.coll == nil {
		return durable.Stats{}
	}
	return v.coll.Stats()
}

func (v *vibeDBEngine) Close() error {
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

// vibeDBEngineSession is one client's private view onto the shared durable
// collection. The collection handle is shared and concurrency-safe; the key
// conversion and current-scan reconstruction buffers are per caller.
type vibeDBEngineSession struct {
	coll   *durable.Collection
	keyBuf [vibeDBKeyBytes]byte
	scan   *vibeDBScanState
}

func (s *vibeDBEngineSession) Get(dst []byte, key string) ([]byte, error) {
	keyBytes, err := vibeDBPointKey(&s.keyBuf, key)
	if err != nil {
		return dst, err
	}
	out, ok, err := s.coll.AppendRaw(dst, keyBytes)
	if err != nil {
		return dst, err
	}
	if !ok {
		return dst, fmt.Errorf("missing key %q", key)
	}
	return out, nil
}

func (s *vibeDBEngineSession) Put(key string, doc []byte) error {
	keyBytes, err := vibeDBPointKey(&s.keyBuf, key)
	if err != nil {
		return err
	}
	_, err = s.coll.Put(keyBytes, doc)
	return err
}

func (s *vibeDBEngineSession) Upsert(key string, doc []byte) error { return s.Put(key, doc) }

func (s *vibeDBEngineSession) Delete(key string) error {
	keyBytes, err := vibeDBPointKey(&s.keyBuf, key)
	if err != nil {
		return err
	}
	deleted, err := s.coll.Delete(keyBytes)
	if err == nil && !deleted {
		return fmt.Errorf("missing key %q", key)
	}
	return err
}

func (s *vibeDBEngineSession) ScanAllBytes() (int, error) {
	return s.scan.run(s.coll, s.scan.all)
}

// Session vends per-client key and scan buffers over the shared collection.
func (v *vibeDBEngine) Session(int) EngineSession {
	return &vibeDBEngineSession{coll: v.coll, scan: newVibeDBScanState()}
}
