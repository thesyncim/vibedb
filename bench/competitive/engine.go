package competitive

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"syscall"
)

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

// ErrNoIndex is returned by IndexedCount for an engine that has no secondary
// index over a JSON field. The plain key/value stores all return it: the
// honest answer for them is not a slower number, it is that the capability
// does not exist and the application has to build and maintain the inverted
// mapping itself.
var ErrNoIndex = errors.New("engine has no secondary index over a JSON field")

// Config is the per-instance configuration the harness varies.
type Config struct {
	// Dir is a private, empty directory the engine may fill.
	Dir string
	// Durability selects one explicit acknowledgement and persistence contract.
	// The zero value is buffered-visible.
	Durability DurabilityMode
	// ExactIndexes asks an index-capable engine to maintain the first N entries
	// of ExactIndexDefinitions. Zero is unindexed. The benchmark currently
	// exposes 0, 1, and 3 so every indexed row has one exact physical shape on
	// both VibeDB and SQLite.
	ExactIndexes uint8
	// MaxDocumentBytes is the exact corpus admission bound. Zero selects the
	// inline 1 KiB bound used by the historical corpus.
	MaxDocumentBytes int
	// CacheBytes is the read-cache budget every engine is given, so that no
	// engine wins or loses purely on how much of the corpus it was allowed to
	// keep resident. It is set to VibeDB's default ResidentBytes.
	CacheBytes int64
	// StorageProfile selects which optional, engine-provided storage
	// compression the footprint-only harnesses permit. In the zero-value
	// intrinsic lane, optional competitor compression is forced
	// off so the formats are compared without an extra codec. Production
	// enables the pinned dependency's recommended built-in block compression
	// where one exists. Engines without such a switch accept both profiles as
	// an explicitly labelled no-op.
	//
	// This is deliberately independent of Untuned: the storage profile defines
	// the question a disk-space run asks, while Untuned varies call shape.
	StorageProfile StorageProfile
	// PutLoop asks an engine that has both a bulk path and a mutation-replay
	// path to use the latter. Only store/durable distinguishes them.
	PutLoop bool
	// Untuned reverts the call-shape tuning this harness applies, so a row can
	// show what the engine's own defaults cost and no tuning claim has to be
	// taken on trust. BenchmarkTuning reports every tuned/untuned pair.
	//
	// It reverts only the choices that are this harness's to make — how an
	// operation is spelled against the engine's API. It does not revert the
	// choices that exist to make the engines comparable at all (the selected
	// storage profile and a common 64 MiB read-cache budget): flipping those
	// would not produce a fair "defaults" row, it would produce a different
	// benchmark.
	Untuned bool
}

// ExactIndexDefinition is one matched, scalar exact-index lane.
type ExactIndexDefinition struct {
	Name        string
	JSONPointer string
	QueryPath   string
	SQLitePath  string
}

// ExactIndexDefinitions is ordered so ExactIndexes=1 remains the selective
// country lane and ExactIndexes=3 adds the nested tier and region indexes.
var ExactIndexDefinitions = [...]ExactIndexDefinition{
	{Name: "country", JSONPointer: "/country", QueryPath: "country", SQLitePath: "$.country"},
	{Name: "tier", JSONPointer: "/profile/tier", QueryPath: "profile.tier", SQLitePath: "$.profile.tier"},
	{Name: "region", JSONPointer: "/profile/region", QueryPath: "profile.region", SQLitePath: "$.profile.region"},
}

const MaximumExactIndexes = uint8(len(ExactIndexDefinitions))

// ValidateExactIndexes rejects unmatchable benchmark configurations before an
// adapter creates storage.
func ValidateExactIndexes(count uint8) error {
	if count > MaximumExactIndexes {
		return fmt.Errorf("exact indexes = %d, maximum %d", count, MaximumExactIndexes)
	}
	return nil
}

func validateEngineExactIndexes(engine string, count uint8) error {
	if err := ValidateExactIndexes(count); err != nil {
		return err
	}
	if count != 0 && !IndexCapable(engine) {
		return fmt.Errorf("%s has no native exact index", engine)
	}
	return nil
}

func exactIndexCount(enabled bool) uint8 {
	if enabled {
		return 1
	}
	return 0
}

// ExactIndexProbe is one count plus proof that the adapter's native index
// bounded the query. A configured index that silently falls back to a document
// scan is a benchmark error, not an indexed result.
type ExactIndexProbe struct {
	Count        int
	IndexBounded bool
	IndexLookups int
}

// DefaultCacheBytes matches store/durable Options.ResidentBytes' default.
const DefaultCacheBytes = 64 << 20

// StorageProfile selects the optional compression policy for footprint and
// sustained-churn disk measurements. It is benchmark configuration, not a
// store format or production API.
type StorageProfile uint8

const (
	// StorageProfileIntrinsic forces Badger and Pebble to store uncompressed
	// SST blocks, matching engines that expose no optional compression switch.
	StorageProfileIntrinsic StorageProfile = iota
	// StorageProfileProduction permits each pinned dependency's recommended
	// built-in storage compression. Badger v4.9.5 and Pebble v1.1.5 both use
	// Snappy by default. It does not retrofit compression into engines that do
	// not offer it.
	StorageProfileProduction
)

func (p StorageProfile) String() string {
	switch p {
	case StorageProfileIntrinsic:
		return "intrinsic"
	case StorageProfileProduction:
		return "production"
	default:
		return fmt.Sprintf("storage-profile(%d)", p)
	}
}

// ParseStorageProfile parses the stable command-line spelling used by the disk
// footprint tools.
func ParseStorageProfile(value string) (StorageProfile, error) {
	switch value {
	case "intrinsic":
		return StorageProfileIntrinsic, nil
	case "production":
		return StorageProfileProduction, nil
	default:
		return StorageProfileIntrinsic, fmt.Errorf(
			"unknown storage profile %q (want intrinsic or production)", value,
		)
	}
}

type storageCompression uint8

const (
	storageCompressionUnsupported storageCompression = iota
	storageCompressionNone
	storageCompressionSnappy
)

// StorageProfileResolution is the effective optional-compression setting for
// one engine. Compression and Provenance are stable, whitespace-free output
// fields so a result file remains self-describing after dependencies move on.
type StorageProfileResolution struct {
	Profile     StorageProfile
	Compression string
	Provenance  string
	compression storageCompression
}

// ResolveStorageProfile validates a profile for an engine and describes the
// exact effective setting. Engines with no optional storage-compression knob
// intentionally resolve to unsupported/no-op instead of rejecting the
// production profile: they still belong in that comparison, but must not be
// mislabelled as compressed.
func ResolveStorageProfile(engine string, profile StorageProfile) (StorageProfileResolution, error) {
	if profile != StorageProfileIntrinsic && profile != StorageProfileProduction {
		return StorageProfileResolution{}, fmt.Errorf(
			"unknown storage profile %s", profile,
		)
	}
	switch engine {
	case "badger":
		version := dependencyVersion("github.com/dgraph-io/badger/v4")
		if profile == StorageProfileProduction {
			return StorageProfileResolution{
				Profile:     profile,
				Compression: "snappy-sst-blocks",
				Provenance: fmt.Sprintf(
					"github.com/dgraph-io/badger/v4@%s:WithCompression(options.Snappy);block-cache=CacheBytes;index-cache=0",
					version,
				),
				compression: storageCompressionSnappy,
			}, nil
		}
		return StorageProfileResolution{
			Profile:     profile,
			Compression: "none",
			Provenance: fmt.Sprintf(
				"github.com/dgraph-io/badger/v4@%s:WithCompression(options.None);block-cache=0;index-cache=CacheBytes",
				version,
			),
			compression: storageCompressionNone,
		}, nil
	case "pebble":
		version := dependencyVersion("github.com/cockroachdb/pebble")
		if profile == StorageProfileProduction {
			return StorageProfileResolution{
				Profile:     profile,
				Compression: "snappy-sst-blocks",
				Provenance: fmt.Sprintf(
					"github.com/cockroachdb/pebble@%s:Levels[*].Compression=pebble.SnappyCompression",
					version,
				),
				compression: storageCompressionSnappy,
			}, nil
		}
		return StorageProfileResolution{
			Profile:     profile,
			Compression: "none",
			Provenance: fmt.Sprintf(
				"github.com/cockroachdb/pebble@%s:Levels[*].Compression=pebble.NoCompression",
				version,
			),
			compression: storageCompressionNone,
		}, nil
	case "vibedb", "bbolt", "sqlite":
		return StorageProfileResolution{
			Profile:     profile,
			Compression: "unsupported/no-op",
			Provenance:  "benchmark-adapter:no-optional-compression-switch;profile-no-op",
			compression: storageCompressionUnsupported,
		}, nil
	default:
		return StorageProfileResolution{}, fmt.Errorf("unknown engine %q", engine)
	}
}

func dependencyVersion(path string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range info.Deps {
		if dep.Path != path {
			continue
		}
		if dep.Replace != nil {
			dep = dep.Replace
		}
		if dep.Version != "" {
			return dep.Version
		}
		return "unknown"
	}
	return "unknown"
}

// DurabilityMode is the benchmark contract at a successful mutation return.
// It deliberately separates visibility, ordinary OS sync, and power-safe
// persistence instead of overloading one sync boolean with unlike guarantees.
type DurabilityMode uint8

const (
	// DurabilityBufferedVisible acknowledges reader-visible state without a
	// stable-storage barrier. Checkpoint is the explicit persistence boundary.
	DurabilityBufferedVisible DurabilityMode = iota
	// DurabilityAsyncStableInFlight acknowledges reader-visible state after
	// bounded admission while a stable commit continues in the background.
	DurabilityAsyncStableInFlight
	// DurabilityOrdinarySync waits for the engine's ordinary fsync/msync-class
	// barrier. On Darwin this does not imply that a volatile drive cache drains.
	DurabilityOrdinarySync
	// DurabilityPowerSafe waits for the engine's strongest native power-loss
	// barrier on the benchmark platform.
	DurabilityPowerSafe
)

func (m DurabilityMode) String() string {
	switch m {
	case DurabilityBufferedVisible:
		return "buffered-visible"
	case DurabilityAsyncStableInFlight:
		return "async-stable-in-flight"
	case DurabilityOrdinarySync:
		return "ordinary-sync"
	case DurabilityPowerSafe:
		return "power-safe"
	default:
		return fmt.Sprintf("durability(%d)", m)
	}
}

// ParseDurabilityMode parses the stable command-line spelling.
func ParseDurabilityMode(value string) (DurabilityMode, error) {
	switch value {
	case "buffered-visible":
		return DurabilityBufferedVisible, nil
	case "async-stable-in-flight":
		return DurabilityAsyncStableInFlight, nil
	case "ordinary-sync":
		return DurabilityOrdinarySync, nil
	case "power-safe":
		return DurabilityPowerSafe, nil
	default:
		return DurabilityBufferedVisible, fmt.Errorf(
			"unknown durability mode %q (want buffered-visible, async-stable-in-flight, ordinary-sync, or power-safe)",
			value,
		)
	}
}

// ResolveDurabilityMode rejects guarantee shapes the engine cannot natively
// provide. Unsupported
// modes are not silently weakened or strengthened into a misleading row.
func ResolveDurabilityMode(engine string, requested DurabilityMode) (DurabilityMode, error) {
	supported := false
	switch engine {
	case "vibedb":
		supported = requested == DurabilityBufferedVisible ||
			requested == DurabilityAsyncStableInFlight ||
			requested == DurabilityOrdinarySync ||
			requested == DurabilityPowerSafe
	case "bbolt", "badger", "pebble":
		supported = requested == DurabilityBufferedVisible ||
			requested == DurabilityOrdinarySync
	case "sqlite":
		supported = requested == DurabilityBufferedVisible ||
			requested == DurabilityOrdinarySync ||
			requested == DurabilityPowerSafe
	default:
		return DurabilityBufferedVisible, fmt.Errorf("unknown engine %q", engine)
	}
	if !supported {
		return DurabilityBufferedVisible, fmt.Errorf(
			"%s does not natively support durability mode %s",
			engine, requested,
		)
	}
	return requested, nil
}

// BenchmarkDurabilityModes returns every concrete mode the engine can natively
// provide. Cross-engine tables join rows by the mode name, never by slice
// position.
func BenchmarkDurabilityModes(engine string) []DurabilityMode {
	switch engine {
	case "vibedb":
		return []DurabilityMode{
			DurabilityBufferedVisible,
			DurabilityAsyncStableInFlight,
			DurabilityOrdinarySync,
			DurabilityPowerSafe,
		}
	case "sqlite":
		return []DurabilityMode{
			DurabilityBufferedVisible,
			DurabilityOrdinarySync,
			DurabilityPowerSafe,
		}
	default:
		return []DurabilityMode{
			DurabilityBufferedVisible,
			DurabilityOrdinarySync,
		}
	}
}

// EngineSession is one client's private handle onto a shared engine. It carries
// exactly the per-operation surface the concurrent mixed harness drives from N
// worker goroutines at once. The point of the split is goroutine safety without
// giving up the shared storage handle: the underlying store/*sql.DB/*bolt.DB is
// shared across every session (that is what makes the measurement a concurrency
// measurement), but any mutable scratch an adapter would otherwise keep on the
// engine — a cached read snapshot, a reused decode buffer, a held read
// transaction — moves onto the session so no two clients race on it.
//
// Engine.Session vends these. An adapter whose operation surface is already safe
// for concurrent callers returns the engine itself (it satisfies this interface,
// being a superset); an adapter with per-caller scratch returns a fresh session
// that shares the handle but owns its own scratch.
type EngineSession interface {
	// Get, Put, Upsert, Delete, and ScanAllBytes have the same contracts as the
	// identically named Engine methods; see there.
	Get(dst []byte, key string) ([]byte, error)
	Put(key string, doc []byte) error
	Upsert(key string, doc []byte) error
	Delete(key string) error
	ScanAllBytes() (int, error)
}

// sessionReleaser is implemented by the sessions that hold releasable read state
// (a cached durable snapshot, or a held bbolt/Badger read transaction). The
// harness calls releaseReads on every session once the timed loop ends so no
// reader lease outlives the run and blocks the final checkpoint or Close. A
// self-returning session (a concurrency-safe engine) does not implement it, so
// the harness's type assertion simply skips it.
type sessionReleaser interface{ releaseReads() }

// ReleaseSession releases any read state a session opened (a cached snapshot or
// held read transaction) without closing the shared engine, so no reader lease
// outlives the timed loop and stalls the final checkpoint or Close. A session
// that is the engine itself holds no such state and is left untouched.
func ReleaseSession(s EngineSession) {
	if r, ok := s.(sessionReleaser); ok {
		r.releaseReads()
	}
}

// Engine is the uniform surface every competitor is driven through. Each
// method is the cheapest correct spelling that engine offers for the
// operation; where an engine has no cheap spelling, that is the finding.
type Engine interface {
	// Name identifies the engine and its configuration in benchmark output.
	Name() string
	// DurabilityMode is the resolved, machine-readable acknowledgement mode.
	DurabilityMode() DurabilityMode
	// Durability describes, in one phrase, what this engine was configured to
	// guarantee. It is printed next to every write measurement.
	Durability() string
	// Tuning describes the non-default settings applied and why.
	Tuning() string

	// Load writes the whole corpus using the engine's bulk path.
	Load(docs []Doc) error
	// Get fetches one document by key, appending to dst.
	Get(dst []byte, key string) ([]byte, error)
	// Put replaces a document the harness guarantees currently exists. The
	// existing-key lane charges every engine its existence resolution, but
	// does not claim a generally atomic conditional-replace API in the
	// presence of outside writers.
	Put(key string, doc []byte) error
	// Upsert writes a document whether or not key currently exists. Mixed
	// delete/churn workloads use it to restore a removed key without charging
	// engines whose update-only spelling cannot insert.
	Upsert(key string, doc []byte) error
	// Delete removes a document the harness guarantees currently exists, under
	// the same single-owner contract as Put.
	Delete(key string) error
	// Scan visits every stored document exactly once and returns the count.
	// It touches only the first byte of each value, so it measures iteration
	// and lookup, NOT throughput. See ScanAllBytes.
	Scan() (int, error)
	// ScanAllBytes visits every stored document exactly once and reads every
	// byte of every value through touchAll, so an engine cannot win by never
	// materialising the value. This is the column to read when the question is
	// "how fast can this engine hand me the data".
	ScanAllBytes() (int, error)
	// Visit hands every stored key and its exact value bytes to fn, exactly
	// once each. It exists for TestFullEquivalence, is never benchmarked, and
	// may therefore be spelled in whatever way is clearest rather than fastest.
	// The value slice is only valid for the duration of the call.
	Visit(fn func(key string, value []byte) error) error
	// FilterCount counts documents whose FilterPath equals value using no
	// secondary index.
	FilterCount(value string) (int, error)
	// IndexedCount answers the country-lane question through the first exact
	// index, or ErrNoIndex. ProbeExactIndex is the parametric correctness and
	// plan-proof surface for every configured index.
	IndexedCount(value string) (int, error)
	// ProbeExactIndex queries one configured ExactIndexDefinitions ordinal and
	// returns both its cardinality and native-plan engagement proof.
	ProbeExactIndex(index uint8, value string) (ExactIndexProbe, error)

	// Checkpoint makes every mutation acknowledged before the call recoverable
	// according to the engine's explicitly documented checkpoint barrier. In
	// buffered-visible mode this is the scheduled stable-persistence boundary.
	// An engine with bounded staging may force an earlier checkpoint under
	// pressure; benchmark rows must either expose that event or prove the
	// configured interval stays below it.
	Checkpoint() error
	// DiskBytes is the total on-disk footprint. Zero for a pure heap engine.
	DiskBytes() (int64, error)
	// Close releases the engine.
	Close() error

	// Session returns a per-client handle for concurrent operation. client is a
	// zero-based worker index in [0, N). Every session returned by one Engine
	// shares that engine's storage handle, so N sessions driven from N
	// goroutines exercise the store under real concurrent load; what a session
	// does NOT share is per-caller scratch (a cached read snapshot, a reused
	// buffer, a held read transaction), which is why an adapter that keeps such
	// state must vend a fresh session rather than return itself. An adapter that
	// keeps no per-caller scratch — its operation surface is already
	// goroutine-safe — returns the engine, which satisfies EngineSession. The
	// single-client harness path calls Session(0) too, so the returned handle
	// must reproduce the engine's own per-operation behaviour exactly.
	Session(client int) EngineSession
}

// MaintenanceFloorer is the optional representation-maintenance hook used by
// the sustained-churn disk harness after its final ordinary checkpoint. It is
// deliberately separate from Engine: a checkpoint is a durability boundary,
// while a maintenance floor may compact or rewrite the physical
// representation and must never be introduced into ordinary benchmark paths.
type MaintenanceFloorer interface {
	MaintenanceFloor() error
	MaintenanceFloorDescription() string
}

// touchAll reads every byte of v and folds it into an accumulator. Every
// engine's ScanAllBytes goes through this one function, so the per-byte cost is
// identical across the table and the differences between rows are storage
// differences.
//
// It is deliberately a byte-at-a-time fold rather than a memcpy or a word-wide
// sum. The point is not to benchmark this loop; it is to make it impossible for
// an engine to report a scan number that its own memory bandwidth could not
// support. BenchmarkScan touches value[0] only, and on ~248-byte documents its
// fastest row implies a rate several times this machine's memory bandwidth,
// which is only possible because the bytes are never read.
func touchAll(v []byte) byte {
	var acc byte
	for _, b := range v {
		acc ^= b
	}
	return acc
}

// Factory constructs a fresh engine over an empty directory.
type Factory struct {
	Name string
	New  func(Config) (Engine, error)
}

// Factories is the registry, in report order.
func Factories() []Factory {
	return []Factory{
		{Name: "vibedb", New: newVibeDB},
		{Name: "bbolt", New: newBbolt},
		{Name: "badger", New: newBadger},
		{Name: "pebble", New: newPebble},
		{Name: "sqlite", New: newSQLite},
	}
}

// IndexCapable reports whether the named engine can declare and maintain a
// secondary index over a JSON field. The three plain key/value stores cannot:
// their honest answer is not a slower number but that the application has to
// build and maintain the inverted mapping itself.
//
// It is a list rather than a probe because an indexed engine has to be told at
// creation time, before it holds any data, and asking it afterwards is what
// ErrNoIndex is for. TestFullEquivalence asserts that this list and every
// engine's actual IndexedCount behaviour agree, so the two cannot drift.
func IndexCapable(name string) bool {
	switch name {
	case "vibedb", "sqlite":
		return true
	default:
		return false
	}
}

// FactoryNamed looks a factory up by name.
func FactoryNamed(name string) (Factory, bool) {
	for _, f := range Factories() {
		if f.Name == name {
			return f, true
		}
	}
	return Factory{}, false
}

// dirBytes sums every regular file under root. Every disk-backed engine here
// is a directory or a single file inside one, so this is the one measurement
// that is comparable across all of them.
func dirBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			// A file the engine deleted underneath the walk is not part of
			// the footprint.
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		total += info.Size()
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return total, nil
	}
	return total, err
}

// dirAllocatedBytes sums the blocks the filesystem has actually allocated to
// every regular file under root, rather than the sizes those files report.
//
// The distinction is not academic and it silently wrecked the disk column.
// Badger truncates its value log and its memtable file to twice
// ValueLogFileSize and mmaps them; on this corpus that is two 128 MiB files
// whose st_size sums to 257 MiB and whose allocated blocks sum to 26.6 MiB.
// bbolt grows its file by doubling, leaving the tail unwritten: 45.8 MiB
// apparent, 29.7 MiB allocated. Neither engine is spending that space. Report
// this number alongside dirBytes, never instead of it — a sparse file still
// occupies address space and can still be filled in later.
func dirAllocatedBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			// No block accounting on this platform: fall back to the size, so
			// the column is never silently missing.
			total += info.Size()
			return nil
		}
		total += int64(st.Blocks) * 512
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return total, nil
	}
	return total, err
}

// FileSize is one file's apparent size and its allocated blocks.
type FileSize struct {
	Path           string
	ApparentBytes  int64
	AllocatedBytes int64
}

// DirFileSizes lists every regular file under root with both readings, so a
// disk figure can be attributed to the file that produced it rather than
// argued about. It is what shows that Badger's 257 MiB is two 128 MiB
// truncate-and-mmap files holding 0.02 MiB of blocks between them.
func DirFileSizes(root string) ([]FileSize, error) {
	var out []FileSize
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			rel = path
		}
		f := FileSize{Path: rel, ApparentBytes: info.Size(), AllocatedBytes: info.Size()}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			f.AllocatedBytes = int64(st.Blocks) * 512
		}
		out = append(out, f)
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return out, nil
	}
	return out, err
}

// Footprint is a steady-state memory and disk measurement.
type Footprint struct {
	// DiskBytes is the sum of every file's reported size. For an engine that
	// preallocates or sparsely extends its files this overstates what the
	// engine costs, badly — see DiskAllocatedBytes.
	DiskBytes int64
	// DiskAllocatedBytes is the sum of the blocks the filesystem has actually
	// given those files. Read both: DiskBytes alone reported Badger at 257 MiB
	// for a 24.9 MiB corpus it stores in 26.6 MiB of real blocks.
	DiskAllocatedBytes int64
	// HeapAlloc is runtime.MemStats.HeapAlloc after two collections. It
	// captures only Go-heap residency: an engine that keeps its working set
	// in an mmap (bbolt), in C-style arenas outside the Go heap
	// (modernc.org/sqlite, whose allocator maps its own pages), or in
	// manually managed blocks (pebble's cache is off-heap) will read far
	// lower here than it actually costs. Read it together with MaxRSS.
	HeapAlloc uint64
	// HeapSys is HeapAlloc's counterpart including free spans held by the
	// runtime, i.e. what the Go heap has taken from the OS. The gap between
	// HeapAlloc and HeapSys is transient load garbage the runtime has not
	// returned, not retained data.
	HeapSys uint64
	// Sys is everything the Go runtime has taken from the OS. Subtracting it
	// from MaxRSS estimates the memory an engine holds outside the runtime's
	// accounting entirely: bbolt's mmap of its file, modernc.org/sqlite's own
	// page allocator, Pebble's manually managed block cache, and VibeDB's
	// internal/storemem anonymous blocks.
	Sys uint64
	// RuntimeResident is Sys minus the span memory the runtime has handed back
	// to the OS, read after an explicit debug.FreeOSMemory. Unlike MaxRSS it is
	// an instantaneous steady-state reading rather than a high-water mark, so
	// it does not charge an engine for garbage its bulk load produced and then
	// released. It still cannot see off-runtime memory; MaxRSS is the only
	// column that can.
	RuntimeResident uint64
	// MaxRSS is the process resident-set high-water mark from getrusage.
	// It is a high-water mark, not an instantaneous reading, so it includes
	// the transient cost of the load. It is also the only number here that
	// sees off-heap and page-cache-mapped memory. Units differ by platform:
	// bytes on darwin, kibibytes on linux; MaxRSSBytes normalises.
	MaxRSS int64
}

// DiskFootprint is a read-only snapshot of a directory's apparent file sizes
// and allocated filesystem blocks. Unlike Engine.DiskBytes or Measure, taking
// this snapshot does not checkpoint, flush, compact, vacuum, or otherwise
// perturb the engine being observed.
type DiskFootprint struct {
	ApparentBytes  int64
	AllocatedBytes int64
}

// MeasureDiskFootprint collects both disk series used by the benchmark
// harness. AllocatedBytes is the comparison series; ApparentBytes remains
// beside it so sparse and preallocated files stay visible.
func MeasureDiskFootprint(dir string) (DiskFootprint, error) {
	apparent, err := dirBytes(dir)
	if err != nil {
		return DiskFootprint{}, err
	}
	allocated, err := dirAllocatedBytes(dir)
	if err != nil {
		return DiskFootprint{}, err
	}
	return DiskFootprint{
		ApparentBytes:  apparent,
		AllocatedBytes: allocated,
	}, nil
}

// MaxRSSBytes normalises the platform's getrusage unit.
func (f Footprint) MaxRSSBytes() int64 {
	if runtime.GOOS == "darwin" {
		return f.MaxRSS
	}
	return f.MaxRSS * 1024
}

// Measure collects the steady-state footprint. dir is the engine's directory,
// needed for the allocated-blocks reading; it is ignored when e is nil.
//
// Measure collects the steady-state footprint. Two collections are forced so
// that HeapAlloc reflects retained memory rather than garbage the load left
// behind. A nil engine measures the process baseline: the harness's own cost
// with no engine loaded, which every engine's numbers must be read against.
func Measure(e Engine, dir string) (Footprint, error) {
	runtime.GC()
	runtime.GC()
	// Hand back every span the load left behind, so the reading below is the
	// engine's retained cost and not the allocator's high-water mark.
	debug.FreeOSMemory()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)

	var disk, allocated int64
	if e != nil {
		var err error
		if disk, err = e.DiskBytes(); err != nil {
			return Footprint{}, err
		}
		if allocated, err = dirAllocatedBytes(dir); err != nil {
			return Footprint{}, err
		}
	}
	return Footprint{
		DiskBytes:          disk,
		DiskAllocatedBytes: allocated,
		HeapAlloc:          ms.HeapAlloc,
		HeapSys:            ms.HeapSys,
		Sys:                ms.Sys,
		RuntimeResident:    ms.Sys - ms.HeapReleased,
		MaxRSS:             int64(ru.Maxrss),
	}, nil
}

// tempDir makes a private directory for one engine instance.
func tempDir(prefix string) (string, error) {
	return os.MkdirTemp("", "vibebench-"+prefix+"-")
}
