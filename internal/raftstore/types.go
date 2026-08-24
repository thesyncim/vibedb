package raftstore

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	StaticHeaderBytes = 4096
	CurrentSlotBytes  = 4096
	CurrentSlotCount  = 2
	HeaderBytes       = StaticHeaderBytes + CurrentSlotCount*CurrentSlotBytes

	DefaultMaxFileBytes   = 256 << 20
	DefaultMaxRecordBytes = 80 << 20
	DefaultMaxRecords     = 64 << 10
	DefaultMaxEntries     = 1 << 20
	DefaultMaxLiveBytes   = 128 << 20

	AbsoluteMaxFileBytes   = int64(4) << 30
	AbsoluteMaxRecordBytes = 96 << 20
	AbsoluteMaxRecords     = 1 << 20
	AbsoluteMaxEntries     = 4 << 20
	AbsoluteMaxLiveBytes   = int64(2) << 30

	MaxIdentityComponentBytes = 255
	MaxKeyIDBytes             = 255
	MaxWrappedKeyBytes        = 1024
	// MaxSnapshotBaseBytes bounds the small Raft snapshot certificate sealed
	// into a WAL generation. Bulk state is transferred and verified by the
	// replicated-state streaming artifact, never through this field.
	MaxSnapshotBaseBytes       = 3 << 19
	MaxSnapshotBaseRecordBytes = 2 << 20
	// Compatibility names retained for callers that construct the index-one
	// initial base. The on-disk format has one immutable-base concept.
	MaxBootstrapBytes       = MaxSnapshotBaseBytes
	MaxBootstrapRecordBytes = MaxSnapshotBaseRecordBytes
	MaxBootstrapMembers     = 64
	MaxReadyEntries         = raftmodel.MaxMessageEntries
	MinimumReadyLiveBytes   = raftmodel.MaxUncommittedEntriesSize + 32*MaxReadyEntries
	MinimumReadyRecordBytes = (40 + 32*MaxReadyEntries + raftmodel.MaxUncommittedEntriesSize + recordPrefixBytes + MaxKeyIDBytes + 16 + recordChecksumBytes + recordDamageGranule - 1) &^ (recordDamageGranule - 1)
)

var (
	ErrBounds              = errors.New("raftstore: configured or encoded bound exceeded")
	ErrClosed              = errors.New("raftstore: store is closed")
	ErrCorrupt             = errors.New("raftstore: WAL is corrupt")
	ErrFull                = errors.New("raftstore: preallocated WAL has insufficient Ready headroom")
	ErrIdentityMismatch    = errors.New("raftstore: immutable identity mismatch")
	ErrInvalid             = errors.New("raftstore: invalid argument or state transition")
	ErrKeyMismatch         = errors.New("raftstore: encryption key mismatch")
	ErrLocked              = errors.New("raftstore: another writer holds the WAL lock")
	ErrNamespaceChanged    = errors.New("raftstore: WAL namespace identity changed")
	ErrPersistenceDefinite = errors.New("raftstore: persistence definitely not accepted")
	ErrPersistenceUnknown  = errors.New("raftstore: persistence outcome unknown")
	ErrPlatformUnsupported = errors.New("raftstore: durable WAL publication is unsupported on this platform")
	ErrRetryConflict       = errors.New("raftstore: Ready retry payload changed")
	ErrUnsupportedSnapshot = errors.New("raftstore: Ready snapshots are not supported")
)

// Identity is the complete immutable placement identity sealed into a WAL.
// Every field is compared on Open. Fixed-width IDs are opaque binary values;
// Distribution and Shard are canonical caller-owned names.
type Identity struct {
	ClusterID            [16]byte
	ClusterIncarnation   [16]byte
	Distribution         string
	Shard                string
	AllocationGeneration uint64
	ShardIncarnation     [16]byte
	GroupID              [16]byte
	MemberID             uint64
	StoreID              [16]byte
}

// Bootstrap supplies the immutable snapshot base for a newly created WAL
// generation. Index one is the initial bootstrap; a newer certified base is
// used to start an offline-staged learner without copying the historical log.
// The recovery epoch is mutable committed topology state, so it is sealed into
// the base record and selected current slot rather than immutable identity.
type Bootstrap struct {
	TopologyRecoveryEpoch uint64
	Snapshot              *pb.Snapshot
}

// CapacityFormat identifies an exact WAL capacity-proof contract. Callers must
// reject formats they do not explicitly understand.
type CapacityFormat uint8

const (
	// CapacityFormatImmutableBase is one immutable snapshot base followed by a
	// bounded append-only suffix. Compaction creates another offline generation;
	// it never rewrites the active file in place.
	CapacityFormatImmutableBase CapacityFormat = 1
	CapacityFormatStatic                       = CapacityFormatImmutableBase
)

// CapacityProfile is a detached view of the immutable log-capacity facts
// needed by higher-level admission proofs. MaxEntries is sealed into the WAL's
// authenticated static header. LogBaseIndex is the authenticated durable
// snapshot index selected by this handle. Format is a capability contract, not
// a serving or capacity reservation.
//
// The current WAL format reports CapacityFormatImmutableBase. LogBaseIndex is
// the exact authenticated base and may be newer than one.
type CapacityProfile struct {
	Format       CapacityFormat
	LogBaseIndex uint64
	MaxEntries   uint64
}

// Key identifies and opens one AES-256-GCM key. Wrapped is opaque key-provider
// metadata persisted in the header; it is never interpreted by raftstore.
// Open permits Wrapped to be nil when a key provider learned it from another
// trusted source. A non-nil value must exactly match the header.
type Key struct {
	ID       string
	Material [32]byte
	Wrapped  []byte
}

// Options are hard bounds for both normal operation and recovery. Zero values
// select conservative defaults. The current format seals these exact values, so Open
// must supply the same bounds used by Create.
type Options struct {
	MaxFileBytes   int64
	MaxRecordBytes int
	MaxRecords     uint64
	MaxEntries     uint64
	MaxLiveBytes   int64

	// random and ops are deliberately package-private fault seams. Production
	// callers always use crypto/rand and direct file operations.
	random io.Reader
	ops    fileOps

	// allowSmallBounds is restricted to package tests that exercise exact
	// format geometry without advertising an undersized Store to raftmodel.
	allowSmallBounds bool
}

type normalizedOptions struct {
	maxFileBytes     int64
	maxRecordBytes   int
	maxRecords       uint64
	maxEntries       uint64
	maxLiveBytes     int64
	random           io.Reader
	ops              fileOps
	allowSmallBounds bool
}

type fileOps struct {
	preallocate     func(*os.File, int64) error
	ensureAllocated func(*os.File, int64) error
	writeAt         func(*os.File, []byte, int64) (int, error)
	sync            func(*os.File) error
	syncDirectory   func(*os.Root) error
}

func normalizeOptions(options Options) (normalizedOptions, error) {
	result := normalizedOptions{
		maxFileBytes:     options.MaxFileBytes,
		maxRecordBytes:   options.MaxRecordBytes,
		maxRecords:       options.MaxRecords,
		maxEntries:       options.MaxEntries,
		maxLiveBytes:     options.MaxLiveBytes,
		random:           options.random,
		ops:              options.ops,
		allowSmallBounds: options.allowSmallBounds,
	}
	if result.maxFileBytes == 0 {
		result.maxFileBytes = DefaultMaxFileBytes
	}
	if result.maxRecordBytes == 0 {
		result.maxRecordBytes = DefaultMaxRecordBytes
	}
	if result.maxRecords == 0 {
		result.maxRecords = DefaultMaxRecords
	}
	if result.maxEntries == 0 {
		result.maxEntries = DefaultMaxEntries
	}
	if result.maxLiveBytes == 0 {
		result.maxLiveBytes = DefaultMaxLiveBytes
	}
	if result.random == nil {
		result.random = rand.Reader
	}
	if result.ops.preallocate == nil {
		result.ops.preallocate = preallocate
	}
	if result.ops.ensureAllocated == nil {
		result.ops.ensureAllocated = ensureAllocated
	}
	if result.ops.writeAt == nil {
		result.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) { return file.WriteAt(data, offset) }
	}
	if result.ops.sync == nil {
		result.ops.sync = func(file *os.File) error { return file.Sync() }
	}
	if result.ops.syncDirectory == nil {
		result.ops.syncDirectory = syncPinnedDirectory
	}
	if result.maxFileBytes < HeaderBytes+recordDamageGranule || result.maxFileBytes > AbsoluteMaxFileBytes || result.maxFileBytes%recordDamageGranule != 0 ||
		result.maxRecordBytes < recordDamageGranule || result.maxRecordBytes > AbsoluteMaxRecordBytes || result.maxRecordBytes%recordDamageGranule != 0 ||
		result.maxRecords == 0 || result.maxRecords > AbsoluteMaxRecords ||
		result.maxEntries == 0 || result.maxEntries > AbsoluteMaxEntries ||
		result.maxLiveBytes <= 0 || result.maxLiveBytes > AbsoluteMaxLiveBytes {
		return normalizedOptions{}, fmt.Errorf("%w: invalid Options", ErrBounds)
	}
	if int64(result.maxRecordBytes) > result.maxFileBytes-HeaderBytes {
		return normalizedOptions{}, fmt.Errorf("%w: record bound exceeds WAL capacity", ErrBounds)
	}
	if result.maxLiveBytes > result.maxFileBytes-HeaderBytes-MaxSnapshotBaseRecordBytes-int64(result.maxRecordBytes) {
		return normalizedOptions{}, fmt.Errorf("%w: WAL does not reserve one maximum Ready beyond the live-log bound", ErrBounds)
	}
	if !result.allowSmallBounds && (result.maxRecordBytes < MinimumReadyRecordBytes || result.maxEntries < MaxReadyEntries || result.maxLiveBytes < MinimumReadyLiveBytes || result.maxRecords < 2) {
		return normalizedOptions{}, fmt.Errorf("%w: Options cannot encode every Ready admitted by raftmodel", ErrBounds)
	}
	return result, nil
}

// PersistenceError classifies whether a failed Persist definitely left the
// previous durable image intact or may have reached the WAL. Unknown outcomes
// accept only an exact retry until they are reconciled.
type PersistenceError struct {
	Op      string
	Unknown bool
	Err     error
}

func (e *PersistenceError) Error() string {
	if e == nil {
		return "<nil>"
	}
	classification := "definite"
	if e.Unknown {
		classification = "unknown"
	}
	return fmt.Sprintf("raftstore: %s persistence failure during %s: %v", classification, e.Op, e.Err)
}

func (e *PersistenceError) Unwrap() []error {
	if e == nil {
		return nil
	}
	class := ErrPersistenceDefinite
	if e.Unknown {
		class = ErrPersistenceUnknown
	}
	return []error{class, e.Err}
}

func persistenceError(op string, unknown bool, err error) error {
	if err == nil {
		return nil
	}
	return &PersistenceError{Op: op, Unknown: unknown, Err: err}
}
