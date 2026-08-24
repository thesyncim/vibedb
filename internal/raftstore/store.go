package raftstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/storeio"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Store is one locked, preallocated Raft WAL. All values returned through the
// raft.Storage interface are detached from its mutable recovery image.
type Store struct {
	mu sync.RWMutex

	path          string
	parentPath    string
	base          string
	root          *os.Root
	directoryInfo os.FileInfo
	file          *os.File
	fileInfo      os.FileInfo
	locked        bool

	options    normalizedOptions
	header     headerState
	current    currentState
	image      logImage
	generation generationRecovery

	poisoned             error
	poisonUnknown        bool
	closed               bool
	recoveredTornSlot    bool
	syncCount            uint64
	begun                bool
	observedReadyID      uint64
	observedReadyDigest  [32]byte
	attemptedReady       retryKey
	attemptedReadyDigest [32]byte
	pending              *pendingMutation
}

type pendingKind uint8

const (
	pendingBegin pendingKind = iota + 1
	pendingPersist
)

type pendingMutation struct {
	kind             pendingKind
	key              retryKey
	semanticDigest   [32]byte
	currentAttempted bool
	currentBytes     []byte
	currentOffset    int64
	next             currentState
	delta            imageDelta
}

var _ raftmodel.StableStore = (*Store)(nil)

// Create builds and syncs a complete preallocated image under a unique sibling
// name, then atomically publishes it at path and syncs the parent directory.
func Create(path string, identity Identity, key Key, bootstrap Bootstrap, options Options) (*Store, error) {
	if !platformSupported() {
		return nil, ErrPlatformUnsupported
	}
	normalized, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	staticHeader, header, err := marshalStaticHeader(identity, key, bootstrap, normalized)
	if err != nil {
		return nil, err
	}
	bootstrapPayload, _, err := marshalBootstrap(bootstrap, identity.MemberID)
	if err != nil {
		return nil, err
	}
	bootstrapRecord, bootstrapDigest, _, err := marshalRecord(recordKindBootstrap, 0, 1, 0, 0, header.headerDigest, bootstrapPayload, header, normalized)
	if err != nil {
		return nil, err
	}
	if len(bootstrapRecord) > MaxSnapshotBaseRecordBytes {
		return nil, fmt.Errorf("%w: snapshot-base record %d exceeds reserved %d", ErrBounds, len(bootstrapRecord), MaxSnapshotBaseRecordBytes)
	}
	walEnd := int64(HeaderBytes + len(bootstrapRecord))
	current := initialCurrent(header, walEnd, 1, bootstrapDigest)
	currentBytes, _, err := marshalCurrentSlot(current, 0, header)
	if err != nil {
		return nil, err
	}

	absPath, parentPath, base, root, directoryInfo, err := openNamespace(path)
	if err != nil {
		return nil, err
	}
	keepRoot := false
	defer func() {
		if !keepRoot {
			_ = root.Close()
		}
	}()
	if _, err := root.Lstat(base); err == nil {
		return nil, fmt.Errorf("%w: WAL path already exists", ErrInvalid)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	tempName := ".raftwal-" + hex.EncodeToString(header.fileID[:]) + ".tmp"
	file, err := root.OpenFile(tempName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create temporary WAL: %w", err)
	}
	locked := false
	published := false
	cleanup := func() {
		if locked {
			_ = storeio.UnlockWriter(file)
		}
		_ = file.Close()
		if !published {
			_ = root.Remove(tempName)
			_ = syncPinnedDirectory(root)
		}
	}
	if err := storeio.LockWriter(file); err != nil {
		cleanup()
		return nil, errors.Join(ErrLocked, err)
	}
	locked = true
	if err := normalized.ops.preallocate(file, normalized.maxFileBytes); err != nil {
		cleanup()
		return nil, persistenceError("preallocate", false, err)
	}
	if err := writeExactAt(normalized.ops, file, staticHeader, 0); err != nil {
		cleanup()
		return nil, persistenceError("write static header", false, err)
	}
	if err := writeExactAt(normalized.ops, file, bootstrapRecord, HeaderBytes); err != nil {
		cleanup()
		return nil, persistenceError("write bootstrap record", false, err)
	}
	if err := writeExactAt(normalized.ops, file, currentBytes, StaticHeaderBytes); err != nil {
		cleanup()
		return nil, persistenceError("write initial current slot", false, err)
	}
	if err := normalized.ops.sync(file); err != nil {
		cleanup()
		return nil, persistenceError("sync complete temporary WAL", false, err)
	}
	if err := proveNamedFile(root, parentPath, directoryInfo, tempName, file, normalized.maxFileBytes); err != nil {
		cleanup()
		return nil, persistenceError("prove temporary WAL namespace", false, err)
	}
	if _, err := root.Lstat(base); err == nil {
		cleanup()
		return nil, fmt.Errorf("%w: WAL path appeared during Create", ErrInvalid)
	} else if !errors.Is(err, os.ErrNotExist) {
		cleanup()
		return nil, err
	}
	if err := root.Link(tempName, base); err != nil {
		entry, statErr := root.Lstat(base)
		lockedEntry, fileErr := file.Stat()
		switch {
		case statErr == nil && fileErr == nil && entry.Mode().IsRegular() && os.SameFile(entry, lockedEntry):
			// The no-replace link is official despite the reported error.
		case errors.Is(statErr, os.ErrNotExist) && fileErr == nil:
			cleanup()
			return nil, persistenceError("publish WAL name", false, err)
		case statErr == nil && fileErr == nil:
			cleanup()
			return nil, persistenceError("publish WAL name", false, err)
		default:
			// Settlement itself failed. The official hard link may exist, so do
			// not remove the construction name or claim definite absence.
			published = true
			cleanup()
			return nil, persistenceError("settle WAL publication", true, errors.Join(err, statErr, fileErr))
		}
	}
	published = true
	if err := proveNamedFile(root, parentPath, directoryInfo, base, file, normalized.maxFileBytes); err != nil {
		cleanup()
		return nil, persistenceError("prove published WAL namespace", true, err)
	}
	if err := syncPinnedDirectory(root); err != nil {
		cleanup()
		return nil, persistenceError("sync WAL parent directory", true, err)
	}
	if err := root.Remove(tempName); err == nil {
		_ = syncPinnedDirectory(root)
	}
	if err := proveNamedFile(root, parentPath, directoryInfo, base, file, normalized.maxFileBytes); err != nil {
		cleanup()
		return nil, persistenceError("re-prove published WAL namespace", true, err)
	}
	fileInfo, err := file.Stat()
	if err != nil {
		cleanup()
		return nil, persistenceError("stat published WAL", true, err)
	}
	store := &Store{
		path: absPath, parentPath: parentPath, base: base, root: root, directoryInfo: directoryInfo,
		file: file, fileInfo: fileInfo, locked: true, options: normalized, header: header,
		current: current, image: bootstrapImage(bootstrap.Snapshot), syncCount: 1,
	}
	keepRoot = true
	return store, nil
}

// Open locks and recovers one existing WAL. The topology recovery epoch must
// exactly match the caller's independently trusted expectation before a handle
// is returned. Options are sealed into the static header and must exactly match
// those supplied to Create.
func Open(path string, expected Identity, expectedTopologyRecoveryEpoch uint64, key Key, options Options) (*Store, error) {
	if !platformSupported() {
		return nil, ErrPlatformUnsupported
	}
	if expectedTopologyRecoveryEpoch == 0 {
		return nil, fmt.Errorf("%w: zero expected topology recovery epoch", ErrInvalid)
	}
	normalized, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	if err := validateIdentity(expected); err != nil {
		return nil, err
	}
	if err := validateKey(key, false); err != nil {
		return nil, err
	}
	absPath, parentPath, base, root, directoryInfo, err := openNamespace(path)
	if err != nil {
		return nil, err
	}
	keepRoot := false
	defer func() {
		if !keepRoot {
			_ = root.Close()
		}
	}()
	entryInfo, err := root.Lstat(base)
	if err != nil {
		return nil, err
	}
	if !entryInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: WAL leaf is not a regular file", ErrNamespaceChanged)
	}
	file, err := root.OpenFile(base, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	locked := false
	cleanup := func() {
		if locked {
			_ = storeio.UnlockWriter(file)
		}
		_ = file.Close()
	}
	if err := storeio.LockWriter(file); err != nil {
		cleanup()
		return nil, errors.Join(ErrLocked, err)
	}
	locked = true
	if err := proveNamedFile(root, parentPath, directoryInfo, base, file, normalized.maxFileBytes); err != nil {
		cleanup()
		return nil, err
	}
	staticBytes := make([]byte, StaticHeaderBytes)
	if _, err := file.ReadAt(staticBytes, 0); err != nil {
		cleanup()
		return nil, fmt.Errorf("%w: read static header: %v", ErrCorrupt, err)
	}
	header, _, err := unmarshalStaticHeader(staticBytes, expected, key, normalized)
	if err != nil {
		cleanup()
		return nil, err
	}
	current, recoveredTorn, err := recoverCurrent(file, header, normalized)
	if err != nil {
		cleanup()
		return nil, err
	}
	image, generation, err := recoverRecords(file, &header, current, normalized)
	if err != nil {
		cleanup()
		return nil, err
	}
	if header.topologyRecoveryEpoch != expectedTopologyRecoveryEpoch {
		cleanup()
		return nil, fmt.Errorf("%w: expected topology recovery epoch %d, sealed %d", ErrIdentityMismatch, expectedTopologyRecoveryEpoch, header.topologyRecoveryEpoch)
	}
	if err := normalized.ops.ensureAllocated(file, normalized.maxFileBytes); err != nil {
		cleanup()
		return nil, persistenceError("restore physical WAL allocation", false, err)
	}
	if err := normalized.ops.sync(file); err != nil {
		cleanup()
		return nil, persistenceError("sync restored WAL allocation", false, err)
	}
	if err := proveNamedFile(root, parentPath, directoryInfo, base, file, normalized.maxFileBytes); err != nil {
		cleanup()
		return nil, err
	}
	fileInfo, err := file.Stat()
	if err != nil {
		cleanup()
		return nil, err
	}
	store := &Store{
		path: absPath, parentPath: parentPath, base: base, root: root, directoryInfo: directoryInfo,
		file: file, fileInfo: fileInfo, locked: true, options: normalized, header: header,
		current: current, image: image, generation: generation, recoveredTornSlot: recoveredTorn,
	}
	keepRoot = true
	return store, nil
}

func openNamespace(path string) (string, string, string, *os.Root, os.FileInfo, error) {
	if path == "" {
		return "", "", "", nil, nil, fmt.Errorf("%w: empty WAL path", ErrInvalid)
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", "", "", nil, nil, err
	}
	absPath = filepath.Clean(absPath)
	parentPath := filepath.Dir(absPath)
	base := filepath.Base(absPath)
	if base == "." || base == string(filepath.Separator) || base == "" {
		return "", "", "", nil, nil, fmt.Errorf("%w: invalid WAL leaf", ErrInvalid)
	}
	root, err := os.OpenRoot(parentPath)
	if err != nil {
		return "", "", "", nil, nil, err
	}
	directoryInfo, err := root.Stat(".")
	if err != nil || !directoryInfo.IsDir() {
		_ = root.Close()
		return "", "", "", nil, nil, fmt.Errorf("%w: invalid WAL parent", ErrNamespaceChanged)
	}
	return absPath, parentPath, base, root, directoryInfo, nil
}

func proveNamedFile(root *os.Root, parentPath string, directoryInfo os.FileInfo, base string, file *os.File, expectedSize int64) error {
	if root == nil || directoryInfo == nil || file == nil {
		return ErrNamespaceChanged
	}
	pinnedDirectory, err := root.Stat(".")
	if err != nil || !os.SameFile(directoryInfo, pinnedDirectory) {
		return fmt.Errorf("%w: pinned parent changed", ErrNamespaceChanged)
	}
	pinnedEntry, err := root.Lstat(base)
	if err != nil || !pinnedEntry.Mode().IsRegular() {
		return fmt.Errorf("%w: pinned WAL leaf changed", ErrNamespaceChanged)
	}
	fileInfo, err := file.Stat()
	if err != nil || !os.SameFile(fileInfo, pinnedEntry) || fileInfo.Size() != expectedSize {
		return fmt.Errorf("%w: WAL descriptor no longer names leaf/capacity", ErrNamespaceChanged)
	}
	liveRoot, err := os.OpenRoot(parentPath)
	if err != nil {
		return fmt.Errorf("%w: reopen live parent: %v", ErrNamespaceChanged, err)
	}
	defer liveRoot.Close()
	liveDirectory, err := liveRoot.Stat(".")
	if err != nil || !os.SameFile(directoryInfo, liveDirectory) {
		return fmt.Errorf("%w: live parent path was rebound", ErrNamespaceChanged)
	}
	liveEntry, err := liveRoot.Lstat(base)
	if err != nil || !liveEntry.Mode().IsRegular() || !os.SameFile(fileInfo, liveEntry) {
		return fmt.Errorf("%w: live WAL leaf was replaced", ErrNamespaceChanged)
	}
	return nil
}

func writeExactAt(operations fileOps, file *os.File, data []byte, offset int64) error {
	written, err := operations.writeAt(file, data, offset)
	if err != nil {
		return err
	}
	if written != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func (store *Store) checkBaseLocked() error {
	if store == nil || store.closed || store.file == nil {
		return ErrClosed
	}
	if store.poisoned != nil {
		if store.poisonUnknown {
			return errors.Join(ErrPersistenceUnknown, store.poisoned)
		}
		return store.poisoned
	}
	return nil
}

func (store *Store) checkLocked() error {
	if err := store.checkBaseLocked(); err != nil {
		return err
	}
	if store.pending != nil {
		return ErrPersistenceUnknown
	}
	return nil
}

// BeginIncarnation durably mints the never-reused process-lifetime identity to
// pass to raftmodel.NewNode. A caller must not invent incarnation values.
func (store *Store) BeginIncarnation() (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkBaseLocked(); err != nil {
		return 0, err
	}
	if store.pending != nil {
		if store.pending.kind != pendingBegin {
			return 0, ErrPersistenceUnknown
		}
		if err := store.settlePendingLocked(); err != nil {
			return 0, err
		}
		return store.current.currentIncarnation, nil
	}
	if store.begun {
		return 0, persistenceError("begin incarnation", false, ErrInvalid)
	}
	if store.current.currentIncarnation == math.MaxUint64 || store.current.generation == math.MaxUint64 {
		return 0, persistenceError("begin incarnation", false, ErrBounds)
	}
	if err := store.proveCurrentNamespace(); err != nil {
		store.poisonNamespace(err, false)
		return 0, persistenceError("prove WAL before incarnation", false, err)
	}
	next := store.current
	next.generation++
	next.activeSlot = 1 - store.current.activeSlot
	next.currentIncarnation++
	next.retryPresent = false
	next.retry = retryKey{}
	next.retryDigest = [32]byte{}
	data, _, err := marshalCurrentSlot(next, next.activeSlot, store.header)
	if err != nil {
		return 0, persistenceError("encode incarnation slot", false, err)
	}
	store.pending = &pendingMutation{kind: pendingBegin, currentBytes: data, currentOffset: int64(StaticHeaderBytes + next.activeSlot*CurrentSlotBytes), next: next}
	if err := store.settlePendingLocked(); err != nil {
		return 0, err
	}
	return store.current.currentIncarnation, nil
}

// CurrentIncarnation reports the last durably selected incarnation. Normal
// process startup must call BeginIncarnation rather than reuse this value; it is
// exposed as diagnostic recovery evidence. After Open resolves the selected
// image, BeginIncarnation must mint a fresh value before constructing a Node.
func (store *Store) CurrentIncarnation() uint64 {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store == nil || store.closed {
		return 0
	}
	return store.current.currentIncarnation
}

// CapacityProfile returns the authenticated static-base and sealed live-entry
// bound used by higher-level admission proofs. It neither reserves WAL space
// nor predicts whether ReserveReady will succeed for the next input.
func (store *Store) CapacityProfile() (CapacityProfile, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.checkLocked(); err != nil {
		return CapacityProfile{}, err
	}
	return CapacityProfile{
		Format:       CapacityFormatImmutableBase,
		LogBaseIndex: store.header.snapshot.GetMetadata().GetIndex(),
		MaxEntries:   store.options.maxEntries,
	}, nil
}

// ReserveReady checks proposal or inbound-append admission headroom for one
// worst-case durable Ready. Drivers must still capture and drain already
// available Ready work; empty Ready batches remain zero-write/zero-sync even
// when full (they still perform read-only namespace fencing).
func (store *Store) ReserveReady() error {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.checkLocked(); err != nil {
		return err
	}
	requiredLiveBytes := int64(MinimumReadyLiveBytes)
	if store.options.maxLiveBytes < requiredLiveBytes {
		requiredLiveBytes = store.options.maxLiveBytes
	}
	requiredEntries := uint64(MaxReadyEntries)
	if store.options.maxEntries < requiredEntries {
		requiredEntries = store.options.maxEntries
	}
	if store.current.recordSequence >= store.options.maxRecords ||
		store.options.maxFileBytes-store.current.walEnd < int64(store.options.maxRecordBytes) ||
		store.image.liveBytes > store.options.maxLiveBytes-requiredLiveBytes ||
		uint64(len(store.image.entries)) > store.options.maxEntries-requiredEntries {
		return ErrFull
	}
	return nil
}

// RemainingBytes reports the unused portion of the fixed physical allocation.
func (store *Store) RemainingBytes() int64 {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store == nil || store.closed {
		return 0
	}
	return store.options.maxFileBytes - store.current.walEnd
}

// Persist implements raftmodel.StableStore. Every nonempty accepted batch has
// both its aligned WAL record and selecting current slot synced before return.
func (store *Store) Persist(batch raftmodel.PersistBatch) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkBaseLocked(); err != nil {
		return err
	}
	if !store.begun || batch.NodeIncarnation == 0 || batch.NodeIncarnation != store.current.currentIncarnation || batch.ReadyID == 0 {
		return persistenceError("validate Ready identity", false, ErrInvalid)
	}
	if store.pending != nil && (store.pending.kind != pendingPersist || store.pending.key != (retryKey{incarnation: batch.NodeIncarnation, readyID: batch.ReadyID})) {
		return ErrPersistenceUnknown
	}
	retryingObserved := store.observedReadyID != 0 && batch.ReadyID == store.observedReadyID
	empty, payloadBytes, semanticDigest, delta, err := store.prepareBatchLocked(batch)
	if err != nil && store.pending != nil {
		return persistenceError("validate pending retry", false, ErrRetryConflict)
	}
	if err != nil && retryingObserved {
		return persistenceError("validate exact retry", false, ErrRetryConflict)
	}
	if err != nil {
		return persistenceError("validate Ready", false, err)
	}
	if store.pending != nil {
		if store.pending.semanticDigest != semanticDigest {
			return persistenceError("validate pending retry", false, ErrRetryConflict)
		}
		return store.settlePendingLocked()
	}
	if retryingObserved {
		if semanticDigest != store.observedReadyDigest {
			return persistenceError("validate exact retry", false, ErrRetryConflict)
		}
		if err := store.proveCurrentNamespace(); err != nil {
			store.poisonNamespace(err, false)
			return persistenceError("prove WAL on Ready retry", false, err)
		}
		return nil
	}
	if store.observedReadyID == math.MaxUint64 || batch.ReadyID != store.observedReadyID+1 {
		return persistenceError("validate Ready order", false, ErrInvalid)
	}
	key := retryKey{incarnation: batch.NodeIncarnation, readyID: batch.ReadyID}
	if store.attemptedReady != (retryKey{}) && (store.attemptedReady != key || store.attemptedReadyDigest != semanticDigest) {
		return persistenceError("validate attempted Ready retry", false, ErrRetryConflict)
	}
	if empty {
		if err := store.proveCurrentNamespace(); err != nil {
			store.poisonNamespace(err, false)
			return persistenceError("prove WAL for empty Ready", false, err)
		}
		store.observedReadyID = batch.ReadyID
		store.observedReadyDigest = semanticDigest
		return nil
	}
	if store.attemptedReady == (retryKey{}) {
		store.attemptedReady = key
		store.attemptedReadyDigest = semanticDigest
	}
	if store.current.recordSequence >= store.options.maxRecords {
		return persistenceError("reserve Ready", false, ErrFull)
	}
	if store.current.recordSequence == math.MaxUint64 || store.current.generation == math.MaxUint64 {
		return persistenceError("advance WAL sequence", false, ErrBounds)
	}
	sequence := store.current.recordSequence + 1
	flags := uint8(0)
	if batch.MustSync {
		flags = 1
	}
	record, recordDigest, _, err := marshalRecord(recordKindReady, flags, sequence, batch.NodeIncarnation, batch.ReadyID, store.current.chainDigest, payloadBytes, store.header, store.options)
	if err != nil {
		return persistenceError("encode Ready record", false, err)
	}
	end, ok := addInt64(store.current.walEnd, len(record))
	if !ok || end > store.options.maxFileBytes {
		return persistenceError("reserve Ready record", false, ErrFull)
	}
	if err := store.proveCurrentNamespace(); err != nil {
		store.poisonNamespace(err, false)
		return persistenceError("prove WAL before record", false, err)
	}
	if err := writeExactAt(store.options.ops, store.file, record, store.current.walEnd); err != nil {
		return persistenceError("write Ready record", false, err)
	}
	if err := store.options.ops.sync(store.file); err != nil {
		return persistenceError("sync Ready record", false, err)
	}
	store.syncCount++
	next := store.current
	next.activeSlot = 1 - store.current.activeSlot
	next.generation++
	next.walEnd = end
	next.recordSequence = sequence
	next.chainDigest = recordDigest
	next.hard = cloneHardState(delta.hard)
	next.last = delta.last
	next.retryPresent = true
	next.retry = retryKey{incarnation: batch.NodeIncarnation, readyID: batch.ReadyID}
	next.retryDigest = semanticDigest
	data, _, err := marshalCurrentSlot(next, next.activeSlot, store.header)
	if err != nil {
		return persistenceError("encode current slot", false, err)
	}
	store.pending = &pendingMutation{
		kind: pendingPersist, key: retryKey{incarnation: batch.NodeIncarnation, readyID: batch.ReadyID}, semanticDigest: semanticDigest,
		currentBytes: data, currentOffset: int64(StaticHeaderBytes + next.activeSlot*CurrentSlotBytes), next: next, delta: delta,
	}
	return store.settlePendingLocked()
}

func (store *Store) settlePendingLocked() error {
	pending := store.pending
	if pending == nil {
		return ErrInvalid
	}
	if err := store.proveCurrentNamespace(); err != nil {
		unknown := pending.currentAttempted
		store.poisonNamespace(err, unknown)
		return persistenceError("prove WAL before current slot", unknown, err)
	}
	existing := make([]byte, CurrentSlotBytes)
	if _, err := store.file.ReadAt(existing, pending.currentOffset); err != nil {
		return persistenceError("read current slot during retry settlement", pending.currentAttempted, err)
	}
	pending.currentAttempted = true
	if !bytes.Equal(existing, pending.currentBytes) {
		if err := writeExactAt(store.options.ops, store.file, pending.currentBytes, pending.currentOffset); err != nil {
			return persistenceError("write current slot", true, err)
		}
	}
	if err := store.options.ops.sync(store.file); err != nil {
		return persistenceError("sync current slot", true, err)
	}
	store.syncCount++
	if err := store.proveCurrentNamespace(); err != nil {
		store.poisonNamespace(err, true)
		return persistenceError("prove WAL after current slot", true, err)
	}
	store.current = pending.next
	switch pending.kind {
	case pendingBegin:
		store.begun = true
		store.observedReadyID = 0
		store.observedReadyDigest = [32]byte{}
		store.attemptedReady = retryKey{}
		store.attemptedReadyDigest = [32]byte{}
	case pendingPersist:
		commitImageDelta(&store.image, pending.delta)
		store.observedReadyID = pending.key.readyID
		store.observedReadyDigest = pending.semanticDigest
		store.attemptedReady = retryKey{}
		store.attemptedReadyDigest = [32]byte{}
	}
	store.pending = nil
	return nil
}

func (store *Store) prepareBatchLocked(batch raftmodel.PersistBatch) (bool, []byte, [32]byte, imageDelta, error) {
	if !canonicalEmptySnapshot(batch.Snapshot) {
		return false, nil, [32]byte{}, imageDelta{}, ErrUnsupportedSnapshot
	}
	if len(batch.Entries) > MaxReadyEntries {
		return false, nil, [32]byte{}, imageDelta{}, ErrBounds
	}
	if batch.HardState != nil && len(batch.HardState.ProtoReflect().GetUnknown()) != 0 {
		return false, nil, [32]byte{}, imageDelta{}, ErrInvalid
	}
	empty := isEmptyHardState(batch.HardState) && len(batch.Entries) == 0
	if empty {
		return true, nil, emptyReadyDigest(batch.MustSync), imageDelta{}, nil
	}
	payload := readyPayload{hard: batch.HardState, entries: batch.Entries, mustSync: batch.MustSync}
	delta, err := planReadyPayload(&store.image, payload, store.options)
	if err != nil {
		return false, nil, [32]byte{}, imageDelta{}, err
	}
	encoded, err := marshalReadyPayload(batch)
	if err != nil {
		return false, nil, [32]byte{}, imageDelta{}, err
	}
	return false, encoded, sha256.Sum256(encoded), delta, nil
}

func canonicalEmptySnapshot(snapshot *pb.Snapshot) bool {
	return snapshot == nil || (len(snapshot.GetData()) == 0 && snapshot.GetMetadata() == nil && len(snapshot.ProtoReflect().GetUnknown()) == 0)
}

func emptyReadyDigest(mustSync bool) [32]byte {
	var encoded [40]byte
	binary.LittleEndian.PutUint16(encoded[0:2], codecVersion)
	if mustSync {
		binary.LittleEndian.PutUint16(encoded[2:4], 1)
	}
	return sha256.Sum256(encoded[:])
}

func (store *Store) poisonNamespace(err error, unknown bool) {
	store.poisoned = errors.Join(ErrNamespaceChanged, err)
	store.poisonUnknown = unknown
}

func (store *Store) proveCurrentNamespace() error {
	return proveNamedFile(store.root, store.parentPath, store.directoryInfo, store.base, store.file, store.options.maxFileBytes)
}

// InitialState implements raft.Storage.
func (store *Store) InitialState() (*pb.HardState, *pb.ConfState, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.checkLocked(); err != nil {
		return nil, nil, err
	}
	return cloneHardState(store.image.hard), cloneConfState(store.header.snapshot.GetMetadata().GetConfState()), nil
}

// Entries implements raft.Storage and returns detached entry objects.
func (store *Store) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.checkLocked(); err != nil {
		return nil, err
	}
	if lo < store.image.first {
		return nil, raft.ErrCompacted
	}
	if hi < lo || hi > store.image.last+1 {
		return nil, raft.ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}
	start, end := int(lo-store.image.first), int(hi-store.image.first)
	selected := store.image.entries[start:end]
	limit := 1
	size := uint64(proto.Size(selected[0]))
	for limit < len(selected) {
		next := uint64(proto.Size(selected[limit]))
		if size > math.MaxUint64-next || size+next > maxSize {
			break
		}
		size += next
		limit++
	}
	return cloneEntries(selected[:limit]), nil
}

// Term implements raft.Storage.
func (store *Store) Term(index uint64) (uint64, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.checkLocked(); err != nil {
		return 0, err
	}
	base := store.image.first - 1
	if index == base {
		return store.image.baseTerm, nil
	}
	if index < base {
		return 0, raft.ErrCompacted
	}
	if index > store.image.last {
		return 0, raft.ErrUnavailable
	}
	return store.image.entries[index-store.image.first].GetTerm(), nil
}

// LastIndex implements raft.Storage.
func (store *Store) LastIndex() (uint64, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.checkLocked(); err != nil {
		return 0, err
	}
	return store.image.last, nil
}

// FirstIndex implements raft.Storage.
func (store *Store) FirstIndex() (uint64, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.checkLocked(); err != nil {
		return 0, err
	}
	return store.image.first, nil
}

// Snapshot implements raft.Storage and returns the detached immutable base
// sealed into this WAL generation.
func (store *Store) Snapshot() (*pb.Snapshot, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.checkLocked(); err != nil {
		return nil, err
	}
	return cloneSnapshot(store.header.snapshot), nil
}

// Identity returns a detached copy of the sealed immutable identity.
func (store *Store) Identity() Identity {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store == nil {
		return Identity{}
	}
	result := store.header.identity
	result.Distribution = strings.Clone(result.Distribution)
	result.Shard = strings.Clone(result.Shard)
	return result
}

// WrappedKeyMetadata returns detached opaque provider metadata.
func (store *Store) WrappedKeyMetadata() []byte {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store == nil {
		return nil
	}
	return append([]byte(nil), store.header.wrapped...)
}

func (store *Store) TopologyRecoveryEpoch() uint64 {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store == nil {
		return 0
	}
	return store.header.topologyRecoveryEpoch
}

// RecoveredTornCurrentSlot reports that Open selected one authenticated slot
// while the other was checksum-invalid or unexpectedly all zero. This format cannot
// distinguish a local in-flight tear from post-ack media damage; callers must
// emit high-severity telemetry and quarantine the member before serving or
// topology rejoin.
func (store *Store) RecoveredTornCurrentSlot() bool {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store != nil && store.recoveredTornSlot
}

func (store *Store) SyncCount() uint64 {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store == nil {
		return 0
	}
	return store.syncCount
}

// Close releases the writer lease. A poisoned handle may always be closed and
// then reopened to resolve its selected current slot.
func (store *Store) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	var unlockErr error
	if store.locked {
		unlockErr = storeio.UnlockWriter(store.file)
		store.locked = false
	}
	var closeErr error
	if store.file != nil {
		closeErr = store.file.Close()
		store.file = nil
	}
	var rootErr error
	if store.root != nil {
		rootErr = store.root.Close()
		store.root = nil
	}
	return errors.Join(unlockErr, closeErr, rootErr)
}
