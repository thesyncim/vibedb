package raftstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
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
	logicalPath   string
	parentPath    string
	base          string
	logicalBase   string
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
	family     *familyManifest

	poisoned             error
	poisonUnknown        bool
	closed               bool
	recoveredTornSlot    bool
	activationPending    bool
	syncCount            uint64
	begun                bool
	observedReadyID      uint64
	observedReadyDigest  [32]byte
	attemptedReady       retryKey
	attemptedReadyDigest [32]byte
	pending              *pendingMutation
}

// Metrics is a detached fixed-width view of live WAL retention and durable
// synchronization work. It exposes no path or mutable storage authority.
type Metrics struct {
	LiveBytes uint64
	Entries   uint64
	Syncs     uint64
}

func (store *Store) Metrics() Metrics {
	if store == nil {
		return Metrics{}
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return Metrics{}
	}
	entries := uint64(len(store.image.entries))
	live := store.image.liveBytes
	if live < 0 {
		live = 0
	}
	return Metrics{LiveBytes: uint64(live), Entries: entries, Syncs: store.syncCount}
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
	familyID := generationFamilyID(base, identity)
	createLease, err := acquireWALCreateLease(
		root, directoryInfo, base,
	)
	if err != nil {
		return nil, err
	}
	defer createLease.close()
	familyBase := familyManifestBase(familyID)
	familyEntry, familyErr := root.Lstat(familyBase)
	if familyErr != nil && !errors.Is(familyErr, os.ErrNotExist) {
		return nil, familyErr
	}
	entry, entryErr := root.Lstat(base)
	if entryErr != nil && !errors.Is(entryErr, os.ErrNotExist) {
		return nil, entryErr
	}
	if familyErr == nil {
		if !familyEntry.Mode().IsRegular() {
			return nil, ErrNamespaceChanged
		}
		// The official manifest has its own lifetime writer lease. Release the
		// path-owned creation lease before entering that lock domain so Create
		// never retains cold namespace coordination while opening live state.
		if err := createLease.close(); err != nil {
			return nil, err
		}
		// Create is deliberately resumable. Once both official names exist,
		// strict Open plus an exact pristine-source check settles an outcome-
		// unknown return without inventing a second creation grammar.
		return openCreatedSource(path, identity, key, bootstrap, options)
	}
	if entryErr == nil {
		if !entry.Mode().IsRegular() {
			return nil, ErrNamespaceChanged
		}
		store, resumeErr := resumeCreatedSource(
			absPath, parentPath, base, root, directoryInfo,
			identity, key, bootstrap, normalized,
		)
		if resumeErr != nil {
			return nil, resumeErr
		}
		keepRoot = true
		return store, nil
	}
	tempName := walCreateStageBase(base)
	if err := reclaimWALCreateStage(
		root, directoryInfo, tempName, normalized,
	); err != nil {
		return nil, err
	}
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
	if err := normalized.ops.syncDirectory(root); err != nil {
		cleanup()
		return nil, persistenceError("sync WAL parent directory", true, err)
	}
	if err := root.Remove(tempName); err != nil {
		cleanup()
		return nil, persistenceError("remove WAL creation stage", true, err)
	}
	if err := normalized.ops.syncDirectory(root); err != nil {
		cleanup()
		return nil, persistenceError("sync WAL creation stage removal", true, err)
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
	family, err := createFamilyManifest(
		root, parentPath, directoryInfo,
		familyState{
			slotGeneration: 1, phase: familyPhaseSource,
			familyID:           familyID,
			identityDigest:     generationIdentityDigest(identity),
			activeFileID:       header.fileID,
			activeHeaderDigest: header.headerDigest,
		},
		key, normalized,
	)
	if err != nil {
		cleanup()
		return nil, persistenceError("publish WAL family manifest", true, err)
	}
	store := &Store{
		path: absPath, logicalPath: absPath, parentPath: parentPath,
		base: base, logicalBase: base, root: root, directoryInfo: directoryInfo,
		file: file, fileInfo: fileInfo, locked: true, options: normalized, header: header,
		current: current, image: bootstrapImage(bootstrap.Snapshot), family: family,
		syncCount: 1,
	}
	keepRoot = true
	return store, nil
}

func openCreatedSource(
	path string,
	identity Identity,
	key Key,
	bootstrap Bootstrap,
	options Options,
) (*Store, error) {
	store, err := Open(
		path, identity, bootstrap.TopologyRecoveryEpoch, key, options,
	)
	if err != nil {
		return nil, err
	}
	store.mu.Lock()
	exact := store.pristineCreatedSourceLocked(bootstrap)
	if exact {
		err = store.settleCreatedSourceLocked()
	}
	store.mu.Unlock()
	if !exact {
		return nil, errors.Join(ErrInvalid, store.Close())
	}
	if err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return store, nil
}

func resumeCreatedSource(
	absPath string,
	parentPath string,
	base string,
	root *os.Root,
	directoryInfo os.FileInfo,
	identity Identity,
	key Key,
	bootstrap Bootstrap,
	options normalizedOptions,
) (*Store, error) {
	file, err := root.OpenFile(base, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	locked := false
	cleanup := func(cause error) (*Store, error) {
		if locked {
			cause = errors.Join(cause, storeio.UnlockWriter(file))
		}
		return nil, errors.Join(cause, file.Close())
	}
	if err := storeio.LockWriter(file); err != nil {
		return cleanup(errors.Join(ErrLocked, err))
	}
	locked = true
	if err := proveNamedFile(
		root, parentPath, directoryInfo, base, file, options.maxFileBytes,
	); err != nil {
		return cleanup(err)
	}
	staticBytes := make([]byte, StaticHeaderBytes)
	if _, err := file.ReadAt(staticBytes, 0); err != nil {
		return cleanup(fmt.Errorf("%w: read static header: %v", ErrCorrupt, err))
	}
	header, _, err := unmarshalStaticHeader(staticBytes, identity, key, options)
	if err != nil {
		return cleanup(err)
	}
	current, recoveredTorn, err := recoverCurrent(file, header, options)
	if err != nil {
		return cleanup(err)
	}
	image, generation, err := recoverRecords(file, &header, current, options)
	if err != nil {
		return cleanup(err)
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return cleanup(err)
	}
	store := &Store{
		path: absPath, logicalPath: absPath, parentPath: parentPath,
		base: base, logicalBase: base, root: root, directoryInfo: directoryInfo,
		file: file, fileInfo: fileInfo, locked: true, options: options, header: header,
		current: current, image: image, generation: generation,
		recoveredTornSlot: recoveredTorn, syncCount: 1,
	}
	if !store.pristineCreatedSourceLocked(bootstrap) {
		return cleanup(ErrInvalid)
	}
	family, err := createFamilyManifest(
		root, parentPath, directoryInfo,
		familyState{
			slotGeneration: 1, phase: familyPhaseSource,
			familyID:           generationFamilyID(base, identity),
			identityDigest:     generationIdentityDigest(identity),
			activeFileID:       header.fileID,
			activeHeaderDigest: header.headerDigest,
		},
		key, options,
	)
	if err != nil {
		return cleanup(persistenceError("resume WAL family manifest", true, err))
	}
	store.family = family
	if err := store.settleCreatedSourceLocked(); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return store, nil
}

func (store *Store) settleCreatedSourceLocked() error {
	if store == nil || store.root == nil || store.file == nil || store.family == nil ||
		store.family.file == nil {
		return ErrClosed
	}
	// This barrier is unconditional: Create may be settling an earlier return
	// whose official family link landed but whose directory Sync failed.
	if err := store.options.ops.syncDirectory(store.root); err != nil {
		return persistenceError("settle WAL creation namespace", true, err)
	}
	if err := proveNamedFile(
		store.root, store.parentPath, store.directoryInfo,
		store.base, store.file, store.options.maxFileBytes,
	); err != nil {
		return err
	}
	if err := proveNamedSizedFile(
		store.root, store.parentPath, store.directoryInfo,
		store.family.base, store.family.file, familyManifestBytes,
	); err != nil {
		return err
	}
	stageBase := store.family.base + ".stage"
	stage, err := store.root.Lstat(stageBase)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return err
	case !stage.Mode().IsRegular() || store.family.fileInfo == nil ||
		!os.SameFile(stage, store.family.fileInfo):
		return ErrNamespaceChanged
	default:
		if err := store.root.Remove(stageBase); err != nil {
			return err
		}
		if err := store.options.ops.syncDirectory(store.root); err != nil {
			return persistenceError("settle WAL family stage removal", true, err)
		}
	}
	if err := store.reclaimCreateAliasLocked(); err != nil {
		return err
	}
	if err := proveNamedSizedFile(
		store.root, store.parentPath, store.directoryInfo,
		store.family.base, store.family.file, familyManifestBytes,
	); err != nil {
		return err
	}
	return proveNamedFile(
		store.root, store.parentPath, store.directoryInfo,
		store.base, store.file, store.options.maxFileBytes,
	)
}

func (store *Store) reclaimCreateAliasLocked() error {
	if store == nil || store.root == nil || store.file == nil || store.fileInfo == nil {
		return ErrClosed
	}
	return settleWALCreateStage(
		store.root, store.parentPath, store.directoryInfo, store.base,
		store.file, store.fileInfo, store.family, store.options,
	)
}

func settleWALCreateStage(
	root *os.Root,
	parentPath string,
	directoryInfo os.FileInfo,
	logicalBase string,
	file *os.File,
	fileInfo os.FileInfo,
	family *familyManifest,
	options normalizedOptions,
) error {
	if root == nil || directoryInfo == nil || file == nil || fileInfo == nil ||
		family == nil || family.file == nil {
		return ErrClosed
	}
	// This barrier is unconditional. A prior attempt may have removed the
	// deterministic alias but returned outcome-unknown from the following
	// directory Sync. Absence is not durable evidence until that barrier is
	// retried by a later source-phase Create/Open.
	if err := options.ops.syncDirectory(root); err != nil {
		return persistenceError("settle WAL creation publication", true, err)
	}
	tempName := walCreateStageBase(logicalBase)
	entry, err := root.Lstat(tempName)
	if errors.Is(err, os.ErrNotExist) {
		if err := proveNamedSizedFile(
			root, parentPath, directoryInfo, family.base, family.file, familyManifestBytes,
		); err != nil {
			return err
		}
		return proveNamedFile(
			root, parentPath, directoryInfo, logicalBase, file, options.maxFileBytes,
		)
	}
	if err != nil || !entry.Mode().IsRegular() || !os.SameFile(entry, fileInfo) {
		return errors.Join(ErrNamespaceChanged, err)
	}
	// The surviving hard link witnesses an unsettled prior publication or
	// removal. Confirm both official names before dropping that witness.
	if err := proveNamedFile(
		root, parentPath, directoryInfo, logicalBase, file, options.maxFileBytes,
	); err != nil {
		return err
	}
	if err := proveNamedSizedFile(
		root, parentPath, directoryInfo, family.base, family.file, familyManifestBytes,
	); err != nil {
		return err
	}
	if err := root.Remove(tempName); err != nil {
		return err
	}
	if err := options.ops.syncDirectory(root); err != nil {
		return persistenceError("reclaim WAL creation alias", true, err)
	}
	if err := proveNamedSizedFile(
		root, parentPath, directoryInfo, family.base, family.file, familyManifestBytes,
	); err != nil {
		return err
	}
	return proveNamedFile(root, parentPath, directoryInfo, logicalBase, file, options.maxFileBytes)
}

// inspectWALCreateStage permits an exact same-inode creation witness while the
// source remains authoritative. Ordinary source Open neither removes it nor
// pays a directory barrier: first-generation selection is the irrevocable
// boundary that must settle it before the source can lose its final link.
func inspectWALCreateStage(
	root *os.Root,
	fileInfo os.FileInfo,
	logicalBase string,
) error {
	if root == nil || fileInfo == nil || logicalBase == "" {
		return ErrClosed
	}
	entry, err := root.Lstat(walCreateStageBase(logicalBase))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !entry.Mode().IsRegular() || !os.SameFile(entry, fileInfo) {
		return errors.Join(ErrNamespaceChanged, err)
	}
	return nil
}

func requireWALCreateStageAbsent(root *os.Root, logicalBase string) error {
	if root == nil || logicalBase == "" {
		return ErrClosed
	}
	_, err := root.Lstat(walCreateStageBase(logicalBase))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return errors.Join(ErrNamespaceChanged, err)
}

var walCreateNamespaceDomain = []byte("vibedb/raft-wal/create-namespace/fixed\x00")

func walCreateNamespaceID(logicalBase string) [16]byte {
	h := sha256.New()
	_, _ = h.Write(walCreateNamespaceDomain)
	var width [8]byte
	binary.LittleEndian.PutUint64(width[:], uint64(len(logicalBase)))
	_, _ = h.Write(width[:])
	_, _ = h.Write([]byte(logicalBase))
	var digest [sha256.Size]byte
	_ = h.Sum(digest[:0])
	var id [16]byte
	copy(id[:], digest[:len(id)])
	return id
}

func walCreateStageBase(logicalBase string) string {
	return generationFamilyPrefix(walCreateNamespaceID(logicalBase)) + ".create.stage"
}

func walCreateLockBase(logicalBase string) string {
	return generationFamilyPrefix(walCreateNamespaceID(logicalBase)) + ".create.lock"
}

type walCreateLease struct {
	file   *os.File
	locked bool
}

func acquireWALCreateLease(
	root *os.Root,
	directoryInfo os.FileInfo,
	logicalBase string,
) (*walCreateLease, error) {
	if root == nil || directoryInfo == nil || logicalBase == "" {
		return nil, ErrInvalid
	}
	base := walCreateLockBase(logicalBase)
	file, err := root.OpenFile(base, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	lease := &walCreateLease{file: file}
	fail := func(cause error) (*walCreateLease, error) {
		return nil, errors.Join(cause, lease.close())
	}
	if err := storeio.LockWriter(file); err != nil {
		return fail(errors.Join(ErrLocked, err))
	}
	lease.locked = true
	entry, entryErr := root.Lstat(base)
	fileInfo, fileErr := file.Stat()
	parentInfo, parentErr := root.Stat(".")
	if entryErr != nil || fileErr != nil || parentErr != nil ||
		!entry.Mode().IsRegular() || !os.SameFile(entry, fileInfo) ||
		fileInfo.Size() != 0 || !parentInfo.IsDir() ||
		!os.SameFile(parentInfo, directoryInfo) {
		return fail(errors.Join(
			ErrNamespaceChanged, entryErr, fileErr, parentErr,
		))
	}
	return lease, nil
}

func (lease *walCreateLease) close() error {
	if lease == nil {
		return nil
	}
	var err error
	if lease.locked {
		err = storeio.UnlockWriter(lease.file)
		lease.locked = false
	}
	if lease.file != nil {
		err = errors.Join(err, lease.file.Close())
		lease.file = nil
	}
	return err
}

func reclaimWALCreateStage(
	root *os.Root,
	directoryInfo os.FileInfo,
	base string,
	options normalizedOptions,
) error {
	// Absence is reusable authority only after a parent barrier. A prior crash
	// may have removed this deterministic name but lost or returned an unknown
	// result from its directory Sync; creating a different inode before settling
	// that unlink could resurrect the old name over the new construction image.
	if err := options.ops.syncDirectory(root); err != nil {
		return persistenceError("settle WAL creation stage namespace", true, err)
	}
	pinnedDirectory, err := root.Stat(".")
	if err != nil || !os.SameFile(directoryInfo, pinnedDirectory) {
		return errors.Join(ErrNamespaceChanged, err)
	}
	entry, err := root.Lstat(base)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !entry.Mode().IsRegular() {
		return errors.Join(ErrNamespaceChanged, err)
	}
	file, err := root.OpenFile(base, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	locked := false
	defer func() {
		if locked {
			_ = storeio.UnlockWriter(file)
		}
		_ = file.Close()
	}()
	if err := storeio.LockWriter(file); err != nil {
		return errors.Join(ErrLocked, err)
	}
	locked = true
	pinnedDirectory, err = root.Stat(".")
	if err != nil || !os.SameFile(directoryInfo, pinnedDirectory) {
		return errors.Join(ErrNamespaceChanged, err)
	}
	pinned, entryErr := root.Lstat(base)
	fileInfo, fileErr := file.Stat()
	if entryErr != nil || fileErr != nil || !pinned.Mode().IsRegular() ||
		!os.SameFile(pinned, fileInfo) {
		return errors.Join(ErrNamespaceChanged, entryErr, fileErr)
	}
	if err := root.Remove(base); err != nil {
		return err
	}
	if err := options.ops.syncDirectory(root); err != nil {
		return persistenceError("reclaim WAL creation stage", true, err)
	}
	return nil
}

func (store *Store) pristineCreatedSourceLocked(bootstrap Bootstrap) bool {
	if store == nil || store.file == nil || store.family != nil &&
		(store.family.recoveredTorn || store.family.state.phase != familyPhaseSource) ||
		store.generation.present || store.recoveredTornSlot || store.begun ||
		store.current.generation != 1 || store.current.recordSequence != 1 ||
		store.current.currentIncarnation != 0 || store.current.retryPresent ||
		store.current.retry != (retryKey{}) || store.current.retryDigest != ([32]byte{}) ||
		store.current.chainDigest == ([32]byte{}) ||
		store.current.first != store.header.reference.index+1 ||
		store.current.last != store.header.reference.index ||
		store.current.snapshotID != store.header.reference.id ||
		store.current.snapshotIndex != store.header.reference.index ||
		store.current.snapshotTerm != store.header.reference.term ||
		store.current.snapshotSize != store.header.reference.size ||
		store.current.snapshotChunks != 1 ||
		store.current.snapshotDigest != store.header.reference.digest ||
		store.current.topologyRecoveryEpoch != bootstrap.TopologyRecoveryEpoch ||
		store.header.topologyRecoveryEpoch != bootstrap.TopologyRecoveryEpoch ||
		!proto.Equal(store.header.snapshot, bootstrap.Snapshot) {
		return false
	}
	want := bootstrapImage(bootstrap.Snapshot)
	return proto.Equal(store.current.hard, want.hard) &&
		proto.Equal(store.image.hard, want.hard) &&
		store.image.first == want.first && store.image.last == want.last &&
		store.image.baseTerm == want.baseTerm && len(store.image.entries) == 0 &&
		store.image.liveBytes == 0
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
	logicalPath := absPath
	logicalBase := base
	family, selectedBase, err := openFamilyManifest(
		root, parentPath, directoryInfo, logicalBase, expected, key, normalized,
	)
	if err != nil {
		return nil, err
	}
	keepFamily := false
	defer func() {
		if !keepFamily && family != nil {
			_ = family.close()
		}
	}()
	store, err := openFamilySelectedStore(root, parentPath, directoryInfo, logicalPath,
		logicalBase, selectedBase, expected, expectedTopologyRecoveryEpoch, key, normalized, family)
	if err != nil {
		return nil, err
	}
	keepRoot = true
	keepFamily = true
	return store, nil
}

// openFamilySelectedStore borrows the already locked family and pinned root.
// Both cold Open and live selection adoption use the identical recovery and
// family authentication path. On failure only the new WAL file is released.
func openFamilySelectedStore(root *os.Root, parentPath string, directoryInfo os.FileInfo,
	logicalPath, logicalBase, base string, expected Identity, expectedTopologyRecoveryEpoch uint64,
	key Key, normalized normalizedOptions, family *familyManifest,
) (*Store, error) {
	absPath := filepath.Join(parentPath, base)
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
	state := family.state
	switch state.phase {
	case familyPhaseSource:
		if generation.present || base != logicalBase || header.fileID != state.activeFileID ||
			header.headerDigest != state.activeHeaderDigest {
			cleanup()
			return nil, fmt.Errorf("%w: WAL family source mismatch", ErrCorrupt)
		}
	case familyPhaseSelecting, familyPhaseActive:
		if !generation.present || generation.seal.familyID != state.familyID ||
			generation.seal.generation != state.activeGeneration ||
			generation.seal.bindingDigest != state.activeBindingDigest ||
			generation.seal.parentBindingDigest != state.parentBindingDigest ||
			header.fileID != state.activeFileID ||
			header.headerDigest != state.activeHeaderDigest ||
			generation.seal.sourceFileID != state.sourceFileID ||
			generation.seal.sourceChainDigest != state.sourceCutDigest ||
			generation.seal.snapshotBaseDigest != state.snapshotBaseDigest ||
			generation.seal.retentionCommitment != state.retentionCommitment ||
			(state.phase == familyPhaseActive && base != logicalBase) {
			cleanup()
			return nil, fmt.Errorf("%w: WAL family selection mismatch", ErrCorrupt)
		}
	default:
		cleanup()
		return nil, ErrCorrupt
	}
	if state.phase == familyPhaseSource {
		if err := inspectWALCreateStage(root, entryInfo, logicalBase); err != nil {
			cleanup()
			return nil, err
		}
	} else if err := requireWALCreateStageAbsent(root, logicalBase); err != nil {
		cleanup()
		return nil, err
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
		path: absPath, logicalPath: logicalPath, parentPath: parentPath,
		base: base, logicalBase: logicalBase, root: root, directoryInfo: directoryInfo,
		file: file, fileInfo: fileInfo, locked: true, options: normalized, header: header,
		current: current, image: image, generation: generation, recoveredTornSlot: recoveredTorn,
		family: family, activationPending: family != nil && family.state.phase == familyPhaseSelecting,
	}
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
	return proveNamedSizedFile(root, parentPath, directoryInfo, base, file, expectedSize)
}

func proveNamedSizedFile(root *os.Root, parentPath string, directoryInfo os.FileInfo, base string, file *os.File, expectedSize int64) error {
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
	if store.family != nil && store.family.recoveredTorn {
		return ErrGenerationFamilyQuarantined
	}
	if store.activationPending {
		return ErrGenerationActivationPending
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

// ResumePristineIncarnation reclaims an already minted incarnation only while
// the authenticated WAL proves that incarnation never persisted a Ready or
// changed the immutable snapshot-base image. This narrow activation-retry seam
// is safe because no Raft output can be emitted before its Ready is persisted.
// Ordinary process restart must use BeginIncarnation and may never call this.
func (store *Store) ResumePristineIncarnation(expected uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkBaseLocked(); err != nil {
		return err
	}
	base := bootstrapImage(store.header.snapshot)
	if expected == 0 || expected == math.MaxUint64 || store.begun || store.pending != nil ||
		store.generation.present || store.recoveredTornSlot ||
		store.current.currentIncarnation != expected ||
		store.current.generation != expected+1 ||
		store.current.recordSequence != 1 || store.current.retryPresent ||
		store.current.retry != (retryKey{}) || store.current.retryDigest != ([32]byte{}) ||
		store.current.first != store.header.reference.index+1 ||
		store.current.last != store.header.reference.index ||
		len(store.image.entries) != 0 || store.image.liveBytes != 0 ||
		!proto.Equal(store.current.hard, base.hard) || !proto.Equal(store.image.hard, base.hard) {
		return persistenceError("resume pristine incarnation", false, ErrInvalid)
	}
	store.begun = true
	store.observedReadyID = 0
	store.observedReadyDigest = [32]byte{}
	store.attemptedReady = retryKey{}
	store.attemptedReadyDigest = [32]byte{}
	return nil
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

// DurableCommit returns the exact persisted HardState commit index without
// cloning protobuf state. It is safe for bounded control-plane qualification.
func (store *Store) DurableCommit() (uint64, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.checkLocked(); err != nil {
		return 0, err
	}
	return store.image.hard.GetCommit(), nil
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
	familyErr := store.family.close()
	store.family = nil
	var rootErr error
	if store.root != nil {
		rootErr = store.root.Close()
		store.root = nil
	}
	return errors.Join(unlockErr, closeErr, familyErr, rootErr)
}
