package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/storeio"
)

var (
	ErrRuntimeRegistryCapacity = errors.New("splitcontroller: durable runtime registry capacity exhausted")
	ErrRuntimeRegistryInUse    = errors.New("splitcontroller: durable runtime operation is leased")
	ErrRuntimeTerminal         = errors.New("splitcontroller: durable runtime operation is terminal")
)

const (
	maxRuntimeRegistryOperations = 1 << 20
	runtimeRegistryBindingFormat = uint16(0)
	runtimeRegistryBindingBytes  = 80
	runtimeTerminalFormat        = uint16(0)
	runtimeTerminalBytes         = 144
)

var (
	runtimeTerminalMagic  = [8]byte{'V', 'D', 'B', 'S', 'P', 'G', 'C', 0}
	runtimeTerminalDomain = []byte("vibedb/splitcontroller/runtime-terminal\x00")
	runtimeBindingMagic   = [8]byte{'V', 'D', 'B', 'S', 'P', 'R', 'G', 0}
	runtimeBindingDomain  = []byte("vibedb/splitcontroller/runtime-registry-binding\x00")
)

// RuntimeTerminalAuthority returns an authenticated, nonzero terminal proof
// from the replicated catalog authority. The registry never infers terminal
// state from absent files, elapsed time, or a locally completed action.
type RuntimeTerminalAuthority interface {
	CertifyRuntimeTerminal(
		context.Context,
		OperationID,
		[sha256.Size]byte,
	) (proof [sha256.Size]byte, terminal bool, err error)
}

// RuntimeStoreRegistry pins one exact prepared split_runtime_root from the
// retained member manifest. It bounds concurrently open operation stores and
// shares each operation's exclusive writer lease across local callers.
type RuntimeStoreRegistry struct {
	mu sync.Mutex

	root       *os.Root
	rootPath   string
	lockFile   *os.File
	manifest   [sha256.Size]byte
	maxActive  int
	authority  RuntimeTerminalAuthority
	active     map[OperationID]*runtimeRegistryEntry
	collecting map[OperationID]struct{}
	closed     bool
}

type runtimeRegistryEntry struct {
	store      *DurableRuntimeStore
	references uint32
}

// RuntimeStoreLease is one reference to a shared operation store. Release is
// idempotent; Load and Persist fail after release.
type RuntimeStoreLease struct {
	mu        sync.Mutex
	registry  *RuntimeStoreRegistry
	operation OperationID
	store     *DurableRuntimeStore
	released  bool
}

// OpenRuntimeStoreRegistry opens an already prepared split runtime root. It
// never creates or guesses the root or manifest digest.
func OpenRuntimeStoreRegistry(
	runtimeRoot string,
	manifestDigest [sha256.Size]byte,
	maxActive int,
	authority RuntimeTerminalAuthority,
) (*RuntimeStoreRegistry, error) {
	if !filepath.IsAbs(runtimeRoot) || filepath.Clean(runtimeRoot) != runtimeRoot ||
		runtimeRoot == string(filepath.Separator) || manifestDigest == ([sha256.Size]byte{}) ||
		maxActive <= 0 || maxActive > maxRuntimeRegistryOperations {
		return nil, ErrRuntimeStore
	}
	info, err := os.Lstat(runtimeRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(ErrRuntimeStore, err)
	}
	root, err := os.OpenRoot(runtimeRoot)
	if err != nil {
		return nil, err
	}
	lockFile, err := openRuntimeRegular(root, "registry.lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	if err = storeio.LockWriter(lockFile); err != nil {
		_ = lockFile.Close()
		_ = root.Close()
		return nil, errors.Join(ErrRuntimeStore, err)
	}
	if err = bindRuntimeRegistryManifest(root, manifestDigest); err != nil {
		_ = storeio.UnlockWriter(lockFile)
		_ = lockFile.Close()
		_ = root.Close()
		return nil, err
	}
	return &RuntimeStoreRegistry{
		root: root, rootPath: runtimeRoot, lockFile: lockFile, manifest: manifestDigest,
		maxActive: maxActive, authority: authority,
		active:     make(map[OperationID]*runtimeRegistryEntry, maxActive),
		collecting: make(map[OperationID]struct{}),
	}, nil
}

// TopologySessionJournalPath returns the sole registry-owned durable session
// basename for this leased operation. Callers cannot select a sibling path or
// another operation. The operation directory must still be the exact regular
// directory opened by the registry; replacement with a symlink fails closed.
func (l *RuntimeStoreLease) TopologySessionJournalPath() (string, error) {
	return l.sessionJournalPath("topology-session")
}

// CaptureSessionJournalPath returns the distinct pre-cutover capture-session
// basename. It must not alias the post-cutover retained-prune session because
// those authorities bind different route generations.
func (l *RuntimeStoreLease) CaptureSessionJournalPath() (string, error) {
	return l.sessionJournalPath("capture-session")
}

func (l *RuntimeStoreLease) sessionJournalPath(name string) (string, error) {
	if l == nil {
		return "", ErrRuntimeStore
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.registry == nil || l.store == nil || l.registry.rootPath == "" {
		return "", ErrRuntimeStore
	}
	operation := runtimeOperationName(l.operation)
	info, err := l.registry.root.Lstat(operation)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.Join(ErrRuntimeStore, err)
	}
	return filepath.Join(l.registry.rootPath, operation, name), nil
}

func bindRuntimeRegistryManifest(root *os.Root, manifest [sha256.Size]byte) error {
	const name = "manifest.binding"
	file, err := openRuntimeRegular(root, name, os.O_RDONLY, 0)
	if err == nil {
		var raw [runtimeRegistryBindingBytes]byte
		_, readErr := io.ReadFull(file, raw[:])
		var trailing [1]byte
		trailingBytes, trailingErr := file.Read(trailing[:])
		closeErr := file.Close()
		if readErr != nil || (trailingErr != nil && !errors.Is(trailingErr, io.EOF)) ||
			trailingBytes != 0 || closeErr != nil ||
			!validRuntimeRegistryBinding(raw[:], manifest) {
			return errors.Join(ErrRuntimeStore, readErr, trailingErr, closeErr)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var raw [runtimeRegistryBindingBytes]byte
	copy(raw[:8], runtimeBindingMagic[:])
	binary.LittleEndian.PutUint16(raw[8:10], runtimeRegistryBindingFormat)
	binary.LittleEndian.PutUint32(raw[12:16], runtimeRegistryBindingBytes)
	copy(raw[16:48], manifest[:])
	digest := sha256.New()
	_, _ = digest.Write(runtimeBindingDomain)
	_, _ = digest.Write(raw[:48])
	_ = digest.Sum(raw[48:48])
	temporary, temporaryName, err := createRuntimeTemporary(root, name)
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = root.Remove(temporaryName)
		}
	}()
	if err = writeRuntimeBytes(temporary, raw[:]); err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = replaceRuntimeState(root, temporaryName, name); err != nil {
		return err
	}
	renamed = true
	if err = syncRuntimeRoot(root); err != nil {
		return errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	return nil
}

func validRuntimeRegistryBinding(raw []byte, manifest [sha256.Size]byte) bool {
	if len(raw) != runtimeRegistryBindingBytes ||
		!bytes.Equal(raw[:8], runtimeBindingMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != runtimeRegistryBindingFormat ||
		binary.LittleEndian.Uint16(raw[10:12]) != 0 ||
		binary.LittleEndian.Uint32(raw[12:16]) != runtimeRegistryBindingBytes ||
		!bytes.Equal(raw[16:48], manifest[:]) {
		return false
	}
	digest := sha256.New()
	_, _ = digest.Write(runtimeBindingDomain)
	_, _ = digest.Write(raw[:48])
	var computed [sha256.Size]byte
	_ = digest.Sum(computed[:0])
	return bytes.Equal(raw[48:], computed[:])
}

// Acquire returns a reference-counted lease for an exact operation. Repeated
// local acquisition shares one writer/store and does not consume another
// active-operation slot.
func (r *RuntimeStoreRegistry) Acquire(operation OperationID) (*RuntimeStoreLease, error) {
	if r == nil || operation == (OperationID{}) {
		return nil, ErrRuntimeStore
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrRuntimeStore
	}
	if _, busy := r.collecting[operation]; busy {
		return nil, ErrRuntimeRegistryInUse
	}
	terminal, err := runtimeTerminalExists(r.root, operation, r.manifest)
	if err != nil {
		return nil, err
	}
	if terminal {
		return nil, ErrRuntimeTerminal
	}
	if entry := r.active[operation]; entry != nil {
		if entry.references == ^uint32(0) {
			return nil, ErrRuntimeStore
		}
		entry.references++
		return newRuntimeStoreLease(r, operation, entry.store), nil
	}
	if len(r.active) >= r.maxActive {
		return nil, ErrRuntimeRegistryCapacity
	}
	store, err := openDurableRuntimeStoreAtRoot(r.root, operation, r.manifest, false)
	if err != nil {
		return nil, err
	}
	r.active[operation] = &runtimeRegistryEntry{store: store, references: 1}
	return newRuntimeStoreLease(r, operation, store), nil
}

func newRuntimeStoreLease(
	registry *RuntimeStoreRegistry,
	operation OperationID,
	store *DurableRuntimeStore,
) *RuntimeStoreLease {
	return &RuntimeStoreLease{registry: registry, operation: operation, store: store}
}

func (l *RuntimeStoreLease) Load(kind RuntimeStateKind, child uint8) (RuntimeState, bool, error) {
	if l == nil {
		return RuntimeState{}, false, ErrRuntimeStore
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.store == nil {
		return RuntimeState{}, false, ErrRuntimeStore
	}
	return l.store.Load(kind, child)
}

func (l *RuntimeStoreLease) Persist(
	kind RuntimeStateKind, child uint8, revision uint64, payload []byte,
) error {
	if l == nil {
		return ErrRuntimeStore
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.store == nil {
		return ErrRuntimeStore
	}
	return l.store.Persist(kind, child, revision, payload)
}

// PinnedStore exposes the exact leased durable store to an admitted local
// handle factory. The pointer remains valid only while this lease is retained.
func (l *RuntimeStoreLease) PinnedStore() (*DurableRuntimeStore, error) {
	if l == nil {
		return nil, ErrRuntimeStore
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.store == nil {
		return nil, ErrRuntimeStore
	}
	return l.store, nil
}

// PinnedRegistry returns the manifest-owned registry backing this live lease.
// It is used only to bind exact observation authority for a prepared child.
func (l *RuntimeStoreLease) PinnedRegistry() (*RuntimeStoreRegistry, error) {
	if l == nil {
		return nil, ErrRuntimeStore
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.registry == nil || l.store == nil {
		return nil, ErrRuntimeStore
	}
	return l.registry, nil
}

func (l *RuntimeStoreLease) Release() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	l.released = true
	registry, operation, store := l.registry, l.operation, l.store
	l.registry, l.store = nil, nil
	if registry == nil || store == nil {
		return ErrRuntimeStore
	}
	return registry.release(operation, store)
}

func (r *RuntimeStoreRegistry) release(operation OperationID, store *DurableRuntimeStore) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.active[operation]
	if entry == nil || entry.store != store || entry.references == 0 {
		return ErrRuntimeStore
	}
	entry.references--
	if entry.references != 0 {
		return nil
	}
	delete(r.active, operation)
	return entry.store.Close()
}

// CollectTerminal installs a durable terminal witness before reclaiming an
// operation directory. Once that witness exists, Acquire and direct store
// opening fail closed, including across a crash between marker publication and
// directory removal. Collection never runs while an operation is leased.
func (r *RuntimeStoreRegistry) CollectTerminal(
	ctx context.Context,
	operation OperationID,
) (bool, error) {
	if r == nil || ctx == nil || operation == (OperationID{}) {
		return false, ErrRuntimeStore
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false, ErrRuntimeStore
	}
	if r.active[operation] != nil {
		r.mu.Unlock()
		return false, ErrRuntimeRegistryInUse
	}
	if _, exists := r.collecting[operation]; exists {
		r.mu.Unlock()
		return false, ErrRuntimeRegistryInUse
	}
	r.collecting[operation] = struct{}{}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.collecting, operation)
		r.mu.Unlock()
	}()

	terminal, err := runtimeTerminalExists(r.root, operation, r.manifest)
	if err != nil {
		return false, err
	}
	if !terminal {
		if r.authority == nil {
			return false, ErrRuntimeStore
		}
		proof, certified, certifyErr := r.authority.CertifyRuntimeTerminal(
			ctx, operation, r.manifest,
		)
		if certifyErr != nil {
			return false, certifyErr
		}
		if !certified || proof == ([sha256.Size]byte{}) {
			return false, nil
		}
		if err = persistRuntimeTerminal(r.root, operation, r.manifest, proof); err != nil {
			return false, err
		}
	}
	if err = removeRuntimeOperation(r.root, operation); err != nil {
		return false, err
	}
	return true, nil
}

// CollectCertifiedTerminal consumes a proof already authenticated by the
// serving control protocol. It preserves the same marker-before-removal crash
// ordering as authority-driven collection without a shard-to-gateway RTT.
func (r *RuntimeStoreRegistry) CollectCertifiedTerminal(
	operation OperationID, proof [sha256.Size]byte,
) (bool, error) {
	if r == nil || operation == (OperationID{}) || proof == ([sha256.Size]byte{}) {
		return false, ErrRuntimeStore
	}
	r.mu.Lock()
	if r.closed || r.active[operation] != nil {
		r.mu.Unlock()
		return false, ErrRuntimeRegistryInUse
	}
	if _, busy := r.collecting[operation]; busy {
		r.mu.Unlock()
		return false, ErrRuntimeRegistryInUse
	}
	r.collecting[operation] = struct{}{}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.collecting, operation)
		r.mu.Unlock()
	}()
	terminal, err := runtimeTerminalExists(r.root, operation, r.manifest)
	if err != nil {
		return false, err
	}
	if !terminal {
		if err = persistRuntimeTerminal(r.root, operation, r.manifest, proof); err != nil {
			return false, err
		}
	}
	if err = removeRuntimeOperation(r.root, operation); err != nil {
		return false, err
	}
	return true, nil
}

// Close requires every lease to have been released. This avoids invalidating
// a caller in the middle of a durable write.
func (r *RuntimeStoreRegistry) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	if len(r.active) != 0 || len(r.collecting) != 0 {
		return ErrRuntimeRegistryInUse
	}
	r.closed = true
	err := errors.Join(storeio.UnlockWriter(r.lockFile), r.lockFile.Close(), r.root.Close())
	r.lockFile, r.root = nil, nil
	return err
}

func runtimeTerminalName(operation OperationID) string {
	return "terminal-" + runtimeOperationName(operation) + ".state"
}

// HasRuntimeTerminalWitness verifies an existing exact terminal witness
// without acquiring execution authority or creating a runtime namespace.
// Callers use this only with their already-authenticated operation/root cut.
func HasRuntimeTerminalWitness(path string, operation OperationID, manifest [sha256.Size]byte) (bool, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || operation == (OperationID{}) || manifest == ([sha256.Size]byte{}) {
		return false, ErrRuntimeStore
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, errors.Join(ErrRuntimeStore, err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return false, err
	}
	found, err := runtimeTerminalExists(root, operation, manifest)
	return found, errors.Join(err, root.Close())
}

func runtimeTerminalExists(
	root *os.Root, operation OperationID, manifest [sha256.Size]byte,
) (bool, error) {
	if root == nil {
		return false, ErrRuntimeStore
	}
	file, err := openRuntimeRegular(root, runtimeTerminalName(operation), os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var raw [runtimeTerminalBytes]byte
	_, readErr := io.ReadFull(file, raw[:])
	var trailing [1]byte
	trailingBytes, trailingErr := file.Read(trailing[:])
	closeErr := file.Close()
	if readErr != nil || (trailingErr != nil && !errors.Is(trailingErr, io.EOF)) ||
		trailingBytes != 0 || closeErr != nil {
		return false, errors.Join(ErrRuntimeStore, readErr, trailingErr, closeErr)
	}
	if !validRuntimeTerminal(raw[:], operation, manifest) {
		return false, ErrRuntimeStore
	}
	return true, nil
}

func persistRuntimeTerminal(
	root *os.Root,
	operation OperationID,
	manifest [sha256.Size]byte,
	proof [sha256.Size]byte,
) error {
	if proof == ([sha256.Size]byte{}) {
		return ErrRuntimeStore
	}
	name := runtimeTerminalName(operation)
	if exists, err := runtimeTerminalExists(root, operation, manifest); err != nil || exists {
		return err
	}
	var raw [runtimeTerminalBytes]byte
	copy(raw[:8], runtimeTerminalMagic[:])
	binary.LittleEndian.PutUint16(raw[8:10], runtimeTerminalFormat)
	binary.LittleEndian.PutUint32(raw[12:16], runtimeTerminalBytes)
	copy(raw[16:48], operation[:])
	copy(raw[48:80], manifest[:])
	copy(raw[80:112], proof[:])
	digest := sha256.New()
	_, _ = digest.Write(runtimeTerminalDomain)
	_, _ = digest.Write(raw[:112])
	_ = digest.Sum(raw[112:112])
	temporary, temporaryName, err := createRuntimeTemporary(root, name)
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = root.Remove(temporaryName)
		}
	}()
	if err = writeRuntimeBytes(temporary, raw[:]); err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = replaceRuntimeState(root, temporaryName, name); err != nil {
		return err
	}
	renamed = true
	if err = syncRuntimeRoot(root); err != nil {
		return errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	return nil
}

func validRuntimeTerminal(
	raw []byte, operation OperationID, manifest [sha256.Size]byte,
) bool {
	var zero [sha256.Size]byte
	if len(raw) != runtimeTerminalBytes || !bytes.Equal(raw[:8], runtimeTerminalMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != runtimeTerminalFormat ||
		binary.LittleEndian.Uint16(raw[10:12]) != 0 ||
		binary.LittleEndian.Uint32(raw[12:16]) != runtimeTerminalBytes ||
		!bytes.Equal(raw[16:48], operation[:]) || !bytes.Equal(raw[48:80], manifest[:]) ||
		bytes.Equal(raw[80:112], zero[:]) {
		return false
	}
	digest := sha256.New()
	_, _ = digest.Write(runtimeTerminalDomain)
	_, _ = digest.Write(raw[:112])
	var computed [sha256.Size]byte
	_ = digest.Sum(computed[:0])
	return bytes.Equal(raw[112:], computed[:])
}

func removeRuntimeOperation(root *os.Root, operation OperationID) error {
	name := runtimeOperationName(operation)
	info, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(ErrRuntimeStore, err)
	}
	operationRoot, err := root.OpenRoot(name)
	if err != nil {
		return err
	}
	lock, err := openRuntimeRegular(operationRoot, "writer.lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err == nil {
		err = storeio.LockWriter(lock)
	}
	if err != nil {
		_ = operationRoot.Close()
		if lock != nil {
			_ = lock.Close()
		}
		return errors.Join(ErrRuntimeRegistryInUse, err)
	}
	err = errors.Join(storeio.UnlockWriter(lock), lock.Close(), operationRoot.Close())
	if err != nil {
		return err
	}
	if err = root.RemoveAll(name); err != nil {
		return err
	}
	return syncRuntimeRoot(root)
}
