package splitcontroller

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/storeio"
)

var (
	ErrRuntimeStore               = errors.New("splitcontroller: invalid durable split runtime store")
	ErrRuntimeStoreOutcomeUnknown = errors.New("splitcontroller: durable split runtime outcome unknown")
)

const (
	runtimeStateFormat = uint16(0)
	runtimeHeaderBytes = 96
	runtimeDigestBytes = sha256.Size

	// These slots retain only bounded control state. Source transition rows and
	// child artifact payloads stay in their independently durable collections
	// and repositories; copying either into this namespace would make restart
	// work and disk amplification proportional to shard cardinality.
	MaxCaptureControlBytes  = 64 << 10
	MaxArtifactControlBytes = 256 << 10
	MaxTailControlBytes     = 64 << 10
	MaxCertificateBytes     = 4 << 10
	MaxPruneControlBytes    = 16 << 20
	// Plan admission retains only the compact canonical PlanIntent plus fixed
	// catalog/digest authority. The transient catalog image used to authenticate
	// installation is deliberately not copied into every shard runtime.
	MaxPlanAdmissionControlBytes = MaxPlanIntentBytes + 160
)

var (
	runtimeStateMagic  = [8]byte{'V', 'D', 'B', 'S', 'P', 'R', 'T', 0}
	runtimeStateDomain = []byte("vibedb/splitcontroller/runtime-state\x00")
)

// RuntimeStateKind identifies one independently replaceable bounded control
// record. A stage record is additionally indexed by child ordinal.
type RuntimeStateKind uint8

const (
	RuntimeStateCapture RuntimeStateKind = iota + 1
	RuntimeStateArtifacts
	RuntimeStateStage
	RuntimeStateTail
	RuntimeStateCertificate
	RuntimeStatePrune
	RuntimeStatePlanAdmission
	RuntimeStateActionWitness
)

// RuntimeState is a detached recovered control record. Payload never aliases
// the store's fixed recovery buffers.
type RuntimeState struct {
	Revision uint64
	Payload  []byte
}

// DurableRuntimeStore owns one exact split operation beneath a retained member
// root. manifestDigest must authenticate the retained serving manifest used to
// open the member; it prevents copied control files from being resumed under a
// different local authority. Opening performs a fixed number of direct reads:
// it never lists or scans the member root.
type DurableRuntimeStore struct {
	mu sync.Mutex

	memberRoot    *os.Root
	runtimeRoot   *os.Root
	operationRoot *os.Root
	lockFile      *os.File
	operation     OperationID
	manifest      [sha256.Size]byte
	states        [7 + autosplit.MaxSplitChildren]runtimeStoredState
	ownsRuntime   bool
	closed        bool
	uncertain     bool
	syncOperation func(*os.Root) error
}

type runtimeStoredState struct {
	revision uint64
	payload  []byte
	has      bool
}

// OpenDurableRuntimeStore opens or creates the bounded state namespace for one
// operation. memberRoot must be the already-retained member root selected by
// the startup manifest, not an operator scratch directory.
func OpenDurableRuntimeStore(
	memberRoot string,
	operation OperationID,
	manifestDigest [sha256.Size]byte,
) (*DurableRuntimeStore, error) {
	if !filepath.IsAbs(memberRoot) || filepath.Clean(memberRoot) != memberRoot ||
		memberRoot == string(filepath.Separator) || operation == (OperationID{}) ||
		manifestDigest == ([sha256.Size]byte{}) {
		return nil, ErrRuntimeStore
	}
	memberInfo, err := os.Lstat(memberRoot)
	if err != nil || !memberInfo.IsDir() || memberInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(ErrRuntimeStore, err)
	}
	member, err := os.OpenRoot(memberRoot)
	if err != nil {
		return nil, err
	}
	closeMember := true
	defer func() {
		if closeMember {
			_ = member.Close()
		}
	}()
	if err = ensureRuntimeDirectory(member, "split-runtime"); err != nil {
		return nil, err
	}
	runtimeRoot, err := member.OpenRoot("split-runtime")
	if err != nil {
		return nil, err
	}
	closeRuntime := true
	defer func() {
		if closeRuntime {
			_ = runtimeRoot.Close()
		}
	}()
	store, err := openDurableRuntimeStoreAtRoot(runtimeRoot, operation, manifestDigest, true)
	if err != nil {
		return nil, err
	}
	store.memberRoot = member
	closeMember, closeRuntime = false, false
	return store, nil
}

func openDurableRuntimeStoreAtRoot(
	runtimeRoot *os.Root,
	operation OperationID,
	manifestDigest [sha256.Size]byte,
	ownsRuntime bool,
) (*DurableRuntimeStore, error) {
	return openDurableRuntimeStoreAtRootWithSync(runtimeRoot, operation, manifestDigest, ownsRuntime, syncRuntimeRoot)
}

func openDurableRuntimeStoreAtRootWithSync(
	runtimeRoot *os.Root,
	operation OperationID,
	manifestDigest [sha256.Size]byte,
	ownsRuntime bool,
	syncOperation func(*os.Root) error,
) (*DurableRuntimeStore, error) {
	if runtimeRoot == nil || operation == (OperationID{}) ||
		manifestDigest == ([sha256.Size]byte{}) {
		return nil, ErrRuntimeStore
	}
	terminal, err := runtimeTerminalExists(runtimeRoot, operation, manifestDigest)
	if err != nil {
		return nil, err
	}
	if terminal {
		return nil, ErrRuntimeTerminal
	}
	operationName := runtimeOperationName(operation)
	if err := ensureRuntimeDirectory(runtimeRoot, operationName); err != nil {
		return nil, err
	}
	operationRoot, err := runtimeRoot.OpenRoot(operationName)
	if err != nil {
		return nil, err
	}
	closeOperation := true
	defer func() {
		if closeOperation {
			_ = operationRoot.Close()
		}
	}()
	lockFile, err := openRuntimeRegular(operationRoot, "writer.lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err = storeio.LockWriter(lockFile); err != nil {
		_ = lockFile.Close()
		return nil, errors.Join(ErrRuntimeStore, err)
	}
	store := &DurableRuntimeStore{
		runtimeRoot: runtimeRoot, operationRoot: operationRoot,
		lockFile: lockFile, operation: operation, manifest: manifestDigest,
		ownsRuntime: ownsRuntime, syncOperation: syncOperation,
	}
	if err = store.recover(); err != nil {
		_ = store.Close()
		return nil, err
	}
	closeOperation = false
	return store, nil
}

// Load returns one detached state record. Stage requires a child ordinal;
// every other kind requires child zero.
func (s *DurableRuntimeStore) Load(kind RuntimeStateKind, child uint8) (RuntimeState, bool, error) {
	if s == nil {
		return RuntimeState{}, false, ErrRuntimeStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	index, _, _, err := runtimeStateSlot(kind, child)
	if err != nil || s.closed {
		return RuntimeState{}, false, errors.Join(ErrRuntimeStore, err)
	}
	if s.uncertain {
		return RuntimeState{}, false, ErrRuntimeStoreOutcomeUnknown
	}
	state := s.states[index]
	if !state.has {
		return RuntimeState{}, false, nil
	}
	return RuntimeState{Revision: state.revision, Payload: bytes.Clone(state.payload)}, true, nil
}

// Persist atomically advances one control record. Exact retries are
// idempotent. A fresh slot starts at revision one; subsequent writes must be
// exactly current+1, preventing stale reconcilers from replacing newer state.
// Returning ErrRuntimeStoreOutcomeUnknown means the caller must reopen before
// deciding whether to retry.
func (s *DurableRuntimeStore) Persist(
	kind RuntimeStateKind,
	child uint8,
	revision uint64,
	payload []byte,
) error {
	if s == nil {
		return ErrRuntimeStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	index, name, limit, err := runtimeStateSlot(kind, child)
	if err != nil || s.closed || revision == 0 || len(payload) == 0 || len(payload) > limit {
		return errors.Join(ErrRuntimeStore, err)
	}
	if s.uncertain {
		return ErrRuntimeStoreOutcomeUnknown
	}
	current := &s.states[index]
	if current.has && revision == current.revision && bytes.Equal(payload, current.payload) {
		return nil
	}
	if (!current.has && revision != 1) ||
		(current.has && (current.revision == ^uint64(0) || revision != current.revision+1)) {
		return ErrRuntimeStore
	}
	raw := make([]byte, runtimeHeaderBytes+len(payload)+runtimeDigestBytes)
	appendRuntimeState(raw, kind, child, revision, s.operation, s.manifest, payload)
	temporary, temporaryName, err := createRuntimeTemporary(s.operationRoot, name)
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = s.operationRoot.Remove(temporaryName)
		}
	}()
	if err = writeRuntimeBytes(temporary, raw); err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = replaceRuntimeState(s.operationRoot, temporaryName, name); err != nil {
		s.uncertain = true
		return errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	renamed = true
	if err = s.syncOperation(s.operationRoot); err != nil {
		s.uncertain = true
		return errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	current.revision = revision
	current.payload = append(current.payload[:0], payload...)
	current.has = true
	return nil
}

// Close releases the operation's exclusive writer lease and pinned roots.
func (s *DurableRuntimeStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	err := errors.Join(storeio.UnlockWriter(s.lockFile), s.lockFile.Close(), s.operationRoot.Close())
	if s.ownsRuntime {
		err = errors.Join(err, s.runtimeRoot.Close())
	}
	if s.memberRoot != nil {
		err = errors.Join(err, s.memberRoot.Close())
	}
	s.lockFile, s.operationRoot, s.runtimeRoot, s.memberRoot = nil, nil, nil, nil
	return err
}

func runtimeOperationName(operation OperationID) string {
	var encoded [64]byte
	hex.Encode(encoded[:], operation[:])
	return string(encoded[:])
}

func (s *DurableRuntimeStore) recover() error {
	for index := range s.states {
		kind, child := runtimeStateIdentity(index)
		_, name, limit, err := runtimeStateSlot(kind, child)
		if err != nil {
			return err
		}
		file, err := openRuntimeRegular(s.operationRoot, name, os.O_RDONLY, 0)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		info, statErr := file.Stat()
		if statErr != nil || info.Size() < runtimeHeaderBytes+runtimeDigestBytes ||
			info.Size() > int64(runtimeHeaderBytes+limit+runtimeDigestBytes) {
			_ = file.Close()
			return errors.Join(ErrRuntimeStore, statErr)
		}
		raw := make([]byte, int(info.Size()))
		_, readErr := io.ReadFull(file, raw)
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
		revision, payload, decodeErr := openRuntimeState(
			raw, kind, child, s.operation, s.manifest, limit,
		)
		if decodeErr != nil {
			return decodeErr
		}
		s.states[index] = runtimeStoredState{revision: revision, payload: payload, has: true}
	}
	// A recovered state file may be readable after a failed rename-directory
	// fence. Complete the exact operation directory's fence before publishing
	// any recovered state, especially a child preimage receipt that authorizes
	// independently durable data writes.
	if err := s.syncOperation(s.operationRoot); err != nil {
		return errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	return nil
}

func runtimeStateSlot(kind RuntimeStateKind, child uint8) (int, string, int, error) {
	if kind != RuntimeStateStage && child != 0 {
		return 0, "", 0, ErrRuntimeStore
	}
	switch kind {
	case RuntimeStateCapture:
		return 0, "capture.state", MaxCaptureControlBytes, nil
	case RuntimeStateArtifacts:
		return 1, "artifacts.state", MaxArtifactControlBytes, nil
	case RuntimeStateTail:
		return 2, "tail.state", MaxTailControlBytes, nil
	case RuntimeStateCertificate:
		return 3, "certificate.state", MaxCertificateBytes, nil
	case RuntimeStatePrune:
		return 4, "prune.state", MaxPruneControlBytes, nil
	case RuntimeStatePlanAdmission:
		return 5, "plan-admission.state", MaxPlanAdmissionControlBytes, nil
	case RuntimeStateActionWitness:
		return 6, "action-witness.state", MaxActionWitnessControlBytes, nil
	case RuntimeStateStage:
		if child >= autosplit.MaxSplitChildren {
			return 0, "", 0, ErrRuntimeStore
		}
		return 7 + int(child), fmt.Sprintf("stage-%d.state", child), MaxTailControlBytes, nil
	default:
		return 0, "", 0, ErrRuntimeStore
	}
}

func runtimeStateIdentity(index int) (RuntimeStateKind, uint8) {
	switch index {
	case 0:
		return RuntimeStateCapture, 0
	case 1:
		return RuntimeStateArtifacts, 0
	case 2:
		return RuntimeStateTail, 0
	case 3:
		return RuntimeStateCertificate, 0
	case 4:
		return RuntimeStatePrune, 0
	case 5:
		return RuntimeStatePlanAdmission, 0
	case 6:
		return RuntimeStateActionWitness, 0
	default:
		return RuntimeStateStage, uint8(index - 7)
	}
}

func appendRuntimeState(
	dst []byte,
	kind RuntimeStateKind,
	child uint8,
	revision uint64,
	operation OperationID,
	manifest [sha256.Size]byte,
	payload []byte,
) {
	copy(dst[0:8], runtimeStateMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], runtimeStateFormat)
	dst[10], dst[11] = byte(kind), child
	binary.LittleEndian.PutUint32(dst[12:16], uint32(len(dst)))
	binary.LittleEndian.PutUint64(dst[16:24], revision)
	copy(dst[24:56], operation[:])
	copy(dst[56:88], manifest[:])
	binary.LittleEndian.PutUint32(dst[88:92], uint32(len(payload)))
	copy(dst[runtimeHeaderBytes:], payload)
	digest := sha256.New()
	_, _ = digest.Write(runtimeStateDomain)
	_, _ = digest.Write(dst[:len(dst)-runtimeDigestBytes])
	_ = digest.Sum(dst[len(dst)-runtimeDigestBytes : len(dst)-runtimeDigestBytes])
}

func openRuntimeState(
	raw []byte,
	kind RuntimeStateKind,
	child uint8,
	operation OperationID,
	manifest [sha256.Size]byte,
	limit int,
) (uint64, []byte, error) {
	if len(raw) < runtimeHeaderBytes+runtimeDigestBytes ||
		!bytes.Equal(raw[:8], runtimeStateMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != runtimeStateFormat ||
		RuntimeStateKind(raw[10]) != kind || raw[11] != child ||
		binary.LittleEndian.Uint32(raw[12:16]) != uint32(len(raw)) ||
		binary.LittleEndian.Uint64(raw[16:24]) == 0 ||
		!bytes.Equal(raw[24:56], operation[:]) || !bytes.Equal(raw[56:88], manifest[:]) ||
		binary.LittleEndian.Uint32(raw[92:96]) != 0 {
		return 0, nil, ErrRuntimeStore
	}
	payloadBytes := int(binary.LittleEndian.Uint32(raw[88:92]))
	if payloadBytes == 0 || payloadBytes > limit ||
		runtimeHeaderBytes+payloadBytes+runtimeDigestBytes != len(raw) {
		return 0, nil, ErrRuntimeStore
	}
	digest := sha256.New()
	_, _ = digest.Write(runtimeStateDomain)
	_, _ = digest.Write(raw[:len(raw)-runtimeDigestBytes])
	var computed [sha256.Size]byte
	_ = digest.Sum(computed[:0])
	if !bytes.Equal(computed[:], raw[len(raw)-runtimeDigestBytes:]) {
		return 0, nil, ErrRuntimeStore
	}
	return binary.LittleEndian.Uint64(raw[16:24]),
		bytes.Clone(raw[runtimeHeaderBytes : len(raw)-runtimeDigestBytes]), nil
}

func ensureRuntimeDirectory(root *os.Root, name string) error {
	err := root.Mkdir(name, 0o700)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := root.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(ErrRuntimeStore, err)
	}
	if err == nil {
		return syncRuntimeRoot(root)
	}
	return nil
}

func openRuntimeRegular(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	file, err := root.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	opened, openErr := file.Stat()
	entry, entryErr := root.Lstat(name)
	if openErr != nil || entryErr != nil || !opened.Mode().IsRegular() ||
		!entry.Mode().IsRegular() || !os.SameFile(opened, entry) {
		_ = file.Close()
		return nil, errors.Join(ErrRuntimeStore, openErr, entryErr)
	}
	return file, nil
}

func createRuntimeTemporary(root *os.Root, base string) (*os.File, string, error) {
	var nonce [16]byte
	var encoded [32]byte
	for range 100 {
		if _, err := rand.Read(nonce[:]); err != nil {
			return nil, "", err
		}
		hex.Encode(encoded[:], nonce[:])
		name := "." + base + ".tmp-" + string(encoded[:])
		file, err := openRuntimeRegular(root, name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return file, name, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, "", err
		}
	}
	return nil, "", ErrRuntimeStore
}

func writeRuntimeBytes(file *os.File, raw []byte) error {
	for len(raw) != 0 {
		written, err := file.Write(raw)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		raw = raw[written:]
	}
	return nil
}
