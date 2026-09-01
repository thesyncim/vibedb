package raftstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	"github.com/thesyncim/vibedb/internal/storeio"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	nodeMetaName        = "NODEMETA.v1"
	nodeLockName        = "LOCK"
	nodeLogDir          = "log"
	nodeCheckpointDir   = "checkpoints"
	nodeMetaHeaderBytes = 48
)

var nodeMetaMagic = [8]byte{'V', 'D', 'B', 'N', 'O', 'D', 'E', 0}

type NodeStoreOptions struct {
	Store                                                        Options
	FrameBytes, Events, WaveIDs, EntriesPerGroup, CachedSegments int
}

type NodeBootstrap struct {
	GroupID         uint64
	NodeIncarnation uint64
	Snapshot        *pb.Snapshot
	HardState       *pb.HardState
}

type NodeReady struct {
	GroupID uint64
	Batch   raftmodel.PersistBatch
}

type NodeStore struct {
	mu                                   sync.Mutex
	dir                                  string
	identity                             Identity
	key                                  Key
	options                              normalizedOptions
	engine                               *seglog.Engine
	lock                                 *os.File
	pinnedDir, metaFile                  *os.File
	metaSize                             int64
	dirPathNUL, metaNameNUL              string
	crypto                               fileCrypto
	cryptoWork                           objectCryptoWorkspace
	groupAAD                             [40]byte
	groupNonce                           [12]byte
	plainArena, cipherArena, digestArena []byte
	readPlain                            []byte
	cachedBatch                          seglog.WaveID
	cachedGroup                          uint64
	cacheValid                           bool
	waveBatches                          [MaxPersistGroupBatches]seglog.ReadyBatch
	waveEntries                          [MaxPersistGroupBatches][]seglog.Entry
	waveHard                             [MaxPersistGroupBatches]seglog.HardState
	waveCheckpoint                       [MaxPersistGroupBatches]seglog.Checkpoint
	persistWaveTest                      func(seglog.Wave) error
	namespaceProofTest                   func() error
	closed                               bool
	poisoned                             error
}

type GroupView struct {
	store *NodeStore
	group uint64
}

var _ raftmodel.StableStore = (*GroupView)(nil)

// BorrowedEntry aliases the caller-provided plaintext buffer passed to
// ReadEntryInto. Data remains valid until that buffer is reused or modified.
type BorrowedEntry struct {
	Index, Term uint64
	Type        pb.EntryType
	Data        []byte
}

func defaultNodeOptions(options NodeStoreOptions) (NodeStoreOptions, normalizedOptions, error) {
	normalized, err := normalizeOptions(options.Store)
	if err != nil {
		return options, normalizedOptions{}, err
	}
	if options.FrameBytes == 0 {
		options.FrameBytes = normalized.maxRecordBytes
	}
	if options.Events == 0 {
		options.Events = int(normalized.maxEntries)
	}
	if options.WaveIDs == 0 {
		options.WaveIDs = int(normalized.maxRecords)
	}
	if options.EntriesPerGroup == 0 {
		options.EntriesPerGroup = int(normalized.maxEntries)
	}
	if options.CachedSegments == 0 {
		options.CachedSegments = 8
	}
	if options.FrameBytes < 72 || options.Events < 1 || options.WaveIDs < 1 || options.EntriesPerGroup < 1 || options.CachedSegments < 0 {
		return options, normalizedOptions{}, ErrBounds
	}
	return options, normalized, nil
}

func CreateNodeStore(dir string, identity Identity, key Key, bootstraps []NodeBootstrap, options NodeStoreOptions) (*NodeStore, error) {
	options, normalized, err := defaultNodeOptions(options)
	if err != nil {
		return nil, err
	}
	if err = validateIdentity(identity); err != nil {
		return nil, err
	}
	if err = validateKey(key, false); err != nil {
		return nil, err
	}
	if len(bootstraps) == 0 {
		return nil, ErrInvalid
	}
	if err = os.Mkdir(dir, 0o700); err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(dir)
		}
	}()
	lock, err := os.OpenFile(filepath.Join(dir, nodeLockName), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err = storeio.LockWriter(lock); err != nil {
		_ = lock.Close()
		return nil, errors.Join(ErrLocked, err)
	}
	if err = os.Mkdir(filepath.Join(dir, nodeLogDir), 0o700); err != nil {
		_ = lock.Close()
		return nil, err
	}
	if err = os.Mkdir(filepath.Join(dir, nodeCheckpointDir), 0o700); err != nil {
		_ = lock.Close()
		return nil, err
	}
	engine, err := seglog.CreateEngine(filepath.Join(dir, nodeLogDir))
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	cryptoState, err := makeFileCrypto(key, engine.LogID())
	if err != nil {
		_ = engine.Close()
		_ = lock.Close()
		return nil, err
	}
	store := &NodeStore{dir: dir, identity: identity, key: key, options: normalized, engine: engine, lock: lock, crypto: cryptoState, cryptoWork: newObjectCryptoWorkspace(cryptoState.dataKey, cryptoState.nonceKey)}
	if err = store.reserve(options, bootstraps); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err = store.bootstrap(bootstraps); err != nil {
		_ = store.Close()
		return nil, err
	}
	firstState, _ := engine.Group(bootstraps[0].GroupID)
	firstSnapshot, err := marshalSnapshot(bootstraps[0].Snapshot)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	ref := snapshotReference{id: firstState.Checkpoint.ID, digest: sha256.Sum256(firstSnapshot), size: uint64(len(firstSnapshot)), index: firstState.Checkpoint.Index, term: firstState.Checkpoint.Term}
	if err = writeNodeMeta(dir, identity, key, engine.LogID(), ref, boundsFromOptions(normalized), cryptoState); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err = store.bindNamespace(); err != nil {
		_ = store.Close()
		return nil, err
	}
	cleanup = false
	return store, nil
}

// OpenNodeStore authenticates node metadata before exposing recovered group
// state. Engine Open performs its startup durability sync before this returns.
func OpenNodeStore(dir string, expected Identity, key Key, options NodeStoreOptions) (*NodeStore, error) {
	options, normalized, err := defaultNodeOptions(options)
	if err != nil {
		return nil, err
	}
	if err = validateIdentity(expected); err != nil {
		return nil, err
	}
	if err = validateKey(key, false); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, nodeLockName), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err = storeio.LockWriter(lock); err != nil {
		_ = lock.Close()
		return nil, errors.Join(ErrLocked, err)
	}
	fail := func(engine *seglog.Engine, cause error) (*NodeStore, error) {
		if engine != nil {
			_ = engine.Close()
		}
		_ = storeio.UnlockWriter(lock)
		_ = lock.Close()
		return nil, cause
	}
	meta, err := os.ReadFile(filepath.Join(dir, nodeMetaName))
	if err != nil {
		return fail(nil, err)
	}
	identity, logID, err := openNodeMeta(meta, expected, key, normalized)
	if err != nil {
		return fail(nil, err)
	}
	engine, err := seglog.OpenEngine(filepath.Join(dir, nodeLogDir))
	if err != nil {
		return fail(nil, err)
	}
	if engine.LogID() != logID {
		return fail(engine, ErrIdentityMismatch)
	}
	cryptoState, err := makeFileCrypto(key, logID)
	if err != nil {
		return fail(engine, err)
	}
	store := &NodeStore{dir: dir, identity: identity, key: key, options: normalized, engine: engine, lock: lock, crypto: cryptoState, cryptoWork: newObjectCryptoWorkspace(cryptoState.dataKey, cryptoState.nonceKey)}
	if err = engine.Reserve(options.FrameBytes, options.Events, options.WaveIDs); err == nil {
		err = engine.ReserveReaders(options.CachedSegments)
	}
	if err == nil {
		for _, group := range engine.GroupIDs() {
			if err = engine.ReserveGroup(group, options.EntriesPerGroup); err != nil {
				break
			}
		}
	}
	if err == nil {
		store.plainArena = make([]byte, 0, options.FrameBytes)
		store.cipherArena = make([]byte, 0, options.FrameBytes)
		store.digestArena = make([]byte, 0, options.FrameBytes)
		store.readPlain = make([]byte, 0, options.FrameBytes)
		for i := range store.waveEntries {
			store.waveEntries[i] = make([]seglog.Entry, 0, MaxReadyEntries)
		}
		err = store.bindNamespace()
	}
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func openNodeMeta(data []byte, expected Identity, key Key, options normalizedOptions) (Identity, [16]byte, error) {
	var zero [16]byte
	if len(data) < nodeMetaHeaderBytes+16 || !bytes.Equal(data[:8], nodeMetaMagic[:]) ||
		binary.LittleEndian.Uint16(data[8:10]) != 1 || !allZero(data[14:16]) || !allZero(data[44:48]) {
		return Identity{}, zero, ErrCorrupt
	}
	keyIDBytes := int(binary.LittleEndian.Uint16(data[10:12]))
	wrappedBytes := int(binary.LittleEndian.Uint16(data[12:14]))
	headerBytes := nodeMetaHeaderBytes + keyIDBytes + wrappedBytes
	if headerBytes > len(data)-16 || !bytes.Equal(data[nodeMetaHeaderBytes:nodeMetaHeaderBytes+keyIDBytes], []byte(key.ID)) ||
		!bytes.Equal(data[nodeMetaHeaderBytes+keyIDBytes:headerBytes], key.Wrapped) {
		return Identity{}, zero, ErrIdentityMismatch
	}
	var logID [16]byte
	copy(logID[:], data[16:32])
	cryptoState, err := makeFileCrypto(key, logID)
	if err != nil {
		return Identity{}, zero, err
	}
	nonce := deriveNonce(cryptoState.nonceKey, "node-meta", 0)
	if !bytes.Equal(data[32:44], nonce[:]) {
		return Identity{}, zero, ErrCorrupt
	}
	plain, err := cryptoState.aead.Open(nil, nonce[:], data[headerBytes:], data[:headerBytes])
	if err != nil {
		return Identity{}, zero, ErrCorrupt
	}
	identity, _, bounds, err := unmarshalIdentity(plain)
	if err != nil {
		return Identity{}, zero, err
	}
	if identity != expected || bounds != boundsFromOptions(options) {
		return Identity{}, zero, ErrIdentityMismatch
	}
	return identity, logID, nil
}

func (s *NodeStore) reserve(options NodeStoreOptions, bootstraps []NodeBootstrap) error {
	if err := s.engine.Reserve(options.FrameBytes, options.Events, options.WaveIDs); err != nil {
		return err
	}
	if err := s.engine.ReserveReaders(options.CachedSegments); err != nil {
		return err
	}
	for _, b := range bootstraps {
		if err := s.engine.ReserveGroup(b.GroupID, options.EntriesPerGroup); err != nil {
			return err
		}
	}
	s.plainArena = make([]byte, 0, options.FrameBytes)
	s.cipherArena = make([]byte, 0, options.FrameBytes)
	s.digestArena = make([]byte, 0, options.FrameBytes)
	s.readPlain = make([]byte, 0, options.FrameBytes)
	for i := range s.waveEntries {
		s.waveEntries[i] = make([]seglog.Entry, 0, MaxReadyEntries)
	}
	return nil
}

func (s *NodeStore) bootstrap(bootstraps []NodeBootstrap) error {
	if !slices.IsSortedFunc(bootstraps, func(a, b NodeBootstrap) int { return intCompare(a.GroupID, b.GroupID) }) {
		return ErrInvalid
	}
	batches := make([]seglog.ReadyBatch, len(bootstraps))
	for i, b := range bootstraps {
		checkpoint, err := s.publishCheckpoint(b.GroupID, b.Snapshot)
		if err != nil {
			return err
		}
		h := b.HardState
		if h == nil {
			h = &pb.HardState{}
			h.Term = uint64Pointer(checkpoint.Term)
			h.Commit = uint64Pointer(checkpoint.Index)
		}
		batches[i] = seglog.ReadyBatch{GroupID: b.GroupID, Checkpoint: &checkpoint, Hard: &seglog.HardState{Term: h.GetTerm(), Vote: h.GetVote(), Commit: h.GetCommit()}}
	}
	id := bootstrapWaveID(batches)
	return s.engine.PersistWave(seglog.Wave{ID: id, Batches: batches})
}

func (s *NodeStore) bindNamespace() error {
	directory, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	meta, err := os.Open(filepath.Join(s.dir, nodeMetaName))
	if err != nil {
		_ = directory.Close()
		return err
	}
	stat, err := meta.Stat()
	if err != nil {
		_ = meta.Close()
		_ = directory.Close()
		return err
	}
	s.pinnedDir, s.metaFile, s.metaSize = directory, meta, stat.Size()
	s.dirPathNUL, s.metaNameNUL = s.dir+"\x00", nodeMetaName+"\x00"
	return s.proveNamespace()
}

func (s *NodeStore) proveNamespace() error {
	if s.namespaceProofTest != nil {
		return s.namespaceProofTest()
	}
	return provePinnedNamedFile(s.pinnedDir, s.dirPathNUL, s.metaNameNUL, s.metaFile, s.metaSize)
}

func intCompare(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func bootstrapWaveID(batches []seglog.ReadyBatch) seglog.WaveID {
	h := sha256.New()
	var word [8]byte
	for i := range batches {
		binary.LittleEndian.PutUint64(word[:], batches[i].GroupID)
		_, _ = h.Write(word[:])
		_, _ = h.Write(batches[i].Checkpoint.ID[:])
	}
	var id seglog.WaveID
	copy(id[:], h.Sum(nil))
	return id
}

func (s *NodeStore) publishCheckpoint(group uint64, snapshot *pb.Snapshot) (seglog.Checkpoint, error) {
	if snapshot == nil || snapshot.GetMetadata() == nil || snapshot.GetMetadata().GetConfState() == nil {
		return seglog.Checkpoint{}, ErrInvalid
	}
	plain, err := marshalSnapshot(snapshot)
	if err != nil {
		return seglog.Checkpoint{}, err
	}
	digest := sha256.Sum256(plain)
	var id [16]byte
	copy(id[:], digest[:16])
	index, term := snapshot.GetMetadata().GetIndex(), snapshot.GetMetadata().GetTerm()
	var aad [56]byte
	logID := s.engine.LogID()
	copy(aad[:16], logID[:])
	binary.LittleEndian.PutUint64(aad[16:24], group)
	binary.LittleEndian.PutUint64(aad[24:32], index)
	binary.LittleEndian.PutUint64(aad[32:40], term)
	copy(aad[40:56], id[:])
	nonce := s.cryptoWork.deriveObjectNonce("node-checkpoint", group, digest)
	ciphertext := s.crypto.aead.Seal(nil, nonce[:], plain, aad[:])
	header := make([]byte, 96)
	copy(header[:8], []byte("VDBCKP\x00\x00"))
	binary.LittleEndian.PutUint16(header[8:10], 1)
	binary.LittleEndian.PutUint64(header[16:24], group)
	binary.LittleEndian.PutUint64(header[24:32], index)
	binary.LittleEndian.PutUint64(header[32:40], term)
	copy(header[40:72], digest[:])
	copy(header[72:84], nonce[:])
	binary.LittleEndian.PutUint32(header[84:88], uint32(len(ciphertext)))
	name := fmt.Sprintf("%x.chk", id)
	tmp := filepath.Join(s.dir, nodeCheckpointDir, "."+name+".tmp")
	final := filepath.Join(s.dir, nodeCheckpointDir, name)
	if _, statErr := os.Stat(final); statErr == nil {
		existing, loadErr := s.loadCheckpoint(group, seglog.Checkpoint{ID: id, Index: index, Term: term})
		if loadErr == nil {
			existingPlain, marshalErr := marshalSnapshot(existing)
			if marshalErr == nil && sha256.Sum256(existingPlain) == digest && bytes.Equal(existingPlain, plain) {
				return seglog.Checkpoint{ID: id, Index: index, Term: term}, nil
			}
		}
		return seglog.Checkpoint{}, ErrCorrupt
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return seglog.Checkpoint{}, statErr
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return seglog.Checkpoint{}, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(header); err == nil {
		_, err = f.Write(ciphertext)
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		err = f.Close()
	}
	if err == nil {
		err = os.Rename(tmp, final)
	}
	if err == nil {
		d, _ := os.Open(filepath.Join(s.dir, nodeCheckpointDir))
		err = errors.Join(d.Sync(), d.Close())
	}
	if err != nil {
		return seglog.Checkpoint{}, err
	}
	ok = true
	return seglog.Checkpoint{ID: id, Index: index, Term: term}, nil
}

func (s *NodeStore) PersistWave(ready []NodeReady) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.poisoned != nil {
		return errors.Join(ErrPersistenceUnknown, s.poisoned)
	}
	if err := s.proveNamespace(); err != nil {
		s.poisoned = err
		return err
	}
	if len(ready) == 0 || len(ready) > MaxPersistGroupBatches {
		return ErrInvalid
	}
	for i := range ready {
		if ready[i].GroupID == 0 || i > 0 && ready[i-1].GroupID >= ready[i].GroupID {
			return ErrInvalid
		}
	}
	id := nodeWaveID(ready)
	totalPlain := 0
	for _, item := range ready {
		for _, entry := range item.Batch.Entries {
			if entry == nil || totalPlain > math.MaxInt-len(entry.GetData()) {
				return ErrBounds
			}
			totalPlain += len(entry.GetData())
		}
	}
	if totalPlain > cap(s.plainArena) || totalPlain+len(ready)*s.crypto.aead.Overhead() > cap(s.cipherArena) {
		return ErrBounds
	}
	s.plainArena = s.plainArena[:totalPlain]
	s.cipherArena = s.cipherArena[:0]
	plainOffset, mappedCount, duplicateCount := 0, 0, 0
	for _, item := range ready {
		batch := item.Batch
		state, ok := s.engine.Summary(item.GroupID)
		if !ok {
			return ErrInvalid
		}
		s.digestArena = s.digestArena[:0]
		encoded, err := marshalReadyPayloadInto(s.digestArena, batch)
		if err != nil {
			return err
		}
		s.digestArena = encoded
		if !canonicalEmptySnapshot(batch.Snapshot) {
			snapshotBytes, snapshotErr := marshalSnapshot(batch.Snapshot)
			if snapshotErr != nil {
				return snapshotErr
			}
			if len(s.digestArena)+len(snapshotBytes) > cap(s.digestArena) {
				return ErrBounds
			}
			s.digestArena = append(s.digestArena, snapshotBytes...)
		}
		digest := sha256.Sum256(s.digestArena)
		var retryDigest [16]byte
		copy(retryDigest[:], digest[:16])
		if batch.NodeIncarnation == state.NodeIncarnation && batch.ReadyID == state.ReadyID {
			if retryDigest != state.ReadyDigest {
				return ErrRetryConflict
			}
			duplicateCount++
			continue
		}
		nonceInput := &s.groupAAD
		logID := s.engine.LogID()
		copy(nonceInput[:16], logID[:])
		copy(nonceInput[16:32], id[:])
		binary.LittleEndian.PutUint64(nonceInput[32:40], item.GroupID)
		nonceDigest := sha256.Sum256(nonceInput[:])
		s.groupNonce = s.cryptoWork.deriveObjectNonce("node-group", item.GroupID, nonceDigest)
		dataStart := plainOffset
		entries := s.waveEntries[mappedCount][:0]
		for _, entry := range batch.Entries {
			data := entry.GetData()
			copy(s.plainArena[plainOffset:], data)
			entries = append(entries, seglog.Entry{Index: entry.GetIndex(), Term: entry.GetTerm(), Type: entry.GetType(), DataOffset: uint64(plainOffset - dataStart), DataBytes: uint64(len(data))})
			plainOffset += len(data)
		}
		s.waveEntries[mappedCount] = entries
		cipherStart := len(s.cipherArena)
		s.cipherArena = s.crypto.aead.Seal(s.cipherArena, s.groupNonce[:], s.plainArena[dataStart:plainOffset], nonceInput[:])
		ciphertext := s.cipherArena[cipherStart:]
		mapped := seglog.ReadyBatch{GroupID: item.GroupID, NodeIncarnation: batch.NodeIncarnation, ReadyID: batch.ReadyID, ReadyDigest: retryDigest, Entries: entries, Blob: ciphertext}
		if len(entries) > 0 && entries[0].Index <= state.LastIndex {
			mapped.ReplaceFrom = entries[0].Index
		}
		if batch.HardState != nil && !isEmptyHardState(batch.HardState) {
			s.waveHard[mappedCount] = seglog.HardState{Term: batch.HardState.GetTerm(), Vote: batch.HardState.GetVote(), Commit: batch.HardState.GetCommit()}
			mapped.Hard = &s.waveHard[mappedCount]
		}
		if !canonicalEmptySnapshot(batch.Snapshot) {
			cp, err := s.publishCheckpoint(item.GroupID, batch.Snapshot)
			if err != nil {
				return err
			}
			s.waveCheckpoint[mappedCount] = cp
			mapped.Checkpoint = &s.waveCheckpoint[mappedCount]
			mapped.TruncateIndex, mapped.TruncateTerm = cp.Index, cp.Term
		}
		s.waveBatches[mappedCount] = mapped
		mappedCount++
	}
	if duplicateCount == len(ready) {
		return nil
	}
	wave := seglog.Wave{ID: id, Batches: s.waveBatches[:mappedCount]}
	var persistErr error
	if s.persistWaveTest != nil {
		persistErr = s.persistWaveTest(wave)
	} else {
		persistErr = s.engine.PersistWave(wave)
	}
	if persistErr != nil {
		if fatal := s.engine.FatalError(); fatal != nil {
			s.poisoned = fatal
			return errors.Join(ErrPersistenceUnknown, persistErr, fatal)
		}
		return errors.Join(ErrInvalid, persistErr)
	}
	if err := s.proveNamespace(); err != nil {
		s.poisoned = err
		return errors.Join(ErrPersistenceUnknown, err)
	}
	s.cacheValid = false
	return nil
}

func nodeWaveID(ready []NodeReady) seglog.WaveID {
	var canonical [MaxPersistGroupBatches * 24]byte
	for i, item := range ready {
		word := canonical[i*24 : i*24+24]
		binary.LittleEndian.PutUint64(word[0:8], item.GroupID)
		binary.LittleEndian.PutUint64(word[8:16], item.Batch.NodeIncarnation)
		binary.LittleEndian.PutUint64(word[16:24], item.Batch.ReadyID)
	}
	digest := sha256.Sum256(canonical[:len(ready)*24])
	var id seglog.WaveID
	copy(id[:], digest[:16])
	return id
}

func (s *NodeStore) Group(group uint64) *GroupView { return &GroupView{store: s, group: group} }
func (v *GroupView) Persist(batch raftmodel.PersistBatch) error {
	return v.store.PersistWave([]NodeReady{{GroupID: v.group, Batch: batch}})
}

func (s *NodeStore) usable() error {
	if s.closed {
		return ErrClosed
	}
	if s.poisoned != nil {
		return errors.Join(ErrPersistenceUnknown, s.poisoned)
	}
	return nil
}

func (s *NodeStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.engine != nil {
		err = s.engine.Close()
	}
	if s.metaFile != nil {
		err = errors.Join(err, s.metaFile.Close())
	}
	if s.pinnedDir != nil {
		err = errors.Join(err, s.pinnedDir.Close())
	}
	if s.lock != nil {
		err = errors.Join(err, storeio.UnlockWriter(s.lock), s.lock.Close())
	}
	return err
}

func (v *GroupView) Term(index uint64) (uint64, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return 0, err
	}
	_, term, compacted, ok := v.store.engine.Lookup(v.group, index)
	if compacted {
		return 0, raft.ErrCompacted
	}
	if !ok {
		return 0, raft.ErrUnavailable
	}
	return term, nil
}
func (v *GroupView) FirstIndex() (uint64, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return 0, err
	}
	state, ok := v.store.engine.Group(v.group)
	if !ok {
		return 0, raft.ErrUnavailable
	}
	return state.Checkpoint.Index + 1, nil
}

// ReadEntryInto authenticates and decrypts exactly the containing group batch.
// It does not decode or copy values from any other group or wave.
func (v *GroupView) ReadEntryInto(index uint64, ciphertext, plaintext []byte) (BorrowedEntry, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return BorrowedEntry{}, err
	}
	loc, _, compacted, ok := v.store.engine.Lookup(v.group, index)
	if compacted {
		return BorrowedEntry{}, raft.ErrCompacted
	}
	if !ok {
		return BorrowedEntry{}, raft.ErrUnavailable
	}
	if loc.Bytes < uint64(v.store.crypto.aead.Overhead()) || uint64(len(ciphertext)) < loc.Bytes || cap(plaintext) < int(loc.Bytes)-v.store.crypto.aead.Overhead() {
		return BorrowedEntry{}, ErrBounds
	}
	if err := v.store.engine.PrepareSegment(loc.SegmentID); err != nil {
		return BorrowedEntry{}, err
	}
	ciphertext, err := v.store.engine.ReadLocation(loc, ciphertext[:loc.Bytes])
	if err != nil {
		return BorrowedEntry{}, err
	}
	aad := &v.store.groupAAD
	logID := v.store.engine.LogID()
	copy(aad[:16], logID[:])
	copy(aad[16:32], loc.BatchID[:])
	binary.LittleEndian.PutUint64(aad[32:40], v.group)
	nonceDigest := sha256.Sum256(aad[:])
	v.store.groupNonce = v.store.cryptoWork.deriveObjectNonce("node-group", v.group, nonceDigest)
	plain, err := v.store.crypto.aead.Open(plaintext[:0], v.store.groupNonce[:], ciphertext, aad[:])
	if err != nil || loc.DataOffset > uint64(len(plain)) || loc.DataBytes > uint64(len(plain))-loc.DataOffset {
		return BorrowedEntry{}, ErrCorrupt
	}
	return BorrowedEntry{Index: loc.Index, Term: loc.Term, Type: loc.Type, Data: plain[loc.DataOffset : loc.DataOffset+loc.DataBytes]}, nil
}
func (v *GroupView) LastIndex() (uint64, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return 0, err
	}
	state, ok := v.store.engine.Group(v.group)
	if !ok {
		return 0, raft.ErrUnavailable
	}
	if len(state.Entries) == 0 {
		return state.Checkpoint.Index, nil
	}
	return state.Entries[len(state.Entries)-1].Index, nil
}
func (v *GroupView) InitialState() (*pb.HardState, *pb.ConfState, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return nil, nil, err
	}
	state, ok := v.store.engine.Group(v.group)
	if !ok {
		return nil, nil, raft.ErrUnavailable
	}
	snapshot, err := v.store.loadCheckpoint(v.group, state.Checkpoint)
	if err != nil {
		return nil, nil, err
	}
	term, vote, commit := state.Hard.Term, state.Hard.Vote, state.Hard.Commit
	return &pb.HardState{Term: &term, Vote: &vote, Commit: &commit}, cloneConfState(snapshot.GetMetadata().GetConfState()), nil
}
func (v *GroupView) Snapshot() (*pb.Snapshot, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return nil, err
	}
	state, ok := v.store.engine.Group(v.group)
	if !ok {
		return nil, raft.ErrUnavailable
	}
	return v.store.loadCheckpoint(v.group, state.Checkpoint)
}
func (v *GroupView) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return nil, err
	}
	state, ok := v.store.engine.Group(v.group)
	if !ok {
		return nil, raft.ErrUnavailable
	}
	first := state.Checkpoint.Index + 1
	if lo < first {
		return nil, raft.ErrCompacted
	}
	if hi < lo || hi > first+uint64(len(state.Entries)) {
		return nil, raft.ErrUnavailable
	}
	result := make([]*pb.Entry, 0, hi-lo)
	var size uint64
	for index := lo; index < hi; index++ {
		loc := state.Entries[index-first]
		entry, err := v.store.readEntry(v.group, loc)
		if err != nil {
			return nil, err
		}
		entrySize := uint64(proto.Size(entry))
		if len(result) > 0 && (size > math.MaxUint64-entrySize || size+entrySize > maxSize) {
			break
		}
		size += entrySize
		result = append(result, entry)
	}
	return result, nil
}

func (s *NodeStore) readEntry(group uint64, loc seglog.EntryLocation) (*pb.Entry, error) {
	if !s.cacheValid || s.cachedGroup != group || s.cachedBatch != loc.BatchID {
		if err := s.engine.PrepareSegment(loc.SegmentID); err != nil {
			return nil, err
		}
		if loc.Bytes > uint64(cap(s.cipherArena)) {
			return nil, ErrBounds
		}
		ciphertext, err := s.engine.ReadLocation(loc, s.cipherArena[:loc.Bytes])
		if err != nil {
			return nil, err
		}
		aad := &s.groupAAD
		logID := s.engine.LogID()
		copy(aad[:16], logID[:])
		copy(aad[16:32], loc.BatchID[:])
		binary.LittleEndian.PutUint64(aad[32:40], group)
		nonceDigest := sha256.Sum256(aad[:])
		s.groupNonce = s.cryptoWork.deriveObjectNonce("node-group", group, nonceDigest)
		s.readPlain, err = s.crypto.aead.Open(s.readPlain[:0], s.groupNonce[:], ciphertext, aad[:])
		if err != nil {
			s.cacheValid = false
			return nil, ErrCorrupt
		}
		s.cachedGroup, s.cachedBatch, s.cacheValid = group, loc.BatchID, true
	}
	if loc.DataOffset > uint64(len(s.readPlain)) || loc.DataBytes > uint64(len(s.readPlain))-loc.DataOffset {
		return nil, ErrCorrupt
	}
	term, index, entryType := loc.Term, loc.Index, loc.Type
	data := append([]byte(nil), s.readPlain[loc.DataOffset:loc.DataOffset+loc.DataBytes]...)
	return &pb.Entry{Term: &term, Index: &index, Type: &entryType, Data: data}, nil
}

func (s *NodeStore) loadCheckpoint(group uint64, cp seglog.Checkpoint) (*pb.Snapshot, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, nodeCheckpointDir, fmt.Sprintf("%x.chk", cp.ID)))
	if err != nil {
		return nil, err
	}
	if len(data) < 96 || string(data[:8]) != "VDBCKP\x00\x00" || binary.LittleEndian.Uint16(data[8:10]) != 1 ||
		!allZero(data[10:16]) || binary.LittleEndian.Uint64(data[16:24]) != group ||
		binary.LittleEndian.Uint64(data[24:32]) != cp.Index || binary.LittleEndian.Uint64(data[32:40]) != cp.Term ||
		!allZero(data[88:96]) {
		return nil, ErrCorrupt
	}
	var digest [32]byte
	copy(digest[:], data[40:72])
	if !bytes.Equal(cp.ID[:], digest[:16]) {
		return nil, ErrCorrupt
	}
	var nonce [12]byte
	copy(nonce[:], data[72:84])
	length := int(binary.LittleEndian.Uint32(data[84:88]))
	if length != len(data)-96 {
		return nil, ErrCorrupt
	}
	var aad [56]byte
	logID := s.engine.LogID()
	copy(aad[:16], logID[:])
	binary.LittleEndian.PutUint64(aad[16:24], group)
	binary.LittleEndian.PutUint64(aad[24:32], cp.Index)
	binary.LittleEndian.PutUint64(aad[32:40], cp.Term)
	copy(aad[40:56], cp.ID[:])
	wantNonce := s.cryptoWork.deriveObjectNonce("node-checkpoint", group, digest)
	if nonce != wantNonce {
		return nil, ErrCorrupt
	}
	plain, err := s.crypto.aead.Open(nil, nonce[:], data[96:], aad[:])
	if err != nil || sha256.Sum256(plain) != digest {
		return nil, ErrCorrupt
	}
	return unmarshalSnapshot(plain, s.identity.MemberID)
}

func writeNodeMeta(dir string, identity Identity, key Key, logID [16]byte, reference snapshotReference, bounds formatBounds, cryptoState fileCrypto) error {
	plain, err := marshalIdentity(identity, reference, bounds)
	if err != nil {
		return err
	}
	if len(key.ID) > math.MaxUint16 || len(key.Wrapped) > math.MaxUint16 {
		return ErrBounds
	}
	header := make([]byte, nodeMetaHeaderBytes+len(key.ID)+len(key.Wrapped))
	copy(header[:8], nodeMetaMagic[:])
	binary.LittleEndian.PutUint16(header[8:10], 1)
	binary.LittleEndian.PutUint16(header[10:12], uint16(len(key.ID)))
	binary.LittleEndian.PutUint16(header[12:14], uint16(len(key.Wrapped)))
	copy(header[16:32], logID[:])
	nonce := deriveNonce(cryptoState.nonceKey, "node-meta", 0)
	copy(header[32:44], nonce[:])
	copy(header[nodeMetaHeaderBytes:], key.ID)
	copy(header[nodeMetaHeaderBytes+len(key.ID):], key.Wrapped)
	ciphertext := cryptoState.aead.Seal(nil, nonce[:], plain, header)
	path := filepath.Join(dir, nodeMetaName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.Write(header); err == nil {
		_, err = f.Write(ciphertext)
	}
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
