package raftstore

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	"github.com/thesyncim/vibedb/internal/storeio"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	nodeMetaName        = "NODEMETA"
	nodeLockName        = "LOCK"
	nodeLogDir          = "log"
	nodeCheckpointDir   = "checkpoints"
	nodeMetaHeaderBytes = 48

	DefaultNodeSegmentBytes       = 32 << 20
	DefaultNodeMaxWaveBytes       = 20 << 20
	DefaultNodeMaxSegmentEvents   = 32 << 10
	DefaultNodeRecentWaves        = 4 << 10
	DefaultNodeMaxEntriesPerGroup = raftmodel.MaxMessageEntries
	DefaultNodeReaderSlots        = 4
	DefaultNodeMaxGroups          = 4 << 10
)

var nodeMetaMagic = [8]byte{'V', 'D', 'B', 'N', 'O', 'D', 'E', 0}

// NodeStoreOptions is the complete immutable node-log geometry. Zero values
// select the exported defaults. SegmentBytes controls only one physical log
// segment; it is deliberately independent from maximum wave, index, cache,
// and group capacities so reserve space cannot silently scale with a legacy
// per-group WAL bound.
type NodeStoreOptions struct {
	SegmentBytes, MaxWaveBytes, MaxSegmentEvents, RecentWaves int
	MaxEntriesPerGroup, ReaderSlots, MaxGroups                int
}

type NodeIdentity struct {
	ClusterID, ClusterIncarnation, NodeID [16]byte
}

// GroupDescriptor contains only placement/member coordinates that vary by
// Raft group. LogKey is an immutable, monotonically allocated local identity.
type GroupDescriptor struct {
	LogKey, TopologyRecoveryEpoch, AllocationGeneration, MemberID uint64
	GroupID, ShardIncarnation, StoreID                            [16]byte
	Distribution, Shard                                           string
}

type NodeBootstrap struct {
	Descriptor GroupDescriptor
	Snapshot   *pb.Snapshot
	HardState  *pb.HardState
}

type NodeReady struct {
	GroupID uint64
	Batch   raftmodel.PersistBatch
}

type GroupIncarnation struct {
	GroupID, Incarnation uint64
}

const nodeDataExtentBytes = 32 << 10

type pageEntryRef struct{ batch, entry uint32 }

type CheckpointPhase uint8

const (
	CheckpointTempWritten CheckpointPhase = iota + 1
	CheckpointFileSynced
	CheckpointRenamed
	CheckpointDirectorySynced
	CheckpointBeforeLogReference
)

type NodeStore struct {
	mu                                        sync.Mutex
	catalogMu                                 sync.Mutex
	maintenance                               sync.WaitGroup
	dir                                       string
	identity                                  NodeIdentity
	descriptors                               []GroupDescriptor
	descriptorOrder                           []uint32
	nextLogKey                                uint64
	key                                       Key
	bounds                                    nodeStoreBounds
	engine                                    *seglog.Engine
	lock                                      *os.File
	pinnedDir, metaFile                       *os.File
	metaSize                                  int64
	dirPathNUL, metaNameNUL                   string
	crypto                                    fileCrypto
	cryptoWork                                objectCryptoWorkspace
	groupAAD                                  [40]byte
	groupNonce                                [12]byte
	plainArena, cipherArena, digestArena      []byte
	readPlain                                 []byte
	cachedBatch                               seglog.WaveID
	cachedSegment, cachedOffset, cachedExtent uint64
	cacheValid                                bool
	waveBatches                               [MaxPersistGroupBatches]seglog.ReadyBatch
	waveEntries                               [MaxPersistGroupBatches][]seglog.Entry
	waveHard                                  [MaxPersistGroupBatches]seglog.HardState
	waveCheckpoint                            [MaxPersistGroupBatches]seglog.Checkpoint
	pageRefs                                  []pageEntryRef
	persistWaveTest                           func(seglog.Wave) error
	namespaceProofTest                        func() error
	checkpointHookTest                        func(CheckpointPhase) error
	checkpointLeaveTempTest                   bool
	descriptorCheckpointHookTest              func(DescriptorCheckpointPhase) error
	descriptorCheckpointLeaveTempTest         bool
	sequencer                                 *NodeSubmissionSequencer
	closing                                   bool
	closeDone                                 chan struct{}
	closeErr                                  error
	closeInit                                 sync.Once
	closingFlag                               atomic.Bool
	closed                                    bool
	poisoned                                  error
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

func defaultNodeOptions(options NodeStoreOptions) (NodeStoreOptions, error) {
	if options.SegmentBytes == 0 {
		options.SegmentBytes = DefaultNodeSegmentBytes
	}
	if options.MaxWaveBytes == 0 {
		options.MaxWaveBytes = DefaultNodeMaxWaveBytes
	}
	if options.MaxSegmentEvents == 0 {
		options.MaxSegmentEvents = DefaultNodeMaxSegmentEvents
	}
	if options.RecentWaves == 0 {
		options.RecentWaves = DefaultNodeRecentWaves
	}
	if options.MaxEntriesPerGroup == 0 {
		options.MaxEntriesPerGroup = DefaultNodeMaxEntriesPerGroup
	}
	if options.ReaderSlots == 0 {
		options.ReaderSlots = DefaultNodeReaderSlots
	}
	if options.MaxGroups == 0 {
		options.MaxGroups = DefaultNodeMaxGroups
	}
	if options.SegmentBytes < 1<<20 || uint64(options.SegmentBytes) >= 1<<32 ||
		options.MaxWaveBytes < 72 || options.MaxWaveBytes > seglog.MaximumWaveBytes ||
		options.MaxSegmentEvents < 1 || uint64(options.MaxSegmentEvents) > AbsoluteMaxEntries ||
		options.RecentWaves < 1 || uint64(options.RecentWaves) > AbsoluteMaxRecords ||
		options.MaxEntriesPerGroup < 1 || options.MaxEntriesPerGroup > MaxReadyEntries ||
		options.ReaderSlots < 0 || uint64(options.ReaderSlots) > math.MaxUint32 ||
		options.MaxGroups < 1 || uint64(options.MaxGroups) > math.MaxUint32 {
		return options, ErrBounds
	}
	return options, nil
}

func nodeBoundsFromOptions(options NodeStoreOptions) nodeStoreBounds {
	return nodeStoreBounds{
		segmentBytes: uint64(options.SegmentBytes), maxWaveBytes: uint64(options.MaxWaveBytes),
		maxSegmentEvents: uint64(options.MaxSegmentEvents), recentWaves: uint64(options.RecentWaves),
		maxEntriesPerGroup: uint64(options.MaxEntriesPerGroup), readerSlots: uint64(options.ReaderSlots),
		maxGroups: uint64(options.MaxGroups),
	}
}

func CreateNodeStore(dir string, identity NodeIdentity, key Key, bootstraps []NodeBootstrap, options NodeStoreOptions) (*NodeStore, error) {
	options, err := defaultNodeOptions(options)
	if err != nil {
		return nil, err
	}
	if err = validateNodeIdentity(identity); err != nil {
		return nil, err
	}
	if err = validateKey(key, false); err != nil {
		return nil, err
	}
	bootstraps, descriptors, order, err := canonicalInitialDescriptors(bootstraps)
	if err != nil || len(bootstraps) >= MaxPersistGroupBatches || len(bootstraps) > options.MaxGroups {
		return nil, ErrInvalid
	}
	if err = os.Mkdir(dir, 0o700); err != nil {
		return nil, err
	}
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
	var logID [16]byte
	if _, err = rand.Read(logID[:]); err != nil || logID == ([16]byte{}) {
		_ = lock.Close()
		return nil, errors.Join(ErrInvalid, err)
	}
	cryptoState, err := makeFileCrypto(key, logID)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	authKey := deriveFileSecret(key.Material, logID, "seglog-auth-key")
	engine, err := seglog.CreateEngineAuthenticated(filepath.Join(dir, nodeLogDir), logID, authKey, uint64(options.SegmentBytes))
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	storedDescriptors := make([]GroupDescriptor, len(descriptors), options.MaxGroups)
	copy(storedDescriptors, descriptors)
	storedOrder := make([]uint32, len(order), options.MaxGroups)
	copy(storedOrder, order)
	bounds := nodeBoundsFromOptions(options)
	store := &NodeStore{dir: dir, identity: identity, descriptors: storedDescriptors, descriptorOrder: storedOrder, nextLogKey: uint64(len(descriptors)) + 1, key: key, bounds: bounds, engine: engine, lock: lock, crypto: cryptoState, cryptoWork: newObjectCryptoWorkspace(cryptoState.dataKey, cryptoState.nonceKey)}
	if err = store.reserve(options, bootstraps); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err = store.bootstrap(bootstraps); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err = writeNodeMeta(dir, identity, key, engine.LogID(), bounds, cryptoState); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err = store.bindNamespace(); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

// OpenNodeStore authenticates node metadata before exposing recovered group
// state. Engine Open performs its startup durability sync before this returns.
func OpenNodeStore(dir string, expected NodeIdentity, key Key, options NodeStoreOptions) (*NodeStore, error) {
	options, err := defaultNodeOptions(options)
	if err != nil {
		return nil, err
	}
	if err = validateNodeIdentity(expected); err != nil {
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
	bounds := nodeBoundsFromOptions(options)
	identity, logID, err := openNodeMeta(meta, expected, key, bounds)
	if err != nil {
		return fail(nil, err)
	}
	cryptoState, err := makeFileCrypto(key, logID)
	if err != nil {
		return fail(nil, err)
	}
	authKey := deriveFileSecret(key.Material, logID, "seglog-auth-key")
	engine, err := seglog.OpenEngineAuthenticated(filepath.Join(dir, nodeLogDir), logID, authKey)
	if err != nil {
		return fail(nil, err)
	}
	if engine.LogID() != logID {
		return fail(engine, ErrIdentityMismatch)
	}
	store := &NodeStore{dir: dir, identity: identity, key: key, bounds: bounds, engine: engine, lock: lock, crypto: cryptoState, cryptoWork: newObjectCryptoWorkspace(cryptoState.dataKey, cryptoState.nonceKey)}
	if err = engine.Reserve(options.MaxWaveBytes, options.MaxSegmentEvents, options.RecentWaves); err == nil {
		err = engine.ReserveReaders(options.ReaderSlots)
	}
	if err == nil {
		if err = engine.ReserveGroup(nodeDescriptorGroup, options.MaxGroups); err != nil {
		}
	}
	if err == nil {
		for _, group := range engine.GroupIDs() {
			if group == nodeDescriptorGroup {
				continue
			}
			if err = engine.ReserveGroup(group, options.MaxEntriesPerGroup); err != nil {
				break
			}
		}
	}
	if err == nil {
		store.plainArena = make([]byte, 0, options.MaxWaveBytes)
		store.cipherArena = make([]byte, 0, options.MaxWaveBytes)
		store.digestArena = make([]byte, 0, options.MaxWaveBytes)
		store.readPlain = make([]byte, 0, options.MaxWaveBytes)
		store.pageRefs = make([]pageEntryRef, 0, options.MaxSegmentEvents)
		for i := range store.waveEntries {
			store.waveEntries[i] = make([]seglog.Entry, 0, options.MaxEntriesPerGroup)
		}
		if err = store.rebuildDescriptors(options.MaxGroups); err == nil {
			ids := engine.GroupIDs()
			if len(ids) != len(store.descriptors)+1 || ids[len(ids)-1] != nodeDescriptorGroup {
				err = ErrCorrupt
			} else {
				for i := range store.descriptors {
					if ids[i] != uint64(i+1) {
						err = ErrCorrupt
						break
					}
				}
			}
			if err == nil {
				err = store.bindNamespace()
			}
		}
	}
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func openNodeMeta(data []byte, expected NodeIdentity, key Key, bounds nodeStoreBounds) (NodeIdentity, [16]byte, error) {
	var zero [16]byte
	if len(data) < nodeMetaHeaderBytes+16 || !bytes.Equal(data[:8], nodeMetaMagic[:]) ||
		binary.LittleEndian.Uint16(data[8:10]) != 1 || !allZero(data[14:16]) || !allZero(data[44:48]) {
		return NodeIdentity{}, zero, ErrCorrupt
	}
	keyIDBytes := int(binary.LittleEndian.Uint16(data[10:12]))
	wrappedBytes := int(binary.LittleEndian.Uint16(data[12:14]))
	headerBytes := nodeMetaHeaderBytes + keyIDBytes + wrappedBytes
	if headerBytes > len(data)-16 || !bytes.Equal(data[nodeMetaHeaderBytes:nodeMetaHeaderBytes+keyIDBytes], []byte(key.ID)) ||
		!bytes.Equal(data[nodeMetaHeaderBytes+keyIDBytes:headerBytes], key.Wrapped) {
		return NodeIdentity{}, zero, ErrIdentityMismatch
	}
	var logID [16]byte
	copy(logID[:], data[16:32])
	cryptoState, err := makeFileCrypto(key, logID)
	if err != nil {
		return NodeIdentity{}, zero, err
	}
	nonce := deriveNonce(cryptoState.nonceKey, "node-meta", 0)
	if !bytes.Equal(data[32:44], nonce[:]) {
		return NodeIdentity{}, zero, ErrCorrupt
	}
	plain, err := cryptoState.aead.Open(nil, nonce[:], data[headerBytes:], data[:headerBytes])
	if err != nil {
		return NodeIdentity{}, zero, ErrCorrupt
	}
	identity, storedBounds, err := unmarshalNodeIdentity(plain)
	if err != nil {
		return NodeIdentity{}, zero, err
	}
	if identity != expected || storedBounds != bounds {
		return NodeIdentity{}, zero, ErrIdentityMismatch
	}
	return identity, logID, nil
}

func (s *NodeStore) reserve(options NodeStoreOptions, bootstraps []NodeBootstrap) error {
	if err := s.engine.Reserve(options.MaxWaveBytes, options.MaxSegmentEvents, options.RecentWaves); err != nil {
		return err
	}
	if err := s.engine.ReserveReaders(options.ReaderSlots); err != nil {
		return err
	}
	for _, b := range bootstraps {
		if err := s.engine.ReserveGroup(b.Descriptor.LogKey, options.MaxEntriesPerGroup); err != nil {
			return err
		}
	}
	if err := s.engine.ReserveGroup(nodeDescriptorGroup, options.MaxGroups); err != nil {
		return err
	}
	s.plainArena = make([]byte, 0, options.MaxWaveBytes)
	s.cipherArena = make([]byte, 0, options.MaxWaveBytes)
	s.digestArena = make([]byte, 0, options.MaxWaveBytes)
	s.readPlain = make([]byte, 0, options.MaxWaveBytes)
	s.pageRefs = make([]pageEntryRef, 0, options.MaxSegmentEvents)
	for i := range s.waveEntries {
		s.waveEntries[i] = make([]seglog.Entry, 0, options.MaxEntriesPerGroup)
	}
	return nil
}

func (s *NodeStore) bootstrap(bootstraps []NodeBootstrap) error {
	if !slices.IsSortedFunc(bootstraps, func(a, b NodeBootstrap) int { return intCompare(a.Descriptor.LogKey, b.Descriptor.LogKey) }) {
		return ErrInvalid
	}
	batches := make([]seglog.ReadyBatch, len(bootstraps)+1)
	for i, b := range bootstraps {
		if err := validateSnapshotBase(b.Snapshot, b.Descriptor.MemberID); err != nil {
			return err
		}
		checkpoint, err := s.publishCheckpoint(b.Descriptor.LogKey, b.Snapshot)
		if err != nil {
			return err
		}
		h := b.HardState
		if h == nil {
			h = &pb.HardState{}
			h.Term = uint64Pointer(checkpoint.Term)
			h.Commit = uint64Pointer(checkpoint.Index)
		}
		batches[i] = seglog.ReadyBatch{GroupID: b.Descriptor.LogKey, Checkpoint: &checkpoint, Hard: &seglog.HardState{Term: h.GetTerm(), Vote: h.GetVote(), Commit: h.GetCommit()}}
	}
	s.plainArena = s.plainArena[:0]
	descriptorEntries := make([]seglog.Entry, 0, len(bootstraps))
	for i := range bootstraps {
		start := len(s.plainArena)
		var err error
		s.plainArena, err = appendGroupDescriptor(s.plainArena, bootstraps[i].Descriptor)
		if err != nil {
			return err
		}
		descriptorEntries = append(descriptorEntries, seglog.Entry{Index: uint64(i + 1), Term: 1, DataOffset: uint64(start), DataBytes: uint64(len(s.plainArena) - start)})
	}
	descriptorHard := seglog.HardState{Term: 1, Commit: uint64(len(descriptorEntries))}
	batches[len(bootstraps)] = seglog.ReadyBatch{GroupID: nodeDescriptorGroup, Entries: descriptorEntries, Hard: &descriptorHard}
	id := bootstrapWaveID(batches)
	copy(s.waveBatches[:], batches)
	if err := s.packWaveExtents(id, len(batches)); err != nil {
		return err
	}
	return s.engine.PersistWave(seglog.Wave{ID: id, Batches: s.waveBatches[:len(batches)], Blob: s.cipherArena})
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
		if batches[i].Checkpoint != nil {
			_, _ = h.Write(batches[i].Checkpoint.ID[:])
		} else {
			binary.LittleEndian.PutUint64(word[:], uint64(len(batches[i].Entries)))
			_, _ = h.Write(word[:])
		}
	}
	var id seglog.WaveID
	copy(id[:], h.Sum(nil))
	return id
}

func checkpointWaveID(group uint64, checkpoint seglog.Checkpoint) seglog.WaveID {
	var canonical [48]byte
	copy(canonical[:16], []byte("node-checkpoint\x00"))
	binary.LittleEndian.PutUint64(canonical[16:24], group)
	copy(canonical[24:40], checkpoint.ID[:])
	binary.LittleEndian.PutUint64(canonical[40:48], checkpoint.Index)
	digest := sha256.Sum256(canonical[:])
	var id seglog.WaveID
	copy(id[:], digest[:])
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
	if orphan, readErr := os.ReadFile(tmp); readErr == nil {
		if len(orphan) == len(header)+len(ciphertext) && bytes.Equal(orphan[:len(header)], header) && bytes.Equal(orphan[len(header):], ciphertext) {
			orphanFile, openErr := os.OpenFile(tmp, os.O_RDWR, 0)
			if openErr != nil {
				return seglog.Checkpoint{}, openErr
			}
			if syncErr := errors.Join(orphanFile.Sync(), orphanFile.Close()); syncErr != nil {
				return seglog.Checkpoint{}, syncErr
			}
			if renameErr := os.Rename(tmp, final); renameErr != nil {
				return seglog.Checkpoint{}, renameErr
			}
			if syncErr := syncNodeDirectory(filepath.Join(s.dir, nodeCheckpointDir)); syncErr != nil {
				return seglog.Checkpoint{}, syncErr
			}
			return seglog.Checkpoint{ID: id, Index: index, Term: term}, nil
		}
		if removeErr := os.Remove(tmp); removeErr != nil {
			return seglog.Checkpoint{}, removeErr
		}
		if syncErr := syncNodeDirectory(filepath.Join(s.dir, nodeCheckpointDir)); syncErr != nil {
			return seglog.Checkpoint{}, syncErr
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return seglog.Checkpoint{}, readErr
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return seglog.Checkpoint{}, err
	}
	ok := false
	defer func() {
		if !ok && !s.checkpointLeaveTempTest {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(header); err == nil {
		_, err = f.Write(ciphertext)
	}
	if err == nil && s.checkpointHookTest != nil {
		err = s.checkpointHookTest(CheckpointTempWritten)
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil && s.checkpointHookTest != nil {
		err = s.checkpointHookTest(CheckpointFileSynced)
	}
	if err == nil {
		err = f.Close()
	}
	if err == nil {
		err = os.Rename(tmp, final)
	}
	if err == nil && s.checkpointHookTest != nil {
		err = s.checkpointHookTest(CheckpointRenamed)
	}
	if err == nil {
		err = syncNodeDirectory(filepath.Join(s.dir, nodeCheckpointDir))
	}
	if err == nil && s.checkpointHookTest != nil {
		err = s.checkpointHookTest(CheckpointDirectorySynced)
	}
	if err != nil {
		return seglog.Checkpoint{}, err
	}
	ok = true
	return seglog.Checkpoint{ID: id, Index: index, Term: term}, nil
}

func syncNodeDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

// publishGroupCheckpointSequenced durably publishes an application checkpoint
// and then references it from the shared log. It is called only by the device
// sequencer, which makes the logical prefix truncation an ordered control wave.
func (s *NodeStore) publishGroupCheckpointSequenced(group uint64, snapshot *pb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.poisoned != nil {
		if s.poisoned != nil {
			return errors.Join(ErrPersistenceUnknown, s.poisoned)
		}
		return ErrClosed
	}
	if err := s.proveNamespace(); err != nil {
		s.poisoned = err
		return errors.Join(ErrPersistenceUnknown, err)
	}
	descriptor, registered := s.descriptorForLogKey(group)
	if !registered || validateSnapshotBase(snapshot, descriptor.MemberID) != nil {
		return ErrInvalid
	}
	metadata, ok := s.engine.Metadata(group)
	if !ok {
		return ErrInvalid
	}
	index, term := snapshot.GetMetadata().GetIndex(), snapshot.GetMetadata().GetTerm()
	if index < metadata.Checkpoint.Index || index > metadata.Hard.Commit {
		return ErrInvalid
	}
	if index == metadata.Checkpoint.Index {
		if term != metadata.Checkpoint.Term {
			return ErrRetryConflict
		}
		current, err := s.loadCheckpoint(group, metadata.Checkpoint)
		if err != nil {
			return err
		}
		currentBytes, err := marshalSnapshot(current)
		if err != nil {
			return err
		}
		candidateBytes, err := marshalSnapshot(snapshot)
		if err != nil {
			return err
		}
		if !bytes.Equal(currentBytes, candidateBytes) {
			return ErrRetryConflict
		}
		return nil
	}
	_, storedTerm, compacted, found, err := s.engine.LookupExact(group, index)
	if err != nil {
		return err
	}
	if compacted || !found || storedTerm != term {
		return ErrInvalid
	}
	checkpoint, err := s.publishCheckpoint(group, snapshot)
	if err != nil {
		return err
	}
	s.waveCheckpoint[0] = checkpoint
	s.waveBatches[0] = seglog.ReadyBatch{GroupID: group, Checkpoint: &s.waveCheckpoint[0]}
	if s.checkpointHookTest != nil {
		if err = s.checkpointHookTest(CheckpointBeforeLogReference); err != nil {
			return err
		}
	}
	wave := seglog.Wave{ID: checkpointWaveID(group, checkpoint), Batches: s.waveBatches[:1]}
	if s.persistWaveTest != nil {
		err = s.persistWaveTest(wave)
	} else {
		err = s.engine.PersistWave(wave)
	}
	if err != nil {
		if fatal := s.engine.FatalError(); fatal != nil {
			s.poisoned = fatal
			return errors.Join(ErrPersistenceUnknown, err, fatal)
		}
		if errors.Is(err, seglog.ErrBackpressure) {
			return errors.Join(ErrDurabilityBackpressure, err)
		}
		return errors.Join(ErrInvalid, err)
	}
	if err = s.proveNamespace(); err != nil {
		s.poisoned = err
		return errors.Join(ErrPersistenceUnknown, err)
	}
	s.cacheValid = false
	return nil
}

func (s *NodeStore) PersistWave(ready []NodeReady) error { return s.persistWave(ready, false) }

func (s *NodeStore) persistSequencedWave(ready []NodeReady) error { return s.persistWave(ready, true) }

func (s *NodeStore) persistWave(ready []NodeReady, sequenced bool) error {
	if !sequenced && s.closingFlag.Load() {
		return ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.closing && !sequenced {
		return ErrClosed
	}
	if !sequenced && s.sequencer != nil {
		return ErrSequencerActive
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
		_, registered := s.descriptorForLogKey(ready[i].GroupID)
		if !registered || i > 0 && ready[i-1].GroupID >= ready[i].GroupID {
			return ErrInvalid
		}
	}
	id := nodeWaveID(ready)
	totalPlain, totalEntries := 0, 0
	for _, item := range ready {
		for _, entry := range item.Batch.Entries {
			if entry == nil || totalPlain > math.MaxInt-len(entry.GetData()) {
				return ErrBounds
			}
			totalPlain += len(entry.GetData())
			totalEntries++
		}
	}
	if totalPlain > cap(s.plainArena) || totalEntries > cap(s.pageRefs) || totalEntries > (cap(s.cipherArena)-totalPlain)/s.crypto.aead.Overhead() {
		return ErrBounds
	}
	s.plainArena = s.plainArena[:totalPlain]
	s.cipherArena = s.cipherArena[:0]
	plainOffset, mappedCount, duplicateCount := 0, 0, 0
	for _, item := range ready {
		batch := item.Batch
		state, ok := s.engine.Summary(item.GroupID)
		if !ok || batch.NodeIncarnation == 0 || batch.NodeIncarnation != state.NodeIncarnation {
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
		entries := s.waveEntries[mappedCount][:0]
		for _, entry := range batch.Entries {
			data := entry.GetData()
			copy(s.plainArena[plainOffset:], data)
			entries = append(entries, seglog.Entry{Index: entry.GetIndex(), Term: entry.GetTerm(), Type: entry.GetType(), DataOffset: uint64(plainOffset), DataBytes: uint64(len(data))})
			plainOffset += len(data)
		}
		s.waveEntries[mappedCount] = entries
		mapped := seglog.ReadyBatch{GroupID: item.GroupID, NodeIncarnation: batch.NodeIncarnation, ReadyID: batch.ReadyID, ReadyDigest: retryDigest, Entries: entries}
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
			if s.checkpointHookTest != nil {
				if err = s.checkpointHookTest(CheckpointBeforeLogReference); err != nil {
					return err
				}
			}
		}
		s.waveBatches[mappedCount] = mapped
		mappedCount++
	}
	if duplicateCount == len(ready) {
		return nil
	}
	s.plainArena = s.plainArena[:plainOffset]
	if err := s.packWaveExtents(id, mappedCount); err != nil {
		return err
	}
	wave := seglog.Wave{ID: id, Batches: s.waveBatches[:mappedCount], Blob: s.cipherArena}
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
		if errors.Is(persistErr, seglog.ErrBackpressure) {
			return errors.Join(ErrDurabilityBackpressure, persistErr)
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

// packWaveExtents packs caller-sorted group data into entry-aligned shared
// pages. The tag is paid once per page, including pages shared by tiny groups;
// an entry larger than the target receives one dedicated extent.
func (s *NodeStore) packWaveExtents(id seglog.WaveID, batches int) error {
	s.pageRefs = s.pageRefs[:0]
	if len(s.plainArena) == 0 {
		s.cipherArena = s.cipherArena[:0]
		return nil
	}
	for bi := 0; bi < batches; bi++ {
		hasData := false
		for ei := range s.waveBatches[bi].Entries {
			hasData = hasData || s.waveBatches[bi].Entries[ei].DataBytes != 0
		}
		if !hasData {
			for ei := range s.waveBatches[bi].Entries {
				s.waveBatches[bi].Entries[ei].DataOffset = 0
			}
			continue
		}
		for ei := range s.waveBatches[bi].Entries {
			s.pageRefs = append(s.pageRefs, pageEntryRef{batch: uint32(bi), entry: uint32(ei)})
		}
	}
	s.cipherArena = s.cipherArena[:0]
	for first, extentID := 0, uint64(1); first < len(s.pageRefs); extentID++ {
		firstEntry := &s.waveBatches[s.pageRefs[first].batch].Entries[s.pageRefs[first].entry]
		if firstEntry.DataOffset > uint64(len(s.plainArena)) || firstEntry.DataBytes > uint64(len(s.plainArena))-firstEntry.DataOffset {
			return ErrBounds
		}
		plainStart, plainEnd := firstEntry.DataOffset, firstEntry.DataOffset+firstEntry.DataBytes
		last := first + 1
		for last < len(s.pageRefs) {
			entry := &s.waveBatches[s.pageRefs[last].batch].Entries[s.pageRefs[last].entry]
			if entry.DataOffset < plainEnd || entry.DataOffset > uint64(len(s.plainArena)) || entry.DataBytes > uint64(len(s.plainArena))-entry.DataOffset {
				return ErrBounds
			}
			end := entry.DataOffset + entry.DataBytes
			if entry.DataBytes != 0 && plainEnd > plainStart && end > plainStart+nodeDataExtentBytes {
				break
			}
			plainEnd = end
			last++
		}
		if plainEnd > uint64(len(s.plainArena)) || plainStart > plainEnd {
			return ErrBounds
		}
		extentOffset := uint64(len(s.cipherArena))
		aad := &s.groupAAD
		logID := s.engine.LogID()
		copy(aad[:16], logID[:])
		copy(aad[16:32], id[:])
		binary.LittleEndian.PutUint64(aad[32:40], extentID)
		nonceDigest := sha256.Sum256(aad[:])
		s.groupNonce = s.cryptoWork.deriveObjectNonce("node-page", extentID, nonceDigest)
		s.cipherArena = s.crypto.aead.Seal(s.cipherArena, s.groupNonce[:], s.plainArena[plainStart:plainEnd], aad[:])
		extentBytes := uint64(len(s.cipherArena)) - extentOffset
		for i := first; i < last; i++ {
			entry := &s.waveBatches[s.pageRefs[i].batch].Entries[s.pageRefs[i].entry]
			entry.DataOffset -= plainStart
			entry.ExtentID, entry.ExtentOffset, entry.ExtentBytes = extentID, extentOffset, extentBytes
		}
		first = last
	}
	return nil
}

// BeginIncarnations durably allocates the exact next incarnation for each
// caller-sorted group in one control wave and one durability barrier.
func (s *NodeStore) BeginIncarnations(groups []uint64) ([]GroupIncarnation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	if s.sequencer != nil {
		return nil, ErrSequencerActive
	}
	if len(groups) == 0 || len(groups) > MaxPersistGroupBatches {
		return nil, ErrInvalid
	}
	requests := make([]GroupIncarnation, len(groups))
	if err := s.beginIncarnationsLocked(groups, requests); err != nil {
		return requests, err
	}
	return requests, nil
}

func (s *NodeStore) beginIncarnationsSequenced(groups []uint64, requests []GroupIncarnation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if s.sequencer == nil {
		return ErrInvalid
	}
	return s.beginIncarnationsLocked(groups, requests)
}

func (s *NodeStore) beginIncarnationsLocked(groups []uint64, requests []GroupIncarnation) error {
	if len(groups) == 0 || len(groups) > MaxPersistGroupBatches || len(requests) != len(groups) {
		return ErrInvalid
	}
	for i, group := range groups {
		_, registered := s.descriptorForLogKey(group)
		if !registered || i > 0 && groups[i-1] >= group {
			return ErrInvalid
		}
		state, ok := s.engine.Summary(group)
		if !ok || state.NodeIncarnation == math.MaxUint64 {
			return ErrInvalid
		}
		requests[i] = GroupIncarnation{GroupID: group, Incarnation: state.NodeIncarnation + 1}
	}
	if err := s.persistIncarnationsLocked(requests); err != nil {
		return err
	}
	return nil
}

// PersistIncarnations is the exact retry form for an allocation whose sync
// outcome was unknown. A request already durable with ReadyID zero is omitted.
func (s *NodeStore) PersistIncarnations(requests []GroupIncarnation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if s.sequencer != nil {
		return ErrSequencerActive
	}
	return s.persistIncarnationsLocked(requests)
}

func (s *NodeStore) persistIncarnationsSequenced(requests []GroupIncarnation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if s.sequencer == nil {
		return ErrInvalid
	}
	return s.persistIncarnationsLocked(requests)
}

func (s *NodeStore) persistIncarnationsLocked(requests []GroupIncarnation) error {
	if len(requests) == 0 || len(requests) > MaxPersistGroupBatches {
		return ErrInvalid
	}
	if err := s.proveNamespace(); err != nil {
		s.poisoned = err
		return err
	}
	mapped := 0
	var canonical [MaxPersistGroupBatches * 16]byte
	for i, request := range requests {
		_, registered := s.descriptorForLogKey(request.GroupID)
		if !registered || request.Incarnation == 0 || i > 0 && requests[i-1].GroupID >= request.GroupID {
			return ErrInvalid
		}
		state, ok := s.engine.Summary(request.GroupID)
		if !ok {
			return ErrInvalid
		}
		if request.Incarnation == state.NodeIncarnation && state.ReadyID == 0 {
			continue
		}
		if request.Incarnation != state.NodeIncarnation+1 {
			return ErrInvalid
		}
		binary.LittleEndian.PutUint64(canonical[mapped*16:mapped*16+8], request.GroupID)
		binary.LittleEndian.PutUint64(canonical[mapped*16+8:mapped*16+16], request.Incarnation)
		s.waveBatches[mapped] = seglog.ReadyBatch{GroupID: request.GroupID, BeginIncarnation: request.Incarnation}
		mapped++
	}
	if mapped == 0 {
		return nil
	}
	digest := sha256.Sum256(canonical[:mapped*16])
	var id seglog.WaveID
	copy(id[:], digest[:16])
	if err := s.engine.PersistWave(seglog.Wave{ID: id, Batches: s.waveBatches[:mapped]}); err != nil {
		if fatal := s.engine.FatalError(); fatal != nil {
			s.poisoned = fatal
			return errors.Join(ErrPersistenceUnknown, err, fatal)
		}
		return errors.Join(ErrInvalid, err)
	}
	if err := s.proveNamespace(); err != nil {
		s.poisoned = err
		return errors.Join(ErrPersistenceUnknown, err)
	}
	return nil
}

// RegisterGroup atomically publishes an exact portable group descriptor and
// begins incarnation one in the same authenticated log frame and data sync.
// It is disabled once the node sequencer owns submission ordering.
func (s *NodeStore) RegisterGroup(descriptor GroupDescriptor) (GroupIncarnation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return GroupIncarnation{}, err
	}
	if s.sequencer != nil {
		return GroupIncarnation{}, ErrSequencerActive
	}
	return s.registerGroupLocked(descriptor)
}

func (s *NodeStore) registerGroupSequenced(descriptor GroupDescriptor) (GroupIncarnation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return GroupIncarnation{}, err
	}
	if s.sequencer == nil {
		return GroupIncarnation{}, ErrInvalid
	}
	return s.registerGroupLocked(descriptor)
}

func (s *NodeStore) registerGroupLocked(descriptor GroupDescriptor) (GroupIncarnation, error) {
	if descriptor.LogKey != 0 || validateGroupDescriptor(descriptor, true) != nil || len(s.descriptors) == cap(s.descriptors) || s.nextLogKey == 0 || s.nextLogKey >= nodeDescriptorGroup {
		return GroupIncarnation{}, ErrBounds
	}
	position, found := slices.BinarySearchFunc(s.descriptorOrder, descriptor.GroupID, func(index uint32, target [16]byte) int {
		return bytes.Compare(s.descriptors[index].GroupID[:], target[:])
	})
	if found {
		existing := s.descriptors[s.descriptorOrder[position]]
		request := descriptor
		request.LogKey = existing.LogKey
		if existing != request {
			return GroupIncarnation{}, ErrIdentityMismatch
		}
		state, ok := s.engine.Summary(existing.LogKey)
		if !ok || state.NodeIncarnation == 0 {
			return GroupIncarnation{}, ErrCorrupt
		}
		return GroupIncarnation{GroupID: existing.LogKey, Incarnation: state.NodeIncarnation}, nil
	}
	descriptor.LogKey = s.nextLogKey
	if err := s.engine.ReserveGroup(descriptor.LogKey, cap(s.waveEntries[0])); err != nil {
		return GroupIncarnation{}, err
	}
	s.plainArena = s.plainArena[:0]
	var err error
	s.plainArena, err = appendGroupDescriptor(s.plainArena, descriptor)
	if err != nil {
		return GroupIncarnation{}, err
	}
	s.waveEntries[0] = append(s.waveEntries[0][:0], seglog.Entry{Index: descriptor.LogKey, Term: 1, DataOffset: 0, DataBytes: uint64(len(s.plainArena))})
	s.waveBatches[0] = seglog.ReadyBatch{GroupID: descriptor.LogKey, BeginIncarnation: 1}
	s.waveHard[1] = seglog.HardState{Term: 1, Commit: descriptor.LogKey}
	s.waveBatches[1] = seglog.ReadyBatch{GroupID: nodeDescriptorGroup, Entries: s.waveEntries[0], Hard: &s.waveHard[1]}
	digest := sha256.Sum256(s.plainArena)
	var waveID seglog.WaveID
	copy(waveID[:], digest[:16])
	if err = s.packWaveExtents(waveID, 2); err != nil {
		return GroupIncarnation{}, err
	}
	if err = s.engine.PersistWave(seglog.Wave{ID: waveID, Batches: s.waveBatches[:2], Blob: s.cipherArena}); err != nil {
		if fatal := s.engine.FatalError(); fatal != nil {
			s.poisoned = fatal
			return GroupIncarnation{}, errors.Join(ErrPersistenceUnknown, err, fatal)
		}
		return GroupIncarnation{}, err
	}
	if err = s.proveNamespace(); err != nil {
		s.poisoned = err
		return GroupIncarnation{}, errors.Join(ErrPersistenceUnknown, err)
	}
	s.descriptors = append(s.descriptors, descriptor)
	s.descriptorOrder = append(s.descriptorOrder, 0)
	copy(s.descriptorOrder[position+1:], s.descriptorOrder[position:len(s.descriptorOrder)-1])
	s.descriptorOrder[position] = uint32(len(s.descriptors) - 1)
	s.nextLogKey++
	return GroupIncarnation{GroupID: descriptor.LogKey, Incarnation: 1}, nil
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

func (s *NodeStore) NodeIdentity() NodeIdentity { return s.identity }

func (s *NodeStore) SetDataSyncForTesting(sync func(*os.File) error) {
	s.engine.SetDataSyncForTesting(sync)
}

func (s *NodeStore) descriptorForLogKey(group uint64) (GroupDescriptor, bool) {
	if group == 0 || group >= s.nextLogKey || group > uint64(len(s.descriptors)) {
		return GroupDescriptor{}, false
	}
	d := s.descriptors[group-1]
	return d, d.LogKey == group
}

func (s *NodeStore) GroupByID(groupID [16]byte) (*GroupView, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	position, found := slices.BinarySearchFunc(s.descriptorOrder, groupID, func(index uint32, target [16]byte) int {
		return bytes.Compare(s.descriptors[index].GroupID[:], target[:])
	})
	if !found {
		return nil, false
	}
	return &GroupView{store: s, group: s.descriptors[s.descriptorOrder[position]].LogKey}, true
}

func (v *GroupView) Descriptor() (GroupDescriptor, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return GroupDescriptor{}, err
	}
	d, ok := v.store.descriptorForLogKey(v.group)
	if !ok {
		return GroupDescriptor{}, ErrInvalid
	}
	return d, nil
}

func (v *GroupView) NodeIncarnation() (uint64, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return 0, err
	}
	state, ok := v.store.engine.Summary(v.group)
	if !ok || state.NodeIncarnation == 0 {
		return 0, ErrInvalid
	}
	return state.NodeIncarnation, nil
}

// NodeIdentity returns the immutable physical-node identity authenticated by
// this group view's owning node log.
func (v *GroupView) NodeIdentity() (NodeIdentity, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return NodeIdentity{}, err
	}
	if _, ok := v.store.descriptorForLogKey(v.group); !ok {
		return NodeIdentity{}, ErrInvalid
	}
	return v.store.identity, nil
}

// CapacityProfile returns the authenticated immutable checkpoint base and the
// exact per-group entry-index capacity sealed into NODEMETA.
func (v *GroupView) CapacityProfile() (CapacityProfile, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return CapacityProfile{}, err
	}
	state, ok := v.store.engine.Metadata(v.group)
	if !ok || state.Checkpoint.Index == 0 {
		return CapacityProfile{}, ErrCorrupt
	}
	return CapacityProfile{
		Format: CapacityFormatImmutableBase, LogBaseIndex: state.Checkpoint.Index,
		MaxEntries: v.store.bounds.maxEntriesPerGroup,
	}, nil
}

func (s *NodeStore) rebuildDescriptors(limit int) error {
	metadata, ok := s.engine.Metadata(nodeDescriptorGroup)
	if !ok || metadata.LastIndex == 0 || metadata.LastIndex > uint64(limit) || metadata.Hard.Term != 1 || metadata.Hard.Vote != 0 || metadata.Hard.Commit != metadata.LastIndex {
		return ErrCorrupt
	}
	descriptors := make([]GroupDescriptor, 0, limit)
	order := make([]uint32, 0, limit)
	first := uint64(1)
	if metadata.Checkpoint != (seglog.Checkpoint{}) {
		if metadata.Checkpoint.Index != metadata.TruncateIndex || metadata.Checkpoint.Term != 1 || metadata.TruncateTerm != 1 || metadata.FirstIndex != metadata.Checkpoint.Index+1 {
			return ErrCorrupt
		}
		var err error
		descriptors, err = s.readDescriptorCatalog(metadata.Checkpoint, limit)
		if err != nil {
			return err
		}
		for index := range descriptors {
			order = append(order, uint32(index))
		}
		first = metadata.Checkpoint.Index + 1
	} else if metadata.FirstIndex != 1 || metadata.TruncateIndex != 0 {
		return ErrCorrupt
	}
	for index := first; index <= metadata.LastIndex; index++ {
		location, _, compacted, found, err := s.engine.LookupExact(nodeDescriptorGroup, index)
		if err != nil || compacted || !found {
			return ErrCorrupt
		}
		entry, err := s.readEntry(nodeDescriptorGroup, location)
		if err != nil {
			return err
		}
		descriptor, err := decodeGroupDescriptor(entry.GetData())
		if err != nil || descriptor.LogKey != index {
			return ErrCorrupt
		}
		if descriptor.LogKey != uint64(len(descriptors))+1 {
			return ErrCorrupt
		}
		descriptors = append(descriptors, descriptor)
		order = append(order, uint32(len(descriptors)-1))
	}
	slices.SortFunc(order, func(a, b uint32) int { return bytes.Compare(descriptors[a].GroupID[:], descriptors[b].GroupID[:]) })
	for i := 1; i < len(order); i++ {
		if bytes.Compare(descriptors[order[i-1]].GroupID[:], descriptors[order[i]].GroupID[:]) >= 0 {
			return ErrCorrupt
		}
	}
	s.descriptors, s.descriptorOrder, s.nextLogKey = descriptors, order, uint64(len(descriptors))+1
	return nil
}
func (v *GroupView) Persist(batch raftmodel.PersistBatch) error {
	return v.store.PersistWave([]NodeReady{{GroupID: v.group, Batch: batch}})
}

func (s *NodeStore) usable() error {
	if s.closed || s.closing {
		return ErrClosed
	}
	if s.poisoned != nil {
		return errors.Join(ErrPersistenceUnknown, s.poisoned)
	}
	return nil
}

func (s *NodeStore) Close() error {
	s.closeInit.Do(func() { s.closeDone = make(chan struct{}) })
	done := s.closeDone
	if !s.closingFlag.CompareAndSwap(false, true) {
		<-done
		return s.closeErr
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		close(done)
		return s.closeErr
	}
	s.closing = true
	sequencer := s.sequencer
	s.mu.Unlock()
	var err error
	if sequencer != nil {
		err = sequencer.Close()
	}
	s.maintenance.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.closing = false
	if s.engine != nil {
		err = errors.Join(err, s.engine.Close())
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
	s.closeErr = err
	close(done)
	return err
}

func (v *GroupView) Term(index uint64) (uint64, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return 0, err
	}
	_, term, compacted, ok, lookupErr := v.store.engine.LookupExact(v.group, index)
	if lookupErr != nil {
		return 0, lookupErr
	}
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
	state, ok := v.store.engine.Metadata(v.group)
	if !ok {
		return 0, raft.ErrUnavailable
	}
	return state.FirstIndex, nil
}

// ReadEntryInto authenticates and decrypts exactly the containing group batch.
// It does not decode or copy values from any other group or wave.
func (v *GroupView) ReadEntryInto(index uint64, ciphertext, plaintext []byte) (BorrowedEntry, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return BorrowedEntry{}, err
	}
	loc, _, compacted, ok, lookupErr := v.store.engine.LookupExact(v.group, index)
	if lookupErr != nil {
		return BorrowedEntry{}, lookupErr
	}
	if compacted {
		return BorrowedEntry{}, raft.ErrCompacted
	}
	if !ok {
		return BorrowedEntry{}, raft.ErrUnavailable
	}
	if loc.Bytes == 0 {
		return BorrowedEntry{Index: loc.Index, Term: loc.Term, Type: loc.Type, Data: plaintext[:0]}, nil
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
	binary.LittleEndian.PutUint64(aad[32:40], loc.ExtentID)
	nonceDigest := sha256.Sum256(aad[:])
	v.store.groupNonce = v.store.cryptoWork.deriveObjectNonce("node-page", loc.ExtentID, nonceDigest)
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
	state, ok := v.store.engine.Metadata(v.group)
	if !ok {
		return 0, raft.ErrUnavailable
	}
	return state.LastIndex, nil
}
func (v *GroupView) InitialState() (*pb.HardState, *pb.ConfState, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return nil, nil, err
	}
	state, ok := v.store.engine.Metadata(v.group)
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
	state, ok := v.store.engine.Metadata(v.group)
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
	state, ok := v.store.engine.Metadata(v.group)
	if !ok {
		return nil, raft.ErrUnavailable
	}
	first := state.FirstIndex
	if lo < first {
		return nil, raft.ErrCompacted
	}
	if hi < lo || hi > state.LastIndex+1 {
		return nil, raft.ErrUnavailable
	}
	result := make([]*pb.Entry, 0, hi-lo)
	var size uint64
	for index := lo; index < hi; index++ {
		loc, _, compacted, found, lookupErr := v.store.engine.LookupExact(v.group, index)
		if lookupErr != nil {
			return nil, lookupErr
		}
		if compacted {
			return nil, raft.ErrCompacted
		}
		if !found {
			return nil, raft.ErrUnavailable
		}
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
	if loc.Bytes == 0 {
		index, term, entryType := loc.Index, loc.Term, loc.Type
		return &pb.Entry{Index: &index, Term: &term, Type: &entryType}, nil
	}
	if !s.cacheValid || s.cachedSegment != loc.SegmentID || s.cachedOffset != loc.Offset || s.cachedBatch != loc.BatchID || s.cachedExtent != loc.ExtentID {
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
		binary.LittleEndian.PutUint64(aad[32:40], loc.ExtentID)
		nonceDigest := sha256.Sum256(aad[:])
		s.groupNonce = s.cryptoWork.deriveObjectNonce("node-page", loc.ExtentID, nonceDigest)
		s.readPlain, err = s.crypto.aead.Open(s.readPlain[:0], s.groupNonce[:], ciphertext, aad[:])
		if err != nil {
			s.cacheValid = false
			return nil, ErrCorrupt
		}
		s.cachedSegment, s.cachedOffset, s.cachedBatch, s.cachedExtent, s.cacheValid = loc.SegmentID, loc.Offset, loc.BatchID, loc.ExtentID, true
	}
	if loc.DataOffset > uint64(len(s.readPlain)) || loc.DataBytes > uint64(len(s.readPlain))-loc.DataOffset {
		return nil, ErrCorrupt
	}
	term, index, entryType := loc.Term, loc.Index, loc.Type
	data := append([]byte(nil), s.readPlain[loc.DataOffset:loc.DataOffset+loc.DataBytes]...)
	return &pb.Entry{Term: &term, Index: &index, Type: &entryType, Data: data}, nil
}

func (s *NodeStore) loadCheckpoint(group uint64, cp seglog.Checkpoint) (*pb.Snapshot, error) {
	descriptor, ok := s.descriptorForLogKey(group)
	if !ok {
		return nil, ErrCorrupt
	}
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
	return unmarshalSnapshot(plain, descriptor.MemberID)
}

func writeNodeMeta(dir string, identity NodeIdentity, key Key, logID [16]byte, bounds nodeStoreBounds, cryptoState fileCrypto) error {
	plain, err := marshalNodeIdentity(identity, bounds)
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
