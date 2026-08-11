package raftsim

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrPersistInjected is the stable cause used by the in-memory disk fault
	// seam. PersistError from raftmodel preserves it through errors.Is.
	ErrPersistInjected = errors.New("raftsim: injected persist failure")
	// ErrStoreInvariant reports a malformed or non-monotonic durable image.
	ErrStoreInvariant = errors.New("raftsim: durable store invariant violated")
)

const MaxSnapshotBytes = raftmodel.MaxSnapshotBytes

// PersistFault selects the next Persist boundary result.
type PersistFault uint8

const (
	PersistNoFault PersistFault = iota
	// PersistFailBefore returns a definite error without changing durability.
	PersistFailBefore
	// PersistThenError durably installs the entire batch but returns an error.
	// Retrying the exact (incarnation, ReadyID) settles it successfully.
	PersistThenError
)

// MemoryStore is an atomic in-memory StableStore used by the simulator. The
// underlying MemoryStorage is never exposed: Persist clones a complete image,
// mutates the clone, then swaps it, giving each Ready one indivisible durable
// boundary. Physical torn-write and corruption states belong to the Phase-1
// WAL simulator rather than this logical store.
type MemoryStore struct {
	storage *raft.MemoryStorage

	fault PersistFault

	lastIncarnation uint64
	lastReadyID     uint64
	lastBatchDigest [32]byte
	syncs           uint64
	persists        uint64
}

// NewMemoryStore returns a canonical initial snapshot at index/term one for a
// static nonempty voter set. IDs are sorted and must be unique and nonzero.
func NewMemoryStore(voters []uint64) (*MemoryStore, error) {
	if len(voters) == 0 || len(voters) > MaxMembers {
		return nil, fmt.Errorf("%w: voter count %d", ErrStoreInvariant, len(voters))
	}
	ids := slices.Clone(voters)
	slices.Sort(ids)
	for i, id := range ids {
		if id == raft.None || raft.IsLocalMsgTarget(id) || (i != 0 && id == ids[i-1]) {
			return nil, fmt.Errorf("%w: invalid voter set", ErrStoreInvariant)
		}
	}
	index, term := uint64(1), uint64(1)
	snapshot := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		ConfState: &pb.ConfState{Voters: ids}, Index: &index, Term: &term,
	}}
	storage := raft.NewMemoryStorage()
	if err := storage.ApplySnapshot(snapshot); err != nil {
		return nil, fmt.Errorf("%w: initialize snapshot: %v", ErrStoreInvariant, err)
	}
	if err := storage.SetHardState(&pb.HardState{Term: &term, Commit: &index}); err != nil {
		return nil, fmt.Errorf("%w: initialize HardState: %v", ErrStoreInvariant, err)
	}
	return &MemoryStore{storage: storage}, nil
}

// SetNextPersistFault arms exactly one persistence attempt.
func (s *MemoryStore) SetNextPersistFault(fault PersistFault) {
	if s != nil {
		s.fault = fault
	}
}

// SyncCount reports successful Ready durability barriers with MustSync=true.
func (s *MemoryStore) SyncCount() uint64 {
	if s == nil {
		return 0
	}
	return s.syncs
}

// PersistCount reports distinct Ready batches durably installed.
func (s *MemoryStore) PersistCount() uint64 {
	if s == nil {
		return 0
	}
	return s.persists
}

// Persist atomically installs one Ready's durable fields. Exact retries are
// idempotent; a skipped or regressed ReadyID within one node incarnation fails
// closed rather than silently dropping a batch.
func (s *MemoryStore) Persist(batch raftmodel.PersistBatch) error {
	if s == nil || s.storage == nil || batch.NodeIncarnation == 0 || batch.ReadyID == 0 {
		return ErrStoreInvariant
	}
	if err := validatePersistBatchShape(batch); err != nil {
		return err
	}
	digest, err := persistBatchDigest(batch)
	if err != nil {
		return err
	}
	if batch.NodeIncarnation == s.lastIncarnation {
		if batch.ReadyID == s.lastReadyID {
			if digest != s.lastBatchDigest {
				return fmt.Errorf("%w: Ready retry payload changed", ErrStoreInvariant)
			}
			return nil
		}
		if s.lastReadyID == math.MaxUint64 || batch.ReadyID != s.lastReadyID+1 {
			return fmt.Errorf("%w: Ready sequence %d after %d", ErrStoreInvariant, batch.ReadyID, s.lastReadyID)
		}
	} else if batch.NodeIncarnation < s.lastIncarnation || batch.ReadyID != 1 {
		return fmt.Errorf("%w: new incarnation starts at Ready %d", ErrStoreInvariant, batch.ReadyID)
	}
	previous, previousCommit, previousFirst, previousLast, err := validateStorageImage(s.storage)
	if err != nil {
		return err
	}
	fault := s.fault
	s.fault = PersistNoFault
	if fault == PersistFailBefore {
		return ErrPersistInjected
	}

	next, err := cloneMemoryStorage(s.storage)
	if err != nil {
		return err
	}
	snapshotAdvanced := false
	if !raft.IsEmptySnap(batch.Snapshot) {
		current, snapshotErr := next.Snapshot()
		if snapshotErr != nil {
			return fmt.Errorf("%w: current snapshot: %v", ErrStoreInvariant, snapshotErr)
		}
		switch {
		case batch.Snapshot.GetMetadata().GetIndex() < current.GetMetadata().GetIndex():
			return fmt.Errorf("%w: snapshot regression", ErrStoreInvariant)
		case batch.Snapshot.GetMetadata().GetIndex() == current.GetMetadata().GetIndex():
			if !proto.Equal(batch.Snapshot, current) {
				return fmt.Errorf("%w: same-index snapshot mismatch", ErrStoreInvariant)
			}
		default:
			if batch.Snapshot.GetMetadata().GetIndex() < previousCommit {
				return fmt.Errorf("%w: snapshot index %d erases committed index %d", ErrStoreInvariant, batch.Snapshot.GetMetadata().GetIndex(), previousCommit)
			}
			if err := next.ApplySnapshot(cloneSnapshot(batch.Snapshot)); err != nil {
				return fmt.Errorf("%w: apply snapshot: %v", ErrStoreInvariant, err)
			}
			snapshotAdvanced = true
		}
	}
	if len(batch.Entries) != 0 {
		first, firstErr := next.FirstIndex()
		last, lastErr := next.LastIndex()
		if firstErr != nil || lastErr != nil || first == 0 || last == math.MaxUint64 {
			return fmt.Errorf("%w: inspect append range", ErrStoreInvariant)
		}
		base := first - 1
		firstEntry := batch.Entries[0].GetIndex()
		if firstEntry <= base || firstEntry > last+1 {
			return fmt.Errorf("%w: entries start at %d outside append range [%d,%d]", ErrStoreInvariant, firstEntry, base+1, last+1)
		}
		if !snapshotAdvanced {
			for _, entry := range batch.Entries {
				index := entry.GetIndex()
				if index < previousFirst || index > previousCommit || index > previousLast {
					continue
				}
				existing, entryErr := s.storage.Entries(index, index+1, math.MaxUint64)
				if entryErr != nil || len(existing) != 1 || !proto.Equal(existing[0], entry) {
					return fmt.Errorf("%w: entry %d overwrites committed prefix", ErrStoreInvariant, index)
				}
			}
		}
		entries := cloneEntries(batch.Entries)
		if err := next.Append(entries); err != nil {
			return fmt.Errorf("%w: append entries: %v", ErrStoreInvariant, err)
		}
	}
	if !raft.IsEmptyHardState(batch.HardState) {
		if batch.HardState.GetTerm() < previous.GetTerm() ||
			batch.HardState.GetCommit() < previousCommit ||
			(batch.HardState.GetTerm() == previous.GetTerm() && previous.GetVote() != 0 &&
				batch.HardState.GetVote() != previous.GetVote()) {
			return fmt.Errorf("%w: HardState regression", ErrStoreInvariant)
		}
		last, lastErr := next.LastIndex()
		if lastErr != nil || batch.HardState.GetCommit() > last {
			return fmt.Errorf("%w: commit exceeds durable log", ErrStoreInvariant)
		}
		if err := next.SetHardState(cloneHardState(batch.HardState)); err != nil {
			return fmt.Errorf("%w: set HardState: %v", ErrStoreInvariant, err)
		}
	}
	if _, _, _, _, err := validateStorageImage(next); err != nil {
		return err
	}

	s.storage = next
	s.lastIncarnation = batch.NodeIncarnation
	s.lastReadyID = batch.ReadyID
	s.lastBatchDigest = digest
	s.persists++
	if batch.MustSync {
		s.syncs++
	}
	if fault == PersistThenError {
		return ErrPersistInjected
	}
	return nil
}

// InitialState implements raft.Storage and returns detached protobuf values.
func (s *MemoryStore) InitialState() (*pb.HardState, *pb.ConfState, error) {
	if s == nil || s.storage == nil {
		return nil, nil, ErrStoreInvariant
	}
	hard, conf, err := s.storage.InitialState()
	return cloneHardState(hard), cloneConfState(conf), err
}

func (s *MemoryStore) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	if s == nil || s.storage == nil {
		return nil, ErrStoreInvariant
	}
	entries, err := s.storage.Entries(lo, hi, maxSize)
	if err != nil {
		return nil, err
	}
	return cloneEntries(entries), nil
}

func (s *MemoryStore) Term(index uint64) (uint64, error) {
	if s == nil || s.storage == nil {
		return 0, ErrStoreInvariant
	}
	return s.storage.Term(index)
}

func (s *MemoryStore) LastIndex() (uint64, error) {
	if s == nil || s.storage == nil {
		return 0, ErrStoreInvariant
	}
	return s.storage.LastIndex()
}

func (s *MemoryStore) FirstIndex() (uint64, error) {
	if s == nil || s.storage == nil {
		return 0, ErrStoreInvariant
	}
	return s.storage.FirstIndex()
}

func (s *MemoryStore) Snapshot() (*pb.Snapshot, error) {
	if s == nil || s.storage == nil {
		return nil, ErrStoreInvariant
	}
	snapshot, err := s.storage.Snapshot()
	return cloneSnapshot(snapshot), err
}

func validatePersistBatchShape(batch raftmodel.PersistBatch) error {
	if len(batch.Entries) > MaxAppliedEntries {
		return fmt.Errorf("%w: Ready entries %d exceed %d", ErrStoreInvariant, len(batch.Entries), MaxAppliedEntries)
	}
	var previous uint64
	totalEntryBytes := 0
	for i, entry := range batch.Entries {
		if entry == nil || entry.GetIndex() == 0 || entry.GetTerm() == 0 || entry.GetIndex() == math.MaxUint64 ||
			(i != 0 && entry.GetIndex() != previous+1) || len(entry.GetData()) > raftmodel.MaxProposalBytes ||
			len(entry.GetData()) > raftmodel.MaxUncommittedEntriesSize-totalEntryBytes {
			return fmt.Errorf("%w: malformed entry ordinal %d", ErrStoreInvariant, i)
		}
		totalEntryBytes += len(entry.GetData())
		previous = entry.GetIndex()
	}
	if !raft.IsEmptySnap(batch.Snapshot) {
		metadata := batch.Snapshot.GetMetadata()
		if metadata == nil || metadata.GetIndex() == 0 || metadata.GetIndex() == math.MaxUint64 ||
			metadata.GetTerm() == 0 || metadata.GetConfState() == nil {
			return fmt.Errorf("%w: malformed snapshot", ErrStoreInvariant)
		}
		if len(batch.Snapshot.GetData()) > MaxSnapshotBytes {
			return fmt.Errorf("%w: snapshot bytes %d exceed %d", ErrStoreInvariant, len(batch.Snapshot.GetData()), MaxSnapshotBytes)
		}
	}
	return nil
}

func validateStorageImage(storage *raft.MemoryStorage) (*pb.HardState, uint64, uint64, uint64, error) {
	if storage == nil {
		return nil, 0, 0, 0, ErrStoreInvariant
	}
	hard, _, err := storage.InitialState()
	if err != nil {
		return nil, 0, 0, 0, fmt.Errorf("%w: initial state: %v", ErrStoreInvariant, err)
	}
	first, err := storage.FirstIndex()
	if err != nil || first == 0 {
		return nil, 0, 0, 0, fmt.Errorf("%w: first index", ErrStoreInvariant)
	}
	last, err := storage.LastIndex()
	base := first - 1
	if err != nil || last < base || last == math.MaxUint64 {
		return nil, 0, 0, 0, fmt.Errorf("%w: durable range [%d,%d]", ErrStoreInvariant, base, last)
	}
	snapshot, err := storage.Snapshot()
	if err != nil || snapshot.GetMetadata().GetIndex() != base ||
		(base != 0 && (snapshot.GetMetadata().GetTerm() == 0 || snapshot.GetMetadata().GetConfState() == nil)) {
		return nil, 0, 0, 0, fmt.Errorf("%w: snapshot/log base mismatch", ErrStoreInvariant)
	}
	baseTerm, err := storage.Term(base)
	if err != nil || baseTerm != snapshot.GetMetadata().GetTerm() {
		return nil, 0, 0, 0, fmt.Errorf("%w: snapshot/log term mismatch", ErrStoreInvariant)
	}
	committed := base
	if !raft.IsEmptyHardState(hard) {
		lastTerm, termErr := storage.Term(last)
		if hard.GetCommit() < base || hard.GetCommit() > last ||
			(hard.GetTerm() == 0 && hard.GetVote() != 0) || termErr != nil || hard.GetTerm() < lastTerm {
			return nil, 0, 0, 0, fmt.Errorf("%w: durable HardState", ErrStoreInvariant)
		}
		committed = hard.GetCommit()
	} else if last != base {
		return nil, 0, 0, 0, fmt.Errorf("%w: log entries without HardState", ErrStoreInvariant)
	}
	return cloneHardState(hard), committed, first, last, nil
}

func cloneMemoryStorage(source *raft.MemoryStorage) (*raft.MemoryStorage, error) {
	hard, _, err := source.InitialState()
	if err != nil {
		return nil, fmt.Errorf("%w: clone initial state: %v", ErrStoreInvariant, err)
	}
	snapshot, err := source.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("%w: clone snapshot: %v", ErrStoreInvariant, err)
	}
	first, err := source.FirstIndex()
	if err != nil {
		return nil, fmt.Errorf("%w: clone first index: %v", ErrStoreInvariant, err)
	}
	last, err := source.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("%w: clone last index: %v", ErrStoreInvariant, err)
	}

	next := raft.NewMemoryStorage()
	if !raft.IsEmptySnap(snapshot) {
		if err := next.ApplySnapshot(snapshot); err != nil {
			return nil, fmt.Errorf("%w: clone apply snapshot: %v", ErrStoreInvariant, err)
		}
	}
	if first <= last {
		if last == math.MaxUint64 {
			return nil, fmt.Errorf("%w: Raft log index exhausted", ErrStoreInvariant)
		}
		entries, entriesErr := source.Entries(first, last+1, math.MaxUint64)
		if entriesErr != nil {
			return nil, fmt.Errorf("%w: clone entries: %v", ErrStoreInvariant, entriesErr)
		}
		if err := next.Append(cloneEntries(entries)); err != nil {
			return nil, fmt.Errorf("%w: clone append: %v", ErrStoreInvariant, err)
		}
	}
	if !raft.IsEmptyHardState(hard) {
		if err := next.SetHardState(cloneHardState(hard)); err != nil {
			return nil, fmt.Errorf("%w: clone HardState: %v", ErrStoreInvariant, err)
		}
	}
	return next, nil
}

func persistBatchDigest(batch raftmodel.PersistBatch) ([32]byte, error) {
	hasher := sha256.New()
	var fixed [25]byte
	binary.LittleEndian.PutUint64(fixed[0:8], batch.NodeIncarnation)
	binary.LittleEndian.PutUint64(fixed[8:16], batch.ReadyID)
	binary.LittleEndian.PutUint64(fixed[16:24], uint64(len(batch.Entries)))
	if batch.MustSync {
		fixed[24] = 1
	}
	_, _ = hasher.Write(fixed[:])
	if err := writeProtoDigest(hasher, batch.HardState); err != nil {
		return [32]byte{}, err
	}
	if err := writeProtoDigest(hasher, batch.Snapshot); err != nil {
		return [32]byte{}, err
	}
	for _, entry := range batch.Entries {
		if err := writeProtoDigest(hasher, entry); err != nil {
			return [32]byte{}, err
		}
	}
	var digest [32]byte
	_ = hasher.Sum(digest[:0])
	return digest, nil
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeProtoDigest(dst digestWriter, message proto.Message) error {
	if message == nil || !message.ProtoReflect().IsValid() {
		var size [8]byte
		binary.LittleEndian.PutUint64(size[:], math.MaxUint64)
		_, _ = dst.Write(size[:])
		return nil
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return fmt.Errorf("%w: deterministic protobuf marshal: %v", ErrStoreInvariant, err)
	}
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], uint64(len(encoded)))
	_, _ = dst.Write(size[:])
	_, _ = dst.Write(encoded)
	return nil
}

func cloneEntries(entries []*pb.Entry) []*pb.Entry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]*pb.Entry, len(entries))
	for i, entry := range entries {
		if entry != nil {
			cloned[i] = proto.Clone(entry).(*pb.Entry)
		}
	}
	return cloned
}

func cloneHardState(state *pb.HardState) *pb.HardState {
	if state == nil {
		return nil
	}
	return proto.Clone(state).(*pb.HardState)
}

func cloneConfState(state *pb.ConfState) *pb.ConfState {
	if state == nil {
		return nil
	}
	return proto.Clone(state).(*pb.ConfState)
}

func cloneSnapshot(snapshot *pb.Snapshot) *pb.Snapshot {
	if snapshot == nil {
		return nil
	}
	return proto.Clone(snapshot).(*pb.Snapshot)
}
