package raftsim

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	// MaxAppliedEntries bounds exact prefix evidence retained by the Phase-0
	// machine. Later snapshots replace this bounded model history.
	MaxAppliedEntries = 4096

	modelCommandHeaderBytes = 16
)

var modelCommandMagic = [8]byte{'V', 'D', 'B', 'M', 'O', 'D', 0, 1}

var (
	// ErrMachineFull reports the exact applied-record bound.
	ErrMachineFull = errors.New("raftsim: state machine applied-record limit reached")
	// ErrMachineInvariant reports non-contiguous or contradictory application.
	ErrMachineInvariant = errors.New("raftsim: state machine invariant violated")
)

// AppliedEntry is exact prefix evidence for one applied log position.
type AppliedEntry struct {
	Index       uint64
	Term        uint64
	Type        pb.EntryType
	ProposalRef uint64
	Digest      [32]byte
}

// MemoryMachine is a crash-persistent deterministic model state machine. It
// deliberately treats normal entry data as opaque; the frozen replication
// command codec is tested independently until the production apply adapter is
// implemented.
type MemoryMachine struct {
	publication raftmodel.Publication
	entries     []AppliedEntry
	baseIndex   uint64
	chain       [32]byte

	// snapshotIdentity binds the exact canonical snapshot from which the
	// machine's base publication was restored. Phase 0 does not model
	// advancing application snapshots yet, but restart reconciliation must
	// still prove that an already-published base came from the same durable
	// snapshot rather than merely sharing its index and ConfState.
	snapshotIdentity [32]byte
	snapshotTerm     uint64
}

// NewMemoryMachine restores the same canonical index-one cut used by
// NewMemoryStore.
func NewMemoryMachine(voters []uint64) (*MemoryMachine, error) {
	store, err := NewMemoryStore(voters)
	if err != nil {
		return nil, err
	}
	_, conf, err := store.InitialState()
	if err != nil {
		return nil, err
	}
	snapshot, err := store.Snapshot()
	if err != nil {
		return nil, err
	}
	identity, err := modelSnapshotIdentity(snapshot)
	if err != nil {
		return nil, err
	}
	return &MemoryMachine{
		publication: raftmodel.Publication{
			Applied: 1, ConfState: cloneConfState(conf), ReplicaSetVersion: 1,
		},
		baseIndex:        1,
		snapshotIdentity: identity,
		snapshotTerm:     snapshot.GetMetadata().GetTerm(),
	}, nil
}

func (m *MemoryMachine) Applied() uint64 {
	if m == nil {
		return 0
	}
	return m.publication.Applied
}

func (m *MemoryMachine) Published() raftmodel.Publication {
	if m == nil {
		return raftmodel.Publication{}
	}
	publication := m.publication
	publication.ConfState = cloneConfState(publication.ConfState)
	return publication
}

// Entry returns exact retained evidence for one post-baseline position.
func (m *MemoryMachine) Entry(index uint64) (AppliedEntry, bool) {
	if m == nil || index <= m.baseIndex {
		return AppliedEntry{}, false
	}
	offset := index - m.baseIndex - 1
	if offset >= uint64(len(m.entries)) {
		return AppliedEntry{}, false
	}
	return m.entries[offset], true
}

// EntryCount reports retained exact applied evidence.
func (m *MemoryMachine) EntryCount() int {
	if m == nil {
		return 0
	}
	return len(m.entries)
}

// Completed reports the exact applied index for one simulator proposal. The
// linear scan is deliberately bounded by MaxAppliedEntries and keeps this
// foundation free of a second unbounded completion directory.
func (m *MemoryMachine) Completed(reference uint64) (uint64, bool) {
	if m == nil || reference == 0 {
		return 0, false
	}
	for i := len(m.entries) - 1; i >= 0; i-- {
		if m.entries[i].ProposalRef == reference {
			return m.entries[i].Index, true
		}
	}
	return 0, false
}

func (m *MemoryMachine) ApplyNormal(meta raftmodel.ApplyMeta, data []byte) (raftmodel.Publication, error) {
	if err := m.validateNext(meta); err != nil {
		return raftmodel.Publication{}, err
	}
	record := appliedEntry(m.chain, meta, data)
	next := m.publication
	next.Applied = meta.Index
	if len(data) != 0 {
		hasher := sha256.New()
		_, _ = hasher.Write([]byte("VDBLOGICAL/v1\x00"))
		_, _ = hasher.Write(next.LogicalDigest[:])
		_, _ = hasher.Write(data)
		_ = hasher.Sum(next.LogicalDigest[:0])
	}
	m.entries = append(m.entries, record)
	m.chain = record.Digest
	m.publication = next
	return m.Published(), nil
}

func (m *MemoryMachine) ApplyConfiguration(meta raftmodel.ApplyMeta, state *pb.ConfState) (raftmodel.Publication, error) {
	if err := m.validateNext(meta); err != nil {
		return raftmodel.Publication{}, err
	}
	if state == nil {
		return raftmodel.Publication{}, fmt.Errorf("%w: nil ConfState", ErrMachineInvariant)
	}
	encoded, err := marshalConfState(state)
	if err != nil {
		return raftmodel.Publication{}, err
	}
	record := appliedEntry(m.chain, meta, encoded)
	next := m.publication
	next.Applied = meta.Index
	next.ConfState = cloneConfState(state)
	next.ReplicaSetVersion = meta.Index
	m.entries = append(m.entries, record)
	m.chain = record.Digest
	m.publication = next
	return m.Published(), nil
}

// InstallSnapshot reconciles the exact static bootstrap snapshot idempotently.
// Advancing application snapshots remain outside the Phase-0 model, but an
// already-published cut must still be bound to the exact durable snapshot on
// every restart.
func (m *MemoryMachine) InstallSnapshot(snapshot *pb.Snapshot) (raftmodel.Publication, error) {
	if m == nil || snapshot == nil || snapshot.GetMetadata() == nil {
		return raftmodel.Publication{}, fmt.Errorf("%w: nil snapshot", ErrMachineInvariant)
	}
	metadata := snapshot.GetMetadata()
	identity, err := modelSnapshotIdentity(snapshot)
	if err != nil {
		return raftmodel.Publication{}, err
	}
	if metadata.GetIndex() != m.baseIndex || m.publication.Applied != m.baseIndex ||
		metadata.GetTerm() != m.snapshotTerm || identity != m.snapshotIdentity ||
		m.publication.ConfState == nil || metadata.GetConfState() == nil ||
		m.publication.ConfState.Equivalent(metadata.GetConfState()) != nil ||
		m.publication.ReplicaSetVersion != m.baseIndex {
		return raftmodel.Publication{}, fmt.Errorf(
			"%w: snapshot does not match the exact static publication", ErrMachineInvariant,
		)
	}
	return m.Published(), nil
}

func modelSnapshotIdentity(snapshot *pb.Snapshot) ([32]byte, error) {
	if snapshot == nil {
		return [32]byte{}, fmt.Errorf("%w: nil snapshot", ErrMachineInvariant)
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
	if err != nil {
		return [32]byte{}, fmt.Errorf("%w: marshal snapshot: %v", ErrMachineInvariant, err)
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("VDBSIMSNAP/v1\x00"))
	_, _ = hasher.Write(encoded)
	var identity [32]byte
	_ = hasher.Sum(identity[:0])
	return identity, nil
}

func (m *MemoryMachine) validateNext(meta raftmodel.ApplyMeta) error {
	if m == nil || m.publication.Applied == ^uint64(0) ||
		meta.Index != m.publication.Applied+1 || meta.Term == 0 {
		return fmt.Errorf("%w: apply index=%d term=%d after=%d", ErrMachineInvariant, meta.Index, meta.Term, m.Applied())
	}
	if len(m.entries) >= MaxAppliedEntries {
		return ErrMachineFull
	}
	return nil
}

func appliedEntry(previous [32]byte, meta raftmodel.ApplyMeta, data []byte) AppliedEntry {
	var header [25]byte
	copy(header[0:8], []byte("VDBAPPL\x00"))
	binary.LittleEndian.PutUint64(header[8:16], meta.Index)
	binary.LittleEndian.PutUint64(header[16:24], meta.Term)
	header[24] = byte(meta.Type)
	hasher := sha256.New()
	_, _ = hasher.Write(previous[:])
	_, _ = hasher.Write(header[:])
	_, _ = hasher.Write(data)
	var digest [32]byte
	_ = hasher.Sum(digest[:0])
	return AppliedEntry{
		Index: meta.Index, Term: meta.Term, Type: meta.Type,
		ProposalRef: modelCommandReference(data), Digest: digest,
	}
}

// appendModelCommand builds the deliberately simulator-local proposal payload.
// It is not the production replication command codec and has no compatibility
// promise outside TraceFormatVersion.
func appendModelCommand(dst []byte, reference uint64, payload []byte) []byte {
	dst = append(dst, modelCommandMagic[:]...)
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], reference)
	dst = append(dst, encoded[:]...)
	return append(dst, payload...)
}

func modelCommandReference(data []byte) uint64 {
	if len(data) < modelCommandHeaderBytes || string(data[:8]) != string(modelCommandMagic[:]) {
		return 0
	}
	return binary.LittleEndian.Uint64(data[8:16])
}

func marshalConfState(state *pb.ConfState) ([]byte, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal ConfState: %v", ErrMachineInvariant, err)
	}
	return encoded, nil
}
