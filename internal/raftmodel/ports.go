package raftmodel

import (
	"fmt"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// PersistBatch is the durable portion of one Ready. ReadyID is stable across
// retries of the same batch and is scoped to a Node incarnation.
// NodeIncarnation is a durable, never-reused, strictly increasing boot counter
// for one member store, not a random process token. A newly constructed Node
// starts at ReadyID 1; StableStore rejects a regressed incarnation or skipped
// ReadyID. Once bounded canonical admission succeeds or persistence may have
// begun, reuse of that key with different batch bytes must also be rejected.
//
// The pointed-to values are owned by Node and are read-only. By default,
// Persist must not retain or mutate them. TransferOwnership is the asynchronous
// local-storage fast path: it transfers immutable Entries and HardState to the
// store on canonical admission, and the caller must never mutate them again.
// A nil return means that Snapshot, Entries, and HardState have reached stable
// storage in that order. When MustSync is true, the durability barrier must
// also be complete before Persist returns.
type PersistBatch struct {
	NodeIncarnation   uint64
	ReadyID           uint64
	HardState         *pb.HardState
	Entries           []*pb.Entry
	Snapshot          *pb.Snapshot
	MustSync          bool
	TransferOwnership bool
}

// StableStore is both the recovery view consumed by raft and the atomic,
// retry-safe persistence boundary for Ready batches. Repeating a ReadyID after
// an error must be safe, including when the earlier call reached storage before
// returning the error. The idempotency key is (NodeIncarnation, ReadyID).
// A store may avoid retaining malformed or unsupported input that failed before
// canonical admission and before any storage mutation.
// Production construction must allocate NodeIncarnation from the same durable
// member store before the Node can accept protocol input; a caller-supplied
// random value is not a stale-process fence.
type StableStore interface {
	raft.Storage
	Persist(PersistBatch) error
}

// ApplyMeta identifies an ordered committed log entry. Data is passed
// separately so implementations can keep the metadata allocation-free.
type ApplyMeta struct {
	Index uint64
	Term  uint64
	Type  pb.EntryType
}

const (
	// MaxNormalApplyBatchEntries bounds the pointer/metadata work in one
	// state-machine call independently of the encoded byte limit. Callers split
	// a longer committed run and may always fall back to ApplyNormal at a
	// state-machine boundary.
	MaxNormalApplyBatchEntries = 128
	// MaxNormalApplyBatchBytes bounds the sum of borrowed entry data in one
	// call. It deliberately matches the independently checked single-proposal
	// bound: a maximum-sized entry still fits, while a batch cannot retain an
	// unbounded collection of smaller command envelopes.
	MaxNormalApplyBatchBytes = MaxProposalBytes
	// MaxNormalApplyCompletionBytes bounds the ephemeral singleton settlement
	// result. Large retained results continue to use the durable lookup path.
	MaxNormalApplyCompletionBytes = 4 << 10
)

// NormalApply is one borrowed normal-entry input to NormalBatchStateMachine.
// Data is read-only and remains owned by the caller.
type NormalApply struct {
	Meta ApplyMeta
	Data []byte
}

// NormalApplyBatchWorkspace is zero-value scratch for one bounded committed
// normal-entry apply. A serialized scheduler lane should reuse one workspace
// across all of its Nodes so the fixed arrays do not scale with group count.
// It must not be shared by concurrent calls.
type NormalApplyBatchWorkspace struct {
	entries     [MaxNormalApplyBatchEntries]NormalApply
	witnesses   [MaxNormalApplyBatchEntries][32]byte
	completions NormalApplyBatchCompletions
}

// NormalApplyBatchCompletions is caller-owned bounded scratch for original
// results produced by a normal batch apply. Results are ephemeral settlement
// evidence, never a replacement for the durable retry record. The arena is
// reused by one serialized Ready lane and therefore does not scale with group
// count.
type NormalApplyBatchCompletions struct {
	arena   []byte
	offsets [MaxNormalApplyBatchEntries]uint32
	lengths [MaxNormalApplyBatchEntries]uint16
}

// Reset clears every result while retaining at most the fixed aggregate bound.
func (completions *NormalApplyBatchCompletions) Reset() {
	if completions == nil {
		return
	}
	clear(completions.arena)
	completions.arena = completions.arena[:0]
	clear(completions.offsets[:])
	clear(completions.lengths[:])
}

// Store copies one nonempty original result into bounded caller-owned storage.
// Each logical entry may publish at most one result.
func (completions *NormalApplyBatchCompletions) Store(index int, result []byte) error {
	if completions == nil || index < 0 || index >= MaxNormalApplyBatchEntries ||
		len(result) == 0 || len(result) > MaxNormalApplyCompletionBytes ||
		cap(result) > MaxNormalApplyCompletionBytes || completions.lengths[index] != 0 ||
		len(completions.arena) > MaxNormalApplyBatchEntries*MaxNormalApplyCompletionBytes-len(result) {
		return fmt.Errorf("raftmodel: invalid normal batch completion")
	}
	completions.offsets[index] = uint32(len(completions.arena))
	completions.lengths[index] = uint16(len(result))
	completions.arena = append(completions.arena, result...)
	return nil
}

// Completion returns one borrowed original result, if the batch apply supplied
// it. Bytes remain valid only until the pending range is settled.
func (completions *NormalApplyBatchCompletions) Completion(index int) ([]byte, bool) {
	if completions == nil || index < 0 || index >= MaxNormalApplyBatchEntries {
		return nil, false
	}
	length := int(completions.lengths[index])
	offset := int(completions.offsets[index])
	if length == 0 || offset < 0 || offset > len(completions.arena)-length {
		return nil, false
	}
	return completions.arena[offset : offset+length : offset+length], true
}

// AppliedNormalBatch identifies one atomically published normal-entry range.
// Its entry data and final ConfState borrow Node-owned storage. They remain
// valid until the range is settled and the next Node lifecycle call begins.
// Callers must not mutate or retain borrowed values.
type AppliedNormalBatch struct {
	owner       *Node
	readyID     uint64
	entries     []*pb.Entry
	publication Publication
	completion  []byte
	completions *NormalApplyBatchCompletions
}

// Completion returns the optional original singleton apply result. Bytes are
// borrowed with the same lifetime as Entry; absence requests durable lookup.
func (batch AppliedNormalBatch) Completion(index int) ([]byte, bool) {
	if batch.completions != nil {
		if completion, ok := batch.completions.Completion(index); ok {
			return completion, true
		}
	}
	if index == 0 && len(batch.entries) == 1 && len(batch.completion) != 0 {
		return batch.completion, true
	}
	return nil, false
}

// HasCompletionForEveryCommand reports whether settlement can consume this
// entire range without opening a durable completion snapshot. Empty Raft
// no-ops have no result and do not prevent the fast path.
func (batch AppliedNormalBatch) HasCompletionForEveryCommand() bool {
	for index, entry := range batch.entries {
		if entry == nil {
			return false
		}
		if len(entry.GetData()) == 0 {
			continue
		}
		if _, ok := batch.Completion(index); !ok {
			return false
		}
	}
	return len(batch.entries) != 0
}

// Len returns the exact number of atomically published normal entries.
func (batch AppliedNormalBatch) Len() int { return len(batch.entries) }

// ReadyID identifies the Ready containing this applied range.
func (batch AppliedNormalBatch) ReadyID() uint64 { return batch.readyID }

// Entry returns one borrowed logical normal apply.
func (batch AppliedNormalBatch) Entry(index int) (NormalApply, bool) {
	if index < 0 || index >= len(batch.entries) || batch.entries[index] == nil {
		return NormalApply{}, false
	}
	entry := batch.entries[index]
	return NormalApply{
		Meta: ApplyMeta{Index: entry.GetIndex(), Term: entry.GetTerm(), Type: entry.GetType()},
		Data: entry.GetData(),
	}, true
}

// FirstIndex returns the first applied index, or zero for an empty value.
func (batch AppliedNormalBatch) FirstIndex() uint64 {
	entry, ok := batch.Entry(0)
	if !ok {
		return 0
	}
	return entry.Meta.Index
}

// LastIndex returns the final applied index, or zero for an empty value.
func (batch AppliedNormalBatch) LastIndex() uint64 {
	entry, ok := batch.Entry(len(batch.entries) - 1)
	if !ok {
		return 0
	}
	return entry.Meta.Index
}

// FinalPublication returns the sole reader-visible publication for the range.
// Its ConfState is borrowed and immutable.
func (batch AppliedNormalBatch) FinalPublication() Publication { return batch.publication }

// ApplyBatchResult reports one bounded committed-entry apply operation. Applied
// counts all consumed entries. Normal is nonempty only when the consumed range
// contains normal entries whose results require settlement.
type ApplyBatchResult struct {
	Applied int
	Normal  AppliedNormalBatch
}

// NormalCompletionStateMachine optionally preserves an original singleton
// result which cannot be reconstructed from the post-apply state (for example,
// creation versus an exact duplicate). A successful nonempty result transfers
// ownership of at most MaxNormalApplyCompletionBytes to the caller. It may not
// alias input or mutable machine storage. Nil uses ordinary durable lookup.
// Errors must not expose a completion; publication obeys StateMachine rules.
type NormalCompletionStateMachine interface {
	ApplyNormalWithCompletion(ApplyMeta, []byte) (Publication, []byte, error)
}

// Publication is the state visible to readers after an apply operation
// returns. ConfState is owned by the StateMachine and must be treated as
// immutable by callers.
type Publication struct {
	Applied           uint64
	DataChainDigest   [32]byte
	ConfState         *pb.ConfState
	ReplicaSetVersion uint64
}

// StateMachine is the ordered apply and reader-publication boundary. Each
// mutating method must publish its returned state atomically before returning
// nil. ApplyConfiguration must durably retain the returned ConfState as part of
// Published so restart can recover the exact applied membership independently
// of an older snapshot's ConfState. InstallSnapshot must be idempotent for the
// same exact snapshot: restart calls it both when the durable base is newer and
// when it equals the published cut. The state machine must durably bind the
// snapshot's exact identity/manifest, data-chain digest, ConfState, and expected
// ReplicaSetVersion, reject different bytes at the same cut, and reject every
// regressing field even after an earlier call published before returning an
// error. A later Published call must expose an ambiguous successful
// publication so retry does not reapply it.
// Normal entry data, configuration state, and snapshots are borrowed only for
// the duration of the call and must not be mutated or retained.
type StateMachine interface {
	Applied() uint64
	Published() Publication
	ApplyNormal(ApplyMeta, []byte) (Publication, error)
	ApplyConfiguration(ApplyMeta, *pb.ConfState) (Publication, error)
	InstallSnapshot(*pb.Snapshot) (Publication, error)
}

// NormalBatchStateMachine is the optional atomic local-publication lane for a
// bounded run of consecutive normal entries. dataChainWitnesses must have at
// least len(entries) elements. A nil error with applied > 0 means exactly
// entries[:applied] were published as one local transaction. Witness slot i is
// that logical entry's data-chain digest, while publication is the sole
// reader-visible final cut. Intermediate witnesses are validation facts, not
// publications. A nil error with
// applied == 0 and a zero publication reports a clean boundary that the caller
// must apply through ApplyNormal. An error returns applied == 0 and a zero
// publication, certifying no prefix to the caller. As with ApplyNormal, an
// outcome-unknown error may have physically published the complete selected
// batch before failing closed. Recovery or a later Published observation may
// reveal the old cut or the complete final cut, never an intermediate prefix.
// Implementations clear the witness slots corresponding to entries before
// planning. Therefore every unselected slot, and every slot returned with an
// error, is zero rather than stale caller scratch.
//
// Implementations may stop before an ownership/control boundary or before the
// next entry would exceed their frozen aggregate transaction profile. They
// must consume at least the first entry when it is an otherwise valid batchable
// normal transition.
type NormalBatchStateMachine interface {
	ApplyNormalBatch(entries []NormalApply, dataChainWitnesses [][32]byte) (
		applied int,
		publication Publication,
		err error,
	)
}

// NormalCompletionBatchStateMachine optionally carries original bounded
// results from an atomic batch directly into synchronous settlement. Missing
// results retain the ordinary durable lookup behavior. On error, or when no
// prefix is applied, completions must be empty.
type NormalCompletionBatchStateMachine interface {
	ApplyNormalBatchWithCompletions(
		entries []NormalApply,
		dataChainWitnesses [][32]byte,
		completions *NormalApplyBatchCompletions,
	) (applied int, publication Publication, err error)
}

// validateNormalBatchDataChainWitnesses checks the logical digest walk before
// a caller advances its committed-entry cursor. Only the final publication is
// reader-visible. Empty normal entries still need an intermediate witness so a
// faulty implementation cannot hide a data-chain change behind the final cut.
func validateNormalBatchDataChainWitnesses(
	previous [32]byte,
	entries []NormalApply,
	applied int,
	witnesses [][32]byte,
	final Publication,
) error {
	if previous == ([32]byte{}) || applied <= 0 || applied > len(entries) ||
		applied > len(witnesses) || final.Applied != entries[applied-1].Meta.Index ||
		final.DataChainDigest != witnesses[applied-1] {
		return fmt.Errorf("invalid normal-batch final data-chain witness")
	}
	for index := 0; index < applied; index++ {
		witness := witnesses[index]
		if witness == ([32]byte{}) {
			return fmt.Errorf("normal-batch entry %d returned a zero data-chain witness", index)
		}
		if len(entries[index].Data) == 0 && witness != previous {
			return fmt.Errorf("normal-batch no-op entry %d changed data-chain digest", index)
		}
		previous = witness
	}
	return nil
}

// CheckpointedStateMachine optionally exposes the authenticated contiguous
// apply cut used as one input to WAL retention. It may trail
// StateMachine.Applied when local apply is replay-backed. The index alone is
// not deletion authority: a compactor must also bind term, configuration,
// member lineage, certificate witness, and the retained suffix.
type CheckpointedStateMachine interface {
	CheckpointAppliedIndex() uint64
}
