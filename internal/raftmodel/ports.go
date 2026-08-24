package raftmodel

import (
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
// The pointed-to values are owned by Node and are read-only. Persist must not
// retain or mutate them. A nil return means that Snapshot, Entries, and
// HardState have reached stable storage in that order. When MustSync is true,
// the durability barrier must also be complete before Persist returns.
type PersistBatch struct {
	NodeIncarnation uint64
	ReadyID         uint64
	HardState       *pb.HardState
	Entries         []*pb.Entry
	Snapshot        *pb.Snapshot
	MustSync        bool
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

// CheckpointedStateMachine optionally exposes the authenticated contiguous cut
// safe for WAL retention. It may trail StateMachine.Applied when local apply is
// replay-backed; implementations without it retain their existing per-entry
// durability contract.
type CheckpointedStateMachine interface {
	CheckpointAppliedIndex() uint64
}
