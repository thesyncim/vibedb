package raftmodel

import (
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// PersistBatch is the durable portion of one Ready. ReadyID is stable across
// retries of the same batch and is scoped to a Node incarnation.
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
	LogicalDigest     [32]byte
	ConfState         *pb.ConfState
	ReplicaSetVersion uint64
}

// StateMachine is the ordered apply and reader-publication boundary. Each
// mutating method must publish its returned state atomically before returning
// nil. ApplyConfiguration must durably retain the returned ConfState as part of
// Published so restart can recover the exact applied membership independently
// of an older snapshot's ConfState. Normal entry data, configuration state, and
// snapshots are borrowed only for the duration of the call and must not be
// mutated or retained.
type StateMachine interface {
	Applied() uint64
	Published() Publication
	ApplyNormal(ApplyMeta, []byte) (Publication, error)
	ApplyConfiguration(ApplyMeta, *pb.ConfState) (Publication, error)
	InstallSnapshot(*pb.Snapshot) (Publication, error)
}
