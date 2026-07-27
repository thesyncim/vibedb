package store

import vibejson "github.com/thesyncim/vibejson"

// IndexSource is the shape query's candidate-generation code plans against: a
// declared-index catalog plus exact/candidate mask probes. *Snapshot and
// package store/durable's snapshot type (through a small adapter — durable's
// probes carry extra I/O-workspace and stats parameters an interface method
// can't take) both satisfy it, so query's mask-based planner has one
// implementation shared by both backends instead of two.
//
// Deliberately excluded: Snapshot.AppendLiveMasks and the Segment posting-
// ordinal path. A durable snapshot cannot materialize a full live-row mask
// without cost the way a heap Snapshot can, and Segment's candidate
// representation ([]int ordinals) is a different shape in kind, not just in
// data access — callers that need either fall outside this interface by
// design rather than forcing a fourth method onto it.
type IndexSource interface {
	AppendIndexes(dst []IndexInfo) []IndexInfo
	AppendIndexMasks(dst []Mask, name string, values ...vibejson.Index) ([]Mask, error)
	AppendIndexCandidateMasks(dst []Mask, name string, values ...vibejson.Index) ([]Mask, error)
}

var _ IndexSource = Snapshot{}

// LiveMaskSource is an optional IndexSource capability: some backends (the
// heap Snapshot) can cheaply materialize the full live-row universe as
// masks, which lets query complement a candidate set for NOT without a
// second index probe. durable does not implement it — materializing that
// universe needs real page I/O, not a metadata-only operation — so query's
// NOT handling checks for this capability with a type assertion instead of
// requiring it on IndexSource.
type LiveMaskSource interface {
	AppendLiveMasks(dst []Mask) []Mask
}

var _ LiveMaskSource = Snapshot{}

// Reader is the read-only surface both backends' snapshot types share:
// everything query's execution core needs regardless of which Mutable produced
// the snapshot. It embeds IndexSource because candidate planning and row
// extraction are both plan-time-only, read-only operations, and every
// concrete Reader is also an IndexSource.
type Reader interface {
	IndexSource
	Len() int
	Generation() uint64
}

var _ Reader = Snapshot{}

// Mutable is the mutable, keyed, generation-producing shape both *Collection and
// package store/durable's collection type satisfy: S is the concrete Snapshot
// type each side produces (Snapshot for *Collection, *durable.Snapshot for
// durable's collection), so Mutable[S] costs nothing beyond what each side already
// does — no boxing, no adapter allocation.
//
// Deliberately excluded: index-lifecycle mutation (AddIndex, CreateIndex,
// DropIndex, BackfillIndex, ReclaimIndexes). durable's indexes are frozen at
// construction; that is a real capability difference, not incidental
// duplication, and belongs on the separate IndexManager interface that only
// *Collection implements.
type Mutable[S any] interface {
	Put(key string, src []byte) (bool, error)
	Delete(key string) (bool, error)
	Snapshot() (S, error)
	Len() uint64
	Generation() uint64
}

var _ Mutable[Snapshot] = (*Collection)(nil)

// IndexManager is the heap-only index-lifecycle extension described on
// Mutable. Generic code that needs online index mutation type-asserts for it
// (`im, ok := any(t).(IndexManager)`) rather than requiring every Mutable to
// carry it.
type IndexManager interface {
	AddIndex(name string, kind IndexKind) (IndexInfo, error)
	CreateIndex(def IndexDefinition) (IndexInfo, error)
	DropIndex(name string) error
	BackfillIndex(name string, maxChunks int) (IndexInfo, error)
	ReclaimIndexes(maxChunks int) (rebuilt int, done bool)
}

var _ IndexManager = (*Collection)(nil)
