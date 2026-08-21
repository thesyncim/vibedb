package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	ErrTailCursor = errors.New("rangesplit: invalid source-tail cursor")
	ErrTailEntry  = errors.New("rangesplit: invalid source-tail entry")
)

var (
	tailTranslationDomain  = []byte("vibedb/range-split/tail-translation\x00")
	tailChildDomain        = []byte("vibedb/range-split/tail-child\x00")
	tailRetainedBaseDomain = []byte("vibedb/range-split/retained-base\x00")
)

const tailChildMissing = uint8(math.MaxUint8)

// TailTransition is one exact final key transition observed while applying a
// source entry. Before and After are canonical document bytes; nil means the
// key was absent on that side. Both cannot be nil. Input keys must be strictly
// byte ordered and distinct so every derived child batch remains ordered.
type TailTransition struct {
	Key    []byte
	Before []byte
	After  []byte
}

// TailEntry is one consecutive source publication after the artifact cut.
// Empty Transitions are required for no-op, rejected, and configuration
// entries so every child can advance through the exact same applied sequence.
type TailEntry struct {
	Applied             uint64
	Term                uint64
	RouteGeneration     uint64
	PreviousEntryDigest [sha256.Size]byte
	EntryDigest         [sha256.Size]byte
	BeforeLogicalDigest [sha256.Size]byte
	AfterLogicalDigest  [sha256.Size]byte
	Transitions         []TailTransition
}

// TailCursor is a constant-size, exact translated source prefix. Its fields
// are private so callers cannot manufacture progress. Persisted cursor framing
// is deliberately left to the destination staging layer.
type TailCursor struct {
	planDigest       [sha256.Size]byte
	placementDigest  [sha256.Size]byte
	logicalDigest    [sha256.Size]byte
	baseDigest       [sha256.Size]byte
	entryDigest      [sha256.Size]byte
	childBaseDigests [autosplit.MaxSplitChildren][sha256.Size]byte
	applied          uint64
	term             uint64
	routeGeneration  uint64
}

// SourceCut returns the exact source publication represented by c.
func (c TailCursor) SourceCut() ChildArtifactSourceCut {
	return ChildArtifactSourceCut{
		LogicalDigest: c.logicalDigest, BaseDigest: c.baseDigest,
		EntryDigest: c.entryDigest, Applied: c.applied, Term: c.term,
		RouteGeneration: c.routeGeneration,
	}
}

// PlanDigest returns the split-plan identity bound to c.
func (c TailCursor) PlanDigest() [sha256.Size]byte { return c.planDigest }

// PlacementDigest returns the placement-program identity bound to c.
func (c TailCursor) PlacementDigest() [sha256.Size]byte { return c.placementDigest }

// TailOperation is one borrowed child-local mutation. A moved row becomes a
// delete in its old child and a put in its new child.
type TailOperation struct {
	Kind  replication.MutationKind
	Key   []byte
	Value []byte
}

type tailRoute struct {
	before uint8
	after  uint8
}

// TailBatch is one atomic, idempotence-addressable child advance. Even an
// empty batch is semantically required. The sink may iterate it only during
// the callback and must durably order the operations and Digest together.
type TailBatch struct {
	Child               uint8
	PlanDigest          [sha256.Size]byte
	PlacementDigest     [sha256.Size]byte
	TranslationDigest   [sha256.Size]byte
	Digest              [sha256.Size]byte
	PreviousEntryDigest [sha256.Size]byte
	EntryDigest         [sha256.Size]byte
	BeforeLogicalDigest [sha256.Size]byte
	AfterLogicalDigest  [sha256.Size]byte
	SourceBaseDigest    [sha256.Size]byte
	ChildBaseDigest     [sha256.Size]byte
	Applied             uint64
	Term                uint64
	RouteGeneration     uint64
	Operations          uint64
	Bytes               uint64
	transitions         []TailTransition
	routes              []tailRoute
}

// Iterator returns a zero-allocation forward iterator over child-local
// operations in strict key order.
func (b TailBatch) Iterator() TailOperationIterator {
	return TailOperationIterator{
		child: b.Child, transitions: b.transitions, routes: b.routes,
	}
}

// TailOperationIterator derives at most one operation per child per key.
type TailOperationIterator struct {
	child       uint8
	transitions []TailTransition
	routes      []tailRoute
	next        int
	current     TailOperation
}

// Next advances to the next operation.
func (i *TailOperationIterator) Next() bool {
	if i == nil {
		return false
	}
	for i.next < len(i.transitions) {
		ordinal := i.next
		i.next++
		transition, route := &i.transitions[ordinal], i.routes[ordinal]
		switch {
		case route.after == i.child:
			i.current = TailOperation{
				Kind: replication.MutationPut, Key: transition.Key, Value: transition.After,
			}
			return true
		case route.before == i.child:
			i.current = TailOperation{
				Kind: replication.MutationDelete, Key: transition.Key,
			}
			return true
		}
	}
	return false
}

// Operation returns the current borrowed operation.
func (i *TailOperationIterator) Operation() TailOperation {
	if i == nil {
		return TailOperation{}
	}
	return i.current
}

// TailSink atomically accepts or idempotently recognizes one exact child
// batch. A failure leaves the returned cursor unchanged, so retrying the same
// entry is safe when sinks use batch Digest as their idempotence key.
type TailSink func(TailBatch) error

// TailStats is fixed-size translation evidence for one source entry.
type TailStats struct {
	TranslationDigest [sha256.Size]byte
	ChildDigests      [autosplit.MaxSplitChildren][sha256.Size]byte
	Operations        [autosplit.MaxSplitChildren]uint64
	Bytes             [autosplit.MaxSplitChildren]uint64
}

// TailWorkspace owns the compact two-byte route table, reusable vibejson
// index, and four reusable SHA-256 states. Reuse it serially.
type TailWorkspace struct {
	routes    []tailRoute
	document  distribution.DocumentPointWorkspace
	hashers   [autosplit.MaxSplitChildren + 1]hash.Hash
	digests   [autosplit.MaxSplitChildren + 1][sha256.Size]byte
	fixed     [256]byte
	size      [8]byte
	childBase [sha256.Size]byte
	stats     TailStats
}

// InitialTailCursor proves that every non-retained child artifact belongs to
// one complete partition scan and returns the exact source prefix to follow.
func (p *Partitioner) InitialTailCursor(set ChildArtifactSet) (TailCursor, error) {
	if p == nil || set.Partition.PlanDigest != p.digest ||
		set.Partition.SourceDigest == ([sha256.Size]byte{}) ||
		set.Partition.SourceBase == ([sha256.Size]byte{}) ||
		set.Partition.SourceEntry == ([sha256.Size]byte{}) ||
		set.Partition.SourceApplied == 0 || set.Partition.SourceTerm == 0 ||
		set.Partition.RouteGeneration == 0 {
		return TailCursor{}, ErrTailCursor
	}
	placement := p.program.Digest()
	cut := ChildArtifactSourceCut{
		LogicalDigest: set.Partition.SourceDigest,
		BaseDigest:    set.Partition.SourceBase,
		EntryDigest:   set.Partition.SourceEntry,
		Applied:       set.Partition.SourceApplied, Term: set.Partition.SourceTerm,
		RouteGeneration: set.Partition.RouteGeneration,
	}
	var childBases [autosplit.MaxSplitChildren][sha256.Size]byte
	for child := 0; child < int(p.childCount); child++ {
		manifest := set.Children[child]
		if child == int(p.retained) {
			if manifest != (ChildArtifactManifest{}) {
				return TailCursor{}, ErrTailCursor
			}
			childBases[child] = p.retainedTailBaseDigest(set.Partition)
			continue
		}
		if !manifest.Present || manifest.Child != uint8(child) ||
			manifest.PlanDigest != p.digest || manifest.PlacementDigest != placement ||
			manifest.Source != cut || manifest.TargetRoutingVersion != p.target ||
			manifest.Descriptor != p.artifactDescriptor(uint8(child)) ||
			manifest.Rows != set.Partition.Rows[child] ||
			manifest.RowBytes != set.Partition.Bytes[child] ||
			manifest.HeaderDigest == ([sha256.Size]byte{}) ||
			manifest.LastChunkDigest == ([sha256.Size]byte{}) ||
			manifest.Digest == ([sha256.Size]byte{}) || manifest.EncodedBytes == 0 {
			return TailCursor{}, ErrTailCursor
		}
		childBases[child] = manifest.Digest
	}
	for child := int(p.childCount); child < autosplit.MaxSplitChildren; child++ {
		if set.Children[child] != (ChildArtifactManifest{}) {
			return TailCursor{}, ErrTailCursor
		}
	}
	return TailCursor{
		planDigest: p.digest, placementDigest: placement,
		logicalDigest: cut.LogicalDigest, baseDigest: cut.BaseDigest,
		entryDigest: cut.EntryDigest, applied: cut.Applied, term: cut.Term,
		routeGeneration: cut.RouteGeneration, childBaseDigests: childBases,
	}, nil
}

// TranslateTailEntry parses each before/after document at most once, derives
// fixed child-local batches, invokes every child sink (including empty
// batches), and advances only after all sinks succeed.
func (p *Partitioner) TranslateTailEntry(
	cursor TailCursor,
	entry TailEntry,
	sinks []TailSink,
	workspace *TailWorkspace,
) (TailCursor, TailStats, error) {
	if p == nil || workspace == nil || len(sinks) != int(p.childCount) ||
		!p.validTailCursor(cursor) || !validTailEntry(cursor, entry) {
		return cursor, TailStats{}, ErrTailEntry
	}
	for _, sink := range sinks {
		if sink == nil {
			return cursor, TailStats{}, ErrTailEntry
		}
	}
	if len(entry.Transitions) > replication.MaxMutations {
		return cursor, TailStats{}, ErrTailEntry
	}
	if cap(workspace.routes) < len(entry.Transitions) {
		workspace.routes = make([]tailRoute, len(entry.Transitions))
	} else {
		workspace.routes = workspace.routes[:len(entry.Transitions)]
	}
	workspace.prepareTailHashes(p, cursor, entry)
	workspace.stats = TailStats{}
	stats := &workspace.stats
	var previousKey []byte
	for ordinal := range entry.Transitions {
		transition := &entry.Transitions[ordinal]
		if !validTailTransition(transition, previousKey) {
			return cursor, TailStats{}, ErrTailEntry
		}
		route := tailRoute{before: tailChildMissing, after: tailChildMissing}
		if transition.Before != nil {
			point, err := p.program.Point(transition.Before, &workspace.document)
			if err != nil {
				return cursor, TailStats{}, fmt.Errorf("%w: before placement", ErrTailEntry)
			}
			child := p.childFor(point)
			if child < 0 {
				return cursor, TailStats{}, fmt.Errorf("%w: before outside source", ErrTailEntry)
			}
			route.before = uint8(child)
		}
		if transition.After != nil {
			if transition.Before != nil && bytes.Equal(transition.Before, transition.After) {
				route.after = route.before
			} else {
				point, err := p.program.Point(transition.After, &workspace.document)
				if err != nil {
					return cursor, TailStats{}, fmt.Errorf("%w: after placement", ErrTailEntry)
				}
				child := p.childFor(point)
				if child < 0 {
					return cursor, TailStats{}, fmt.Errorf("%w: after outside source", ErrTailEntry)
				}
				route.after = uint8(child)
			}
		}
		workspace.routes[ordinal] = route
		workspace.hashTransition(transition, route)
		if route.after != tailChildMissing {
			child := int(route.after)
			if !addTailOperation(stats, child, transition.Key, transition.After) {
				return cursor, TailStats{}, ErrTailEntry
			}
			workspace.hashTailOperation(child, replication.MutationPut, transition.Key, transition.After)
		}
		if route.before != tailChildMissing && route.before != route.after {
			child := int(route.before)
			if !addTailOperation(stats, child, transition.Key, nil) {
				return cursor, TailStats{}, ErrTailEntry
			}
			workspace.hashTailOperation(child, replication.MutationDelete, transition.Key, nil)
		}
		previousKey = transition.Key
	}
	workspace.finishTailHashes(p.childCount, stats)
	for child := 0; child < int(p.childCount); child++ {
		batch := TailBatch{
			Child: uint8(child), PlanDigest: p.digest,
			PlacementDigest:     p.program.Digest(),
			TranslationDigest:   stats.TranslationDigest,
			Digest:              stats.ChildDigests[child],
			PreviousEntryDigest: entry.PreviousEntryDigest,
			EntryDigest:         entry.EntryDigest,
			BeforeLogicalDigest: entry.BeforeLogicalDigest,
			AfterLogicalDigest:  entry.AfterLogicalDigest,
			SourceBaseDigest:    cursor.baseDigest,
			ChildBaseDigest:     cursor.childBaseDigests[child],
			Applied:             entry.Applied, Term: entry.Term,
			RouteGeneration: entry.RouteGeneration,
			Operations:      stats.Operations[child], Bytes: stats.Bytes[child],
			transitions: entry.Transitions, routes: workspace.routes,
		}
		if err := sinks[child](batch); err != nil {
			return cursor, TailStats{}, err
		}
	}
	next := cursor
	next.logicalDigest = entry.AfterLogicalDigest
	next.entryDigest = entry.EntryDigest
	next.applied = entry.Applied
	next.term = entry.Term
	return next, *stats, nil
}

func (p *Partitioner) validTailCursor(cursor TailCursor) bool {
	if cursor.planDigest != p.digest || cursor.placementDigest != p.program.Digest() ||
		cursor.logicalDigest == ([sha256.Size]byte{}) ||
		cursor.baseDigest == ([sha256.Size]byte{}) ||
		cursor.entryDigest == ([sha256.Size]byte{}) || cursor.applied == 0 ||
		cursor.term == 0 || cursor.routeGeneration == 0 {
		return false
	}
	for child := 0; child < int(p.childCount); child++ {
		if cursor.childBaseDigests[child] == ([sha256.Size]byte{}) {
			return false
		}
	}
	return true
}

func validTailEntry(cursor TailCursor, entry TailEntry) bool {
	return cursor.applied != math.MaxUint64 && entry.Applied == cursor.applied+1 &&
		entry.Applied != math.MaxUint64 && entry.Term >= cursor.term &&
		entry.Term != 0 && entry.Term != math.MaxUint64 &&
		entry.RouteGeneration == cursor.routeGeneration &&
		entry.PreviousEntryDigest == cursor.entryDigest &&
		entry.EntryDigest != ([sha256.Size]byte{}) &&
		entry.BeforeLogicalDigest == cursor.logicalDigest &&
		entry.AfterLogicalDigest != ([sha256.Size]byte{})
}

func validTailTransition(transition *TailTransition, previousKey []byte) bool {
	if transition == nil || len(transition.Key) == 0 ||
		len(transition.Key) > replication.MaxMutationKeyBytes ||
		transition.Before == nil && transition.After == nil ||
		len(transition.Before) > replication.MaxMutationValueBytes ||
		len(transition.After) > replication.MaxMutationValueBytes ||
		previousKey != nil && bytes.Compare(previousKey, transition.Key) >= 0 {
		return false
	}
	if transition.Before != nil && len(transition.Before) == 0 ||
		transition.After != nil && len(transition.After) == 0 {
		return false
	}
	return true
}

func addTailOperation(stats *TailStats, child int, key, value []byte) bool {
	if stats.Operations[child] == math.MaxUint64 {
		return false
	}
	bytes := uint64(len(key)) + uint64(len(value))
	if stats.Bytes[child] > math.MaxUint64-bytes {
		return false
	}
	stats.Operations[child]++
	stats.Bytes[child] += bytes
	return true
}

func (p *Partitioner) retainedTailBaseDigest(stats PartitionStats) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(tailRetainedBaseDomain)
	_, _ = h.Write(p.digest[:])
	placement := p.program.Digest()
	_, _ = h.Write(placement[:])
	_, _ = h.Write(stats.SourceDigest[:])
	_, _ = h.Write(stats.SourceBase[:])
	_, _ = h.Write(stats.SourceEntry[:])
	var fixed [41]byte
	binary.LittleEndian.PutUint64(fixed[0:8], stats.SourceApplied)
	binary.LittleEndian.PutUint64(fixed[8:16], stats.SourceTerm)
	binary.LittleEndian.PutUint64(fixed[16:24], stats.RouteGeneration)
	fixed[24] = p.retained
	binary.LittleEndian.PutUint64(fixed[25:33], stats.Rows[p.retained])
	binary.LittleEndian.PutUint64(fixed[33:41], stats.Bytes[p.retained])
	_, _ = h.Write(fixed[:])
	var digest [sha256.Size]byte
	_ = h.Sum(digest[:0])
	return digest
}

func (w *TailWorkspace) prepareTailHashes(
	p *Partitioner,
	cursor TailCursor,
	entry TailEntry,
) {
	for ordinal := 0; ordinal < int(p.childCount)+1; ordinal++ {
		if w.hashers[ordinal] == nil {
			w.hashers[ordinal] = sha256.New()
		}
		w.hashers[ordinal].Reset()
	}
	fixed := w.fixed[:0]
	fixed = append(fixed, p.digest[:]...)
	placement := p.program.Digest()
	fixed = append(fixed, placement[:]...)
	fixed = binary.LittleEndian.AppendUint64(fixed, entry.Applied)
	fixed = binary.LittleEndian.AppendUint64(fixed, entry.Term)
	fixed = binary.LittleEndian.AppendUint64(fixed, entry.RouteGeneration)
	fixed = binary.LittleEndian.AppendUint64(fixed, uint64(len(entry.Transitions)))
	fixed = append(fixed, entry.PreviousEntryDigest[:]...)
	fixed = append(fixed, entry.EntryDigest[:]...)
	fixed = append(fixed, entry.BeforeLogicalDigest[:]...)
	fixed = append(fixed, entry.AfterLogicalDigest[:]...)
	fixed = append(fixed, cursor.baseDigest[:]...)
	overall := w.hashers[0]
	_, _ = overall.Write(tailTranslationDomain)
	_, _ = overall.Write(fixed)
	for child := 0; child < int(p.childCount); child++ {
		h := w.hashers[child+1]
		_, _ = h.Write(tailChildDomain)
		_, _ = h.Write(fixed)
		w.size[0] = byte(child)
		_, _ = h.Write(w.size[:1])
		w.childBase = cursor.childBaseDigests[child]
		_, _ = h.Write(w.childBase[:])
	}
}

func (w *TailWorkspace) hashTransition(transition *TailTransition, route tailRoute) {
	h := w.hashers[0]
	w.hashFrame(h, transition.Key)
	w.hashOptionalFrame(h, transition.Before)
	w.hashOptionalFrame(h, transition.After)
	w.fixed[0], w.fixed[1] = route.before, route.after
	_, _ = h.Write(w.fixed[:2])
}

func (w *TailWorkspace) hashTailOperation(
	child int,
	kind replication.MutationKind,
	key, value []byte,
) {
	h := w.hashers[child+1]
	w.fixed[0] = byte(kind)
	_, _ = h.Write(w.fixed[:1])
	w.hashFrame(h, key)
	w.hashFrame(h, value)
}

func (w *TailWorkspace) hashOptionalFrame(h hash.Hash, value []byte) {
	if value == nil {
		w.fixed[0] = 0
		_, _ = h.Write(w.fixed[:1])
		return
	}
	w.fixed[0] = 1
	_, _ = h.Write(w.fixed[:1])
	w.hashFrame(h, value)
}

func (w *TailWorkspace) hashFrame(h hash.Hash, value []byte) {
	binary.LittleEndian.PutUint64(w.size[:], uint64(len(value)))
	_, _ = h.Write(w.size[:])
	_, _ = h.Write(value)
}

func (w *TailWorkspace) finishTailHashes(childCount uint8, stats *TailStats) {
	_ = w.hashers[0].Sum(w.digests[0][:0])
	stats.TranslationDigest = w.digests[0]
	for child := 0; child < int(childCount); child++ {
		_, _ = w.hashers[child+1].Write(stats.TranslationDigest[:])
		_ = w.hashers[child+1].Sum(w.digests[child+1][:0])
		stats.ChildDigests[child] = w.digests[child+1]
	}
}
