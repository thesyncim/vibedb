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

// TailBeforeWitness is the compact exact source-side evidence needed after a
// split cut. Point is interpreted only by the placement program already bound
// into the tail cursor. DocumentBytes retains exact logical byte accounting
// without retaining the potentially large source document.
type TailBeforeWitness struct {
	Present       bool
	Point         distribution.KeyspacePoint
	DocumentBytes uint32
	Digest        [sha256.Size]byte
}

// TailTransition is one exact final key transition observed while applying a
// source entry. Before is accepted only as borrowed construction input; a
// durable captured transition instead carries BeforeWitness. After remains a
// valid JSON document because child puts need its exact bytes. Input keys must
// be strictly byte ordered and distinct.
type TailTransition struct {
	Key           []byte
	Before        []byte
	BeforeWitness TailBeforeWitness
	After         []byte
}

// TailEntry is one consecutive source publication after the artifact cut.
// Empty Transitions are required for no-op, rejected, and configuration
// entries so every child can advance through the exact same applied sequence.
type TailEntry struct {
	Applied               uint64
	Term                  uint64
	BeforeOwnershipEpoch  uint64
	AfterOwnershipEpoch   uint64
	BeforeRoutingVersion  uint64
	AfterRoutingVersion   uint64
	BeforeRouteGeneration uint64
	AfterRouteGeneration  uint64
	PreviousEntryDigest   [sha256.Size]byte
	EntryDigest           [sha256.Size]byte
	BeforeDataChainDigest [sha256.Size]byte
	AfterDataChainDigest  [sha256.Size]byte
	Transitions           []TailTransition
}

// TailSourceCoordinates are the mutable source-serving fences represented by
// a translated prefix.
type TailSourceCoordinates struct {
	OwnershipEpoch  uint64
	RoutingVersion  uint64
	RouteGeneration uint64
}

// TailCursor is a constant-size, exact translated source prefix. Its fields
// are private so callers cannot manufacture progress. Persisted cursor framing
// is deliberately left to the destination staging layer.
type TailCursor struct {
	planDigest       [sha256.Size]byte
	placementDigest  [sha256.Size]byte
	dataChainDigest  [sha256.Size]byte
	baseDigest       [sha256.Size]byte
	entryDigest      [sha256.Size]byte
	childBaseDigests [autosplit.MaxSplitChildren][sha256.Size]byte
	applied          uint64
	term             uint64
	ownershipEpoch   uint64
	routingVersion   uint64
	routeGeneration  uint64
	sealed           bool
}

// SourceCut returns the exact source publication represented by c.
func (c TailCursor) SourceCut() ChildArtifactSourceCut {
	return ChildArtifactSourceCut{
		DataChainDigest: c.dataChainDigest, BaseDigest: c.baseDigest,
		EntryDigest: c.entryDigest, Applied: c.applied, Term: c.term,
		RouteGeneration: c.routeGeneration,
	}
}

// PlanDigest returns the split-plan identity bound to c.
func (c TailCursor) PlanDigest() [sha256.Size]byte { return c.planDigest }

// PlacementDigest returns the placement-program identity bound to c.
func (c TailCursor) PlacementDigest() [sha256.Size]byte { return c.placementDigest }

// SourceCoordinates returns the mutable source fences represented by c.
func (c TailCursor) SourceCoordinates() TailSourceCoordinates {
	return TailSourceCoordinates{
		OwnershipEpoch: c.ownershipEpoch, RoutingVersion: c.routingVersion,
		RouteGeneration: c.routeGeneration,
	}
}

// Sealed reports whether c ends at the terminal ownership fence.
func (c TailCursor) Sealed() bool { return c.sealed }

// ValidateTailCursor checks that c is an authentic cursor for this exact
// partitioner. It performs no I/O and grants no source or serving authority.
func (p *Partitioner) ValidateTailCursor(c TailCursor) error {
	if p == nil || !p.validTailCursor(c) {
		return ErrTailCursor
	}
	return nil
}

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
	Child                 uint8
	PlanDigest            [sha256.Size]byte
	PlacementDigest       [sha256.Size]byte
	TranslationDigest     [sha256.Size]byte
	Digest                [sha256.Size]byte
	PreviousEntryDigest   [sha256.Size]byte
	EntryDigest           [sha256.Size]byte
	BeforeDataChainDigest [sha256.Size]byte
	AfterDataChainDigest  [sha256.Size]byte
	SourceBaseDigest      [sha256.Size]byte
	ChildBaseDigest       [sha256.Size]byte
	Applied               uint64
	Term                  uint64
	BeforeOwnershipEpoch  uint64
	AfterOwnershipEpoch   uint64
	BeforeRoutingVersion  uint64
	AfterRoutingVersion   uint64
	BeforeRouteGeneration uint64
	AfterRouteGeneration  uint64
	TransitionCount       uint64
	Operations            uint64
	Bytes                 uint64
	transitions           []TailTransition
	routes                []tailRoute
	translated            bool
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

// TailBatchVerifyWorkspace retains one SHA-256 state. Reuse it serially when
// a destination verifies translated batches before durable apply.
type TailBatchVerifyWorkspace struct {
	hasher   hash.Hash
	digest   [sha256.Size]byte
	identity [sha256.Size]byte
	fixed    [320]byte
	size     [8]byte
}

// TailWorkspace owns the compact two-byte route table, reusable vibejson
// index, and four reusable SHA-256 states. Reuse it serially.
type TailWorkspace struct {
	routes    []tailRoute
	document  distribution.DocumentPointWorkspace
	hashers   [autosplit.MaxSplitChildren + 1]hash.Hash
	digests   [autosplit.MaxSplitChildren + 1][sha256.Size]byte
	fixed     [320]byte
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
		DataChainDigest: set.Partition.SourceDigest,
		BaseDigest:      set.Partition.SourceBase,
		EntryDigest:     set.Partition.SourceEntry,
		Applied:         set.Partition.SourceApplied, Term: set.Partition.SourceTerm,
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
		dataChainDigest: cut.DataChainDigest, baseDigest: cut.BaseDigest,
		entryDigest: cut.EntryDigest, applied: cut.Applied, term: cut.Term,
		ownershipEpoch:  uint64(p.source.OwnershipEpoch),
		routingVersion:  uint64(p.source.RoutingVersion),
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
		!p.validTailCursor(cursor) || !p.validTailEntry(cursor, entry) {
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
		before, err := p.tailBeforeWitness(transition, &workspace.document)
		if err != nil {
			return cursor, TailStats{}, err
		}
		if before.Present {
			child := p.childFor(before.Point)
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
		workspace.hashTransition(transition, before, route)
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
			PlacementDigest:       p.program.Digest(),
			TranslationDigest:     stats.TranslationDigest,
			Digest:                stats.ChildDigests[child],
			PreviousEntryDigest:   entry.PreviousEntryDigest,
			EntryDigest:           entry.EntryDigest,
			BeforeDataChainDigest: entry.BeforeDataChainDigest,
			AfterDataChainDigest:  entry.AfterDataChainDigest,
			SourceBaseDigest:      cursor.baseDigest,
			ChildBaseDigest:       cursor.childBaseDigests[child],
			Applied:               entry.Applied, Term: entry.Term,
			BeforeOwnershipEpoch:  entry.BeforeOwnershipEpoch,
			AfterOwnershipEpoch:   entry.AfterOwnershipEpoch,
			BeforeRoutingVersion:  entry.BeforeRoutingVersion,
			AfterRoutingVersion:   entry.AfterRoutingVersion,
			BeforeRouteGeneration: entry.BeforeRouteGeneration,
			AfterRouteGeneration:  entry.AfterRouteGeneration,
			TransitionCount:       uint64(len(entry.Transitions)),
			Operations:            stats.Operations[child], Bytes: stats.Bytes[child],
			transitions: entry.Transitions, routes: workspace.routes, translated: true,
		}
		if err := sinks[child](batch); err != nil {
			return cursor, TailStats{}, err
		}
	}
	next := cursor
	next.dataChainDigest = entry.AfterDataChainDigest
	next.entryDigest = entry.EntryDigest
	next.applied = entry.Applied
	next.term = entry.Term
	next.ownershipEpoch = entry.AfterOwnershipEpoch
	next.routingVersion = entry.AfterRoutingVersion
	next.routeGeneration = entry.AfterRouteGeneration
	next.sealed = tailEntrySeals(entry)
	return next, *stats, nil
}

// VerifyTailBatch recomputes one child batch digest and validates its ordered
// borrowed operation stream. It does not grant serving or cutover authority.
func (p *Partitioner) VerifyTailBatch(
	batch TailBatch,
	workspace *TailBatchVerifyWorkspace,
) error {
	if p == nil || workspace == nil || !batch.translated ||
		uint64(len(batch.transitions)) != batch.TransitionCount ||
		len(batch.routes) != len(batch.transitions) ||
		int(batch.Child) >= int(p.childCount) ||
		batch.PlanDigest != p.digest || batch.PlacementDigest != p.program.Digest() ||
		batch.TranslationDigest == ([sha256.Size]byte{}) ||
		batch.SourceBaseDigest == ([sha256.Size]byte{}) ||
		batch.ChildBaseDigest == ([sha256.Size]byte{}) ||
		batch.PreviousEntryDigest == ([sha256.Size]byte{}) ||
		batch.EntryDigest == ([sha256.Size]byte{}) ||
		batch.BeforeDataChainDigest == ([sha256.Size]byte{}) ||
		batch.AfterDataChainDigest == ([sha256.Size]byte{}) ||
		batch.Applied == 0 || batch.Applied == math.MaxUint64 ||
		batch.Term == 0 || batch.Term == math.MaxUint64 ||
		!validTailBatchCoordinates(batch) ||
		batch.TransitionCount > replication.MaxMutations ||
		batch.Operations > batch.TransitionCount {
		return ErrTailEntry
	}
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	h := workspace.hasher
	h.Reset()
	fixed := appendTailFixed(
		workspace.fixed[:0], batch.PlanDigest, batch.PlacementDigest,
		batch.Applied, batch.Term, batch.TransitionCount,
		batch.beforeCoordinates(), batch.afterCoordinates(),
		batch.PreviousEntryDigest, batch.EntryDigest,
		batch.BeforeDataChainDigest, batch.AfterDataChainDigest, batch.SourceBaseDigest,
	)
	_, _ = h.Write(tailChildDomain)
	_, _ = h.Write(fixed)
	workspace.size[0] = batch.Child
	_, _ = h.Write(workspace.size[:1])
	workspace.identity = batch.ChildBaseDigest
	_, _ = h.Write(workspace.identity[:])
	iterator := TailOperationIterator{
		child: batch.Child, transitions: batch.transitions, routes: batch.routes,
	}
	var previousKey []byte
	var operations, bytesCount uint64
	for iterator.Next() {
		operation := iterator.Operation()
		if len(operation.Key) == 0 || len(operation.Key) > replication.MaxMutationKeyBytes ||
			previousKey != nil && bytes.Compare(previousKey, operation.Key) >= 0 {
			return ErrTailEntry
		}
		switch operation.Kind {
		case replication.MutationPut:
			if len(operation.Value) == 0 || len(operation.Value) > replication.MaxMutationValueBytes {
				return ErrTailEntry
			}
		case replication.MutationDelete:
			if operation.Value != nil {
				return ErrTailEntry
			}
		default:
			return ErrTailEntry
		}
		if operations == math.MaxUint64 ||
			bytesCount > math.MaxUint64-uint64(len(operation.Key))-uint64(len(operation.Value)) {
			return ErrTailEntry
		}
		operations++
		bytesCount += uint64(len(operation.Key) + len(operation.Value))
		workspace.fixed[0] = byte(operation.Kind)
		_, _ = h.Write(workspace.fixed[:1])
		hashTailFrame(h, &workspace.size, operation.Key)
		hashTailFrame(h, &workspace.size, operation.Value)
		previousKey = operation.Key
	}
	if operations != batch.Operations || bytesCount != batch.Bytes {
		return ErrTailEntry
	}
	workspace.identity = batch.TranslationDigest
	_, _ = h.Write(workspace.identity[:])
	_ = h.Sum(workspace.digest[:0])
	if workspace.digest != batch.Digest {
		return ErrTailEntry
	}
	return nil
}

func (p *Partitioner) validTailCursor(cursor TailCursor) bool {
	if cursor.planDigest != p.digest || cursor.placementDigest != p.program.Digest() ||
		cursor.dataChainDigest == ([sha256.Size]byte{}) ||
		cursor.baseDigest == ([sha256.Size]byte{}) ||
		cursor.entryDigest == ([sha256.Size]byte{}) || cursor.applied == 0 ||
		cursor.term == 0 || cursor.ownershipEpoch == 0 ||
		cursor.routingVersion == 0 || cursor.routeGeneration == 0 {
		return false
	}
	for child := 0; child < int(p.childCount); child++ {
		if cursor.childBaseDigests[child] == ([sha256.Size]byte{}) {
			return false
		}
	}
	return true
}

func (p *Partitioner) validTailEntry(cursor TailCursor, entry TailEntry) bool {
	if cursor.sealed || cursor.applied == math.MaxUint64 ||
		entry.Applied != cursor.applied+1 ||
		entry.Applied == math.MaxUint64 || entry.Term < cursor.term ||
		entry.Term == 0 || entry.Term == math.MaxUint64 ||
		entry.PreviousEntryDigest != cursor.entryDigest ||
		entry.EntryDigest == ([sha256.Size]byte{}) ||
		entry.BeforeDataChainDigest != cursor.dataChainDigest ||
		entry.AfterDataChainDigest == ([sha256.Size]byte{}) {
		return false
	}
	before, after := entry.beforeCoordinates(), entry.afterCoordinates()
	if before != cursor.coordinates() {
		return false
	}
	if after == before {
		return true
	}
	retained := p.children[p.retained]
	return len(entry.Transitions) == 0 &&
		entry.AfterDataChainDigest == entry.BeforeDataChainDigest &&
		before.incremented() == after &&
		after.OwnershipEpoch == uint64(retained.OwnershipEpoch) &&
		after.RoutingVersion == uint64(p.target)
}

func (c TailCursor) coordinates() TailSourceCoordinates {
	return TailSourceCoordinates{
		OwnershipEpoch: c.ownershipEpoch, RoutingVersion: c.routingVersion,
		RouteGeneration: c.routeGeneration,
	}
}

func (e TailEntry) beforeCoordinates() TailSourceCoordinates {
	return TailSourceCoordinates{
		OwnershipEpoch: e.BeforeOwnershipEpoch, RoutingVersion: e.BeforeRoutingVersion,
		RouteGeneration: e.BeforeRouteGeneration,
	}
}

func (e TailEntry) afterCoordinates() TailSourceCoordinates {
	return TailSourceCoordinates{
		OwnershipEpoch: e.AfterOwnershipEpoch, RoutingVersion: e.AfterRoutingVersion,
		RouteGeneration: e.AfterRouteGeneration,
	}
}

func (b TailBatch) beforeCoordinates() TailSourceCoordinates {
	return TailSourceCoordinates{
		OwnershipEpoch: b.BeforeOwnershipEpoch, RoutingVersion: b.BeforeRoutingVersion,
		RouteGeneration: b.BeforeRouteGeneration,
	}
}

func (b TailBatch) afterCoordinates() TailSourceCoordinates {
	return TailSourceCoordinates{
		OwnershipEpoch: b.AfterOwnershipEpoch, RoutingVersion: b.AfterRoutingVersion,
		RouteGeneration: b.AfterRouteGeneration,
	}
}

func (c TailSourceCoordinates) incremented() TailSourceCoordinates {
	if c.OwnershipEpoch == math.MaxUint64 || c.RoutingVersion == math.MaxUint64 ||
		c.RouteGeneration == math.MaxUint64 {
		return TailSourceCoordinates{}
	}
	return TailSourceCoordinates{
		OwnershipEpoch: c.OwnershipEpoch + 1, RoutingVersion: c.RoutingVersion + 1,
		RouteGeneration: c.RouteGeneration + 1,
	}
}

func tailEntrySeals(entry TailEntry) bool {
	return entry.beforeCoordinates() != entry.afterCoordinates()
}

func validTailBatchCoordinates(batch TailBatch) bool {
	before, after := batch.beforeCoordinates(), batch.afterCoordinates()
	return before.OwnershipEpoch != 0 && before.RoutingVersion != 0 &&
		before.RouteGeneration != 0 &&
		(after == before || before.incremented() == after && batch.TransitionCount == 0 &&
			batch.AfterDataChainDigest == batch.BeforeDataChainDigest)
}

func validTailTransition(transition *TailTransition, previousKey []byte) bool {
	if transition == nil || len(transition.Key) == 0 ||
		len(transition.Key) > replication.MaxMutationKeyBytes ||
		transition.Before == nil && !transition.BeforeWitness.Present && transition.After == nil ||
		transition.Before != nil && transition.BeforeWitness.Present ||
		len(transition.Before) > replication.MaxMutationValueBytes ||
		len(transition.After) > replication.MaxMutationValueBytes ||
		previousKey != nil && bytes.Compare(previousKey, transition.Key) >= 0 {
		return false
	}
	if transition.Before != nil && len(transition.Before) == 0 ||
		transition.After != nil && len(transition.After) == 0 ||
		transition.BeforeWitness.Present && (transition.BeforeWitness.DocumentBytes == 0 ||
			transition.BeforeWitness.DocumentBytes > replication.MaxMutationValueBytes ||
			transition.BeforeWitness.Digest == ([sha256.Size]byte{})) ||
		!transition.BeforeWitness.Present &&
			(transition.BeforeWitness.DocumentBytes != 0 ||
				transition.BeforeWitness.Point != (distribution.KeyspacePoint{}) ||
				transition.BeforeWitness.Digest != ([sha256.Size]byte{})) {
		return false
	}
	return true
}

func (p *Partitioner) tailBeforeWitness(
	transition *TailTransition,
	workspace *distribution.DocumentPointWorkspace,
) (TailBeforeWitness, error) {
	if transition.Before == nil {
		if transition.BeforeWitness.Present && p.childFor(transition.BeforeWitness.Point) < 0 {
			return TailBeforeWitness{}, fmt.Errorf("%w: before outside source", ErrTailEntry)
		}
		return transition.BeforeWitness, nil
	}
	point, err := p.program.Point(transition.Before, workspace)
	if err != nil {
		return TailBeforeWitness{}, fmt.Errorf("%w: before placement", ErrTailEntry)
	}
	digest := sha256.Sum256(transition.Before)
	if digest == ([sha256.Size]byte{}) {
		return TailBeforeWitness{}, ErrTailEntry
	}
	return TailBeforeWitness{
		Present: true, Point: point, DocumentBytes: uint32(len(transition.Before)),
		Digest: digest,
	}, nil
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
	placement := p.program.Digest()
	fixed := appendTailFixed(
		w.fixed[:0], p.digest, placement,
		entry.Applied, entry.Term, uint64(len(entry.Transitions)),
		entry.beforeCoordinates(), entry.afterCoordinates(),
		entry.PreviousEntryDigest, entry.EntryDigest,
		entry.BeforeDataChainDigest, entry.AfterDataChainDigest, cursor.baseDigest,
	)
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

func (w *TailWorkspace) hashTransition(
	transition *TailTransition,
	before TailBeforeWitness,
	route tailRoute,
) {
	h := w.hashers[0]
	w.hashFrame(h, transition.Key)
	w.hashBeforeWitness(h, before)
	w.hashOptionalFrame(h, transition.After)
	w.fixed[0], w.fixed[1] = route.before, route.after
	_, _ = h.Write(w.fixed[:2])
}

func (w *TailWorkspace) hashBeforeWitness(h hash.Hash, before TailBeforeWitness) {
	if !before.Present {
		w.fixed[0] = 0
		_, _ = h.Write(w.fixed[:1])
		return
	}
	w.fixed[0] = 1
	copy(w.fixed[1:9], before.Point[:])
	binary.LittleEndian.PutUint32(w.fixed[9:13], before.DocumentBytes)
	copy(w.fixed[13:45], before.Digest[:])
	_, _ = h.Write(w.fixed[:45])
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
	hashTailFrame(h, &w.size, value)
}

func appendTailFixed(
	dst []byte,
	plan, placement [sha256.Size]byte,
	applied, term, transitions uint64,
	before, after TailSourceCoordinates,
	previousEntry, entry, beforeLogical, afterLogical, sourceBase [sha256.Size]byte,
) []byte {
	dst = append(dst, plan[:]...)
	dst = append(dst, placement[:]...)
	dst = binary.LittleEndian.AppendUint64(dst, applied)
	dst = binary.LittleEndian.AppendUint64(dst, term)
	dst = binary.LittleEndian.AppendUint64(dst, transitions)
	for _, coordinates := range [2]TailSourceCoordinates{before, after} {
		dst = binary.LittleEndian.AppendUint64(dst, coordinates.OwnershipEpoch)
		dst = binary.LittleEndian.AppendUint64(dst, coordinates.RoutingVersion)
		dst = binary.LittleEndian.AppendUint64(dst, coordinates.RouteGeneration)
	}
	dst = append(dst, previousEntry[:]...)
	dst = append(dst, entry[:]...)
	dst = append(dst, beforeLogical[:]...)
	dst = append(dst, afterLogical[:]...)
	dst = append(dst, sourceBase[:]...)
	return dst
}

func hashTailFrame(h hash.Hash, size *[8]byte, value []byte) {
	binary.LittleEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
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
