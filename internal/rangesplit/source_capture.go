package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash"
	"math"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

var ErrSourceCapture = errors.New("rangesplit: invalid source transition capture")

var (
	sourceCaptureHeaderDomain = []byte("vibedb/range-split/source-capture-header\x00")
	sourceCaptureEntryDomain  = []byte("vibedb/range-split/source-capture-entry\x00")
)

const (
	sourceCaptureHeaderKind   = uint64(1)
	sourceCaptureEntryKind    = uint64(2)
	sourceCaptureHeaderKey    = uint64(0)
	sourceCaptureArrayFields  = 15
	sourceCaptureHeaderFields = 14
)

type sourceCapturePublication struct {
	applied         uint64
	term            uint64
	ownershipEpoch  uint64
	routingVersion  uint64
	routeGeneration uint64
	entryDigest     [sha256.Size]byte
	logicalDigest   [sha256.Size]byte
}

// SourceCapture stores every exact before-and-after source transition in one
// private durable collection. The replicated state machine commits the record
// in the same multi-collection transaction as the source publication.
type SourceCapture struct {
	mu sync.Mutex

	partitioner *Partitioner
	placement   [sha256.Size]byte
	target      replicatedstate.TransitionCaptureTarget
	base        ChildArtifactSourceCut
	current     sourceCapturePublication
	encode      SourceCaptureWorkspace
	key         [8]byte
	begun       atomic.Bool
	head        atomic.Uint64
}

// SourceCaptureWorkspace owns all decode, base64, transition, and SHA state.
// Reuse it serially. Returned TailEntry slices remain valid until its next use.
type SourceCaptureWorkspace struct {
	raw         []byte
	entries     []vibejson.IndexEntry
	decoded     []byte
	transitions []TailTransition
	record      sourceCaptureEntry
	hasher      hash.Hash
	digest      [sha256.Size]byte
	fixed       [72]byte
	size        [8]byte
	key         [8]byte
}

// NewSourceCapture binds one private collection to an exact split plan.
func NewSourceCapture(
	partitioner *Partitioner,
	name string,
	collection *durable.Collection,
) (*SourceCapture, error) {
	if partitioner == nil || name == "" || collection == nil {
		return nil, ErrSourceCapture
	}
	return &SourceCapture{
		partitioner: partitioner,
		placement:   partitioner.program.Digest(),
		target:      replicatedstate.TransitionCaptureTarget{Name: name, Collection: collection},
	}, nil
}

// Target implements replicatedstate.TransitionCapture.
func (c *SourceCapture) Target() replicatedstate.TransitionCaptureTarget {
	if c == nil {
		return replicatedstate.TransitionCaptureTarget{}
	}
	return c.target
}

// MaxEncodedBytes returns a conservative exact-format bound. Before and after
// documents use raw base64 JSON strings so a valid maximum-depth user document
// remains valid inside the capture envelope.
func (c *SourceCapture) MaxEncodedBytes(
	bounds replicatedstate.TransitionCaptureBounds,
) (int, error) {
	if c == nil || bounds.Transitions > replicatedstate.MaxDistinctMutations {
		return 0, ErrSourceCapture
	}
	// Fixed metadata, punctuation, eight uint64 spellings, five digests, and
	// one plan-independent safety margin for the compact array grammar.
	total := uint64(640)
	add := func(value uint64) bool {
		if total > math.MaxUint64-value {
			return false
		}
		total += value
		return true
	}
	encoded := func(raw, values uint64) (uint64, bool) {
		if values > (math.MaxUint64-raw)/2 {
			return 0, false
		}
		adjusted := raw + 2*values
		if adjusted > math.MaxUint64/4*3 {
			return 0, false
		}
		return 4 * (adjusted / 3), true
	}
	keys, ok := encoded(bounds.KeyBytes, bounds.Transitions)
	if !ok || !add(keys) || !add(12*bounds.Transitions) {
		return 0, ErrSourceCapture
	}
	before, ok := encoded(bounds.BeforeBytes, bounds.Transitions)
	if !ok || !add(before) {
		return 0, ErrSourceCapture
	}
	after, ok := encoded(bounds.AfterBytes, bounds.Transitions)
	if !ok || !add(after) || total > uint64(math.MaxInt) {
		return 0, ErrSourceCapture
	}
	return int(total), nil
}

// Begin creates an exact capture base or recovers and verifies every retained
// record against the current replicated publication.
func (c *SourceCapture) Begin(state replicatedstate.State) error {
	if c == nil {
		return ErrSourceCapture
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.begun.Load() {
		return ErrSourceCapture
	}
	if c.target.Collection.Len() == 0 {
		if !c.partitioner.matchesSource(state) {
			return ErrSourceCapture
		}
		header, cut, err := c.appendHeader(c.encode.raw[:0], state, &c.encode)
		if err != nil || len(header) > c.target.Collection.MaxDocumentBytes() {
			return errors.Join(ErrSourceCapture, err)
		}
		binary.BigEndian.PutUint64(c.key[:], sourceCaptureHeaderKey)
		if err := c.target.Collection.Update(func(batch *durable.WriteBatch) error {
			return batch.Put(c.key[:], header)
		}); err != nil {
			return err
		}
		c.encode.raw = header
		c.base = cut
		c.current = publicationFromState(state)
		c.begun.Store(true)
		c.head.Store(state.Applied)
		return nil
	}
	if err := c.recover(state, &c.encode); err != nil {
		return err
	}
	c.begun.Store(true)
	c.head.Store(c.current.applied)
	return nil
}

// AppendTransition implements replicatedstate.TransitionCapture.
func (c *SourceCapture) AppendTransition(
	dst []byte,
	transition replicatedstate.CapturedTransition,
) ([]byte, error) {
	if c == nil {
		return dst, ErrSourceCapture
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.begun.Load() || !c.transitionFollowsCurrent(transition) {
		return dst, ErrSourceCapture
	}
	record, err := c.appendEntry(dst, transition, &c.encode)
	if err != nil {
		return dst, err
	}
	return record, nil
}

// Published advances the in-memory head after the atomic source transaction.
func (c *SourceCapture) Published(transition replicatedstate.CapturedTransition) error {
	if c == nil {
		return ErrSourceCapture
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.begun.Load() || !c.transitionFollowsCurrent(transition) {
		return ErrSourceCapture
	}
	c.current = publicationFromTransition(transition)
	c.head.Store(transition.Applied)
	return nil
}

// Head returns the latest source publication known to be atomically captured.
func (c *SourceCapture) Head() uint64 {
	if c == nil {
		return 0
	}
	return c.head.Load()
}

// NextTailEntry reads and verifies the exact next record after cursor. false
// means that the capture currently has no newer committed publication.
func (c *SourceCapture) NextTailEntry(
	cursor TailCursor,
	workspace *SourceCaptureWorkspace,
) (TailEntry, bool, error) {
	if c == nil || workspace == nil || !c.begun.Load() ||
		cursor.planDigest != c.partitioner.digest ||
		cursor.placementDigest != c.placement ||
		cursor.applied < c.base.Applied || cursor.applied == math.MaxUint64 {
		return TailEntry{}, false, ErrSourceCapture
	}
	next := cursor.applied + 1
	if next > c.head.Load() {
		return TailEntry{}, false, nil
	}
	binary.BigEndian.PutUint64(workspace.key[:], next)
	raw, found, err := c.target.Collection.AppendRaw(workspace.raw[:0], workspace.key[:])
	if err != nil || !found {
		return TailEntry{}, false, errors.Join(ErrSourceCapture, err)
	}
	workspace.raw = raw
	record, err := c.decodeEntry(raw, workspace)
	if err != nil || record.Applied != next ||
		record.PreviousEntryDigest != cursor.entryDigest ||
		record.BeforeLogicalDigest != cursor.logicalDigest ||
		record.BeforeRouteGeneration != cursor.routeGeneration {
		return TailEntry{}, false, errors.Join(ErrSourceCapture, err)
	}
	entry := TailEntry{
		Applied: record.Applied, Term: record.Term,
		RouteGeneration:     record.AfterRouteGeneration,
		PreviousEntryDigest: record.PreviousEntryDigest,
		EntryDigest:         record.EntryDigest,
		BeforeLogicalDigest: record.BeforeLogicalDigest,
		AfterLogicalDigest:  record.AfterLogicalDigest,
		Transitions:         record.Transitions,
	}
	return entry, true, nil
}

type sourceCaptureEntry struct {
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
	BeforeLogicalDigest   [sha256.Size]byte
	AfterLogicalDigest    [sha256.Size]byte
	Transitions           []TailTransition
	Digest                [sha256.Size]byte
}

func (c *SourceCapture) appendHeader(
	dst []byte,
	state replicatedstate.State,
	workspace *SourceCaptureWorkspace,
) ([]byte, ChildArtifactSourceCut, error) {
	cut := ChildArtifactSourceCut{
		LogicalDigest: state.LogicalDigest, BaseDigest: state.SnapshotBaseDigest,
		EntryDigest: state.LastEntryDigest, Applied: state.Applied,
		Term: state.LastTerm, RouteGeneration: state.Binding.RouteGeneration,
	}
	if cut.Applied == 0 || cut.Term == 0 || cut.LogicalDigest == ([32]byte{}) ||
		cut.BaseDigest == ([32]byte{}) || cut.EntryDigest == ([32]byte{}) {
		return dst, ChildArtifactSourceCut{}, ErrSourceCapture
	}
	digest := c.hashHeader(state, workspace)
	dst = append(dst, '[')
	dst = strconv.AppendUint(dst, sourceCaptureHeaderKind, 10)
	dst = append(dst, ',')
	dst = appendBase64String(dst, c.partitioner.digest[:])
	dst = append(dst, ',')
	dst = appendBase64String(dst, c.placement[:])
	dst = append(dst, ',')
	dst = appendBase64String(dst, []byte(c.partitioner.collection))
	for _, value := range [][32]byte{state.LogicalDigest, state.SnapshotBaseDigest, state.LastEntryDigest} {
		dst = append(dst, ',')
		dst = appendBase64String(dst, value[:])
	}
	for _, value := range []uint64{
		state.Applied, state.LastTerm, state.Binding.OwnershipEpoch,
		state.Binding.RoutingVersion, state.Binding.RouteGeneration,
		uint64(c.partitioner.target),
	} {
		dst = append(dst, ',')
		dst = strconv.AppendUint(dst, value, 10)
	}
	dst = append(dst, ',')
	dst = appendBase64String(dst, digest[:])
	dst = append(dst, ']')
	return dst, cut, nil
}

func (c *SourceCapture) appendEntry(
	dst []byte,
	transition replicatedstate.CapturedTransition,
	workspace *SourceCaptureWorkspace,
) ([]byte, error) {
	digest := c.hashTransition(transition, workspace)
	dst = append(dst, '[')
	values := [...]uint64{
		sourceCaptureEntryKind, transition.Applied, transition.Term,
		transition.BeforeOwnershipEpoch, transition.AfterOwnershipEpoch,
		transition.BeforeRoutingVersion, transition.AfterRoutingVersion,
		transition.BeforeRouteGeneration, transition.AfterRouteGeneration,
	}
	for index, value := range values {
		if index != 0 {
			dst = append(dst, ',')
		}
		dst = strconv.AppendUint(dst, value, 10)
	}
	for _, value := range [][32]byte{
		transition.PreviousEntryDigest, transition.EntryDigest,
		transition.BeforeLogicalDigest, transition.AfterLogicalDigest,
	} {
		dst = append(dst, ',')
		dst = appendBase64String(dst, value[:])
	}
	dst = append(dst, ',', '[')
	for index := 0; index < transition.MutationCount(); index++ {
		if index != 0 {
			dst = append(dst, ',')
		}
		mutation := transition.Mutation(index)
		dst = append(dst, '[')
		dst = appendBase64String(dst, mutation.Key)
		dst = append(dst, ',')
		dst = appendOptionalBase64String(dst, mutation.Before)
		dst = append(dst, ',')
		dst = appendOptionalBase64String(dst, mutation.After)
		dst = append(dst, ']')
	}
	dst = append(dst, ']', ',')
	dst = appendBase64String(dst, digest[:])
	dst = append(dst, ']')
	return dst, nil
}

func (c *SourceCapture) recover(
	state replicatedstate.State,
	workspace *SourceCaptureWorkspace,
) error {
	publication := sourceCapturePublication{}
	seenHeader := false
	err := c.target.Collection.RangeRawCurrent(func(key, value []byte) error {
		if len(key) != 8 {
			return ErrSourceCapture
		}
		applied := binary.BigEndian.Uint64(key)
		if !seenHeader {
			if applied != sourceCaptureHeaderKey {
				return ErrSourceCapture
			}
			cut, initial, err := c.decodeHeader(value, workspace)
			if err != nil {
				return err
			}
			c.base, publication, seenHeader = cut, initial, true
			return nil
		}
		if applied == 0 || publication.applied == math.MaxUint64 ||
			applied != publication.applied+1 {
			return ErrSourceCapture
		}
		record, err := c.decodeEntry(value, workspace)
		if err != nil || record.Applied != applied ||
			record.PreviousEntryDigest != publication.entryDigest ||
			record.BeforeLogicalDigest != publication.logicalDigest ||
			record.BeforeOwnershipEpoch != publication.ownershipEpoch ||
			record.BeforeRoutingVersion != publication.routingVersion ||
			record.BeforeRouteGeneration != publication.routeGeneration {
			return errors.Join(ErrSourceCapture, err)
		}
		publication = publicationFromEntry(record)
		return nil
	})
	if err != nil || !seenHeader || !publicationMatchesState(publication, state) {
		return errors.Join(ErrSourceCapture, err)
	}
	c.current = publication
	return nil
}

func (c *SourceCapture) decodeHeader(
	raw []byte,
	workspace *SourceCaptureWorkspace,
) (ChildArtifactSourceCut, sourceCapturePublication, error) {
	root, err := buildCaptureIndex(raw, workspace)
	if err != nil {
		return ChildArtifactSourceCut{}, sourceCapturePublication{}, err
	}
	count, ok := root.ArrayLen()
	if !ok || count != sourceCaptureHeaderFields || nodeUint(root, 0) != sourceCaptureHeaderKind {
		return ChildArtifactSourceCut{}, sourceCapturePublication{}, ErrSourceCapture
	}
	var plan, placement, logical, base, entry, digest [32]byte
	if !decodeNodeDigest(root, 1, &plan) || !decodeNodeDigest(root, 2, &placement) ||
		plan != c.partitioner.digest || placement != c.placement ||
		!decodeNodeDigest(root, 4, &logical) || !decodeNodeDigest(root, 5, &base) ||
		!decodeNodeDigest(root, 6, &entry) || nodeUint(root, 12) != uint64(c.partitioner.target) ||
		!decodeNodeDigest(root, 13, &digest) {
		return ChildArtifactSourceCut{}, sourceCapturePublication{}, ErrSourceCapture
	}
	workspace.decoded = workspace.decoded[:0]
	if cap(workspace.decoded) < len(raw) {
		workspace.decoded = make([]byte, 0, len(raw))
	}
	collection, collectionOK := decodeNodeBase64(root, 3, workspace)
	if !collectionOK || !bytes.Equal(collection, []byte(c.partitioner.collection)) {
		return ChildArtifactSourceCut{}, sourceCapturePublication{}, ErrSourceCapture
	}
	publication := sourceCapturePublication{
		applied: nodeUint(root, 7), term: nodeUint(root, 8),
		ownershipEpoch: nodeUint(root, 9), routingVersion: nodeUint(root, 10),
		routeGeneration: nodeUint(root, 11), entryDigest: entry, logicalDigest: logical,
	}
	state := replicatedstate.State{
		Applied: publication.applied, LastTerm: publication.term,
		LastEntryDigest: entry, LogicalDigest: logical, SnapshotBaseDigest: base,
		Binding: replicatedstate.Binding{
			OwnershipEpoch:  publication.ownershipEpoch,
			RoutingVersion:  publication.routingVersion,
			RouteGeneration: publication.routeGeneration,
		},
	}
	if digest != c.hashHeader(state, workspace) {
		return ChildArtifactSourceCut{}, sourceCapturePublication{}, ErrSourceCapture
	}
	cut := ChildArtifactSourceCut{
		LogicalDigest: logical, BaseDigest: base, EntryDigest: entry,
		Applied: publication.applied, Term: publication.term,
		RouteGeneration: publication.routeGeneration,
	}
	return cut, publication, nil
}

func (c *SourceCapture) decodeEntry(
	raw []byte,
	workspace *SourceCaptureWorkspace,
) (sourceCaptureEntry, error) {
	root, err := buildCaptureIndex(raw, workspace)
	if err != nil {
		return sourceCaptureEntry{}, err
	}
	count, ok := root.ArrayLen()
	if !ok || count != sourceCaptureArrayFields || nodeUint(root, 0) != sourceCaptureEntryKind {
		return sourceCaptureEntry{}, ErrSourceCapture
	}
	record := &workspace.record
	*record = sourceCaptureEntry{
		Applied: nodeUint(root, 1), Term: nodeUint(root, 2),
		BeforeOwnershipEpoch: nodeUint(root, 3), AfterOwnershipEpoch: nodeUint(root, 4),
		BeforeRoutingVersion: nodeUint(root, 5), AfterRoutingVersion: nodeUint(root, 6),
		BeforeRouteGeneration: nodeUint(root, 7), AfterRouteGeneration: nodeUint(root, 8),
	}
	if !decodeNodeDigest(root, 9, &record.PreviousEntryDigest) ||
		!decodeNodeDigest(root, 10, &record.EntryDigest) ||
		!decodeNodeDigest(root, 11, &record.BeforeLogicalDigest) ||
		!decodeNodeDigest(root, 12, &record.AfterLogicalDigest) ||
		!decodeNodeDigest(root, 14, &record.Digest) {
		return sourceCaptureEntry{}, ErrSourceCapture
	}
	array, ok := root.Index(13)
	if !ok {
		return sourceCaptureEntry{}, ErrSourceCapture
	}
	transitionCount, ok := array.ArrayLen()
	if !ok || transitionCount > replicatedstate.MaxDistinctMutations {
		return sourceCaptureEntry{}, ErrSourceCapture
	}
	workspace.decoded = workspace.decoded[:0]
	if cap(workspace.decoded) < len(raw) {
		workspace.decoded = make([]byte, 0, len(raw))
	}
	if cap(workspace.transitions) < transitionCount {
		workspace.transitions = make([]TailTransition, transitionCount)
	} else {
		workspace.transitions = workspace.transitions[:transitionCount]
	}
	var previous []byte
	for index := 0; index < transitionCount; index++ {
		item, ok := array.Index(index)
		itemCount, itemOK := item.ArrayLen()
		if !ok || !itemOK || itemCount != 3 {
			return sourceCaptureEntry{}, ErrSourceCapture
		}
		key, ok := decodeNodeBase64(item, 0, workspace)
		if !ok {
			return sourceCaptureEntry{}, ErrSourceCapture
		}
		before, beforeOK := decodeOptionalNodeBase64(item, 1, workspace)
		after, afterOK := decodeOptionalNodeBase64(item, 2, workspace)
		if !beforeOK || !afterOK || before == nil && after == nil ||
			previous != nil && bytes.Compare(previous, key) >= 0 ||
			before != nil && vibejson.Validate(before) != nil ||
			after != nil && vibejson.Validate(after) != nil {
			return sourceCaptureEntry{}, ErrSourceCapture
		}
		workspace.transitions[index] = TailTransition{Key: key, Before: before, After: after}
		previous = key
	}
	record.Transitions = workspace.transitions
	if !validSourceCaptureEntry(record) || c.hashEntry(record, workspace) != record.Digest {
		return sourceCaptureEntry{}, ErrSourceCapture
	}
	return *record, nil
}

func buildCaptureIndex(raw []byte, workspace *SourceCaptureWorkspace) (vibejson.Node, error) {
	needed, err := vibejson.RequiredIndexEntries(raw)
	if err != nil {
		return vibejson.Node{}, ErrSourceCapture
	}
	if cap(workspace.entries) < needed {
		workspace.entries = make([]vibejson.IndexEntry, needed)
	} else {
		workspace.entries = workspace.entries[:needed]
	}
	index, err := vibejson.BuildIndex(raw, workspace.entries)
	if err != nil {
		return vibejson.Node{}, ErrSourceCapture
	}
	return index.Root(), nil
}

func (c *SourceCapture) hashHeader(
	state replicatedstate.State,
	workspace *SourceCaptureWorkspace,
) [32]byte {
	h := captureHasher(workspace)
	_, _ = h.Write(sourceCaptureHeaderDomain)
	_, _ = h.Write(c.partitioner.digest[:])
	_, _ = h.Write(c.placement[:])
	hashTailFrame(h, &workspace.size, []byte(c.partitioner.collection))
	_, _ = h.Write(state.LogicalDigest[:])
	_, _ = h.Write(state.SnapshotBaseDigest[:])
	_, _ = h.Write(state.LastEntryDigest[:])
	fixed := workspace.fixed[:0]
	fixed = binary.LittleEndian.AppendUint64(fixed, state.Applied)
	fixed = binary.LittleEndian.AppendUint64(fixed, state.LastTerm)
	fixed = binary.LittleEndian.AppendUint64(fixed, state.Binding.OwnershipEpoch)
	fixed = binary.LittleEndian.AppendUint64(fixed, state.Binding.RoutingVersion)
	fixed = binary.LittleEndian.AppendUint64(fixed, state.Binding.RouteGeneration)
	fixed = binary.LittleEndian.AppendUint64(fixed, uint64(c.partitioner.target))
	_, _ = h.Write(fixed)
	_ = h.Sum(workspace.digest[:0])
	return workspace.digest
}

func (c *SourceCapture) hashTransition(
	transition replicatedstate.CapturedTransition,
	workspace *SourceCaptureWorkspace,
) [32]byte {
	record := &workspace.record
	*record = sourceCaptureEntry{
		Applied: transition.Applied, Term: transition.Term,
		BeforeOwnershipEpoch:  transition.BeforeOwnershipEpoch,
		AfterOwnershipEpoch:   transition.AfterOwnershipEpoch,
		BeforeRoutingVersion:  transition.BeforeRoutingVersion,
		AfterRoutingVersion:   transition.AfterRoutingVersion,
		BeforeRouteGeneration: transition.BeforeRouteGeneration,
		AfterRouteGeneration:  transition.AfterRouteGeneration,
		PreviousEntryDigest:   transition.PreviousEntryDigest,
		EntryDigest:           transition.EntryDigest,
		BeforeLogicalDigest:   transition.BeforeLogicalDigest,
		AfterLogicalDigest:    transition.AfterLogicalDigest,
	}
	if cap(workspace.transitions) < transition.MutationCount() {
		workspace.transitions = make([]TailTransition, transition.MutationCount())
	} else {
		workspace.transitions = workspace.transitions[:transition.MutationCount()]
	}
	for index := range workspace.transitions {
		mutation := transition.Mutation(index)
		workspace.transitions[index] = TailTransition{
			Key: mutation.Key, Before: mutation.Before, After: mutation.After,
		}
	}
	record.Transitions = workspace.transitions
	return c.hashEntry(record, workspace)
}

func (c *SourceCapture) hashEntry(
	record *sourceCaptureEntry,
	workspace *SourceCaptureWorkspace,
) [32]byte {
	h := captureHasher(workspace)
	_, _ = h.Write(sourceCaptureEntryDomain)
	_, _ = h.Write(c.partitioner.digest[:])
	_, _ = h.Write(c.placement[:])
	fixed := workspace.fixed[:0]
	for _, value := range []uint64{
		record.Applied, record.Term,
		record.BeforeOwnershipEpoch, record.AfterOwnershipEpoch,
		record.BeforeRoutingVersion, record.AfterRoutingVersion,
		record.BeforeRouteGeneration, record.AfterRouteGeneration,
		uint64(len(record.Transitions)),
	} {
		fixed = binary.LittleEndian.AppendUint64(fixed, value)
	}
	_, _ = h.Write(fixed)
	_, _ = h.Write(record.PreviousEntryDigest[:])
	_, _ = h.Write(record.EntryDigest[:])
	_, _ = h.Write(record.BeforeLogicalDigest[:])
	_, _ = h.Write(record.AfterLogicalDigest[:])
	for index := range record.Transitions {
		transition := &record.Transitions[index]
		hashTailFrame(h, &workspace.size, transition.Key)
		hashOptionalCaptureFrame(h, &workspace.size, transition.Before)
		hashOptionalCaptureFrame(h, &workspace.size, transition.After)
	}
	_ = h.Sum(workspace.digest[:0])
	return workspace.digest
}

func captureHasher(workspace *SourceCaptureWorkspace) hash.Hash {
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	workspace.hasher.Reset()
	return workspace.hasher
}

func hashOptionalCaptureFrame(h hash.Hash, size *[8]byte, value []byte) {
	if value == nil {
		size[0] = 0
		_, _ = h.Write(size[:1])
		return
	}
	size[0] = 1
	_, _ = h.Write(size[:1])
	hashTailFrame(h, size, value)
}

func appendBase64String(dst, raw []byte) []byte {
	dst = append(dst, '"')
	dst = base64.RawURLEncoding.AppendEncode(dst, raw)
	return append(dst, '"')
}

func appendOptionalBase64String(dst, raw []byte) []byte {
	if raw == nil {
		return append(dst, 'n', 'u', 'l', 'l')
	}
	return appendBase64String(dst, raw)
}

func nodeUint(root vibejson.Node, index int) uint64 {
	node, ok := root.Index(index)
	if !ok {
		return 0
	}
	value, _ := node.Uint64()
	return value
}

func decodeNodeDigest(root vibejson.Node, index int, dst *[32]byte) bool {
	node, ok := root.Index(index)
	if !ok {
		return false
	}
	raw, ok := node.StringBytes()
	if !ok || base64.RawURLEncoding.DecodedLen(len(raw)) != len(dst) {
		return false
	}
	n, err := base64.RawURLEncoding.Decode(dst[:], raw)
	return err == nil && n == len(dst)
}

func decodeNodeBase64(
	root vibejson.Node,
	index int,
	workspace *SourceCaptureWorkspace,
) ([]byte, bool) {
	node, ok := root.Index(index)
	if !ok {
		return nil, false
	}
	raw, ok := node.StringBytes()
	if !ok {
		return nil, false
	}
	decoded := base64.RawURLEncoding.DecodedLen(len(raw))
	start := len(workspace.decoded)
	workspace.decoded = workspace.decoded[:start+decoded]
	n, err := base64.RawURLEncoding.Decode(workspace.decoded[start:], raw)
	if err != nil || n != decoded {
		workspace.decoded = workspace.decoded[:start]
		return nil, false
	}
	return workspace.decoded[start : start+n : start+n], true
}

func decodeOptionalNodeBase64(
	root vibejson.Node,
	index int,
	workspace *SourceCaptureWorkspace,
) ([]byte, bool) {
	node, ok := root.Index(index)
	if !ok {
		return nil, false
	}
	if node.IsNull() {
		return nil, true
	}
	return decodeNodeBase64(root, index, workspace)
}

func (c *SourceCapture) transitionFollowsCurrent(
	transition replicatedstate.CapturedTransition,
) bool {
	current := c.current
	return current.applied != math.MaxUint64 && transition.Applied == current.applied+1 &&
		transition.Term >= current.term &&
		transition.PreviousEntryDigest == current.entryDigest &&
		transition.BeforeLogicalDigest == current.logicalDigest &&
		transition.BeforeOwnershipEpoch == current.ownershipEpoch &&
		transition.BeforeRoutingVersion == current.routingVersion &&
		transition.BeforeRouteGeneration == current.routeGeneration
}

func validSourceCaptureEntry(record *sourceCaptureEntry) bool {
	return record.Applied != 0 && record.Applied != math.MaxUint64 &&
		record.Term != 0 && record.Term != math.MaxUint64 &&
		record.BeforeOwnershipEpoch != 0 && record.AfterOwnershipEpoch != 0 &&
		record.BeforeRoutingVersion != 0 && record.AfterRoutingVersion != 0 &&
		record.BeforeRouteGeneration != 0 && record.AfterRouteGeneration != 0 &&
		record.PreviousEntryDigest != ([32]byte{}) && record.EntryDigest != ([32]byte{}) &&
		record.BeforeLogicalDigest != ([32]byte{}) && record.AfterLogicalDigest != ([32]byte{})
}

func publicationFromState(state replicatedstate.State) sourceCapturePublication {
	return sourceCapturePublication{
		applied: state.Applied, term: state.LastTerm,
		ownershipEpoch:  state.Binding.OwnershipEpoch,
		routingVersion:  state.Binding.RoutingVersion,
		routeGeneration: state.Binding.RouteGeneration,
		entryDigest:     state.LastEntryDigest, logicalDigest: state.LogicalDigest,
	}
}

func publicationFromTransition(
	transition replicatedstate.CapturedTransition,
) sourceCapturePublication {
	return sourceCapturePublication{
		applied: transition.Applied, term: transition.Term,
		ownershipEpoch:  transition.AfterOwnershipEpoch,
		routingVersion:  transition.AfterRoutingVersion,
		routeGeneration: transition.AfterRouteGeneration,
		entryDigest:     transition.EntryDigest, logicalDigest: transition.AfterLogicalDigest,
	}
}

func publicationFromEntry(record sourceCaptureEntry) sourceCapturePublication {
	return sourceCapturePublication{
		applied: record.Applied, term: record.Term,
		ownershipEpoch:  record.AfterOwnershipEpoch,
		routingVersion:  record.AfterRoutingVersion,
		routeGeneration: record.AfterRouteGeneration,
		entryDigest:     record.EntryDigest, logicalDigest: record.AfterLogicalDigest,
	}
}

func publicationMatchesState(publication sourceCapturePublication, state replicatedstate.State) bool {
	return publication == publicationFromState(state)
}
