package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

var ErrSourceCapture = errors.New("rangesplit: invalid source transition capture")

var (
	sourceCaptureHeaderDomain = []byte("vibedb/range-split/source-capture-header\x00")
	sourceCaptureEntryDomain  = []byte("vibedb/range-split/source-capture-entry\x00")
)

const (
	// sourceCaptureFormat is the sole current-format corruption sentinel. Zero
	// selects the only grammar; every other value fails closed rather than
	// dispatching to a compatibility decoder.
	sourceCaptureFormat = uint16(0)

	sourceCaptureHeaderKind = uint8(1)
	sourceCaptureEntryKind  = uint8(2)
	sourceCaptureHeaderKey  = uint64(0)

	sourceCaptureEnvelopeBytes         = 16
	sourceCaptureHeaderFixedBytes      = 264
	sourceCaptureEntryFixedBytes       = 248
	sourceCaptureTransitionHeaderBytes = 56

	sourceCaptureBeforePresent = uint8(1 << 0)
	sourceCaptureAfterPresent  = uint8(1 << 1)
	sourceCapturePresenceMask  = sourceCaptureBeforePresent | sourceCaptureAfterPresent
)

var sourceCaptureMagic = [8]byte{'V', 'D', 'B', 'C', 'A', 'P', 0, 0}

// Both record kinds start with magic[8], format[2], kind[1], reserved[1], and
// totalBytes[4]. Bytes 16:20 are the collection length or transition count;
// bytes 20:24 are reserved. Header metadata occupies bytes 24:264 before the
// collection. Entry metadata and all five digests occupy bytes 24:248 before
// the packed transition frames.

type sourceCapturePublication struct {
	applied         uint64
	term            uint64
	ownershipEpoch  uint64
	routingVersion  uint64
	routeGeneration uint64
	entryDigest     [sha256.Size]byte
	dataChainDigest [sha256.Size]byte
}

// SourceCapture stores every exact source transition in one private opaque
// durable collection. Old documents are represented by fixed-size witnesses;
// after documents remain byte-exact for destination puts. The replicated state
// machine commits the record in the same transaction as source publication.
type SourceCapture struct {
	mu sync.Mutex

	partitioner *Partitioner
	placement   [sha256.Size]byte
	target      replicatedstate.TransitionCaptureTarget
	base        ChildArtifactSourceCut
	current     sourceCapturePublication
	encode      SourceCaptureWorkspace
	key         [8]byte
	pending     uint64
	begun       atomic.Bool
	head        atomic.Uint64
}

// SourceCaptureWorkspace owns all raw read, transition, and SHA state. Reuse it
// serially. Returned TailEntry slices borrow its raw buffer and remain valid
// until the workspace's next use.
type SourceCaptureWorkspace struct {
	raw         []byte
	transitions []TailTransition
	record      sourceCaptureEntry
	hasher      hash.Hash
	digest      [sha256.Size]byte
	document    distribution.DocumentPointWorkspace
	fixed       [144]byte
	size        [8]byte
	key         [8]byte
}

// MaximumSourceCaptureRecordBytes returns the exact provisioning bound for one
// admitted replicated mutation batch.
func MaximumSourceCaptureRecordBytes(
	maxMutations, maxKeyBytes, maxDocumentBytes, maxBatchBytes int,
) (int, error) {
	if maxMutations <= 0 || maxKeyBytes <= 0 || maxDocumentBytes <= 0 || maxBatchBytes <= 0 ||
		maxMutations > replicatedstate.MaxDistinctMutations ||
		uint64(maxMutations) > math.MaxUint32 || uint64(maxKeyBytes) > math.MaxUint32 ||
		uint64(maxDocumentBytes) > math.MaxUint32 || uint64(maxBatchBytes) > math.MaxUint32 {
		return 0, ErrSourceCapture
	}
	product := func(value int) (uint64, bool) {
		if uint64(maxMutations) > math.MaxUint32/uint64(value) {
			return 0, false
		}
		return uint64(maxMutations) * uint64(value), true
	}
	transitionBytes, ok := product(sourceCaptureTransitionHeaderBytes)
	if !ok {
		return 0, ErrSourceCapture
	}
	total := uint64(sourceCaptureEntryFixedBytes)
	// Every key and after image is already inside the canonical replicated
	// command envelope. Before images contribute only fixed-size witnesses.
	perMutationBytes := uint64(maxKeyBytes) + uint64(maxDocumentBytes)
	structuralPayloadBytes := uint64(maxMutations) * perMutationBytes
	payloadBytes := min(
		uint64(maxBatchBytes), uint64(replication.MaxCommandBytes), structuralPayloadBytes,
	)
	for _, addition := range [...]uint64{transitionBytes, payloadBytes} {
		if total > math.MaxUint32-addition {
			return 0, ErrSourceCapture
		}
		total += addition
	}
	return int(total), nil
}

// NewSourceCapture binds one private collection to an exact split plan.
func NewSourceCapture(
	partitioner *Partitioner,
	name string,
	collection *durable.Collection,
) (*SourceCapture, error) {
	if partitioner == nil || name == "" || collection == nil ||
		!collection.HasOpaqueValues() {
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

// MaxEncodedBytes returns the exact raw-binary size implied by bounds.
func (c *SourceCapture) MaxEncodedBytes(
	bounds replicatedstate.TransitionCaptureBounds,
) (int, error) {
	if c == nil || bounds.Transitions > replicatedstate.MaxDistinctMutations {
		return 0, ErrSourceCapture
	}
	if bounds.KeyBytes > bounds.Transitions*replication.MaxMutationKeyBytes ||
		bounds.BeforeBytes > bounds.Transitions*replication.MaxMutationValueBytes ||
		bounds.AfterBytes > bounds.Transitions*replication.MaxMutationValueBytes {
		return 0, ErrSourceCapture
	}
	total := uint64(sourceCaptureEntryFixedBytes)
	for _, addition := range [...]uint64{
		bounds.Transitions * sourceCaptureTransitionHeaderBytes,
		bounds.KeyBytes,
		bounds.AfterBytes,
	} {
		if total > math.MaxUint32 || addition > math.MaxUint32-total {
			return 0, ErrSourceCapture
		}
		total += addition
	}
	if bounds.KeyBytes > replication.MaxCommandBytes ||
		bounds.AfterBytes > replication.MaxCommandBytes-bounds.KeyBytes {
		return 0, ErrSourceCapture
	}
	if total > uint64(math.MaxInt) {
		return 0, ErrSourceCapture
	}
	return int(total), nil
}

// Begin creates an exact capture base or recovers and verifies every retained
// record against the current replicated publication.
func (c *SourceCapture) Begin(
	state replicatedstate.State,
	publish func(key, value []byte) error,
) error {
	if c == nil {
		return ErrSourceCapture
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.begun.Load() {
		return ErrSourceCapture
	}
	if c.target.Collection.Len() == 0 {
		if publish == nil || !c.partitioner.matchesSource(state) {
			return ErrSourceCapture
		}
		header, cut, err := c.appendHeader(c.encode.raw[:0], state, &c.encode)
		if err != nil || len(header) > c.target.Collection.MaxDocumentBytes() {
			return errors.Join(ErrSourceCapture, err)
		}
		binary.BigEndian.PutUint64(c.key[:], sourceCaptureHeaderKey)
		if err := publish(c.key[:], header); err != nil {
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
	if !c.begun.Load() || c.pending != 0 || !c.transitionFollowsCurrent(transition) {
		return dst, ErrSourceCapture
	}
	record, err := c.appendEntry(dst, transition, &c.encode)
	if err != nil {
		return dst, err
	}
	c.pending = transition.Applied
	return record, nil
}

// Published advances the in-memory head after the atomic source transaction.
func (c *SourceCapture) Published(transition replicatedstate.CapturedTransition) error {
	if c == nil {
		return ErrSourceCapture
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.begun.Load() || c.pending != transition.Applied ||
		!c.transitionFollowsCurrent(transition) {
		return ErrSourceCapture
	}
	c.current = publicationFromTransition(transition)
	c.pending = 0
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

// PartitionerDigest identifies the immutable portable recipe recovered by this capture.
func (c *SourceCapture) PartitionerDigest() [sha256.Size]byte {
	if c == nil || c.partitioner == nil {
		return [sha256.Size]byte{}
	}
	return c.partitioner.Digest()
}

// ValidateDescriptorAncestor proves that descriptor names an exact published
// boundary in this capture's authenticated chain. It performs one bounded
// point read regardless of capture length. This lets a restarted controller
// advance a lagging control record without rescanning or trusting a merely
// monotonic applied index.
func (c *SourceCapture) ValidateDescriptorAncestor(
	descriptor SourceCaptureDescriptor,
	workspace *SourceCaptureWorkspace,
) error {
	if c == nil || workspace == nil || !c.begun.Load() ||
		c.partitioner.ValidateSourceCaptureDescriptor(descriptor) != nil {
		return ErrSourceCapture
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending != 0 || descriptor.Base != c.base ||
		descriptor.Head.BaseDigest != c.base.BaseDigest ||
		descriptor.Head.Applied < c.base.Applied ||
		descriptor.Head.Applied > c.current.applied {
		return ErrSourceCapture
	}
	key := descriptor.Head.Applied
	if descriptor.Head.Applied == c.base.Applied {
		key = sourceCaptureHeaderKey
	}
	binary.BigEndian.PutUint64(workspace.key[:], key)
	raw, found, err := c.target.Collection.AppendRaw(workspace.raw[:0], workspace.key[:])
	if err != nil || !found {
		return errors.Join(ErrSourceCapture, err)
	}
	workspace.raw = raw
	publication := sourceCapturePublication{}
	if descriptor.Head.Applied == c.base.Applied {
		base, opened, decodeErr := c.decodeHeader(raw, workspace)
		if decodeErr != nil || base != c.base {
			return errors.Join(ErrSourceCapture, decodeErr)
		}
		publication = opened
	} else {
		record, decodeErr := c.decodeEntry(raw, workspace)
		if decodeErr != nil || record.Applied != descriptor.Head.Applied {
			return errors.Join(ErrSourceCapture, decodeErr)
		}
		publication = publicationFromEntry(record)
	}
	if descriptor.Head.DataChainDigest != publication.dataChainDigest ||
		descriptor.Head.EntryDigest != publication.entryDigest ||
		descriptor.Head.Term != publication.term ||
		descriptor.Head.RouteGeneration != publication.routeGeneration ||
		descriptor.Coordinates.OwnershipEpoch != publication.ownershipEpoch ||
		descriptor.Coordinates.RoutingVersion != publication.routingVersion ||
		descriptor.Coordinates.RouteGeneration != publication.routeGeneration {
		return ErrSourceCapture
	}
	return nil
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
		record.Term < cursor.term ||
		record.PreviousEntryDigest != cursor.entryDigest ||
		record.BeforeDataChainDigest != cursor.dataChainDigest ||
		record.BeforeOwnershipEpoch != cursor.ownershipEpoch ||
		record.BeforeRoutingVersion != cursor.routingVersion ||
		record.BeforeRouteGeneration != cursor.routeGeneration {
		return TailEntry{}, false, errors.Join(ErrSourceCapture, err)
	}
	entry := TailEntry{
		Applied: record.Applied, Term: record.Term,
		BeforeOwnershipEpoch:  record.BeforeOwnershipEpoch,
		AfterOwnershipEpoch:   record.AfterOwnershipEpoch,
		BeforeRoutingVersion:  record.BeforeRoutingVersion,
		AfterRoutingVersion:   record.AfterRoutingVersion,
		BeforeRouteGeneration: record.BeforeRouteGeneration,
		AfterRouteGeneration:  record.AfterRouteGeneration,
		PreviousEntryDigest:   record.PreviousEntryDigest,
		EntryDigest:           record.EntryDigest,
		BeforeDataChainDigest: record.BeforeDataChainDigest,
		AfterDataChainDigest:  record.AfterDataChainDigest,
		Transitions:           record.Transitions,
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
	BeforeDataChainDigest [sha256.Size]byte
	AfterDataChainDigest  [sha256.Size]byte
	Transitions           []TailTransition
	Digest                [sha256.Size]byte
}

func (c *SourceCapture) appendHeader(
	dst []byte,
	state replicatedstate.State,
	workspace *SourceCaptureWorkspace,
) ([]byte, ChildArtifactSourceCut, error) {
	cut := ChildArtifactSourceCut{
		DataChainDigest: state.DataChainDigest, BaseDigest: state.SnapshotBaseDigest,
		EntryDigest: state.LastEntryDigest, Applied: state.Applied,
		Term: state.LastTerm, RouteGeneration: state.Binding.RouteGeneration,
	}
	collection := byteview.Bytes(c.partitioner.collection)
	total := uint64(sourceCaptureHeaderFixedBytes) + uint64(len(collection))
	if cut.Applied == 0 || cut.Applied == math.MaxUint64 ||
		cut.Term == 0 || cut.Term == math.MaxUint64 ||
		state.Binding.OwnershipEpoch == 0 || state.Binding.RoutingVersion == 0 ||
		state.Binding.RouteGeneration == 0 || uint64(c.partitioner.target) == 0 ||
		cut.DataChainDigest == ([32]byte{}) || cut.BaseDigest == ([32]byte{}) ||
		cut.EntryDigest == ([32]byte{}) || len(collection) == 0 ||
		len(collection) > replication.MaxCollectionBytes ||
		total > math.MaxUint32 || total > uint64(math.MaxInt) {
		return dst, ChildArtifactSourceCut{}, ErrSourceCapture
	}
	digest := c.hashHeader(state, workspace)
	var frame []byte
	dst, frame = appendSourceCaptureEnvelope(dst, sourceCaptureHeaderKind, int(total))
	binary.LittleEndian.PutUint32(frame[16:20], uint32(len(collection)))
	copy(frame[24:56], c.partitioner.digest[:])
	copy(frame[56:88], c.placement[:])
	copy(frame[88:120], state.DataChainDigest[:])
	copy(frame[120:152], state.SnapshotBaseDigest[:])
	copy(frame[152:184], state.LastEntryDigest[:])
	copy(frame[184:216], digest[:])
	binary.LittleEndian.PutUint64(frame[216:224], state.Applied)
	binary.LittleEndian.PutUint64(frame[224:232], state.LastTerm)
	binary.LittleEndian.PutUint64(frame[232:240], state.Binding.OwnershipEpoch)
	binary.LittleEndian.PutUint64(frame[240:248], state.Binding.RoutingVersion)
	binary.LittleEndian.PutUint64(frame[248:256], state.Binding.RouteGeneration)
	binary.LittleEndian.PutUint64(frame[256:264], uint64(c.partitioner.target))
	copy(frame[sourceCaptureHeaderFixedBytes:], collection)
	return dst, cut, nil
}

func (c *SourceCapture) appendEntry(
	dst []byte,
	transition replicatedstate.CapturedTransition,
	workspace *SourceCaptureWorkspace,
) ([]byte, error) {
	encodedBytes, err := c.MaxEncodedBytes(transition.Bounds())
	if err != nil {
		return dst, err
	}
	clear(workspace.transitions)
	if cap(workspace.transitions) < transition.MutationCount() {
		workspace.transitions = make([]TailTransition, transition.MutationCount())
	} else {
		workspace.transitions = workspace.transitions[:transition.MutationCount()]
	}
	workspace.record.Transitions = nil
	defer workspace.releaseTransitions()
	var previous []byte
	for index := 0; index < transition.MutationCount(); index++ {
		mutation := transition.Mutation(index)
		if len(mutation.Key) == 0 || len(mutation.Key) > replication.MaxMutationKeyBytes ||
			previous != nil && bytes.Compare(previous, mutation.Key) >= 0 ||
			mutation.Before == nil && mutation.After == nil ||
			mutation.Before != nil && (len(mutation.Before) == 0 ||
				len(mutation.Before) > replication.MaxMutationValueBytes) ||
			mutation.After != nil && (len(mutation.After) == 0 ||
				len(mutation.After) > replication.MaxMutationValueBytes) {
			return dst, ErrSourceCapture
		}
		witness, witnessErr := c.captureBeforeWitness(mutation.Before, workspace)
		if witnessErr != nil {
			return dst, witnessErr
		}
		workspace.transitions[index] = TailTransition{
			Key: mutation.Key, BeforeWitness: witness, After: mutation.After,
		}
		previous = mutation.Key
	}
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
		BeforeDataChainDigest: transition.BeforeDataChainDigest,
		AfterDataChainDigest:  transition.AfterDataChainDigest,
		Transitions:           workspace.transitions,
	}
	digest := c.hashEntry(record, workspace)
	var frame []byte
	dst, frame = appendSourceCaptureEnvelope(dst, sourceCaptureEntryKind, encodedBytes)
	binary.LittleEndian.PutUint32(frame[16:20], uint32(transition.MutationCount()))
	values := [...]uint64{
		transition.Applied, transition.Term,
		transition.BeforeOwnershipEpoch, transition.AfterOwnershipEpoch,
		transition.BeforeRoutingVersion, transition.AfterRoutingVersion,
		transition.BeforeRouteGeneration, transition.AfterRouteGeneration,
	}
	for index, value := range values {
		start := 24 + index*8
		binary.LittleEndian.PutUint64(frame[start:start+8], value)
	}
	for index, value := range [][32]byte{
		transition.PreviousEntryDigest, transition.EntryDigest,
		transition.BeforeDataChainDigest, transition.AfterDataChainDigest, digest,
	} {
		start := 88 + index*sha256.Size
		copy(frame[start:start+sha256.Size], value[:])
	}
	cursor := sourceCaptureEntryFixedBytes
	for index := 0; index < transition.MutationCount(); index++ {
		mutation := &workspace.transitions[index]
		header := frame[cursor : cursor+sourceCaptureTransitionHeaderBytes]
		if mutation.BeforeWitness.Present {
			header[0] |= sourceCaptureBeforePresent
		}
		if mutation.After != nil {
			header[0] |= sourceCaptureAfterPresent
		}
		binary.LittleEndian.PutUint32(header[4:8], uint32(len(mutation.Key)))
		binary.LittleEndian.PutUint32(header[8:12], mutation.BeforeWitness.DocumentBytes)
		binary.LittleEndian.PutUint32(header[12:16], uint32(len(mutation.After)))
		copy(header[16:24], mutation.BeforeWitness.Point[:])
		copy(header[24:56], mutation.BeforeWitness.Digest[:])
		cursor += sourceCaptureTransitionHeaderBytes
		copy(frame[cursor:], mutation.Key)
		cursor += len(mutation.Key)
		copy(frame[cursor:], mutation.After)
		cursor += len(mutation.After)
	}
	if cursor != len(frame) {
		panic("rangesplit: source capture size invariant")
	}
	return dst, nil
}

func (workspace *SourceCaptureWorkspace) releaseTransitions() {
	clear(workspace.transitions)
	workspace.transitions = workspace.transitions[:0]
	workspace.record.Transitions = nil
}

func (c *SourceCapture) captureBeforeWitness(
	document []byte,
	workspace *SourceCaptureWorkspace,
) (TailBeforeWitness, error) {
	if document == nil {
		return TailBeforeWitness{}, nil
	}
	point, err := c.partitioner.program.Point(document, &workspace.document)
	if err != nil || len(document) == 0 || len(document) > replication.MaxMutationValueBytes {
		return TailBeforeWitness{}, errors.Join(ErrSourceCapture, err)
	}
	if c.partitioner.childFor(point) < 0 {
		return TailBeforeWitness{}, ErrSourceCapture
	}
	digest := sha256.Sum256(document)
	if digest == ([sha256.Size]byte{}) {
		return TailBeforeWitness{}, ErrSourceCapture
	}
	return TailBeforeWitness{
		Present: true, Point: point, DocumentBytes: uint32(len(document)),
		Digest: digest,
	}, nil
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
			record.Term < publication.term ||
			record.PreviousEntryDigest != publication.entryDigest ||
			record.BeforeDataChainDigest != publication.dataChainDigest ||
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
	if !validSourceCaptureEnvelope(raw, sourceCaptureHeaderKind, sourceCaptureHeaderFixedBytes) ||
		binary.LittleEndian.Uint32(raw[20:24]) != 0 {
		return ChildArtifactSourceCut{}, sourceCapturePublication{}, ErrSourceCapture
	}
	collectionBytes := uint64(binary.LittleEndian.Uint32(raw[16:20]))
	if collectionBytes == 0 || collectionBytes > replication.MaxCollectionBytes ||
		collectionBytes != uint64(len(raw)-sourceCaptureHeaderFixedBytes) {
		return ChildArtifactSourceCut{}, sourceCapturePublication{}, ErrSourceCapture
	}
	collection := raw[sourceCaptureHeaderFixedBytes:len(raw):len(raw)]
	if !bytes.Equal(collection, byteview.Bytes(c.partitioner.collection)) {
		return ChildArtifactSourceCut{}, sourceCapturePublication{}, ErrSourceCapture
	}
	var plan, placement, dataChain, base, entry, digest [sha256.Size]byte
	copy(plan[:], raw[24:56])
	copy(placement[:], raw[56:88])
	copy(dataChain[:], raw[88:120])
	copy(base[:], raw[120:152])
	copy(entry[:], raw[152:184])
	copy(digest[:], raw[184:216])
	if plan != c.partitioner.digest || placement != c.placement ||
		binary.LittleEndian.Uint64(raw[256:264]) != uint64(c.partitioner.target) {
		return ChildArtifactSourceCut{}, sourceCapturePublication{}, ErrSourceCapture
	}
	publication := sourceCapturePublication{
		applied:         binary.LittleEndian.Uint64(raw[216:224]),
		term:            binary.LittleEndian.Uint64(raw[224:232]),
		ownershipEpoch:  binary.LittleEndian.Uint64(raw[232:240]),
		routingVersion:  binary.LittleEndian.Uint64(raw[240:248]),
		routeGeneration: binary.LittleEndian.Uint64(raw[248:256]),
		entryDigest:     entry,
		dataChainDigest: dataChain,
	}
	if (TailSourceCoordinates{OwnershipEpoch: publication.ownershipEpoch, RoutingVersion: publication.routingVersion, RouteGeneration: publication.routeGeneration}) != c.partitioner.initialCoordinates(publication.routeGeneration) {
		return ChildArtifactSourceCut{}, sourceCapturePublication{}, ErrSourceCapture
	}
	if publication.applied == 0 || publication.applied == math.MaxUint64 ||
		publication.term == 0 || publication.term == math.MaxUint64 ||
		publication.ownershipEpoch == 0 || publication.routingVersion == 0 ||
		publication.routeGeneration == 0 || dataChain == ([sha256.Size]byte{}) ||
		base == ([sha256.Size]byte{}) || entry == ([sha256.Size]byte{}) {
		return ChildArtifactSourceCut{}, sourceCapturePublication{}, ErrSourceCapture
	}
	state := replicatedstate.State{
		Applied: publication.applied, LastTerm: publication.term,
		LastEntryDigest: entry, DataChainDigest: dataChain, SnapshotBaseDigest: base,
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
		DataChainDigest: dataChain, BaseDigest: base, EntryDigest: entry,
		Applied: publication.applied, Term: publication.term,
		RouteGeneration: publication.routeGeneration,
	}
	return cut, publication, nil
}

func (c *SourceCapture) decodeEntry(
	raw []byte,
	workspace *SourceCaptureWorkspace,
) (sourceCaptureEntry, error) {
	if !validSourceCaptureEnvelope(raw, sourceCaptureEntryKind, sourceCaptureEntryFixedBytes) ||
		binary.LittleEndian.Uint32(raw[20:24]) != 0 {
		return sourceCaptureEntry{}, ErrSourceCapture
	}
	transitionCount := uint64(binary.LittleEndian.Uint32(raw[16:20]))
	if transitionCount > replicatedstate.MaxDistinctMutations {
		return sourceCaptureEntry{}, ErrSourceCapture
	}
	maximumBytes := uint64(sourceCaptureEntryFixedBytes) +
		transitionCount*sourceCaptureTransitionHeaderBytes + replication.MaxCommandBytes
	if uint64(len(raw)) > maximumBytes {
		return sourceCaptureEntry{}, ErrSourceCapture
	}
	record := &workspace.record
	*record = sourceCaptureEntry{
		Applied:               binary.LittleEndian.Uint64(raw[24:32]),
		Term:                  binary.LittleEndian.Uint64(raw[32:40]),
		BeforeOwnershipEpoch:  binary.LittleEndian.Uint64(raw[40:48]),
		AfterOwnershipEpoch:   binary.LittleEndian.Uint64(raw[48:56]),
		BeforeRoutingVersion:  binary.LittleEndian.Uint64(raw[56:64]),
		AfterRoutingVersion:   binary.LittleEndian.Uint64(raw[64:72]),
		BeforeRouteGeneration: binary.LittleEndian.Uint64(raw[72:80]),
		AfterRouteGeneration:  binary.LittleEndian.Uint64(raw[80:88]),
	}
	for index, target := range [...]*[sha256.Size]byte{
		&record.PreviousEntryDigest,
		&record.EntryDigest,
		&record.BeforeDataChainDigest,
		&record.AfterDataChainDigest,
		&record.Digest,
	} {
		start := 88 + index*sha256.Size
		copy(target[:], raw[start:start+sha256.Size])
	}
	count := int(transitionCount)
	clear(workspace.transitions)
	if cap(workspace.transitions) < count {
		workspace.transitions = make([]TailTransition, count)
	} else {
		workspace.transitions = workspace.transitions[:count]
	}
	cursor := sourceCaptureEntryFixedBytes
	var previous []byte
	for index := 0; index < count; index++ {
		if len(raw)-cursor < sourceCaptureTransitionHeaderBytes {
			return sourceCaptureEntry{}, ErrSourceCapture
		}
		header := raw[cursor : cursor+sourceCaptureTransitionHeaderBytes]
		flags := header[0]
		keyBytes := uint64(binary.LittleEndian.Uint32(header[4:8]))
		beforeBytes := uint64(binary.LittleEndian.Uint32(header[8:12]))
		afterBytes := uint64(binary.LittleEndian.Uint32(header[12:16]))
		beforePresent := flags&sourceCaptureBeforePresent != 0
		afterPresent := flags&sourceCaptureAfterPresent != 0
		var beforePoint distribution.KeyspacePoint
		copy(beforePoint[:], header[16:24])
		var beforeDigest [sha256.Size]byte
		copy(beforeDigest[:], header[24:56])
		if flags == 0 || flags&^sourceCapturePresenceMask != 0 ||
			header[1] != 0 || binary.LittleEndian.Uint16(header[2:4]) != 0 ||
			keyBytes == 0 || keyBytes > replication.MaxMutationKeyBytes ||
			beforePresent != (beforeBytes != 0) || afterPresent != (afterBytes != 0) ||
			beforeBytes > replication.MaxMutationValueBytes ||
			afterBytes > replication.MaxMutationValueBytes ||
			beforePresent && beforeDigest == ([sha256.Size]byte{}) ||
			beforePresent && c.partitioner.childFor(beforePoint) < 0 ||
			!beforePresent && (beforePoint != (distribution.KeyspacePoint{}) ||
				beforeDigest != ([sha256.Size]byte{})) {
			return sourceCaptureEntry{}, ErrSourceCapture
		}
		payloadBytes := keyBytes + afterBytes
		cursor += sourceCaptureTransitionHeaderBytes
		if payloadBytes > uint64(len(raw)-cursor) {
			return sourceCaptureEntry{}, ErrSourceCapture
		}
		keyEnd := cursor + int(keyBytes)
		afterEnd := keyEnd + int(afterBytes)
		key := raw[cursor:keyEnd:keyEnd]
		var after []byte
		if afterPresent {
			after = raw[keyEnd:afterEnd:afterEnd]
		}
		if previous != nil && bytes.Compare(previous, key) >= 0 ||
			after != nil && vibejson.Validate(after) != nil {
			return sourceCaptureEntry{}, ErrSourceCapture
		}
		workspace.transitions[index] = TailTransition{
			Key: key,
			BeforeWitness: TailBeforeWitness{
				Present: beforePresent, Point: beforePoint, DocumentBytes: uint32(beforeBytes),
				Digest: beforeDigest,
			},
			After: after,
		}
		previous = key
		cursor = afterEnd
	}
	if cursor != len(raw) {
		return sourceCaptureEntry{}, ErrSourceCapture
	}
	record.Transitions = workspace.transitions[:count:count]
	if !validSourceCaptureEntry(record) || c.hashEntry(record, workspace) != record.Digest {
		return sourceCaptureEntry{}, ErrSourceCapture
	}
	return *record, nil
}

func appendSourceCaptureEnvelope(dst []byte, kind uint8, total int) ([]byte, []byte) {
	start := len(dst)
	dst = slices.Grow(dst, total)
	dst = dst[:start+total]
	frame := dst[start:]
	clear(frame)
	copy(frame[0:8], sourceCaptureMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], sourceCaptureFormat)
	frame[10] = kind
	binary.LittleEndian.PutUint32(frame[12:16], uint32(total))
	return dst, frame
}

func validSourceCaptureEnvelope(raw []byte, kind uint8, fixed int) bool {
	return len(raw) >= sourceCaptureEnvelopeBytes && len(raw) >= fixed &&
		uint64(len(raw)) <= math.MaxUint32 && bytes.Equal(raw[0:8], sourceCaptureMagic[:]) &&
		binary.LittleEndian.Uint16(raw[8:10]) == sourceCaptureFormat &&
		raw[10] == kind && raw[11] == 0 &&
		binary.LittleEndian.Uint32(raw[12:16]) == uint32(len(raw))
}

func (c *SourceCapture) hashHeader(
	state replicatedstate.State,
	workspace *SourceCaptureWorkspace,
) [32]byte {
	h := captureHasher(workspace)
	_, _ = h.Write(sourceCaptureHeaderDomain)
	_, _ = h.Write(c.partitioner.digest[:])
	_, _ = h.Write(c.placement[:])
	hashTailFrame(h, &workspace.size, byteview.Bytes(c.partitioner.collection))
	fixed := workspace.fixed[:0]
	fixed = append(fixed, state.DataChainDigest[:]...)
	fixed = append(fixed, state.SnapshotBaseDigest[:]...)
	fixed = append(fixed, state.LastEntryDigest[:]...)
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
	_, _ = h.Write(record.BeforeDataChainDigest[:])
	_, _ = h.Write(record.AfterDataChainDigest[:])
	for index := range record.Transitions {
		transition := &record.Transitions[index]
		hashTailFrame(h, &workspace.size, transition.Key)
		hashBeforeWitness(h, &workspace.fixed, transition.BeforeWitness)
		hashOptionalCaptureFrame(h, &workspace.size, transition.After)
	}
	_ = h.Sum(workspace.digest[:0])
	return workspace.digest
}

func hashBeforeWitness(h hash.Hash, fixed *[144]byte, witness TailBeforeWitness) {
	if !witness.Present {
		fixed[0] = 0
		_, _ = h.Write(fixed[:1])
		return
	}
	fixed[0] = 1
	copy(fixed[1:9], witness.Point[:])
	binary.LittleEndian.PutUint32(fixed[9:13], witness.DocumentBytes)
	copy(fixed[13:45], witness.Digest[:])
	_, _ = h.Write(fixed[:45])
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

func (c *SourceCapture) transitionFollowsCurrent(
	transition replicatedstate.CapturedTransition,
) bool {
	current := c.current
	before := TailSourceCoordinates{OwnershipEpoch: transition.BeforeOwnershipEpoch, RoutingVersion: transition.BeforeRoutingVersion, RouteGeneration: transition.BeforeRouteGeneration}
	after := TailSourceCoordinates{OwnershipEpoch: transition.AfterOwnershipEpoch, RoutingVersion: transition.AfterRoutingVersion, RouteGeneration: transition.AfterRouteGeneration}
	if after != before && (after != c.partitioner.sealCoordinates(before) || transition.MutationCount() != 0 || transition.BeforeDataChainDigest != transition.AfterDataChainDigest) {
		return false
	}
	return current.applied != math.MaxUint64 && transition.Applied == current.applied+1 &&
		transition.Term >= current.term &&
		transition.PreviousEntryDigest == current.entryDigest &&
		transition.BeforeDataChainDigest == current.dataChainDigest &&
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
		record.BeforeDataChainDigest != ([32]byte{}) && record.AfterDataChainDigest != ([32]byte{})
}

func publicationFromState(state replicatedstate.State) sourceCapturePublication {
	return sourceCapturePublication{
		applied: state.Applied, term: state.LastTerm,
		ownershipEpoch:  state.Binding.OwnershipEpoch,
		routingVersion:  state.Binding.RoutingVersion,
		routeGeneration: state.Binding.RouteGeneration,
		entryDigest:     state.LastEntryDigest, dataChainDigest: state.DataChainDigest,
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
		entryDigest:     transition.EntryDigest, dataChainDigest: transition.AfterDataChainDigest,
	}
}

func publicationFromEntry(record sourceCaptureEntry) sourceCapturePublication {
	return sourceCapturePublication{
		applied: record.Applied, term: record.Term,
		ownershipEpoch:  record.AfterOwnershipEpoch,
		routingVersion:  record.AfterRoutingVersion,
		routeGeneration: record.AfterRouteGeneration,
		entryDigest:     record.EntryDigest, dataChainDigest: record.AfterDataChainDigest,
	}
}

func publicationMatchesState(publication sourceCapturePublication, state replicatedstate.State) bool {
	return publication == publicationFromState(state)
}
