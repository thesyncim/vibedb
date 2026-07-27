package storeio

import (
	"errors"
	"fmt"
	"sync"
)

// BufferedCheckpointLab models true buffered-visible publication. It is an
// isolated correctness model, not a production API or on-disk format.
//
// Readers always select one root and resolve its canonical frame directly.
// Dirty frames are pinned model state; there is no reader-visible log, delta,
// tombstone, or version search. A user snapshot and a sealed checkpoint are
// both generation leases. Their first subsequent write therefore copies the
// selected frame; later writes to the new frame materialize in memory until a
// newer observer appears.
const (
	BufferedCheckpointLabMaxKeys   = 16
	BufferedCheckpointLabMaxFrames = 64
)

var (
	ErrBufferedCheckpointLabState = errors.New(
		"vibejson: invalid buffered-checkpoint lab state",
	)
	ErrBufferedCheckpointLabBackpressure = errors.New(
		"vibejson: buffered-checkpoint lab backpressure",
	)
	ErrBufferedCheckpointLabNoChanges = errors.New(
		"vibejson: buffered-checkpoint lab has no changes",
	)
)

type BufferedCheckpointLabOptions struct {
	Keys               int
	FrameCapacity      int
	MaxDirtyFrames     int
	MaxUserSnapshots   int
	InitialValueOffset uint64
}

func (o BufferedCheckpointLabOptions) normalized() (
	BufferedCheckpointLabOptions, error,
) {
	if o.Keys == 0 {
		o.Keys = 4
	}
	if o.FrameCapacity == 0 {
		o.FrameCapacity = 32
	}
	if o.MaxDirtyFrames == 0 {
		o.MaxDirtyFrames = 8
	}
	if o.MaxUserSnapshots == 0 {
		o.MaxUserSnapshots = 8
	}
	if o.Keys < 1 || o.Keys > BufferedCheckpointLabMaxKeys ||
		o.FrameCapacity < o.Keys+2 ||
		o.FrameCapacity > BufferedCheckpointLabMaxFrames ||
		o.MaxDirtyFrames < 1 ||
		o.MaxDirtyFrames > o.FrameCapacity-o.Keys ||
		o.MaxUserSnapshots < 1 ||
		o.MaxUserSnapshots >= 1<<20 ||
		o.InitialValueOffset >
			^uint64(0)-uint64(o.Keys-1) {
		return BufferedCheckpointLabOptions{}, fmt.Errorf(
			"%w: buffered checkpoint options", ErrInvalidWrite,
		)
	}
	return o, nil
}

// BufferedCheckpointLabFrameRef is the canonical cache identity named
// directly by a root. Identity prevents a reclaimed fixed-pool slot from being
// mistaken for an older frame. Birth is immutable and is the generation-lease
// ownership boundary.
type BufferedCheckpointLabFrameRef struct {
	Identity uint64
	Birth    uint64
	Slot     uint8
}

// BufferedCheckpointLabRoot is a small stand-in for a rooted canonical graph.
// A real Store root reaches frames through catalog/tablet/anchor pages; the
// ownership and checkpoint rules are identical. The fixed array makes root
// cuts allocation-free and comparable.
type BufferedCheckpointLabRoot struct {
	Generation uint64
	Refs       [BufferedCheckpointLabMaxKeys]BufferedCheckpointLabFrameRef
	KeyCount   uint8
}

type bufferedCheckpointLabFrame struct {
	identity         uint64
	birth            uint64
	revision         uint64
	value            uint64
	mutations        uint32
	epoch            uint8
	live             bool
	dirty            bool
	historical       bool
	checkpointOutput bool
}

type BufferedCheckpointLabMutationMode uint8

const (
	BufferedCheckpointLabMaterialized BufferedCheckpointLabMutationMode = iota + 1
	BufferedCheckpointLabCopied
)

type BufferedCheckpointLabMutation struct {
	Generation  uint64
	Mode        BufferedCheckpointLabMutationMode
	DirtyFrames uint8
	Coalesced   bool
}

// BufferedCheckpointLabCheckpoint is a sealed, immutable shadow-checkpoint
// plan. Sources are the canonical frames selected by Sealed. Outputs are the
// pages written before After is published. A differing source/output pair is a
// page that was memory-owned but still reachable by the prior durable root and
// therefore had to be shadowed instead of overwritten.
type BufferedCheckpointLabCheckpoint struct {
	Before BufferedCheckpointLabRoot
	Sealed BufferedCheckpointLabRoot
	After  BufferedCheckpointLabRoot

	BeforeValues [BufferedCheckpointLabMaxKeys]uint64
	AfterValues  [BufferedCheckpointLabMaxKeys]uint64
	Sources      [BufferedCheckpointLabMaxKeys]BufferedCheckpointLabFrameRef
	Outputs      [BufferedCheckpointLabMaxKeys]BufferedCheckpointLabFrameRef
	Values       [BufferedCheckpointLabMaxKeys]uint64

	Sequence           uint64
	LogicalMutations   uint64
	CoalescedMutations uint64
	PageWrites         uint8
	ShadowWrites       uint8
}

type BufferedCheckpointLabCrashPhase uint8

const (
	BufferedCheckpointLabBeforeData BufferedCheckpointLabCrashPhase = iota
	BufferedCheckpointLabAfterPartialData
	BufferedCheckpointLabAfterData
	BufferedCheckpointLabAfterRoot
)

type BufferedCheckpointLabRecovered struct {
	Root   BufferedCheckpointLabRoot
	Values [BufferedCheckpointLabMaxKeys]uint64
}

type BufferedCheckpointLabStats struct {
	VisibleGeneration uint64
	SealedGeneration  uint64
	DurableGeneration uint64

	Mutations          uint64
	Materialized       uint64
	Copied             uint64
	BackpressureEvents uint64

	LiveFrames       uint64
	HistoricalFrames uint64
	ActiveDirty      uint64
	SealedDirty      uint64
	PendingWrites    uint64
	ActiveEpoch      uint8
}

type BufferedCheckpointLab struct {
	mu      sync.Mutex
	options BufferedCheckpointLabOptions
	leases  *GenerationLeases

	frames [BufferedCheckpointLabMaxFrames]bufferedCheckpointLabFrame

	durableValues [BufferedCheckpointLabMaxKeys]uint64
	visibleRoot   BufferedCheckpointLabRoot
	sealedRoot    BufferedCheckpointLabRoot
	durableRoot   BufferedCheckpointLabRoot
	pending       BufferedCheckpointLabCheckpoint

	sealedLease  GenerationLease
	nextIdentity uint64
	nextSequence uint64
	mutations    uint64
	materialized uint64
	copied       uint64
	backpressure uint64
	epochDirty   [2]uint8
	activeEpoch  uint8
	sealedEpoch  uint8
	sealed       bool
}

func NewBufferedCheckpointLab(
	options BufferedCheckpointLabOptions,
) (*BufferedCheckpointLab, error) {
	normalized, err := options.normalized()
	if err != nil {
		return nil, err
	}
	leases, err := NewGenerationLeases(GenerationLeaseOptions{
		MaxLeases: normalized.MaxUserSnapshots + 1,
	})
	if err != nil {
		return nil, err
	}
	lab := &BufferedCheckpointLab{
		options: normalized,
		leases:  leases,
	}
	lab.visibleRoot.Generation = 1
	lab.visibleRoot.KeyCount = uint8(normalized.Keys)
	for key := 0; key < normalized.Keys; key++ {
		lab.nextIdentity++
		frame := &lab.frames[key]
		*frame = bufferedCheckpointLabFrame{
			identity: lab.nextIdentity,
			birth:    1,
			revision: 1,
			value:    normalized.InitialValueOffset + uint64(key),
			live:     true,
		}
		lab.visibleRoot.Refs[key] = lab.refForSlot(key)
		lab.durableValues[key] = frame.value
	}
	lab.durableRoot = lab.visibleRoot
	return lab, nil
}

func (l *BufferedCheckpointLab) VisibleRoot() BufferedCheckpointLabRoot {
	if l == nil {
		return BufferedCheckpointLabRoot{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.visibleRoot
}

func (l *BufferedCheckpointLab) SealedRoot() (
	BufferedCheckpointLabRoot, bool,
) {
	if l == nil {
		return BufferedCheckpointLabRoot{}, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.sealed {
		return BufferedCheckpointLabRoot{}, false
	}
	return l.sealedRoot, true
}

func (l *BufferedCheckpointLab) DurableRoot() BufferedCheckpointLabRoot {
	if l == nil {
		return BufferedCheckpointLabRoot{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.durableRoot
}

func (l *BufferedCheckpointLab) ReadVisible(key int) (uint64, error) {
	if l == nil {
		return 0, ErrBufferedCheckpointLabState
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.readRoot(l.visibleRoot, key)
}

func (l *BufferedCheckpointLab) ReadDurable(key int) (uint64, error) {
	if l == nil {
		return 0, ErrBufferedCheckpointLabState
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if key < 0 || key >= int(l.durableRoot.KeyCount) {
		return 0, ErrBufferedCheckpointLabState
	}
	return l.durableValues[key], nil
}

// ReadRoot performs the complete model read: the selected root names exactly
// one canonical frame. No buffered-mutation structure participates.
func (l *BufferedCheckpointLab) ReadRoot(
	root BufferedCheckpointLabRoot, key int,
) (uint64, error) {
	if l == nil {
		return 0, ErrBufferedCheckpointLabState
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.readRoot(root, key)
}

func (l *BufferedCheckpointLab) readRoot(
	root BufferedCheckpointLabRoot, key int,
) (uint64, error) {
	if l == nil || key < 0 || key >= int(root.KeyCount) ||
		root.Generation == 0 {
		return 0, ErrBufferedCheckpointLabState
	}
	frame, ok := l.resolve(root.Refs[key])
	if !ok || frame.revision > root.Generation {
		return 0, ErrBufferedCheckpointLabState
	}
	return frame.value, nil
}

type BufferedCheckpointLabSnapshot struct {
	lab    *BufferedCheckpointLab
	root   BufferedCheckpointLabRoot
	lease  GenerationLease
	closed bool
}

func (l *BufferedCheckpointLab) Snapshot() (
	BufferedCheckpointLabSnapshot, error,
) {
	if l == nil {
		return BufferedCheckpointLabSnapshot{},
			ErrBufferedCheckpointLabState
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.visibleRoot.Generation == 0 {
		return BufferedCheckpointLabSnapshot{},
			ErrBufferedCheckpointLabState
	}
	lease, err := l.leases.Acquire(l.visibleRoot.Generation)
	if err != nil {
		return BufferedCheckpointLabSnapshot{}, err
	}
	return BufferedCheckpointLabSnapshot{
		lab: l, root: l.visibleRoot, lease: lease,
	}, nil
}

func (s *BufferedCheckpointLabSnapshot) Generation() uint64 {
	if s == nil || s.lab == nil {
		return 0
	}
	s.lab.mu.Lock()
	defer s.lab.mu.Unlock()
	if s.closed {
		return 0
	}
	return s.root.Generation
}

func (s *BufferedCheckpointLabSnapshot) Read(key int) (uint64, error) {
	if s == nil || s.lab == nil {
		return 0, ErrBufferedCheckpointLabState
	}
	s.lab.mu.Lock()
	defer s.lab.mu.Unlock()
	if s.closed {
		return 0, ErrBufferedCheckpointLabState
	}
	return s.lab.readRoot(s.root, key)
}

func (s *BufferedCheckpointLabSnapshot) Close() bool {
	if s == nil || s.lab == nil {
		return false
	}
	s.lab.mu.Lock()
	defer s.lab.mu.Unlock()
	if s.closed {
		return false
	}
	s.closed = true
	s.lease.Release()
	s.lab.reclaimFrames()
	return true
}

// Mutate acknowledges after one canonical in-memory frame and the visible
// root have changed. It performs no checkpoint planning or device work.
func (l *BufferedCheckpointLab) Mutate(
	key int, value uint64,
) (BufferedCheckpointLabMutation, error) {
	if l == nil {
		return BufferedCheckpointLabMutation{},
			ErrBufferedCheckpointLabState
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if key < 0 || key >= int(l.visibleRoot.KeyCount) ||
		l.visibleRoot.Generation == ^uint64(0) {
		return BufferedCheckpointLabMutation{},
			ErrBufferedCheckpointLabState
	}
	oldRef := l.visibleRoot.Refs[key]
	oldFrame, ok := l.resolve(oldRef)
	if !ok {
		return BufferedCheckpointLabMutation{},
			ErrBufferedCheckpointLabState
	}
	nextGeneration := l.visibleRoot.Generation + 1
	exclusive := l.leases.SafeFromSnapshots(oldFrame.birth)
	if exclusive {
		if oldFrame.dirty && oldFrame.epoch != l.activeEpoch {
			return BufferedCheckpointLabMutation{},
				ErrBufferedCheckpointLabState
		}
		if !oldFrame.dirty &&
			int(l.epochDirty[l.activeEpoch]) >= l.options.MaxDirtyFrames {
			l.backpressure++
			return BufferedCheckpointLabMutation{},
				ErrBufferedCheckpointLabBackpressure
		}
		coalesced := oldFrame.dirty
		if !oldFrame.dirty {
			oldFrame.dirty = true
			oldFrame.epoch = l.activeEpoch
			l.epochDirty[l.activeEpoch]++
		}
		oldFrame.revision = nextGeneration
		oldFrame.value = value
		oldFrame.mutations++
		next := l.visibleRoot
		next.Generation = nextGeneration
		l.visibleRoot = next
		l.mutations++
		l.materialized++
		return BufferedCheckpointLabMutation{
			Generation:  nextGeneration,
			Mode:        BufferedCheckpointLabMaterialized,
			DirtyFrames: l.epochDirty[l.activeEpoch],
			Coalesced:   coalesced,
		}, nil
	}

	dirtyCredit := oldFrame.dirty &&
		oldFrame.epoch == l.activeEpoch &&
		!l.rootReferences(l.sealedRoot, oldRef)
	if !dirtyCredit &&
		int(l.epochDirty[l.activeEpoch]) >= l.options.MaxDirtyFrames {
		l.backpressure++
		return BufferedCheckpointLabMutation{},
			ErrBufferedCheckpointLabBackpressure
	}
	slot, ok := l.allocateFrame()
	if !ok || l.nextIdentity == ^uint64(0) {
		l.backpressure++
		return BufferedCheckpointLabMutation{},
			ErrBufferedCheckpointLabBackpressure
	}
	l.nextIdentity++
	nextFrame := &l.frames[slot]
	*nextFrame = bufferedCheckpointLabFrame{
		identity:  l.nextIdentity,
		birth:     nextGeneration,
		revision:  nextGeneration,
		value:     value,
		mutations: 1,
		epoch:     l.activeEpoch,
		live:      true,
		dirty:     true,
	}
	l.epochDirty[l.activeEpoch]++
	next := l.visibleRoot
	next.Generation = nextGeneration
	next.Refs[key] = l.refForSlot(slot)
	l.visibleRoot = next
	if dirtyCredit {
		l.makeHistorical(oldFrame)
	}
	l.mutations++
	l.copied++
	return BufferedCheckpointLabMutation{
		Generation:  nextGeneration,
		Mode:        BufferedCheckpointLabCopied,
		DirtyFrames: l.epochDirty[l.activeEpoch],
	}, nil
}

// SealCheckpoint makes the current visible root an immutable checkpoint cut
// and rotates into the second bounded mutation epoch. Dirty frames still
// reachable by the durable root are copied to shadow outputs in After.
func (l *BufferedCheckpointLab) SealCheckpoint() (
	BufferedCheckpointLabCheckpoint, error,
) {
	if l == nil {
		return BufferedCheckpointLabCheckpoint{},
			ErrBufferedCheckpointLabState
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.sealed {
		l.backpressure++
		return BufferedCheckpointLabCheckpoint{},
			ErrBufferedCheckpointLabBackpressure
	}
	var sourceKeys [BufferedCheckpointLabMaxKeys]int
	var sourceRefs [BufferedCheckpointLabMaxKeys]BufferedCheckpointLabFrameRef
	var shadow [BufferedCheckpointLabMaxKeys]bool
	sourceCount, shadowCount := 0, 0
	for key := 0; key < int(l.visibleRoot.KeyCount); key++ {
		ref := l.visibleRoot.Refs[key]
		frame, ok := l.resolve(ref)
		if !ok {
			return BufferedCheckpointLabCheckpoint{},
				ErrBufferedCheckpointLabState
		}
		if !frame.dirty || frame.epoch != l.activeEpoch {
			continue
		}
		sourceKeys[sourceCount] = key
		sourceRefs[sourceCount] = ref
		shadow[sourceCount] = l.rootReferences(l.durableRoot, ref)
		if shadow[sourceCount] {
			shadowCount++
		}
		sourceCount++
	}
	if sourceCount == 0 {
		return BufferedCheckpointLabCheckpoint{},
			ErrBufferedCheckpointLabNoChanges
	}
	if l.freeFrameCapacity() < shadowCount {
		l.backpressure++
		return BufferedCheckpointLabCheckpoint{},
			ErrBufferedCheckpointLabBackpressure
	}
	if uint64(shadowCount) > ^uint64(0)-l.nextIdentity ||
		l.nextSequence == ^uint64(0) {
		return BufferedCheckpointLabCheckpoint{},
			ErrBufferedCheckpointLabState
	}
	nextEpoch := l.activeEpoch ^ 1
	if l.epochDirty[nextEpoch] != 0 {
		return BufferedCheckpointLabCheckpoint{},
			ErrBufferedCheckpointLabState
	}
	lease, err := l.leases.Acquire(l.visibleRoot.Generation)
	if err != nil {
		return BufferedCheckpointLabCheckpoint{}, err
	}

	l.nextSequence++
	plan := BufferedCheckpointLabCheckpoint{
		Before:       l.durableRoot,
		Sealed:       l.visibleRoot,
		After:        l.visibleRoot,
		BeforeValues: l.durableValues,
		Sequence:     l.nextSequence,
	}
	for key := 0; key < int(plan.Sealed.KeyCount); key++ {
		value, readErr := l.readRoot(plan.Sealed, key)
		if readErr != nil {
			lease.Release()
			return BufferedCheckpointLabCheckpoint{},
				ErrBufferedCheckpointLabState
		}
		plan.AfterValues[key] = value
	}
	for rank := 0; rank < sourceCount; rank++ {
		sourceRef := sourceRefs[rank]
		source, ok := l.resolve(sourceRef)
		if !ok {
			lease.Release()
			return BufferedCheckpointLabCheckpoint{},
				ErrBufferedCheckpointLabState
		}
		outputRef := sourceRef
		if shadow[rank] {
			slot, allocated := l.allocateFrame()
			if !allocated {
				lease.Release()
				return BufferedCheckpointLabCheckpoint{},
					ErrBufferedCheckpointLabState
			}
			l.nextIdentity++
			output := &l.frames[slot]
			*output = bufferedCheckpointLabFrame{
				identity:         l.nextIdentity,
				birth:            plan.Sealed.Generation,
				revision:         source.revision,
				value:            source.value,
				live:             true,
				checkpointOutput: true,
			}
			outputRef = l.refForSlot(slot)
			plan.After.Refs[sourceKeys[rank]] = outputRef
			plan.ShadowWrites++
		}
		plan.Sources[plan.PageWrites] = sourceRef
		plan.Outputs[plan.PageWrites] = outputRef
		plan.Values[plan.PageWrites] = source.value
		plan.LogicalMutations += uint64(source.mutations)
		plan.PageWrites++
	}
	plan.CoalescedMutations =
		plan.LogicalMutations - uint64(plan.PageWrites)

	l.sealed = true
	l.sealedEpoch = l.activeEpoch
	l.activeEpoch = nextEpoch
	l.sealedRoot = plan.Sealed
	l.sealedLease = lease
	l.pending = plan
	return plan, nil
}

// CompleteCheckpoint installs the already data-synchronized After root. It
// also rebases untouched visible references from memory-only source identities
// to their equivalent durable shadow identities.
func (l *BufferedCheckpointLab) CompleteCheckpoint(sequence uint64) error {
	if l == nil {
		return ErrBufferedCheckpointLabState
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.sealed || sequence == 0 ||
		sequence != l.pending.Sequence {
		return ErrBufferedCheckpointLabState
	}
	plan := l.pending
	for rank := 0; rank < int(plan.PageWrites); rank++ {
		sourceRef := plan.Sources[rank]
		outputRef := plan.Outputs[rank]
		source, sourceOK := l.resolve(sourceRef)
		output, outputOK := l.resolve(outputRef)
		if !sourceOK || !outputOK ||
			output.value != plan.Values[rank] {
			return ErrBufferedCheckpointLabState
		}
		if sourceRef != outputRef {
			l.makeHistorical(source)
		} else {
			l.clearDirty(source)
			source.historical = false
		}
		output.checkpointOutput = false
		output.historical = false
		l.clearDirty(output)
	}
	l.durableRoot = plan.After
	l.durableValues = plan.AfterValues
	for key := 0; key < int(l.visibleRoot.KeyCount); key++ {
		if l.visibleRoot.Refs[key] == plan.Sealed.Refs[key] {
			l.visibleRoot.Refs[key] = plan.After.Refs[key]
		}
	}
	l.sealedLease.Release()
	l.sealed = false
	l.sealedRoot = BufferedCheckpointLabRoot{}
	l.pending = BufferedCheckpointLabCheckpoint{}
	l.epochDirty[l.sealedEpoch] = 0
	l.reclaimFrames()
	return nil
}

// RecoverBufferedCheckpointLabCheckpoint models every durable prefix of a
// pure shadow checkpoint. Data written before root publication is unreachable;
// after root synchronization every selected output is complete.
func RecoverBufferedCheckpointLabCheckpoint(
	plan BufferedCheckpointLabCheckpoint,
	phase BufferedCheckpointLabCrashPhase,
) (BufferedCheckpointLabRecovered, error) {
	if plan.Sequence == 0 || plan.Before.Generation == 0 ||
		plan.Sealed.Generation <= plan.Before.Generation ||
		plan.After.Generation != plan.Sealed.Generation ||
		plan.PageWrites == 0 ||
		phase > BufferedCheckpointLabAfterRoot {
		return BufferedCheckpointLabRecovered{},
			ErrBufferedCheckpointLabState
	}
	for rank := 0; rank < int(plan.PageWrites); rank++ {
		if plan.Sources[rank].Identity == 0 ||
			plan.Outputs[rank].Identity == 0 {
			return BufferedCheckpointLabRecovered{},
				ErrBufferedCheckpointLabState
		}
	}
	if phase == BufferedCheckpointLabAfterRoot {
		return BufferedCheckpointLabRecovered{
			Root: plan.After, Values: plan.AfterValues,
		}, nil
	}
	return BufferedCheckpointLabRecovered{
		Root: plan.Before, Values: plan.BeforeValues,
	}, nil
}

func (l *BufferedCheckpointLab) Stats() BufferedCheckpointLabStats {
	if l == nil {
		return BufferedCheckpointLabStats{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	stats := BufferedCheckpointLabStats{
		VisibleGeneration: l.visibleRoot.Generation,
		DurableGeneration: l.durableRoot.Generation,
		Mutations:         l.mutations, Materialized: l.materialized,
		Copied: l.copied, BackpressureEvents: l.backpressure,
		ActiveDirty: uint64(l.epochDirty[l.activeEpoch]),
		ActiveEpoch: l.activeEpoch,
	}
	if l.sealed {
		stats.SealedGeneration = l.sealedRoot.Generation
		stats.SealedDirty = uint64(l.epochDirty[l.sealedEpoch])
		stats.PendingWrites = uint64(l.pending.PageWrites)
	}
	for slot := 0; slot < l.options.FrameCapacity; slot++ {
		frame := &l.frames[slot]
		if frame.live {
			stats.LiveFrames++
		}
		if frame.live && frame.historical {
			stats.HistoricalFrames++
		}
	}
	return stats
}

func (l *BufferedCheckpointLab) refForSlot(
	slot int,
) BufferedCheckpointLabFrameRef {
	frame := &l.frames[slot]
	return BufferedCheckpointLabFrameRef{
		Identity: frame.identity,
		Birth:    frame.birth,
		Slot:     uint8(slot),
	}
}

func (l *BufferedCheckpointLab) resolve(
	ref BufferedCheckpointLabFrameRef,
) (*bufferedCheckpointLabFrame, bool) {
	if ref.Identity == 0 || ref.Birth == 0 ||
		int(ref.Slot) >= l.options.FrameCapacity {
		return nil, false
	}
	frame := &l.frames[ref.Slot]
	return frame, frame.live &&
		frame.identity == ref.Identity &&
		frame.birth == ref.Birth
}

func (l *BufferedCheckpointLab) allocateFrame() (int, bool) {
	for slot := 0; slot < l.options.FrameCapacity; slot++ {
		if !l.frames[slot].live {
			return slot, true
		}
	}
	return 0, false
}

func (l *BufferedCheckpointLab) freeFrameCapacity() int {
	free := 0
	for slot := 0; slot < l.options.FrameCapacity; slot++ {
		if !l.frames[slot].live {
			free++
		}
	}
	return free
}

func (l *BufferedCheckpointLab) rootReferences(
	root BufferedCheckpointLabRoot,
	ref BufferedCheckpointLabFrameRef,
) bool {
	if ref.Identity == 0 || root.Generation == 0 {
		return false
	}
	for key := 0; key < int(root.KeyCount); key++ {
		if root.Refs[key] == ref {
			return true
		}
	}
	return false
}

func (l *BufferedCheckpointLab) frameRooted(
	ref BufferedCheckpointLabFrameRef,
) bool {
	return l.rootReferences(l.visibleRoot, ref) ||
		l.rootReferences(l.sealedRoot, ref) ||
		l.rootReferences(l.pending.After, ref)
}

func (l *BufferedCheckpointLab) clearDirty(
	frame *bufferedCheckpointLabFrame,
) {
	if frame == nil || !frame.dirty {
		return
	}
	if frame.epoch < uint8(len(l.epochDirty)) &&
		l.epochDirty[frame.epoch] != 0 {
		l.epochDirty[frame.epoch]--
	}
	frame.dirty = false
	frame.mutations = 0
}

func (l *BufferedCheckpointLab) makeHistorical(
	frame *bufferedCheckpointLabFrame,
) {
	if frame == nil {
		return
	}
	l.clearDirty(frame)
	frame.historical = true
	frame.checkpointOutput = false
}

func (l *BufferedCheckpointLab) reclaimFrames() {
	for slot := 0; slot < l.options.FrameCapacity; slot++ {
		frame := &l.frames[slot]
		if !frame.live || frame.checkpointOutput {
			continue
		}
		ref := l.refForSlot(slot)
		if l.frameRooted(ref) ||
			!l.leases.SafeFromSnapshots(frame.birth) {
			continue
		}
		l.clearDirty(frame)
		*frame = bufferedCheckpointLabFrame{}
	}
}
