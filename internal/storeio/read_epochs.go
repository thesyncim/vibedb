package storeio

import (
	"sync/atomic"
	"unsafe"
)

// readEpochSlots is fixed so the writer's complete reader scan is a handful of
// loads from one cache line. Direct point reads occupy a slot for well under a
// microsecond, so eight concurrent slots absorb far more read parallelism than
// the store's measured read throughput ever produces; a full table falls back
// to the still-correct generation-lease path rather than spinning.
const readEpochSlots = 8

// readEpochActive distinguishes an occupied slot from the idle zero word while
// keeping the protected generation readable from the same word. Generations
// approaching 1<<63 are unreachable in practice (the buffered lane already
// rejects generations at 1<<48); Enter still range-checks and refuses rather
// than silently aliasing the bit.
const readEpochActive = uint64(1) << 63

// readEpochDivertClosed is the sticky component of the divert word. The low
// bits form a nesting counter for writer fences; the high bit is set once at
// Close and never cleared.
const readEpochDivertClosed = uint32(1) << 31

// ReadEpochs is the direct-read reader registry: a fixed table of per-read
// epoch slots that replaces the per-call snapshot-gate/lease round trip on the
// collection's point-read path. A reader claims one slot, publishes the
// generation it is about to read, and releases the slot when the call returns.
// The serialized writer derives two facts from the table without taking any
// lock:
//
//   - the safe-reclaim horizon: no extent retired at or after the oldest
//     active slot generation may be reused (Minimum), and
//   - destructive-action eligibility: no frame may be patched or immediately
//     retired while any slot is active (AnyActive), evaluated inside a writer
//     fence. A slot's generation is a conservative floor, not a per-page
//     reachability ceiling.
//
// Correctness rests on two store/load protocols, both relying on Go's
// sequentially consistent atomics:
//
// Reclaim-horizon protocol. A reader publishes its slot generation and then
// re-loads the visible-state pointer, proceeding only if the pointer did not
// change. The writer stores a new visible state before any reclaim scan for
// that publication's retirements. If the scan misses a reader's slot store,
// the scan preceded that store in the total order, so the reader's re-load —
// which follows its store — must observe the newer pointer and retry. A reader
// can therefore hold an old generation only if the scan saw its slot, in which
// case the horizon accounts for it.
//
// Writer-fence protocol. Decisions that must remain true while the writer acts
// (in-place patches, immediate retirement of superseded pages) run between
// BeginWriterFence and EndWriterFence. The writer raises the divert word and
// then scans the slots; a reader claims its slot and then loads the divert
// word. If the scan saw an empty table, every concurrent reader's claim
// follows the scan in the total order, so its divert load follows the raised
// fence and the reader routes to the snapshot-gate slow path, which blocks
// until the writer publishes. If a reader instead proceeded, its claim
// preceded the fence store's scan and the scan vetoes the writer's action.
type ReadEpochs struct {
	_     [64]byte
	slots [readEpochSlots]atomic.Uint64
	_     [64]byte
	// divert shares no line with the slots: readers load it on every entry
	// while only the writer mutates it, so it stays in shared state across
	// reader cores instead of bouncing with slot claims.
	divert atomic.Uint32
	_      [60]byte
}

// ReadEpoch is one claimed slot. The zero value is inert; Exit is idempotent
// for one claimed value and must be called on the goroutine that entered.
type ReadEpoch struct {
	owner *ReadEpochs
	index uint32
}

// ReadEpochStats is the bounded direct-reader summary used by reclamation
// diagnostics and pressure recovery.
type ReadEpochStats struct {
	Active            uint64
	MinimumGeneration uint64
}

// NewReadEpochs allocates the fixed table once at collection construction so
// steady-state reads allocate nothing.
func NewReadEpochs() *ReadEpochs {
	return &ReadEpochs{}
}

// epochSlotHint derives a goroutine-affine starting slot from the stack
// address of its own frame. Distribution, not stability, is what matters: two
// goroutines with distinct stacks probe different first slots, and a stale
// hint after stack growth merely shifts the probe start.
func epochSlotHint() uint32 {
	var marker byte
	address := uintptr(unsafe.Pointer(&marker))
	return uint32((uint64(address) >> 4) * 0x9e3779b97f4a7c15 >> 56)
}

// Enter claims one slot protecting generation. ok=false means the caller must
// use the generation-lease slow path: the table is full, a writer fence or
// Close is diverting readers, or generation cannot be encoded. Enter never
// blocks and never allocates.
func (e *ReadEpochs) Enter(generation uint64) (ReadEpoch, bool) {
	if e == nil || generation == 0 || generation >= readEpochActive ||
		e.divert.Load() != 0 {
		return ReadEpoch{}, false
	}
	word := generation | readEpochActive
	start := epochSlotHint()
	for probe := uint32(0); probe < readEpochSlots; probe++ {
		index := (start + probe) % readEpochSlots
		if e.slots[index].CompareAndSwap(0, word) {
			return ReadEpoch{owner: e, index: index}, true
		}
	}
	return ReadEpoch{}, false
}

// Diverted reports whether a writer fence or Close requires the caller to
// abandon its claimed slot for the gated slow path. Readers must check it
// after Enter: the claim/scan ordering documented on ReadEpochs only holds
// when the divert load follows the slot publication.
func (e *ReadEpochs) Diverted() bool {
	return e != nil && e.divert.Load() != 0
}

// Exit releases the slot. The sequentially consistent store orders every page
// read before the release, so a writer that observes the empty slot cannot
// reclaim bytes the reader is still copying.
func (r ReadEpoch) Exit() {
	if r.owner == nil {
		return
	}
	r.owner.slots[r.index].Store(0)
}

// BeginWriterFence diverts new direct readers to the snapshot-gate slow path.
// The caller must already hold the snapshot gate's write side so diverted
// readers block instead of spinning, and must scan AnyActive after this call
// for the fence protocol to hold. Fences nest.
func (e *ReadEpochs) BeginWriterFence() {
	if e != nil {
		e.divert.Add(1)
	}
}

// EndWriterFence releases one fence level after the writer's dependent
// publication is complete.
func (e *ReadEpochs) EndWriterFence() {
	if e != nil {
		e.divert.Add(^uint32(0))
	}
}

// AnyActive reports whether any direct read currently occupies a slot. Used as
// an action veto it is meaningful only inside a writer fence, exactly like
// GenerationLeases.AnyActive is meaningful only under the snapshot gate.
func (e *ReadEpochs) AnyActive() bool {
	if e == nil {
		return false
	}
	for index := range e.slots {
		if e.slots[index].Load() != 0 {
			return true
		}
	}
	return false
}

// Minimum returns the oldest generation an active direct read protects, or
// current's successor when the table is idle, matching the lease table's
// reclamation-floor contract.
func (e *ReadEpochs) Minimum(current uint64) uint64 {
	minimum := generationSuccessor(current)
	if e == nil {
		return minimum
	}
	for index := range e.slots {
		word := e.slots[index].Load()
		if word != 0 {
			if generation := word &^ readEpochActive; generation < minimum {
				minimum = generation
			}
		}
	}
	return minimum
}

// Stats returns active direct-reader count and their conservative reclamation
// floor in one fixed eight-slot scan.
func (e *ReadEpochs) Stats(current uint64) ReadEpochStats {
	minimum := generationSuccessor(current)
	if e == nil {
		return ReadEpochStats{MinimumGeneration: minimum}
	}
	active := uint64(0)
	for index := range e.slots {
		word := e.slots[index].Load()
		if word != 0 {
			active++
			if generation := word &^ readEpochActive; generation < minimum {
				minimum = generation
			}
		}
	}
	return ReadEpochStats{
		Active: active, MinimumGeneration: minimum,
	}
}

// BeginClose permanently diverts new direct readers without requiring existing
// slots to be quiescent. Collection shutdown calls it at admission time, before
// persistence and teardown work, so a later reader cannot fall through to a
// slow registry that has not closed yet.
func (e *ReadEpochs) BeginClose() {
	if e != nil {
		e.divert.Or(readEpochDivertClosed)
	}
}

// Close permanently diverts new readers, then reports whether the table is
// quiescent. Like GenerationLeases.Close it returns ErrLeasesActive until the
// last in-flight read exits and stays closed either way, so a caller retries
// Close rather than racing new entries.
func (e *ReadEpochs) Close() error {
	if e == nil {
		return nil
	}
	e.BeginClose()
	if e.AnyActive() {
		return ErrLeasesActive
	}
	return nil
}
