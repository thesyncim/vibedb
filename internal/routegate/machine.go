package routegate

import "math"

const (
	// MaxSnapshotBytes is the hard encoded-state admission ceiling used to
	// derive the maximum retained record count. It bounds one shard's control
	// state, not the targets in one distributed request.
	MaxSnapshotBytes = uint64(64 << 20)

	// SnapshotHeaderBytes, SnapshotRecordBytes, and SnapshotChecksumBytes are
	// the exact canonical snapshot geometry.
	SnapshotHeaderBytes   = 136
	SnapshotRecordBytes   = 80
	SnapshotChecksumBytes = 4

	// MaxRetainedRecords is the derived per-shard state bound. Distributed
	// requests are streamed and do not encode or consume an aggregate ceiling.
	MaxRetainedRecords = (MaxSnapshotBytes - SnapshotHeaderBytes -
		SnapshotChecksumBytes) / SnapshotRecordBytes
)

// PinState is the durable state of one shared request pin.
type PinState uint8

const (
	PinInvalid PinState = iota
	PinHeld
	PinReleased
)

// DrainState is the durable state of the sole exclusive topology drain.
type DrainState uint8

const (
	DrainNone DrainState = iota
	DrainPending
	DrainActive
	DrainReleased
)

// Reason explains a deterministic transition result. Rejections are normal
// replicated outcomes rather than Go errors.
type Reason uint8

const (
	ReasonInvalid Reason = iota
	ReasonAcquired
	ReasonReleased
	ReasonDrainPending
	ReasonDrainAcquired
	ReasonDrainReleased
	ReasonCompacted
	ReasonIdempotent
	ReasonAlreadyReleased
	ReasonBlockedByDrain
	ReasonIdentityConflict
	ReasonStaleEpoch
	ReasonCapacity
	ReasonNotFound
	ReasonDrainBusy
	ReasonExhausted
)

// PinRecord is a fixed semantic record. Epoch remains the acquisition epoch;
// active pins survive admission-epoch compaction and may release afterward.
type PinRecord struct {
	Identity Identity
	Binding  Binding
	Epoch    uint64
	State    PinState
}

// storedPin deliberately omits Identity because it is already the hash-table
// key. Keeping the duplicate out of every live record saves 32 bytes per pin.
type storedPin struct {
	Binding Binding
	Epoch   uint64
	State   PinState
}

// DrainRecord is the one pending, active, or released exclusive operation.
type DrainRecord struct {
	Identity Identity
	Binding  Binding
	Epoch    uint64
	State    DrainState
}

// Status is a constant-size state cut returned after every transition.
type Status struct {
	Revision        uint64
	Epoch           uint64
	ActivePins      uint64
	ReleasedPins    uint64
	RetainedRecords uint64
	Drain           DrainRecord
}

// Outcome is the deterministic result of one replicated transition. Mutated
// is false for idempotent and rejected commands.
type Outcome struct {
	Reason  Reason
	Mutated bool
	Status  Status
}

// Machine is one shard-local gate. It is intentionally not synchronized:
// callers apply commands on the owning Raft state-machine lane. maxRecords is
// a total retained-state admission bound, never a per-request target cap.
type Machine struct {
	revision     uint64
	epoch        uint64
	activePins   uint64
	releasedPins uint64
	retainedPins uint64
	maxRecords   uint64
	pins         map[Identity]storedPin
	drain        DrainRecord
}

// NewMachine constructs an empty gate. initialEpoch and maxRecords must be
// nonzero; maxRecords is bounded by the exact 64 MiB snapshot geometry.
func NewMachine(initialEpoch, maxRecords uint64) (*Machine, bool) {
	if initialEpoch == 0 || maxRecords == 0 || maxRecords > MaxRetainedRecords {
		return nil, false
	}
	return &Machine{
		epoch: initialEpoch, maxRecords: maxRecords,
		pins: make(map[Identity]storedPin),
	}, true
}

// Status returns the current constant-size counters and drain record.
func (machine *Machine) Status() Status {
	if machine == nil {
		return Status{}
	}
	return Status{
		Revision: machine.revision, Epoch: machine.epoch,
		ActivePins: machine.activePins, ReleasedPins: machine.releasedPins,
		RetainedRecords: machine.retainedPins, Drain: machine.drain,
	}
}

// Pin returns one retained shared record.
func (machine *Machine) Pin(identity Identity) (PinRecord, bool) {
	if machine == nil {
		return PinRecord{}, false
	}
	record, ok := machine.pins[identity]
	if !ok {
		return PinRecord{}, false
	}
	return PinRecord{
		Identity: identity, Binding: record.Binding, Epoch: record.Epoch, State: record.State,
	}, true
}

// Apply performs one deterministic replicated transition.
func (machine *Machine) Apply(command Command) Outcome {
	return machine.apply(command, true)
}

// Preview returns the exact next outcome without mutating the gate. It reads
// at most the addressed pin plus the fixed head and allocates no memory. A
// state-machine adapter can therefore persist the derived rows atomically and
// invoke Apply only after durable commit, without cloning the retained map.
func (machine *Machine) Preview(command Command) Outcome {
	if machine == nil {
		return Outcome{Reason: ReasonInvalid}
	}
	preview := *machine
	return preview.apply(command, false)
}

func (machine *Machine) apply(command Command, writePins bool) Outcome {
	if machine == nil || !validCommand(command) {
		return Outcome{Reason: ReasonInvalid}
	}
	var reason Reason
	var mutated bool
	switch command.Operation {
	case OperationAcquireShared:
		reason, mutated = machine.acquire(command, writePins)
	case OperationReleaseShared:
		reason, mutated = machine.release(command, writePins)
	case OperationBeginExclusive:
		reason, mutated = machine.beginDrain(command)
	case OperationReleaseExclusive:
		reason, mutated = machine.releaseDrain(command)
	case OperationCompactReleased:
		reason, mutated = machine.compact(command, writePins)
	default:
		reason = ReasonInvalid
	}
	return Outcome{Reason: reason, Mutated: mutated, Status: machine.Status()}
}

func (machine *Machine) acquire(command Command, writePin bool) (Reason, bool) {
	if command.Epoch != machine.epoch {
		return ReasonStaleEpoch, false
	}
	if record, ok := machine.pins[command.Identity]; ok {
		if record.Binding != command.Binding || record.Epoch != command.Epoch {
			return ReasonIdentityConflict, false
		}
		if record.State == PinReleased {
			return ReasonAlreadyReleased, false
		}
		return ReasonIdempotent, false
	}
	if machine.drain.State == DrainPending || machine.drain.State == DrainActive {
		return ReasonBlockedByDrain, false
	}
	if machine.retainedPins == machine.maxRecords {
		return ReasonCapacity, false
	}
	if !machine.canMutate() {
		return ReasonExhausted, false
	}
	if writePin {
		machine.pins[command.Identity] = storedPin{
			Binding: command.Binding, Epoch: command.Epoch, State: PinHeld,
		}
	}
	machine.activePins++
	machine.retainedPins++
	machine.revision++
	return ReasonAcquired, true
}

func (machine *Machine) release(command Command, writePin bool) (Reason, bool) {
	if record, ok := machine.pins[command.Identity]; ok {
		if record.Binding != command.Binding || record.Epoch != command.Epoch {
			return ReasonIdentityConflict, false
		}
		if record.State == PinReleased {
			return ReasonAlreadyReleased, false
		}
		if !machine.canMutate() {
			return ReasonExhausted, false
		}
		if writePin {
			record.State = PinReleased
			machine.pins[command.Identity] = record
		}
		machine.activePins--
		machine.releasedPins++
		machine.revision++
		if machine.activePins == 0 && machine.drain.State == DrainPending {
			machine.drain.State = DrainActive
		}
		return ReasonReleased, true
	}
	if command.Epoch != machine.epoch {
		return ReasonStaleEpoch, false
	}
	if machine.retainedPins == machine.maxRecords {
		return ReasonCapacity, false
	}
	if !machine.canMutate() {
		return ReasonExhausted, false
	}
	// Release-before-acquire is a valid terminal ordering: retain a tombstone
	// so a delayed acquire cannot recreate the shared pin.
	if writePin {
		machine.pins[command.Identity] = storedPin{
			Binding: command.Binding, Epoch: command.Epoch, State: PinReleased,
		}
	}
	machine.releasedPins++
	machine.retainedPins++
	machine.revision++
	return ReasonReleased, true
}

func (machine *Machine) beginDrain(command Command) (Reason, bool) {
	if command.Epoch != machine.epoch {
		return ReasonStaleEpoch, false
	}
	if machine.drain.State == DrainPending || machine.drain.State == DrainActive {
		if machine.drain.Identity == command.Identity &&
			machine.drain.Binding == command.Binding && machine.drain.Epoch == command.Epoch {
			return ReasonIdempotent, false
		}
		return ReasonIdentityConflict, false
	}
	if !machine.canMutate() {
		return ReasonExhausted, false
	}
	state, reason := DrainActive, ReasonDrainAcquired
	if machine.activePins != 0 {
		state, reason = DrainPending, ReasonDrainPending
	}
	machine.drain = DrainRecord{
		Identity: command.Identity, Binding: command.Binding,
		Epoch: command.Epoch, State: state,
	}
	machine.revision++
	return reason, true
}

func (machine *Machine) releaseDrain(command Command) (Reason, bool) {
	if machine.drain.Identity == command.Identity &&
		machine.drain.Binding == command.Binding && machine.drain.Epoch == command.Epoch &&
		machine.drain.State == DrainReleased {
		return ReasonIdempotent, false
	}
	if command.Epoch != machine.epoch {
		return ReasonStaleEpoch, false
	}
	if machine.drain.State == DrainNone {
		return ReasonNotFound, false
	}
	if machine.drain.Identity != command.Identity || machine.drain.Binding != command.Binding ||
		machine.drain.Epoch != command.Epoch {
		return ReasonIdentityConflict, false
	}
	if machine.drain.State != DrainActive || machine.activePins != 0 {
		return ReasonDrainBusy, false
	}
	if !machine.canAdvanceEpoch() {
		return ReasonExhausted, false
	}
	machine.drain.State = DrainReleased
	machine.epoch++
	machine.revision++
	// An active drain proves every old shared pin is released. Keep tombstones
	// until bounded incremental compaction: releasing a topology drain must stay
	// constant-write regardless of shard concurrency.
	return ReasonDrainReleased, true
}

func (machine *Machine) compact(command Command, writePin bool) (Reason, bool) {
	if command.Epoch != machine.epoch {
		return ReasonStaleEpoch, false
	}
	if machine.drain.State == DrainPending || machine.drain.State == DrainActive {
		return ReasonDrainBusy, false
	}
	if !machine.canAdvanceEpoch() {
		return ReasonExhausted, false
	}
	identity, found := machine.CompactCandidate()
	if found {
		if writePin {
			delete(machine.pins, identity)
		}
		machine.releasedPins--
		machine.retainedPins--
	}
	if machine.drain.State == DrainReleased {
		machine.drain = DrainRecord{}
	}
	machine.epoch++
	machine.revision++
	return ReasonCompacted, true
}

// CompactCandidate returns the deterministic single tombstone reclaimed by
// the next successful compaction. One-record compaction keeps every replicated
// transition constant-write without imposing a target-count ceiling.
func (machine *Machine) CompactCandidate() (Identity, bool) {
	if machine == nil {
		return Identity{}, false
	}
	var candidate Identity
	found := false
	for identity, record := range machine.pins {
		if record.State != PinReleased || found && !identityLess(identity, candidate) {
			continue
		}
		candidate, found = identity, true
	}
	return candidate, found
}

func identityLess(left, right Identity) bool {
	for index := range left {
		if left[index] != right[index] {
			return left[index] < right[index]
		}
	}
	return false
}

func (machine *Machine) canMutate() bool { return machine.revision != math.MaxUint64 }

func (machine *Machine) canAdvanceEpoch() bool {
	return machine.revision != math.MaxUint64 && machine.epoch != math.MaxUint64
}
