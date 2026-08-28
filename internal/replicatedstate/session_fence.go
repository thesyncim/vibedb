package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
)

// Historical fences exist only while a physical result slot refers to them.
// Current-fence references are derived from the existing slot count, so the
// steady-state command path writes no additional rows.
const sessionFenceBytes = 112

var sessionFencePrefix = [...]byte{0, 1}
var sessionFenceDomain = []byte("vibedb/session-fence\x00")

type sessionFence struct {
	routing, generation, start, end, refs uint64
	origin                                [32]byte
}

type sessionFenceLookup struct {
	snapshot pointSnapshot
	fences   map[[18]byte]sessionFence
}

func (l sessionFenceLookup) get(state State, routing, generation uint64) (sessionFence, error) {
	if l.fences != nil {
		f, ok := l.fences[sessionFenceKey(routing, generation)]
		if !ok || !validHistoricalSessionFence(state, f) {
			return sessionFence{}, ErrSessionCorrupt
		}
		return f, nil
	}
	if l.snapshot.value == nil && l.snapshot.overlay == nil {
		return sessionFence{}, ErrSessionCorrupt
	}
	return sessionFenceAt(l.snapshot, state, routing, generation)
}

func bindingMatchesFenceOrigin(initial Binding, state State) bool {
	if state.FenceOriginDigest == ([32]byte{}) {
		return bindingAdvancesFrom(initial, state.Binding)
	}
	if initial == state.Binding {
		return true
	}
	if sessionFenceOrigin(initial) != state.FenceOriginDigest {
		return false
	}
	// Identity/schema equality remains exact; only coordinates previously
	// advanced by authenticated ownership commands are taken from the state.
	normalized := initial
	normalized.OwnershipEpoch, normalized.RoutingVersion, normalized.RouteGeneration = state.Binding.OwnershipEpoch, state.Binding.RoutingVersion, state.Binding.RouteGeneration
	normalized.OwnedRange = state.Binding.OwnedRange
	return normalized == state.Binding && ownershipRangeContains(initial.OwnedRange, state.Binding.OwnedRange) &&
		initial.OwnershipEpoch <= state.Binding.OwnershipEpoch && initial.RoutingVersion <= state.Binding.RoutingVersion && initial.RouteGeneration <= state.Binding.RouteGeneration
}

func sessionFenceKey(routing, generation uint64) (key [18]byte) {
	copy(key[:2], sessionFencePrefix[:])
	binary.BigEndian.PutUint64(key[2:10], routing)
	binary.BigEndian.PutUint64(key[10:18], generation)
	return key
}

func appendSessionFence(dst []byte, f sessionFence) ([]byte, error) {
	if f.routing == 0 || f.generation == 0 || f.refs == 0 || f.start >= f.end || f.origin == ([32]byte{}) {
		return dst, ErrSessionCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, sessionFenceBytes)...)
	raw := dst[start:]
	copy(raw[:8], "VDBFENCE")
	for i, v := range [...]uint64{f.routing, f.generation, f.start, f.end, f.refs} {
		binary.LittleEndian.PutUint64(raw[8+i*8:16+i*8], v)
	}
	copy(raw[48:80], f.origin[:])
	h := sha256.New()
	_, _ = h.Write(sessionFenceDomain)
	_, _ = h.Write(raw[:80])
	_ = h.Sum(raw[80:80])
	return dst, nil
}

func openSessionFence(raw []byte) (sessionFence, error) {
	if len(raw) != sessionFenceBytes || !bytes.Equal(raw[:8], []byte("VDBFENCE")) {
		return sessionFence{}, ErrSessionCorrupt
	}
	f := sessionFence{routing: binary.LittleEndian.Uint64(raw[8:16]), generation: binary.LittleEndian.Uint64(raw[16:24]), start: binary.LittleEndian.Uint64(raw[24:32]), end: binary.LittleEndian.Uint64(raw[32:40]), refs: binary.LittleEndian.Uint64(raw[40:48])}
	copy(f.origin[:], raw[48:80])
	var canonical [sessionFenceBytes]byte
	encoded, err := appendSessionFence(canonical[:0], f)
	if err != nil || !bytes.Equal(raw, encoded) {
		return sessionFence{}, ErrSessionCorrupt
	}
	return f, nil
}

func sessionFenceOrigin(binding Binding) [32]byte {
	// Schema generation has its own authenticated rollout and can change while
	// the original allocation/serving construction identity remains unchanged.
	binding.SchemaGeneration = 0
	return SplitCaptureBindingDigest(binding)
}

func validHistoricalSessionFence(state State, f sessionFence) bool {
	return f.origin == state.FenceOriginDigest && f.origin != ([32]byte{}) && f.refs != 0 &&
		f.refs <= state.HistoricalFenceSlots && f.start < f.end && f.end <= state.FenceApplied &&
		f.routing < state.Binding.RoutingVersion && f.generation < state.Binding.RouteGeneration
}

func sessionFenceAt(snapshot pointSnapshot, state State, routing, generation uint64) (sessionFence, error) {
	key := sessionFenceKey(routing, generation)
	var buffer [sessionFenceBytes]byte
	raw, found, err := snapshot.appendRaw(buffer[:0], key[:])
	if err != nil || !found {
		return sessionFence{}, fmt.Errorf("%w: missing historical fence: %v", ErrSessionCorrupt, err)
	}
	f, err := openSessionFence(raw)
	if err != nil || f.routing != routing || f.generation != generation || !validHistoricalSessionFence(state, f) {
		return sessionFence{}, ErrSessionCorrupt
	}
	return f, nil
}

type sessionFenceDelta struct {
	historicalSlotsRemoved, historicalRowsRemoved uint64
	unfencedAdded, unfencedRemoved                uint64
}

func applySessionFenceDelta(state *State, d sessionFenceDelta) error {
	if state.HistoricalFenceSlots < d.historicalSlotsRemoved || state.HistoricalFenceCount < d.historicalRowsRemoved ||
		state.UnfencedSessionSlots < d.unfencedRemoved || state.UnfencedSessionSlots-d.unfencedRemoved > math.MaxUint64-d.unfencedAdded {
		return ErrSessionCorrupt
	}
	state.HistoricalFenceSlots -= d.historicalSlotsRemoved
	state.HistoricalFenceCount -= d.historicalRowsRemoved
	state.UnfencedSessionSlots = state.UnfencedSessionSlots - d.unfencedRemoved + d.unfencedAdded
	return nil
}

func accountSessionFencePlan(state State, snapshot pointSnapshot, plan commandPlan) (commandPlan, error) {
	if !plan.writeSlot && !plan.deleteSession {
		return plan, nil
	}
	if plan.writeSlot && plan.resultCode == ResultStaleFence {
		plan.fenceDelta.unfencedAdded++
	}
	if state.HistoricalFenceSlots == 0 && state.UnfencedSessionSlots == 0 {
		return plan, nil
	}
	if plan.writeSlot && plan.newPhysicalSlot {
		return plan, nil
	}
	// Only a release can drain several historical fences. Its ring already has
	// a configured physical bound; the map coalesces each old-fence update once.
	var removals map[[18]byte]uint64
	remove := func(slot SessionSlotView) error {
		if slot.ResultCode == ResultStaleFence {
			plan.fenceDelta.unfencedRemoved++
			return nil
		}
		if slot.RoutingVersion == state.Binding.RoutingVersion && slot.RouteGeneration == state.Binding.RouteGeneration {
			return nil
		}
		if removals == nil {
			removals = make(map[[18]byte]uint64)
		}
		key := sessionFenceKey(slot.RoutingVersion, slot.RouteGeneration)
		removals[key]++
		plan.fenceDelta.historicalSlotsRemoved++
		return nil
	}
	read := func(key []byte) error {
		var buffer [MaxSessionSlotRecordBytes]byte
		raw, found, err := snapshot.appendRaw(buffer[:0], key)
		if err != nil || !found {
			return ErrSessionCorrupt
		}
		slot, err := OpenSessionSlot(raw)
		if err != nil {
			return err
		}
		want, err := SessionSlotStorageKey(slot.SessionDigest, slot.Slot)
		if err != nil || !bytes.Equal(key, want[:]) {
			return ErrSessionCorrupt
		}
		if err := validateStoredSessionSlot(state, slot, sessionFenceLookup{snapshot: snapshot}); err != nil {
			return err
		}
		return remove(slot)
	}
	if plan.deleteSession {
		for i := uint16(0); i < plan.deleteSlots; i++ {
			key, err := SessionSlotStorageKey(plan.sessionDigest, i)
			if err != nil {
				return commandPlan{}, err
			}
			if err := read(key[:]); err != nil {
				return commandPlan{}, err
			}
		}
	} else if err := read(plan.slotKey[:]); err != nil {
		return commandPlan{}, err
	}
	keys := make([][18]byte, 0, len(removals))
	for key := range removals {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b [18]byte) int { return bytes.Compare(a[:], b[:]) })
	for _, key := range keys {
		count := removals[key]
		f, err := sessionFenceAt(snapshot, state, binary.BigEndian.Uint64(key[2:10]), binary.BigEndian.Uint64(key[10:18]))
		if err != nil || f.refs < count {
			return commandPlan{}, ErrSessionCorrupt
		}
		f.refs -= count
		row := transactionRowMutation{key: bytes.Clone(key[:]), delete: f.refs == 0}
		if f.refs == 0 {
			plan.fenceDelta.historicalRowsRemoved++
		} else {
			row.value, err = appendSessionFence(nil, f)
			if err != nil {
				return commandPlan{}, err
			}
		}
		plan.systemRows = append(plan.systemRows, row)
	}
	return plan, nil
}

func archiveSessionFence(current State, next *State) ([]transactionRowMutation, error) {
	if current.HistoricalFenceSlots > current.SessionSlotCount || current.UnfencedSessionSlots > current.SessionSlotCount-current.HistoricalFenceSlots {
		return nil, ErrSessionCorrupt
	}
	next.FenceOriginDigest = current.FenceOriginDigest
	if next.FenceOriginDigest == ([32]byte{}) {
		next.FenceOriginDigest = sessionFenceOrigin(current.Binding)
	}
	next.FenceApplied = next.Applied
	refs := current.SessionSlotCount - current.HistoricalFenceSlots - current.UnfencedSessionSlots
	if refs == 0 {
		return nil, nil
	}
	f := sessionFence{routing: current.Binding.RoutingVersion, generation: current.Binding.RouteGeneration, start: current.FenceApplied, end: next.Applied, refs: refs, origin: next.FenceOriginDigest}
	key := sessionFenceKey(f.routing, f.generation)
	value, err := appendSessionFence(nil, f)
	if err != nil {
		return nil, err
	}
	next.HistoricalFenceCount++
	next.HistoricalFenceSlots += refs
	return []transactionRowMutation{{key: bytes.Clone(key[:]), value: value}}, nil
}
