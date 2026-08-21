package topologyscheduler

import (
	"errors"

	"github.com/thesyncim/vibedb/autosplit"
)

const (
	// MaxFeedbackEntries bounds exact source incarnations retained by one
	// scheduler owner.
	MaxFeedbackEntries = 1024
	feedbackIndexSlots = 2048

	feedbackIndexEmpty     uint16 = 0
	feedbackIndexTombstone uint16 = ^uint16(0)
)

// ErrInvalidFeedback reports a stale, duplicate, malformed, or over-capacity
// feedback transition.
var ErrInvalidFeedback = errors.New("topologyscheduler: invalid split feedback transition")

// FeedbackOutcome classifies one completed scheduling attempt.
type FeedbackOutcome uint8

const (
	// FeedbackSucceeded and FeedbackCancelled release a source immediately;
	// FeedbackRetryable preserves its failure history and applies backoff.
	FeedbackSucceeded FeedbackOutcome = iota + 1
	FeedbackRetryable
	FeedbackCancelled
)

// FeedbackPolicy expresses retry delay in source evidence windows rather than
// wall time. This keeps scheduling deterministic across clock skew.
type FeedbackPolicy struct {
	BaseRetryWindows uint64
	MaxRetryWindows  uint64
}

// DefaultFeedbackPolicy returns a capped 2, 4, 8, ... 64-window retry curve.
func DefaultFeedbackPolicy() FeedbackPolicy {
	return FeedbackPolicy{BaseRetryWindows: 2, MaxRetryWindows: 64}
}

type sourceKey struct {
	low  uint64
	high uint64
}

type feedbackState uint8

const (
	feedbackEmpty feedbackState = iota
	feedbackInFlight
	feedbackCooling
)

type feedbackEntry struct {
	key            sourceKey
	lastWindow     uint64
	eligibleWindow uint64
	stamp          uint64
	failures       uint8
	state          feedbackState
}

// FeedbackTable is a fixed-memory, single-owner retry and in-flight ledger.
// It stores only fixed-width source fingerprints; canonical topology strings
// remain in the catalog and candidates. Fingerprints affect only advisory
// deferral; exact catalog and source proofs remain independent authority.
type FeedbackTable struct {
	clock   uint64
	count   uint16
	index   [feedbackIndexSlots]uint16
	entries [MaxFeedbackEntries]feedbackEntry
}

// FeedbackStats is a fixed-size operational view of the ledger.
type FeedbackStats struct {
	Entries  uint16
	InFlight uint16
	Cooling  uint16
}

// Stats reports current bounded ledger occupancy without allocating.
func (t *FeedbackTable) Stats() FeedbackStats {
	var stats FeedbackStats
	if t == nil {
		return stats
	}
	for index := range t.entries {
		switch t.entries[index].state {
		case feedbackInFlight:
			stats.Entries++
			stats.InFlight++
		case feedbackCooling:
			stats.Entries++
			stats.Cooling++
		}
	}
	return stats
}

// Start atomically marks every selected exact source incarnation in flight.
// On any invalid ordinal, duplicate, active reservation, or capacity failure,
// the table is restored byte-for-byte to its prior logical state.
func (t *FeedbackTable) Start(
	candidates []SplitCandidate,
	decision Decision,
) error {
	if t == nil || decision.Count == 0 || decision.Count > MaxBatch {
		return ErrInvalidFeedback
	}
	var keys [MaxBatch]sourceKey
	for index := 0; index < int(decision.Count); index++ {
		ordinal := int(decision.Ordinals[index])
		if ordinal >= len(candidates) || !candidates[ordinal].Recommendation.Actionable() {
			return ErrInvalidFeedback
		}
		keys[index] = sourceKeyFor(candidates[ordinal].Recommendation.Source)
		for prior := 0; prior < index; prior++ {
			if keys[index] == keys[prior] {
				return ErrInvalidFeedback
			}
		}
	}

	var slots [MaxBatch]uint16
	var existing [MaxBatch]bool
	for index := 0; index < int(decision.Count); index++ {
		ordinal := int(decision.Ordinals[index])
		candidate := &candidates[ordinal]
		slot, exists := t.find(keys[index])
		if exists {
			entry := &t.entries[slot]
			if entry.state == feedbackInFlight ||
				candidate.Recommendation.WindowSequence <= entry.lastWindow ||
				candidate.Recommendation.WindowSequence < entry.eligibleWindow {
				return ErrInvalidFeedback
			}
		} else {
			var ok bool
			slot, ok = t.replacementSlot(slots[:index])
			if !ok {
				return ErrInvalidFeedback
			}
		}
		slots[index], existing[index] = uint16(slot), exists
	}

	plannedIndex := t.index
	nextCount := t.count
	for index := 0; index < int(decision.Count); index++ {
		if existing[index] {
			continue
		}
		slot := int(slots[index])
		if t.entries[slot].state == feedbackEmpty {
			nextCount++
		} else if !deleteFeedbackIndex(&plannedIndex, t.entries[slot].key, &t.entries) {
			return ErrInvalidFeedback
		}
	}
	for index := 0; index < int(decision.Count); index++ {
		if !existing[index] &&
			!insertFeedbackIndex(&plannedIndex, keys[index], slots[index]) {
			return ErrInvalidFeedback
		}
	}

	nextClock := t.clock
	for index := 0; index < int(decision.Count); index++ {
		ordinal := int(decision.Ordinals[index])
		candidate := &candidates[ordinal]
		slot := int(slots[index])
		failures := uint8(0)
		if existing[index] {
			failures = t.entries[slot].failures
		}
		if nextClock != ^uint64(0) {
			nextClock++
		}
		t.entries[slot] = feedbackEntry{
			key: keys[index], lastWindow: candidate.Recommendation.WindowSequence,
			stamp: nextClock, failures: failures, state: feedbackInFlight,
		}
	}
	t.clock, t.count, t.index = nextClock, nextCount, plannedIndex
	return nil
}

// Finish resolves one exact in-flight recommendation. Success and cancellation
// release the entry. A retryable result applies capped exponential backoff in
// source evidence windows.
func (t *FeedbackTable) Finish(
	candidate SplitCandidate,
	outcome FeedbackOutcome,
	policy FeedbackPolicy,
) error {
	if t == nil || !candidate.Recommendation.Actionable() {
		return ErrInvalidFeedback
	}
	key := sourceKeyFor(candidate.Recommendation.Source)
	slot, ok := t.find(key)
	if !ok || t.entries[slot].state != feedbackInFlight ||
		t.entries[slot].lastWindow != candidate.Recommendation.WindowSequence {
		return ErrInvalidFeedback
	}
	switch outcome {
	case FeedbackSucceeded, FeedbackCancelled:
		if !deleteFeedbackIndex(&t.index, key, &t.entries) {
			return ErrInvalidFeedback
		}
		t.entries[slot] = feedbackEntry{}
		t.count--
		return nil
	case FeedbackRetryable:
		if policy.BaseRetryWindows == 0 ||
			policy.MaxRetryWindows < policy.BaseRetryWindows {
			return ErrInvalidFeedback
		}
	default:
		return ErrInvalidFeedback
	}
	entry := &t.entries[slot]
	if entry.failures != ^uint8(0) {
		entry.failures++
	}
	delay := retryDelay(policy, entry.failures)
	if entry.lastWindow > ^uint64(0)-delay {
		entry.eligibleWindow = ^uint64(0)
	} else {
		entry.eligibleWindow = entry.lastWindow + delay
	}
	if t.clock != ^uint64(0) {
		t.clock++
	}
	entry.stamp = t.clock
	entry.state = feedbackCooling
	return nil
}

func (t *FeedbackTable) eligible(candidate *SplitCandidate) bool {
	key := sourceKeyFor(candidate.Recommendation.Source)
	index, ok := t.find(key)
	if !ok {
		return true
	}
	entry := &t.entries[index]
	return entry.state == feedbackCooling &&
		candidate.Recommendation.WindowSequence > entry.lastWindow &&
		candidate.Recommendation.WindowSequence >= entry.eligibleWindow
}

func (t *FeedbackTable) find(key sourceKey) (int, bool) {
	mask := feedbackIndexSlots - 1
	start := int(key.low) & mask
	for probe := 0; probe < feedbackIndexSlots; probe++ {
		reference := t.index[(start+probe)&mask]
		switch reference {
		case feedbackIndexEmpty:
			return 0, false
		case feedbackIndexTombstone:
			continue
		}
		index := int(reference - 1)
		if t.entries[index].state != feedbackEmpty && t.entries[index].key == key {
			return index, true
		}
	}
	return 0, false
}

func (t *FeedbackTable) replacementSlot(reserved []uint16) (int, bool) {
	oldest, oldestStamp := -1, ^uint64(0)
	for index := range t.entries {
		entry := &t.entries[index]
		if feedbackSlotReserved(reserved, index) {
			continue
		}
		if entry.state == feedbackEmpty && t.count < MaxFeedbackEntries {
			return index, true
		}
		if entry.state == feedbackCooling && entry.stamp < oldestStamp {
			oldest, oldestStamp = index, entry.stamp
		}
	}
	return oldest, oldest >= 0
}

func feedbackSlotReserved(reserved []uint16, slot int) bool {
	for index := range reserved {
		if int(reserved[index]) == slot {
			return true
		}
	}
	return false
}

func insertFeedbackIndex(
	index *[feedbackIndexSlots]uint16,
	key sourceKey,
	entry uint16,
) bool {
	mask := feedbackIndexSlots - 1
	start := int(key.low) & mask
	firstTombstone := -1
	for probe := 0; probe < feedbackIndexSlots; probe++ {
		position := (start + probe) & mask
		switch index[position] {
		case feedbackIndexTombstone:
			if firstTombstone < 0 {
				firstTombstone = position
			}
		case feedbackIndexEmpty:
			if firstTombstone >= 0 {
				position = firstTombstone
			}
			index[position] = entry + 1
			return true
		}
	}
	if firstTombstone >= 0 {
		index[firstTombstone] = entry + 1
		return true
	}
	return false
}

func deleteFeedbackIndex(
	index *[feedbackIndexSlots]uint16,
	key sourceKey,
	entries *[MaxFeedbackEntries]feedbackEntry,
) bool {
	mask := feedbackIndexSlots - 1
	start := int(key.low) & mask
	for probe := 0; probe < feedbackIndexSlots; probe++ {
		position := (start + probe) & mask
		reference := index[position]
		switch reference {
		case feedbackIndexEmpty:
			return false
		case feedbackIndexTombstone:
			continue
		}
		if entries[reference-1].key == key {
			index[position] = feedbackIndexTombstone
			return true
		}
	}
	return false
}

func retryDelay(policy FeedbackPolicy, failures uint8) uint64 {
	delay := policy.BaseRetryWindows
	for count := uint8(1); count < failures && delay < policy.MaxRetryWindows; count++ {
		if delay > policy.MaxRetryWindows-delay {
			return policy.MaxRetryWindows
		}
		delay += delay
	}
	return min(delay, policy.MaxRetryWindows)
}

func sourceKeyFor(source autosplit.SourceIdentity) sourceKey {
	key := sourceKey{
		low:  0xcbf29ce484222325,
		high: 0x6eed0e9da4d94a4f,
	}
	mixString := func(value string) {
		key.low = mixSourceUint64(key.low, uint64(len(value)), 0x100000001b3)
		key.high = mixSourceUint64(key.high, uint64(len(value)), 0x9e3779b185ebca87)
		for index := 0; index < len(value); index++ {
			key.low = mixSourceUint64(key.low, uint64(value[index]), 0x100000001b3)
			key.high = mixSourceUint64(key.high, uint64(value[index]), 0x9e3779b185ebca87)
		}
	}
	mixUint64 := func(value uint64) {
		key.low = mixSourceUint64(key.low, value, 0x100000001b3)
		key.high = mixSourceUint64(key.high, value, 0x9e3779b185ebca87)
	}
	mixPoint := func(point [8]byte) {
		for index := range point {
			mixUint64(uint64(point[index]))
		}
	}
	mixString(string(source.Distribution))
	mixString(string(source.Shard))
	mixUint64(uint64(source.AllocationGeneration))
	mixPoint(source.Range.Start)
	mixPoint(source.Range.End.Point)
	if source.Range.End.Max {
		mixUint64(1)
	} else {
		mixUint64(0)
	}
	mixUint64(uint64(source.BucketBits))
	mixUint64(uint64(source.RoutingVersion))
	mixUint64(uint64(source.OwnershipEpoch))
	return key
}

func mixSourceUint64(hash, value, prime uint64) uint64 {
	hash ^= value
	return hash * prime
}
