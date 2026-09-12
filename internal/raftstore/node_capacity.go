package raftstore

import "math"

// CapacityReservationBytes returns the complete authenticated upper bound for
// the node log geometry. It is a conservative physical reservation, not a
// claim about currently occupied bytes; callers must combine it with their
// detached group observations before admitting a target copy.
func (s *NodeStore) CapacityReservationBytes() (uint64, error) {
	if s == nil {
		return 0, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return 0, err
	}
	perEntry, overflow := checkedCapacityAdd(s.bounds.maxWaveBytes, s.bounds.segmentBytes)
	if overflow || s.bounds.maxEntriesPerGroup > math.MaxUint64/perEntry {
		return 0, ErrBounds
	}
	perGroup := s.bounds.maxEntriesPerGroup * perEntry
	if s.bounds.maxGroups > math.MaxUint64/perGroup {
		return 0, ErrBounds
	}
	return s.bounds.maxGroups * perGroup, nil
}

// CapacityReservationBytes is the authenticated per-group upper bound for a
// segmented node log. It is deliberately conservative: every retained group
// entry may occupy a full bounded wave and the segment geometry is included in
// the same reservation. Callers must label the resulting capacity evidence as
// conservative rather than as measured live bytes.
func (v *GroupView) CapacityReservationBytes() (uint64, error) {
	if v == nil || v.store == nil {
		return 0, ErrInvalid
	}
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	if err := v.store.usable(); err != nil {
		return 0, err
	}
	if _, ok := v.store.descriptorForLogKey(v.group); !ok {
		return 0, ErrInvalid
	}
	perEntry, overflow := checkedCapacityAdd(v.store.bounds.maxWaveBytes, v.store.bounds.segmentBytes)
	if overflow || v.store.bounds.maxEntriesPerGroup > math.MaxUint64/perEntry {
		return 0, ErrBounds
	}
	return v.store.bounds.maxEntriesPerGroup * perEntry, nil
}

func checkedCapacityAdd(left, right uint64) (uint64, bool) {
	if right > math.MaxUint64-left {
		return math.MaxUint64, true
	}
	return left + right, false
}
