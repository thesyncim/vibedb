package replicatedstate

import "github.com/thesyncim/vibedb/internal/routegate"

type RouteGateReadResult struct {
	Fence  SnapshotFence
	Status routegate.Status
}

// RouteGateRead copies the committed gate and its publication fence under one
// lock. Serving callers must first obtain a leader quorum ReadIndex barrier.
func (m *Machine) RouteGateRead(minimumApplied uint64) (RouteGateReadResult, error) {
	if m == nil {
		return RouteGateReadResult{}, ErrApplyPoisoned
	}
	if minimumApplied == 0 {
		return RouteGateReadResult{}, ErrWrongBinding
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkUsable(); err != nil {
		return RouteGateReadResult{}, err
	}
	if !m.initialized || m.routeGate == nil {
		return RouteGateReadResult{}, ErrWrongBinding
	}
	if m.publication.Applied < minimumApplied {
		return RouteGateReadResult{}, ErrReadBehind
	}
	return RouteGateReadResult{Fence: m.transactionRecoveryFenceLocked(), Status: m.routeGate.Status()}, nil
}
