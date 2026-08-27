package replicatedstate

// SnapshotAuthorizationFence returns current durable publication metadata for
// authorizing an already-published snapshot artifact. It acquires no snapshot,
// scans no rows and grants no authority to read mutable collection contents.
func (m *Machine) SnapshotAuthorizationFence() (SnapshotFence, error) {
	if m == nil {
		return SnapshotFence{}, ErrApplyPoisoned
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkUsable(); err != nil {
		return SnapshotFence{}, err
	}
	if !m.initialized || m.publication.Applied == 0 {
		return SnapshotFence{}, ErrReadBehind
	}
	return m.transactionRecoveryFenceLocked(), nil
}
