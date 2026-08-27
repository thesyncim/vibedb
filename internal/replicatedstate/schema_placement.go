package replicatedstate

import "crypto/sha256"

// RelationPlacementDigest returns the current authenticated global-index image
// commitment for binding an exact schema transition. Singleton JSON bundles
// have no global placement and return zero.
func (m *Machine) RelationPlacementDigest() ([sha256.Size]byte, error) {
	if m == nil {
		return [sha256.Size]byte{}, ErrApplyPoisoned
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkUsable(); err != nil {
		return [sha256.Size]byte{}, err
	}
	return m.state.RelationPlacementDigest, nil
}
