package executionpin

// ValidateSideEffectFence proves that expected is still the exact active
// controller certificate at a linearizable catalog-group read position.
// Callers must perform this check immediately before every externally visible
// side effect. The read position, not a local clock, decides lease liveness.
func ValidateSideEffectFence(
	expected LeaseCertificate,
	current Record,
	observedApplied uint64,
) error {
	if !expected.Valid() || !current.Valid() || current.Status != StatusActive ||
		observedApplied < current.LeaseApplied ||
		observedApplied > current.LeaseAppliedThrough {
		return ErrCorrupt
	}
	actual, ok := current.LeaseCertificate()
	if !ok || actual != expected {
		return ErrCorrupt
	}
	return nil
}
