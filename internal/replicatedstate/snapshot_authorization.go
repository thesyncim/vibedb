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

// publishLogicalEpochs records the applied command epochs for lock-free
// admission checks. The caller is the apply lane and must not overlap another
// publication of the same machine.
func (m *Machine) publishLogicalEpochs(binding Binding) {
	if m == nil || binding.OwnershipEpoch == 0 {
		return
	}
	m.logicalEpochSeq.Add(1)
	m.logicalPolicy.Store(binding.ActivePolicyGeneration)
	m.logicalProtection.Store(binding.ProtectionEpoch)
	m.logicalOwnership.Store(binding.OwnershipEpoch)
	m.logicalSchema.Store(binding.SchemaGeneration)
	m.logicalRouting.Store(binding.RoutingVersion)
	m.logicalRouteGeneration.Store(binding.RouteGeneration)
	m.logicalEpochSeq.Add(1)
}

// PublishedLogicalEpochs returns the applied policy, protection, ownership,
// schema, routing, and route-generation epochs. ok is false until the first
// durable binding is published.
func (m *Machine) PublishedLogicalEpochs() (policy, protection, ownership, schema, routing, generation uint64, ok bool) {
	if m == nil {
		return 0, 0, 0, 0, 0, 0, false
	}
	for {
		seq := m.logicalEpochSeq.Load()
		if seq&1 != 0 {
			continue
		}
		policy = m.logicalPolicy.Load()
		protection = m.logicalProtection.Load()
		ownership = m.logicalOwnership.Load()
		schema = m.logicalSchema.Load()
		routing = m.logicalRouting.Load()
		generation = m.logicalRouteGeneration.Load()
		if m.logicalEpochSeq.Load() == seq {
			return policy, protection, ownership, schema, routing, generation, seq != 0 && ownership != 0
		}
	}
}
