package driver

import "errors"

// PrepareReplicatedSnapshotTarget reserves the complete allocator-issued apply
// identity in an empty bound SQL root. It creates neither a state machine nor a
// Raft checkpoint. Exact retries are safe; an activated or populated root is
// never repurposed as a cold target. The caller retains the original bootstrap
// separately; certified snapshot activation authenticates its exact digest.
func (d *Database) PrepareReplicatedSnapshotTarget(
	expected ReplicatedShardStoreIdentity, reserved ReplicatedApplyIdentity,
) error {
	return d.reserveReplicatedChildApply(expected, reserved, true)
}

// OpenReplicatedSnapshotTarget opens only the non-serving snapshot installation
// boundary. Both full identities are checked before namespace recovery. A
// reserved target must still be empty; an existing checkpoint must be a seeded
// snapshot transition, whose exact artifact/cursor is checked by the stage.
// Ordinary initialized Raft stores cannot enter this recovery path.
func OpenReplicatedSnapshotTarget(
	path string, expected ReplicatedShardStoreIdentity, expectedApply ReplicatedApplyIdentity,
) (*Database, error) {
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		return nil, err
	}
	if err := validateReplicatedApplyIdentity(expectedApply, expected); err != nil {
		return nil, err
	}
	core, err := openDatabaseWithShardStorePolicy(path, nil, shardStoreOpenPolicy{
		mode:                    shardStoreOpenReplicatedSnapshotTarget,
		expectedReplicated:      ownedReplicatedShardStoreIdentity(expected),
		expectedReplicatedApply: expectedApply,
	})
	if err != nil {
		return nil, err
	}
	d := &Database{connector: &dbConnector{db: core}}
	valid := true
	if core.catalog.ReplicatedChildApply != nil {
		valid = core.checkpointGroup == nil && core.catalog.ReplicatedApply == nil
		for ordinal := 0; valid && ordinal < int(expected.RelationCount); ordinal++ {
			table := core.tables[expected.Relations[ordinal].Table]
			valid = table != nil && table.collection != nil && table.collection.Len() == 0
		}
	} else if core.checkpointGroup != nil {
		_, valid = core.checkpointGroup.SeedAppliedIndex()
	}
	if !valid {
		return nil, errors.Join(ErrReplicatedSnapshotStageProof, d.Close())
	}
	// A missing group may have caused normal collection recovery to clear its
	// pending flag. Snapshot-only opens never authorize ordinary SQL/apply.
	core.replicatedSeedPending = true
	return d, nil
}
