package gateway

import (
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

func TestSchemaRolloutAcceptsDistinctShardMachineManifests(t *testing.T) {
	base, _, _ := replicatedSQLSplitTransactionFixture(t)
	descriptors := base.ReplicatedShardDescriptors()
	profiles := base.replicatedTableProfiles()
	if len(descriptors) != 2 || descriptors[0].Command.RelationManifestDigest == descriptors[1].Command.RelationManifestDigest {
		t.Fatal("fixture does not cover distinct range-dependent machine schemas")
	}
	logical := replication.Digest(sha256.Sum256([]byte("next-shared-logical-schema")))
	for i := range descriptors {
		descriptors[i].Command.SchemaGeneration++
		descriptors[i].Command.RelationManifestDigest = sha256.Sum256([]byte{0x42, byte(i)})
		descriptors[i].LogicalSchemaDigest = logical
	}
	for i := range profiles {
		profiles[i].SchemaGeneration++
		profiles[i].LogicalSchemaDigest = logical
	}
	target, err := NewSnapshotWithReplicatedTableMetadata(base.config, base.endpoints, base.Generation()+1,
		base.indexDescriptors(), base.statistics.Descriptors(), descriptors, profiles, base.ReplicatedTableDeclarations())
	if err != nil {
		t.Fatal(err)
	}
	changes, err := schemaRolloutChanges(base, target)
	if err != nil || len(changes) != 2 {
		t.Fatalf("distributed rollout: %v changes=%d", err, len(changes))
	}
	for _, change := range changes {
		matched := false
		for i, old := range base.ReplicatedShardDescriptors() {
			if old.Group != change.group {
				continue
			}
			matched = change.fromRelationManifestDigest == replication.Digest(old.Command.RelationManifestDigest) &&
				change.toRelationManifestDigest == replication.Digest(descriptors[i].Command.RelationManifestDigest)
		}
		if !matched {
			t.Fatal("rollout lost the shard-specific machine fence")
		}
	}
}

func TestSchemaRolloutRejectsMixedDistributionSchemasWithoutTableProfiles(t *testing.T) {
	base, _, _ := replicatedSQLSplitTransactionFixture(t)
	// Exercise the rollout-level distribution check independently of optional
	// table-profile attachment, which also rejects mixed serving schemas.
	base, err := NewSnapshotWithReplicatedMetadata(base.config, base.endpoints, base.Generation(), nil, nil, base.ReplicatedShardDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"partly-advanced", "different-logical"} {
		descriptors := base.ReplicatedShardDescriptors()
		for i := range descriptors {
			if change == "partly-advanced" && i == 1 {
				continue
			}
			descriptors[i].Command.SchemaGeneration++
			descriptors[i].Command.RelationManifestDigest = sha256.Sum256([]byte{0x42, byte(i)})
			descriptors[i].LogicalSchemaDigest = replication.Digest{0x43}
			if change == "different-logical" {
				descriptors[i].LogicalSchemaDigest[1] = byte(i)
			}
		}
		target, err := NewSnapshotWithReplicatedMetadata(base.config, base.endpoints, base.Generation()+1, nil, nil, descriptors)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := schemaRolloutChanges(base, target); err == nil {
			t.Fatalf("accepted %s rollout", change)
		}
	}
}
