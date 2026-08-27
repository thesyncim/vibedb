package gateway

import (
	"bytes"
	"crypto/sha256"
	"github.com/thesyncim/vibedb/distribution"
	"slices"

	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibejson"
)

// RestoreCatalogProjection constructs the only rows imported into a fresh
// catalog group. Source catalog rows are never inputs to this function.
// snapshot and policy must be authenticated by the enclosing sealed schema set.
func RestoreCatalogProjection(operation clusterrestore.Operation, snapshot *Snapshot, policy []byte) ([]replicatedstate.ProjectionRow, error) {
	if operation.Digest == ([32]byte{}) || snapshot == nil || snapshot.Generation() != 1 || sha256.Sum256(policy) != operation.TargetPolicyDigest {
		return nil, ErrRestoreActivation
	}
	p, err := serviceauthz.Load(policy)
	if err != nil || p.Generation() != operation.PolicyGeneration {
		return nil, ErrRestoreActivation
	}
	descriptors := snapshot.ReplicatedShardDescriptors()
	if len(descriptors) != len(operation.Targets) {
		return nil, ErrRestoreActivation
	}
	seen := make([]bool, len(operation.Targets))
	endpointUnion := make(map[distribution.EndpointID]struct{}, len(descriptors)*9)
	for _, descriptor := range descriptors {
		ordinal := -1
		for i, target := range operation.Targets {
			if descriptor.Group == target.Group {
				ordinal = i
				break
			}
		}
		if ordinal < 0 || seen[ordinal] || descriptor.EnrolledTarget != nil || len(descriptor.Replicas) != 3 {
			return nil, ErrRestoreActivation
		}
		seen[ordinal] = true
		f := descriptor.Command
		if digest := operation.Certificate.Groups[ordinal].RelationManifestDigest; digest != ([32]byte{}) && [32]byte(f.RelationManifestDigest) != digest {
			return nil, ErrRestoreActivation
		}
		if f.ReplicaSetVersion != 1 || f.ActivePolicyGeneration != operation.PolicyGeneration || f.ProtectionEpoch != 1 || f.OwnershipEpoch != 1 || f.RoutingVersion != 1 || f.RouteGeneration != 1 || f.SchemaGeneration != operation.Certificate.Groups[ordinal].SchemaGeneration {
			return nil, ErrRestoreActivation
		}
		if uint32(ordinal) == operation.CatalogOrdinal && (descriptor.Distribution != ReplicatedCatalogDistribution || descriptor.Shard != ReplicatedCatalogShard) {
			return nil, ErrRestoreActivation
		}
		for i, r := range descriptor.Replicas {
			endpointUnion[r.Endpoint] = struct{}{}
			if r.NativeEndpoint != "" {
				endpointUnion[r.NativeEndpoint] = struct{}{}
			}
			if r.ControlEndpoint != "" {
				endpointUnion[r.ControlEndpoint] = struct{}{}
			}
			t := operation.Targets[ordinal].Replicas[i]
			if r.Member != t.Member || r.Node != t.Node || r.StoreID != t.Store || r.NodeIncarnation != t.NodeIncarnation {
				return nil, ErrRestoreActivation
			}
		}
	}
	// No unreplicated/static shard can smuggle source ownership into the head.
	shards := 0
	for _, m := range snapshot.config.Manifests {
		if m.Version() != 1 {
			return nil, ErrRestoreActivation
		}
		for i := 0; i < m.ShardCount(); i++ {
			shard, _ := m.ShardInfo(i)
			if shard.Epoch != 1 {
				return nil, ErrRestoreActivation
			}
		}
		shards += m.ShardCount()
	}
	if shards != len(descriptors) || len(endpointUnion) != len(snapshot.endpoints) {
		return nil, ErrRestoreActivation
	}
	for endpoint := range snapshot.endpoints {
		if _, ok := endpointUnion[endpoint]; !ok {
			return nil, ErrRestoreActivation
		}
	}
	// Reconstructing from the explicit current inventory resets historical
	// lineage/high-water metadata. Exact equality rejects any retained history.
	fresh, err := NewSnapshotWithReplicatedTableMetadata(snapshot.config, snapshot.endpoints, 1, snapshot.indexDescriptors(), snapshot.statistics.Descriptors(), descriptors, snapshot.replicatedTableProfiles())
	if err != nil {
		return nil, err
	}
	actual, err := AppendSnapshotDocument(nil, snapshot)
	expected, freshErr := AppendSnapshotDocument(nil, fresh)
	if err != nil || freshErr != nil || !bytes.Equal(actual, expected) {
		return nil, ErrRestoreActivation
	}
	head, err := appendReplicatedCatalogDocument(nil, snapshot, maxReplicatedCatalogBytes)
	if err != nil {
		return nil, err
	}
	witness, err := appendReplicatedCatalogHeadWitness(nil, 1, head)
	if err != nil {
		return nil, err
	}
	genesis, err := appendReplicatedCatalogGenesis(nil, head)
	if err != nil {
		return nil, err
	}
	commitment := struct {
		Operation [32]byte `json:"operation"`
		Policy    []byte   `json:"policy"`
	}{operation.Digest, policy}
	payload, err := vibejson.Marshal(&commitment)
	if err != nil {
		return nil, err
	}
	id := []byte("catalog/restore-policy")
	policyRow, err := appendControlPlaneDocument(nil, id, payload, maxReplicatedCatalogBytes)
	if err != nil {
		return nil, err
	}
	rows := []replicatedstate.ProjectionRow{
		{Key: bytes.Clone(replicatedCatalogHeadKey), Value: head},
		{Key: bytes.Clone(replicatedCatalogHeadWitnessKey), Value: witness},
		{Key: bytes.Clone(replicatedCatalogGenesisKey), Value: genesis},
		{Key: fixedControlPlaneKey(id), Value: policyRow},
	}
	slices.SortFunc(rows, func(a, b replicatedstate.ProjectionRow) int { return bytes.Compare(a.Key, b.Key) })
	return rows, nil
}
