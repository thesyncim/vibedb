package kubeoperator

import (
	"bytes"
	"crypto/sha256"
	"github.com/thesyncim/vibedb/distribution"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibejson"
)

// RestoreTargetCatalog returns the exact operation-bound generation-one head.
// The command must use it rather than an independently supplied route file.
func RestoreTargetCatalog(raw []byte, operation clusterrestore.Operation) (*gateway.Snapshot, error) {
	if _, err := validateRestoreSchemaSetSnapshot(raw, operation); err != nil {
		return nil, err
	}
	var set restoreSchemaSet
	if err := vibejson.Unmarshal(raw, &set); err != nil {
		return nil, err
	}
	return gateway.OpenSnapshotDocument(set.Catalog)
}

func validateRestoreSchemaSetSnapshot(raw []byte, operation clusterrestore.Operation) ([]replicatedstate.ProjectionRow, error) {
	if _, err := openRestoreSchemaSet(raw, operation, operation.CatalogOrdinal); err != nil {
		return nil, err
	}
	return openRestoreCatalogProjection(raw, operation)
}

func restorePlannedSchemaDigests(raw []byte, template restoreSchemaTemplate) ([32]byte, [32]byte, error) {
	var set restoreSchemaSet
	if err := vibejson.Unmarshal(raw, &set); err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	snapshot, err := gateway.OpenSnapshotDocument(set.Catalog)
	if err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	var replicas [3]gateway.ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute(distribution.DistributionName(template.Distribution), distribution.ShardID(template.Shard), replicas[:0])
	if !ok || route.LogicalSchemaDigest == ([32]byte{}) {
		return [32]byte{}, [32]byte{}, ErrBootstrap
	}
	return [32]byte(route.Command.RelationManifestDigest), [32]byte(route.LogicalSchemaDigest), nil
}

func openRestoreCatalogProjection(raw []byte, operation clusterrestore.Operation) ([]replicatedstate.ProjectionRow, error) {
	if len(raw) == 0 || len(raw) > RestoreTemplateMaxBytes || sha256.Sum256(raw) != operation.TargetCatalogDigest {
		return nil, ErrBootstrap
	}
	var set restoreSchemaSet
	if err := vibejson.Unmarshal(raw, &set); err != nil {
		return nil, err
	}
	canonical, err := vibejson.Marshal(&set)
	if err != nil || !bytes.Equal(canonical, raw) || int(operation.CatalogOrdinal) >= len(set.Groups) {
		return nil, ErrBootstrap
	}
	t := set.Groups[operation.CatalogOrdinal].Schema
	if t.Distribution != string(gateway.ReplicatedCatalogDistribution) || t.Shard != string(gateway.ReplicatedCatalogShard) || t.BaseTable != gateway.ReplicatedCatalogTable || len(t.GlobalIndexes) != 0 {
		return nil, ErrBootstrap
	}
	snapshot, err := gateway.OpenSnapshotDocument(set.Catalog)
	if err != nil {
		return nil, err
	}
	for i, slot := range set.Groups {
		placement, placed := snapshot.Placement(slot.Schema.BaseTable)
		if !placed || placement.Distribution != distribution.DistributionName(slot.Schema.Distribution) {
			return nil, ErrBootstrap
		}
		var scratch [3]gateway.ReplicatedEndpoint
		route, found := snapshot.ResolveReplicatedRoute(distribution.DistributionName(slot.Schema.Distribution), distribution.ShardID(slot.Schema.Shard), scratch[:0])
		if !found || i >= len(operation.Targets) || route.Group != operation.Targets[i].Group || uint64(route.AllocationGeneration) != slot.Schema.AllocationGeneration {
			return nil, ErrBootstrap
		}
	}
	return gateway.RestoreCatalogProjection(operation, snapshot, set.Policy)
}
