package main

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// rf3AllManifestSourcesRetired recognizes only positive, journaled retirement
// of every original manifest replica. It reads the immutable identity files,
// never SQL catalogs, schema lineage, or WALs, so a retired source can answer
// exact control retries without reopening removed storage.
//
// This predicate covers original bundles only. Callers must additionally
// exclude any live adopted-group inventory before selecting control-only
// startup. An originally empty manifest always follows normal node recovery.
func rf3AllManifestSourcesRetired(manifest rf3Manifest, records []replicaaction.Record) (bool, error) {
	bundles := manifest.groupBundles()
	if len(bundles) > maxRF3ManifestGroups || len(records) > replicaaction.AbsoluteMaxReplicaActionRecords {
		return false, errRF3Serving
	}
	if len(bundles) == 0 || len(records) == 0 {
		return false, nil
	}
	// SourceRetirements validates durable records before returning them. Keep
	// this read-only predicate fail-closed for malformed in-memory callers too.
	proofs := make([]replicaaction.Record, 0, len(records))
	for _, record := range records {
		if record.Request.Kind != replicaaction.SourceRetirement ||
			(record.State != replicaaction.RetirementAuthorized && record.State != replicaaction.Complete) {
			continue
		}
		if record.Revision < 2 {
			return false, errRF3Serving
		}
		if _, err := replicaaction.AppendRequest(nil, record.Request); err != nil {
			return false, errors.Join(errRF3Serving, err)
		}
		proofs = append(proofs, record)
	}
	if len(proofs) == 0 {
		return false, nil
	}
	seen := make(map[raftmember.GroupKey]struct{}, len(bundles))
	for index, bundle := range bundles {
		var base sqldriver.ReplicatedShardStoreIdentity
		if err := loadRF3IdentityFile(bundle.SQL.IdentityPath, &base); err != nil {
			return false, fmt.Errorf("%w: retired source %d identity: %w", errRF3Serving, index, err)
		}
		binding := base.Binding
		group := groupFromBinding(binding)
		if base.Format != sqldriver.ReplicatedShardStoreFormat ||
			group.ClusterID == ([16]byte{}) || group.ClusterIncarnation == ([16]byte{}) ||
			group.TopologyRecoveryEpoch == 0 || group.ShardIncarnation == ([16]byte{}) || group.GroupID == ([16]byte{}) ||
			binding.MemberID == 0 || binding.StoreID == ([16]byte{}) || binding.AllocationGeneration == 0 ||
			binding.Distribution == "" || binding.Shard == "" || !rf3RouteMatchesBinding(bundle.Route, binding) {
			return false, fmt.Errorf("%w: retired source %d manifest identity mismatch", errRF3Serving, index)
		}
		if _, duplicate := seen[group]; duplicate {
			return false, fmt.Errorf("%w: duplicate retired source group", errRF3Serving)
		}
		seen[group] = struct{}{}
		if !rf3ReplicaSourceRetired(proofs, group, binding.MemberID, binding.StoreID, binding.AllocationGeneration) {
			return false, nil
		}
	}
	return true, nil
}
