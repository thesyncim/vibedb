package gateway

import (
	"bytes"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
)

const replicatedCatalogGenesisFixedMutationCount = 5

// BuildReplicatedCatalogGenesisMutations constructs the complete generation
// one catalog cut for the private physical bootstrap session. The returned
// mutations are deliberately all PutAbsentOrEqual so identical initial
// producers converge while any divergent plan fails atomically in Raft.
//
// The batch contains the catalog proof and head, every initial physical node
// child, the revision-one node directory, and the empty revision-one service
// directory. It must remain one base-relation transaction; publishing a head
// and adding the directories in separate transactions would expose a partial
// canonical cut to startup readers.
func BuildReplicatedCatalogGenesisMutations(
	snapshot *Snapshot,
	records []NodeRecord,
) ([]NativeMutation, error) {
	if snapshot == nil || snapshot.Generation() != 1 || len(records) == 0 ||
		len(records) > MaxScalingNodes ||
		len(records)+replicatedCatalogGenesisFixedMutationCount > replication.MaxMutations {
		return nil, ErrReplicatedCatalog
	}
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}

	ordered := slices.Clone(records)
	slices.SortFunc(ordered, func(left, right NodeRecord) int {
		return bytes.Compare(left.NodeID[:], right.NodeID[:])
	})
	nodes := make(map[rafttransport.NodeID]NodeRecord, len(ordered))
	entries := make([]scalingNodeDirectoryEntry, 0, len(ordered))
	recordMutations := make([]NativeMutation, 0, len(ordered))
	for _, record := range ordered {
		if !record.Valid() || record.Lifecycle != NodeActive || record.Revision != 1 ||
			record.CatalogGeneration != snapshot.Generation() {
			return nil, ErrInvalidScalingMetadata
		}
		if _, exists := nodes[record.NodeID]; exists {
			return nil, ErrScalingIdentity
		}
		nodes[record.NodeID] = record
		raw, err := appendScalingNodeRecord(nil, record)
		if err != nil {
			return nil, err
		}
		digest := scalingDigest(raw)
		entries = append(entries, scalingNodeDirectoryEntry{
			NodeID: bytes.Clone(record.NodeID[:]), Digest: bytes.Clone(digest[:]),
			Incarnation: record.Incarnation, Revision: record.Revision,
		})
		recordMutations = append(recordMutations, NativeMutation{
			Kind: replication.MutationPutAbsentOrEqual,
			Key:  scalingNodeKey(record.NodeID, record.Incarnation), Value: raw,
		})
	}

	// Every immutable replica in the generation-one catalog must have an
	// exactly matching physical record. Extra records are allowed because the
	// initial physical catalog group can include storage-only catalog voters
	// that are not members of a user-data route.
	for _, replica := range snapshot.replicatedReplicas {
		node, found := nodes[replica.Node]
		if !found || node.Incarnation != replica.NodeIncarnation ||
			node.DataAddress != replica.DataAddress ||
			node.NativeAddress != replica.Address ||
			node.ControlAddress != replica.ControlAddress {
			return nil, ErrScalingIdentity
		}
	}

	head, err := appendReplicatedCatalogDocument(nil, snapshot, maxReplicatedCatalogBytes)
	if err != nil {
		return nil, err
	}
	genesis, err := appendReplicatedCatalogGenesis(nil, head)
	if err != nil {
		return nil, err
	}
	witness, err := appendReplicatedCatalogHeadWitness(nil, snapshot.Generation(), head)
	if err != nil {
		return nil, err
	}
	nodeDirectory, err := appendScalingNodeDirectoryAt(nil, entries, 1)
	if err != nil {
		return nil, err
	}
	serviceDirectory, err := appendReplicatedServiceDirectory(nil, replicatedServiceDirectory{Revision: 1})
	if err != nil {
		return nil, err
	}

	mutations := make([]NativeMutation, 0, len(ordered)+replicatedCatalogGenesisFixedMutationCount)
	mutations = append(mutations,
		NativeMutation{Kind: replication.MutationPutAbsentOrEqual, Key: bytes.Clone(replicatedCatalogGenesisKey), Value: genesis},
		NativeMutation{Kind: replication.MutationPutAbsentOrEqual, Key: bytes.Clone(replicatedCatalogHeadKey), Value: head},
		NativeMutation{Kind: replication.MutationPutAbsentOrEqual, Key: bytes.Clone(replicatedCatalogHeadWitnessKey), Value: witness},
	)
	mutations = append(mutations, recordMutations...)
	mutations = append(mutations,
		NativeMutation{Kind: replication.MutationPutAbsentOrEqual, Key: bytes.Clone(scalingNodeDirectoryKey), Value: nodeDirectory},
		NativeMutation{Kind: replication.MutationPutAbsentOrEqual, Key: bytes.Clone(replicatedServiceDirectoryKey), Value: serviceDirectory},
	)
	if len(mutations) != len(records)+replicatedCatalogGenesisFixedMutationCount {
		return nil, errors.New("gateway: replicated catalog genesis mutation count mismatch")
	}
	return mutations, nil
}
