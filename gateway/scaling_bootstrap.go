package gateway

import (
	"bytes"
	"context"
	"slices"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
)

// BootstrapNodeDirectory installs a complete trusted provisioning cut in one
// catalog transaction. It never appends to or overwrites an existing directory.
func (authority *ReplicatedCatalogAuthority) BootstrapNodeDirectory(ctx context.Context, records []NodeRecord) error {
	if authority == nil || ctx == nil || len(records) == 0 || len(records) > MaxScalingNodes {
		return ErrInvalidScalingMetadata
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if err = authority.requireRouteSeedServingLocked(); err != nil {
		return err
	}
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	directory, err := authority.readRaw(ctx, scalingNodeDirectoryKey, maxScalingNodeDirectoryBytes)
	if err != nil {
		return err
	}
	if directory.Found {
		return nil
	}
	head, err := authority.readRaw(ctx, replicatedCatalogHeadKey, maxReplicatedCatalogBytes)
	if err != nil {
		return err
	}
	if !head.Found {
		return ErrReplicatedCatalogMissing
	}
	payload, err := openTypedControlPlaneDocument(head.Value, replicatedCatalogHeadDocumentID[:], maxReplicatedCatalogBytes)
	if err != nil {
		return err
	}
	snapshot, err := OpenSnapshotDocument(payload)
	if err != nil {
		return err
	}
	records = slices.Clone(records)
	slices.SortFunc(records, func(a, b NodeRecord) int { return bytes.Compare(a.NodeID[:], b.NodeID[:]) })
	nodes := make(map[rafttransport.NodeID]NodeRecord, len(records))
	entries := make([]scalingNodeDirectoryEntry, 0, len(records))
	mutations := make([]NativeMutation, 0, len(records)+2)
	for _, record := range records {
		if !record.Valid() || record.Lifecycle != NodeActive || record.Revision != 1 || record.CatalogGeneration != snapshot.Generation() {
			return ErrInvalidScalingMetadata
		}
		if _, exists := nodes[record.NodeID]; exists {
			return ErrScalingIdentity
		}
		nodes[record.NodeID] = record
		raw, err := appendScalingNodeRecord(nil, record)
		if err != nil {
			return err
		}
		digest := scalingDigest(raw)
		entries = append(entries, scalingNodeDirectoryEntry{NodeID: bytes.Clone(record.NodeID[:]), Incarnation: record.Incarnation, Revision: record.Revision, Digest: bytes.Clone(digest[:])})
		mutations = append(mutations, NativeMutation{Kind: replication.MutationPutAbsentOrEqual, Key: scalingNodeKey(record.NodeID, record.Incarnation), Value: raw})
	}
	for _, replica := range snapshot.replicatedReplicas {
		node, found := nodes[replica.Node]
		if !found || node.Incarnation != replica.NodeIncarnation || node.DataAddress != replica.DataAddress || node.NativeAddress != replica.Address || node.ControlAddress != replica.ControlAddress {
			return ErrScalingIdentity
		}
	}
	raw, err := appendScalingNodeDirectoryAt(nil, entries, 1)
	if err != nil {
		return err
	}
	mutations = append(mutations, scalingDirectoryMutation(directory, scalingNodeDirectoryKey, raw), NativeMutation{Kind: replication.MutationPutDigestEqual, Key: replicatedCatalogHeadKey, Value: head.Value, ExpectedValueLength: uint64(len(head.Value)), ExpectedValueDigest: scalingDigest(head.Value)})
	result, err := authority.session.MutateBatch(ctx, mutations)
	return scalingMutationError(result, err, authority.session)
}
