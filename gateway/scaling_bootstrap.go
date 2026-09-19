package gateway

import (
	"context"

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
	// Keep the public bootstrap entry point as a thin compatibility wrapper for
	// explicit test/operator callers. The canonical builder is the only place
	// that defines the generation-one mutation set; Runtime.Open never invokes
	// this method for managed startup.
	mutations, err := BuildReplicatedCatalogGenesisMutations(snapshot, records)
	if err != nil {
		return err
	}
	result, err := authority.session.MutateBatch(ctx, mutations)
	return scalingMutationError(result, err, authority.session)
}

// EnsureFrontendDrainServiceDirectory installs the revision-1 empty drain
// index for an existing catalog that predates the frontend-drain row. The
// service row is created only after a committed physical directory and
// catalog head have been read, and the same immutable rows are digest-CASed in
// the batch. This is the narrow migration for a legitimate pre-first-drain
// state; it never grants a gateway or fabricates a source cut.
func (authority *ReplicatedCatalogAuthority) EnsureFrontendDrainServiceDirectory(ctx context.Context) error {
	if authority == nil || authority.session == nil || ctx == nil {
		return ErrReplicatedCatalog
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
	current, err := authority.readRaw(ctx, replicatedServiceDirectoryKey, maxReplicatedServiceDirectoryBytes)
	if err != nil {
		return err
	}
	if current.Found {
		_, err = openReplicatedServiceDirectory(current.Value)
		return err
	}
	nodeDirectory, err := authority.readRaw(ctx, scalingNodeDirectoryKey, maxScalingNodeDirectoryBytes)
	if err != nil {
		return err
	}
	if !nodeDirectory.Found {
		// A process before physical-directory genesis has no canonical owner
		// source yet. BootstrapNodeDirectory will create both rows together.
		return ErrScalingNodeMissing
	}
	head, err := authority.readRaw(ctx, replicatedCatalogHeadKey, maxReplicatedCatalogBytes)
	if err != nil {
		return err
	}
	if !head.Found {
		return ErrReplicatedCatalogMissing
	}
	serviceRaw, err := appendReplicatedServiceDirectory(nil, replicatedServiceDirectory{Revision: 1})
	if err != nil {
		return err
	}
	mutations := []NativeMutation{
		NativeMutation{Kind: replication.MutationPutAbsentOrEqual, Key: replicatedServiceDirectoryKey, Value: serviceRaw},
		{Kind: replication.MutationPutDigestEqual, Key: scalingNodeDirectoryKey, Value: nodeDirectory.Value,
			ExpectedValueLength: uint64(len(nodeDirectory.Value)), ExpectedValueDigest: scalingDigest(nodeDirectory.Value)},
		{Kind: replication.MutationPutDigestEqual, Key: replicatedCatalogHeadKey, Value: head.Value,
			ExpectedValueLength: uint64(len(head.Value)), ExpectedValueDigest: scalingDigest(head.Value)},
	}
	result, err := authority.session.MutateBatch(ctx, mutations)
	return scalingMutationError(result, err, authority.session)
}
