package main

import (
	"errors"
	"path/filepath"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// RegisterDynamic joins a certified, locally adopted learner to the same live
// schema directory used by groups present at startup. The caller registers
// before publishing the execution group, and withdraws on publication failure.
// Registration never opens, closes, or transfers ownership of the SQL handles.
func (a *rf3SchemaActivator) RegisterDynamic(identity raftmember.RuntimeIdentity,
	apply *sqldriver.ReplicatedApply, spec nodecontrol.PreparationSpec,
	reservationRoot string, log rf3RecoveryLog,
) error {
	if a == nil || a.owners == nil || apply == nil || !filepath.IsAbs(reservationRoot) ||
		filepath.Clean(reservationRoot) != reservationRoot {
		return schemainstall.ErrInvalid
	}
	groupLog, ok := log.(*raftstore.GroupView)
	if !ok || groupLog == nil {
		return raftmember.ErrWALUnavailable
	}
	raw, err := nodecontrol.AppendPreparationSpec(nil, spec)
	if err != nil {
		return err
	}
	ownedSpec, err := nodecontrol.OpenPreparationSpec(raw)
	if err != nil {
		return err
	}
	profile, err := apply.CapacityQualificationProfile()
	if err != nil {
		return err
	}
	want := raftmember.RuntimeIdentity{Group: groupFromBinding(profile.Binding),
		Distribution: profile.Binding.Distribution, Shard: profile.Binding.Shard,
		AllocationGeneration: profile.Binding.AllocationGeneration, MemberID: profile.Binding.MemberID,
		StoreID: profile.Binding.StoreID, NodeIncarnation: identity.NodeIncarnation,
		RelationManifestDigest: profile.RelationManifestDigest}
	if identity != want || identity.NodeIncarnation == 0 || spec.Group != identity.Group ||
		string(spec.Distribution) != identity.Distribution || string(spec.Shard) != identity.Shard ||
		uint64(spec.AllocationGeneration) != identity.AllocationGeneration ||
		spec.Target.MemberID != identity.MemberID || spec.TargetStoreID != identity.StoreID ||
		spec.TargetNodeIncarnation != identity.NodeIncarnation ||
		spec.LogicalSchemaDigest != spec.SourceCommand.RelationManifestDigest ||
		spec.SourceCommand.SchemaGeneration > profile.Binding.Authority.SchemaGeneration {
		return schemainstall.ErrConflict
	}
	if spec.SourceCommand.SchemaGeneration == profile.Binding.Authority.SchemaGeneration &&
		spec.SourceCommand.RelationManifestDigest != profile.RelationManifestDigest {
		return schemainstall.ErrConflict
	}
	node, err := groupLog.NodeIdentity()
	if err != nil || node.ClusterID != identity.Group.ClusterID || node.ClusterIncarnation != identity.Group.ClusterIncarnation ||
		rafttransport.NodeID(node.NodeID) != spec.Target.Node {
		return errors.Join(err, schemainstall.ErrConflict)
	}
	descriptor, err := groupLog.Descriptor()
	if err != nil {
		return err
	}
	incarnation, err := groupLog.NodeIncarnation()
	if err != nil || incarnation != identity.NodeIncarnation ||
		descriptor.TopologyRecoveryEpoch != identity.Group.TopologyRecoveryEpoch ||
		descriptor.GroupID != identity.Group.GroupID || descriptor.ShardIncarnation != identity.Group.ShardIncarnation ||
		descriptor.AllocationGeneration != identity.AllocationGeneration || descriptor.MemberID != identity.MemberID ||
		descriptor.StoreID != identity.StoreID || descriptor.Distribution != identity.Distribution || descriptor.Shard != identity.Shard {
		return errors.Join(err, schemainstall.ErrConflict)
	}
	path := filepath.Join(reservationRoot, "member.vdb")
	rawCatalog, err := readRF3BoundedFile(path, schemainstall.AbsoluteMaxBundleBytes)
	if err != nil {
		return err
	}
	catalog, err := sqldriver.DescribeReplicatedSchemaCatalogImage(rawCatalog)
	if err != nil {
		return err
	}
	witness, err := sqldriver.ValidateReplicatedSchemaCatalogImage(rawCatalog)
	if err != nil || catalog.Store.Binding != profile.Binding || witness.RelationManifestDigest != profile.RelationManifestDigest {
		return schemainstall.ErrConflict
	}
	applyID, err := apply.Identity()
	if err != nil {
		return err
	}
	state := &rf3SchemaGeneration{identity: identity, path: path, wal: log,
		base: catalog.Store.Clone(), applyID: applyID, apply: apply, preparation: &ownedSpec,
		manifest: rf3Manifest{SQL: rf3ManifestSQL{Path: path,
			IdentityPath:      filepath.Join(reservationRoot, "sql-identity.vibejson"),
			ApplyIdentityPath: filepath.Join(reservationRoot, "apply-identity.vibejson")},
			Route: rf3ManifestGroupRoute{Group: identity.Group, MemberRoot: reservationRoot}},
	}
	// Retain only the source-certified portable donor template. Local split
	// paths and bootstrap authority are never synthesized for an adopted group.
	state.manifest.SplitControl.ChildRegistry = rf3ManifestSplitChildRegistry{
		Table: ownedSpec.Table, CreateTable: ownedSpec.CreateTable, SchemaStatements: ownedSpec.SchemaStatements,
		WAL: rf3ManifestSplitChildWAL{Options: raftstore.Options{MaxFileBytes: ownedSpec.Log.MaxFileBytes,
			MaxRecordBytes: ownedSpec.Log.MaxRecordBytes, MaxRecords: ownedSpec.Log.MaxRecords,
			MaxEntries: ownedSpec.Log.MaxEntries, MaxLiveBytes: ownedSpec.Log.MaxLiveBytes}},
	}
	for _, index := range ownedSpec.GlobalIndexes {
		state.manifest.SplitControl.ChildRegistry.GlobalIndexes = append(state.manifest.SplitControl.ChildRegistry.GlobalIndexes,
			sqldriver.ReplicatedGlobalIndexRelation{Relation: index.Relation, Table: index.Table, IndexID: index.IndexID,
				Incarnation: index.Incarnation, LocatorCount: index.LocatorCount, Unique: index.Unique,
				KeyEncoding: sqldriver.ReplicatedRelationKeyEncoding(index.KeyEncoding), KeyArity: index.KeyArity,
				TupleVersion: distribution.TupleVersion(index.TupleVersion), MapperVersion: distribution.MapperVersion(index.MapperVersion),
				BucketBits: index.BucketBits})
	}
	a.mu.Lock()
	if prior := a.groups[identity.Group]; prior != nil {
		a.mu.Unlock()
		// Schema drain can hold the generation lock while reading the map.
		// Never wait for that lock with the directory exclusively locked.
		prior.mu.Lock()
		defer prior.mu.Unlock()
		a.mu.RLock()
		defer a.mu.RUnlock()
		if a.groups[identity.Group] == prior && prior.identity == identity && prior.apply == apply && prior.path == path &&
			prior.preparation != nil && prior.preparation.Digest() == ownedSpec.Digest() {
			return nil
		}
		return schemainstall.ErrConflict
	}
	defer a.mu.Unlock()
	if len(a.groups) >= maxRF3ManifestGroups {
		return schemainstall.ErrBound
	}
	a.groups[identity.Group] = state
	return nil
}

// UnregisterDynamic rolls back metadata that has not become serving. It does
// not close SQL resources, which remain owned by the execution runtime.
func (a *rf3SchemaActivator) UnregisterDynamic(identity raftmember.RuntimeIdentity) error {
	_, err := a.removeGeneration(identity, false)
	return err
}

// RemoveRetired is called only after the owner has successfully retired the
// authenticated replica. Schema activation may have advanced its manifest
// since the retirement request was certified; immutable replica coordinates
// still have to match before withdrawing static or dynamic metadata.
func (a *rf3SchemaActivator) RemoveRetired(identity raftmember.RuntimeIdentity) (bool, error) {
	return a.removeGeneration(identity, true)
}

func (a *rf3SchemaActivator) removeGeneration(identity raftmember.RuntimeIdentity, retired bool) (bool, error) {
	if a == nil {
		return false, schemainstall.ErrInvalid
	}
	a.mu.RLock()
	state := a.groups[identity.Group]
	a.mu.RUnlock()
	if state == nil {
		return false, nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.groups[identity.Group] == nil {
		return false, nil
	}
	want, actual := identity, state.identity
	if retired {
		want.RelationManifestDigest, actual.RelationManifestDigest = [32]byte{}, [32]byte{}
	}
	if a.groups[identity.Group] != state || actual != want || !retired && state.preparation == nil {
		return false, schemainstall.ErrConflict
	}
	delete(a.groups, identity.Group)
	return true, nil
}
