package raftmember

import (
	"errors"
	"fmt"
	"os"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"google.golang.org/protobuf/proto"
)

// OpenOrCreateStagedChildWAL settles the crash boundary around initial WAL
// publication. An existing WAL is accepted only when its immutable binding and
// exact snapshot base equal the activation certificate byte-for-byte in
// protobuf semantics; absence creates it through CreateStagedChildWAL.
func OpenOrCreateStagedChildWAL(
	path string,
	identity raftstore.Identity,
	key raftstore.Key,
	topologyRecoveryEpoch uint64,
	authority sqldriver.ReplicatedAuthorityProfile,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	activation sqldriver.ReplicatedChildActivation,
	options raftstore.Options,
) (*raftstore.Store, error) {
	wal, err := raftstore.Open(path, identity, topologyRecoveryEpoch, key, options)
	if err == nil {
		live, bindingErr := BindingFromWAL(wal, authority)
		base, snapshotErr := wal.Snapshot()
		if bindingErr == nil && snapshotErr == nil && live == expectedSQL.Binding &&
			activation.SnapshotBase != nil && proto.Equal(base, activation.SnapshotBase) {
			return wal, nil
		}
		closeErr := wal.Close()
		return nil, errors.Join(ErrBindingMismatch, bindingErr, snapshotErr, closeErr)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return CreateStagedChildWAL(path, identity, key, topologyRecoveryEpoch,
		authority, expectedSQL, activation, options)
}

// CreateStagedChildWAL creates the destination WAL exactly once from a
// certified no-copy child snapshot base. It proves the preplanned identity,
// SQL binding, snapshot state, and created live WAL all agree before returning.
// It does not mint a node incarnation or grant serving authority; the caller
// must pass the returned Store and activated apply claim to AdoptRuntime.
func CreateStagedChildWAL(
	path string,
	identity raftstore.Identity,
	key raftstore.Key,
	topologyRecoveryEpoch uint64,
	authority sqldriver.ReplicatedAuthorityProfile,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	activation sqldriver.ReplicatedChildActivation,
	options raftstore.Options,
) (*raftstore.Store, error) {
	planned, err := BindingForNewWAL(identity, topologyRecoveryEpoch, authority)
	if err != nil {
		return nil, err
	}
	if expectedSQL.Binding != planned || expectedSQL.LogID == ([16]byte{}) {
		return nil, ErrBindingMismatch
	}
	if activation.Apply == nil || activation.SnapshotBase == nil ||
		activation.ApplyIdentity == (sqldriver.ReplicatedApplyIdentity{}) {
		return nil, ErrBindingMismatch
	}
	applyIdentity, identityErr := activation.Apply.Identity()
	profile, profileErr := activation.Apply.CapacityQualificationProfile()
	opened, err := replicatedstate.OpenSnapshotBase(activation.SnapshotBase)
	if identityErr != nil || profileErr != nil || err != nil ||
		applyIdentity != activation.ApplyIdentity || profile.Binding != planned ||
		!profile.Initialized || profile.Applied != opened.Manifest.State.Applied ||
		opened.Manifest.Digest != activation.ArtifactManifest.Digest ||
		!stateBindingMatchesSQL(opened.Manifest.State.Binding, planned) {
		return nil, errors.Join(
			ErrBindingMismatch, identityErr, profileErr, err,
		)
	}
	if activation.ArtifactManifest.State.Binding != opened.Manifest.State.Binding ||
		activation.ArtifactManifest.State.Applied != opened.Manifest.State.Applied ||
		activation.ArtifactManifest.State.DataChainDigest != opened.Manifest.State.DataChainDigest {
		return nil, ErrBindingMismatch
	}
	wal, err := raftstore.Create(
		path, identity, key,
		raftstore.Bootstrap{
			TopologyRecoveryEpoch: topologyRecoveryEpoch,
			Snapshot:              activation.SnapshotBase,
		},
		options,
	)
	if err != nil {
		return nil, err
	}
	live, verifyErr := BindingFromWAL(wal, authority)
	if verifyErr == nil && live == expectedSQL.Binding {
		return wal, nil
	}
	closeErr := wal.Close()
	if verifyErr == nil {
		verifyErr = ErrBindingMismatch
	}
	return nil, errors.Join(
		fmt.Errorf("raftmember: verify staged child WAL: %w", verifyErr), closeErr,
	)
}

func stateBindingMatchesSQL(
	state replicatedstate.Binding,
	sql sqldriver.ReplicatedShardStoreBinding,
) bool {
	return state.ClusterID == replication.ID128(sql.ClusterID) &&
		state.ClusterIncarnation == replication.ID128(sql.ClusterIncarnation) &&
		state.TopologyRecoveryEpoch == sql.TopologyRecoveryEpoch &&
		state.Distribution == sql.Distribution && state.Shard == sql.Shard &&
		state.AllocationGeneration == sql.AllocationGeneration &&
		state.ShardIncarnation == replication.ID128(sql.ShardIncarnation) &&
		state.GroupID == replication.ID128(sql.GroupID) &&
		state.ActivePolicyGeneration == sql.Authority.ActivePolicyGeneration &&
		state.ProtectionEpoch == sql.Authority.ProtectionEpoch &&
		state.OwnershipEpoch == sql.Authority.OwnershipEpoch &&
		state.SchemaGeneration == sql.Authority.SchemaGeneration &&
		state.RoutingVersion == sql.Authority.RoutingVersion &&
		state.RouteGeneration == sql.Authority.RouteGeneration
}
