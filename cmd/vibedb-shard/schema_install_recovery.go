package main

import (
	"bytes"
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/schemainstall"
)

func validateRF3SchemaTransition(request schemainstall.Request, authorization schemainstall.Authorization, transition replicatedstate.SchemaTransitionView) error {
	from := transition.From
	group := raftmember.GroupKey{ClusterID: from.ClusterID, ClusterIncarnation: from.ClusterIncarnation,
		TopologyRecoveryEpoch: from.TopologyRecoveryEpoch, ShardIncarnation: from.ShardIncarnation, GroupID: from.GroupID}
	if authorization.Operation != request.Operation || group != request.Group ||
		from.AllocationGeneration != uint64(request.AllocationGeneration) ||
		transition.RequestDigest != request.Operation ||
		transition.AuthorizationDigest != schemainstall.AuthorizationDigest(authorization) ||
		from.SchemaGeneration != request.FromSchemaGeneration ||
		transition.FromManifest != request.FromRelationManifestDigest ||
		transition.ToSchemaGeneration != request.ToSchemaGeneration ||
		transition.ToManifest != request.ToRelationManifestDigest ||
		transition.ToApplyContract != request.ApplyContractDigest {
		return schemainstall.ErrConflict
	}
	return nil
}

// A persisted command owns the membership and CAS witnesses chosen at staging.
// Never rebuild it from the now-advanced source machine after commit or replay.
func rf3SchemaActivationCommand(request schemainstall.Request, authorization schemainstall.Authorization,
	persisted replicatedstate.SchemaTransitionView, found bool, build func() ([]byte, error),
) ([]byte, error) {
	if found {
		if err := validateRF3SchemaTransition(request, authorization, persisted); err != nil {
			return nil, err
		}
		return bytes.Clone(persisted.Bytes()), nil
	}
	if build == nil {
		return nil, schemainstall.ErrConflict
	}
	command, err := build()
	if err != nil {
		return nil, err
	}
	transition, err := replicatedstate.OpenSchemaTransition(command)
	if err != nil {
		return nil, err
	}
	if err := validateRF3SchemaTransition(request, authorization, transition); err != nil {
		return nil, err
	}
	return command, nil
}

// A durable N+1 transition must not be proposed again merely because its
// target catalog has not yet been published. An uncertain proposal is settled
// against the same bytes through the serialized owner lane.
func settleRF3SchemaCommit(ctx context.Context, owners rf3SchemaOwner, group raftmember.GroupKey, command []byte) error {
	committed, err := owners.ObserveSchemaTransition(ctx, group, command)
	if err != nil {
		return err
	}
	if committed {
		return nil
	}
	serving, err := owners.Probe(ctx, group)
	if err != nil {
		return err
	}
	if err = owners.ProposeSchemaTransition(ctx, serving.Fence(), command); err != nil && !errors.Is(err, raftservice.ErrOutcomeUnknown) {
		return err
	}
	return waitRF3SchemaCommit(ctx, owners, group, command)
}
