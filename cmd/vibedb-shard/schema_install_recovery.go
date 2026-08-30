package main

import (
	"bytes"
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	pb "go.etcd.io/raft/v3/raftpb"
)

type rf3SchemaWALReader interface {
	Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error)
}

func rf3SchemaTransitionGroup(binding replicatedstate.Binding) raftmember.GroupKey {
	return raftmember.GroupKey{ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch, ShardIncarnation: binding.ShardIncarnation,
		GroupID: binding.GroupID}
}

// rf3SchemaReplayNeutralSuffix accepts either Raft leader no-ops or complete,
// exact acquire/release transactions for this schema operation's exclusive
// route gate. Older recovery attempts could release and reacquire that gate;
// those four-command transactions mutate only retry/gate state, carry no user
// relation batch, and the final release is deterministically replayed after
// the target activates. Any partial lifecycle, foreign identity, other
// command, membership entry, or user mutation still fails closed.
func rf3SchemaReplayNeutralSuffix(wal rf3SchemaWALReader, sourceApplied, currentApplied uint64,
	operation [32]byte, group raftmember.GroupKey,
) error {
	if wal == nil || sourceApplied == 0 || currentApplied < sourceApplied || operation == ([32]byte{}) || group == (raftmember.GroupKey{}) {
		return schemainstall.ErrConflict
	}
	wantIdentity, wantBinding := schemainstall.SchemaDDLRouteGateIdentity(operation, group)
	phase := 0
	var client replication.ID128
	var epoch uint64
	for next := sourceApplied + 1; next <= currentApplied; {
		entries, err := wal.Entries(next, currentApplied+1, 1<<20)
		if err != nil || len(entries) == 0 {
			return errors.Join(err, schemainstall.ErrConflict)
		}
		for _, entry := range entries {
			if entry.GetIndex() != next || entry.GetType() != pb.EntryNormal {
				return schemainstall.ErrConflict
			}
			next++
			if len(entry.GetData()) == 0 {
				if phase != 0 {
					return schemainstall.ErrConflict
				}
				continue
			}
			command, err := replication.OpenCommand(entry.GetData())
			if err != nil || command.AuthorityClass != replication.CommandAuthorityTopology ||
				command.MutationCount() != 0 || command.RelationCount() != 0 {
				return errors.Join(err, schemainstall.ErrConflict)
			}
			switch phase {
			case 0:
				if command.Kind() != replication.CommandSessionOpen {
					return schemainstall.ErrConflict
				}
				client, phase = command.ClientID, 1
			case 1:
				gate, gateErr := command.OpenRouteGate()
				if gateErr != nil || command.Kind() != replication.CommandRouteGate || command.ClientID != client ||
					(gate.Operation != routegate.OperationBeginExclusive && gate.Operation != routegate.OperationReleaseExclusive) ||
					gate.Identity != wantIdentity || gate.Binding != wantBinding {
					return errors.Join(gateErr, schemainstall.ErrConflict)
				}
				epoch, phase = command.ClientEpoch, 2
			case 2:
				if command.Kind() != replication.CommandSessionRetire || command.ClientID != client || command.ClientEpoch != epoch {
					return schemainstall.ErrConflict
				}
				phase = 3
			case 3:
				if command.Kind() != replication.CommandSessionRelease || command.ClientID != client || command.ClientEpoch != epoch {
					return schemainstall.ErrConflict
				}
				client, epoch, phase = replication.ID128{}, 0, 0
			}
		}
	}
	if phase != 0 {
		return schemainstall.ErrConflict
	}
	return nil
}

// rf3SchemaEmptyNormalSuffix proves that every applied entry after a staged
// source cut is a Raft leader no-op. It is deliberately stricter than merely
// comparing applied indexes: any command or membership entry rejects recovery.
func rf3SchemaEmptyNormalSuffix(wal rf3SchemaWALReader, sourceApplied, currentApplied uint64) error {
	if wal == nil || sourceApplied == 0 || currentApplied < sourceApplied {
		return schemainstall.ErrConflict
	}
	for next := sourceApplied + 1; next <= currentApplied; {
		entries, err := wal.Entries(next, currentApplied+1, 1<<20)
		if err != nil || len(entries) == 0 {
			return errors.Join(err, schemainstall.ErrConflict)
		}
		for _, entry := range entries {
			if entry.GetIndex() != next || entry.GetType() != pb.EntryNormal || len(entry.GetData()) != 0 {
				return schemainstall.ErrConflict
			}
			next++
		}
	}
	return nil
}

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

// A drained N-1 activation remains as bounded lineage evidence while N is
// staged. Its target is the new request's exact source, so it is neither an
// in-flight N command nor a conflict and may be replaced by Persist below.
func rf3SchemaTransitionIsPredecessor(
	request schemainstall.Request, transition replicatedstate.SchemaTransitionView,
) bool {
	from := transition.From
	group := raftmember.GroupKey{ClusterID: from.ClusterID, ClusterIncarnation: from.ClusterIncarnation,
		TopologyRecoveryEpoch: from.TopologyRecoveryEpoch, ShardIncarnation: from.ShardIncarnation, GroupID: from.GroupID}
	return group == request.Group && from.AllocationGeneration == uint64(request.AllocationGeneration) &&
		transition.ToSchemaGeneration == request.FromSchemaGeneration &&
		transition.ToManifest == request.FromRelationManifestDigest
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
	return settleRF3SchemaCommitWithAlias(ctx, owners, group, command, nil)
}

func settleRF3SchemaCommitWithAlias(ctx context.Context, owners rf3SchemaOwner,
	group raftmember.GroupKey, command []byte, alias func() (bool, error),
) error {
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
	if err = owners.ProposeSchemaTransition(ctx, serving.Fence(), command); err != nil &&
		!errors.Is(err, raftservice.ErrOutcomeUnknown) && !errors.Is(err, raftmodel.ErrNotLeader) {
		return err
	}
	return waitRF3SchemaCommit(ctx, owners, group, command, alias)
}
