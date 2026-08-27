package main

import (
	"bytes"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// The target catalog must commit its one-time activation witness before normal
// serving can open. This exception admits only that exact row and its bounded
// session lifecycle, with both internal topology and explicit restore authority.
// It cannot read catalog contents, perform DDL, or mutate other topology rows.
func rf3RestoreCatalogPreparingAuthority(
	authorization *serviceauthz.Gate, operation [32]byte, group raftmember.GroupKey,
	base sqldriver.ReplicatedShardStoreIdentity,
	baseServing func(raftservice.ServingState) bool,
) func(raftservice.ServingState, *shardservice.ReplicatedRequest) bool {
	return func(state raftservice.ServingState, request *shardservice.ReplicatedRequest) bool {
		if authorization == nil || operation == ([32]byte{}) || request == nil ||
			baseServing == nil || !baseServing(state) || state.Identity.Group != group ||
			base.Binding.Distribution != string(gateway.ReplicatedCatalogDistribution) ||
			base.Binding.Shard != string(gateway.ReplicatedCatalogShard) ||
			base.UserTable != gateway.ReplicatedCatalogTable || request.Fence.Group != group ||
			request.Capability != serviceauthz.CapabilityTopology ||
			authorization.CheckAuthority(request.Authority, serviceauthz.CapabilityRestoreActivate) != serviceauthz.DecisionAllow {
			return false
		}
		switch request.Operation {
		case shardservice.ReplicatedProbe:
			return true
		case shardservice.ReplicatedReadLeader:
			// Read admission reserves the frozen relation maximum before lookup;
			// the exact activation row still has a separate 1 KiB logical limit.
			return request.Relation == 1 &&
				request.MaxValueBytes == gateway.RestoreCatalogReadAdmissionBytes &&
				base.UserLimits.MaxDocumentBytes > 0 &&
				base.UserLimits.MaxDocumentBytes <= gateway.RestoreCatalogReadAdmissionBytes &&
				gateway.RestoreCatalogActivationKeyMatches(request.Key)
		case shardservice.ReplicatedPropose:
			command, err := replication.OpenCommand(request.Command)
			if err != nil || command.AuthorityClass != replication.CommandAuthorityTopology ||
				!bytes.Equal(command.Distribution, []byte(gateway.ReplicatedCatalogDistribution)) ||
				!bytes.Equal(command.Shard, []byte(gateway.ReplicatedCatalogShard)) {
				return false
			}
			switch command.Kind() {
			case replication.CommandSessionOpen, replication.CommandSessionRenew,
				replication.CommandSessionRevoke, replication.CommandSessionRetire,
				replication.CommandSessionRelease:
				return true
			case replication.CommandMutationBatch:
				batches := command.RelationBatches()
				if command.MutationCount() != 1 || !batches.Next() {
					return false
				}
				batch := batches.Batch()
				if batch.Relation != 1 || batches.Next() {
					return false
				}
				mutations := batch.Mutations()
				if !mutations.Next() {
					return false
				}
				mutation := mutations.Mutation()
				return mutation.Kind == replication.MutationPutAbsentOrEqual && !mutations.Next() &&
					gateway.RestoreCatalogActivationKeyMatches(mutation.Key) &&
					gateway.RestoreCatalogActivationDocumentMatches(mutation.Value, operation, group)
			}
		}
		return false
	}
}
