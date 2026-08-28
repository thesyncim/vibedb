package raftservice

import (
	"bytes"

	"github.com/thesyncim/vibedb/internal/replication"
)

// CatalogCommandReplayMatchesFence admits an exact older catalog-control
// command to replicated apply under the current serving fence. This does not
// authorize a stale mutation: admission returns its retained byte-identical
// completion or refuses an unapplied stale command. Without this settlement path an
// unknown catalog journal write cannot recover across the membership change
// which that same journal is coordinating. Data commands and changes to
// schema, policy, protection, group, or allocation remain excluded.
func CatalogCommandReplayMatchesFence(command replication.CommandView, fence ServingFence) bool {
	if command.AuthorityClass != replication.CommandAuthorityTopology ||
		!bytes.Equal(command.Distribution, []byte("catalog")) ||
		!bytes.Equal(command.Shard, []byte("controlplane")) ||
		command.ReplicaSetVersion > fence.Command.ReplicaSetVersion ||
		command.OwnershipEpoch > fence.Command.OwnershipEpoch ||
		command.RoutingVersion > fence.Command.RoutingVersion ||
		command.RouteGeneration > fence.Command.RouteGeneration {
		return false
	}
	// Compare every other command field without modifying the encoded bytes.
	fence.Command.ReplicaSetVersion = command.ReplicaSetVersion
	fence.Command.OwnershipEpoch = command.OwnershipEpoch
	fence.Command.RoutingVersion = command.RoutingVersion
	fence.Command.RouteGeneration = command.RouteGeneration
	return commandMatchesFence(command, fence)
}
