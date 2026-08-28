package shardservice

import (
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/raftservice"
)

// BindRestoreServingAuthority keeps a freshly restored shard unavailable until
// the target catalog's replicated activation witness has been validated. Raft
// peer traffic and readiness probing remain independent of this client-serving
// gate, so RF3 can elect and prove readiness before activation.
func (server *ReplicatedServer) BindRestoreServingAuthority(
	authority *clusterrestore.ServingAuthority,
) error {
	if authority == nil {
		return ErrReplicatedWire
	}
	return server.BindServingAuthority(func(state raftservice.ServingState) bool {
		identity := state.Identity
		return authority.AllowsReplica(identity.Group, identity.MemberID, identity.StoreID,
			identity.NodeIncarnation)
	})
}
