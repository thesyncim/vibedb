package schemainstall

import (
	"crypto/sha256"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/routegate"
)

// SchemaDDLRouteGateIdentity derives the exclusive traffic fence owned by one
// schema operation and physical group. It is shared by the coordinator and
// crash recovery so neither side can classify an unrelated control command as
// part of the schema cutover.
func SchemaDDLRouteGateIdentity(operation [32]byte, group raftmember.GroupKey) (routegate.Identity, routegate.Binding) {
	h := sha256.New()
	_, _ = h.Write(operation[:])
	_, _ = h.Write(group.ClusterID[:])
	_, _ = h.Write(group.ClusterIncarnation[:])
	_, _ = h.Write(group.ShardIncarnation[:])
	_, _ = h.Write(group.GroupID[:])
	var raw [32]byte
	h.Sum(raw[:0])
	var identity routegate.Identity
	var binding routegate.Binding
	copy(identity[:], raw[:16])
	copy(binding[:], raw[16:])
	return identity, binding
}
