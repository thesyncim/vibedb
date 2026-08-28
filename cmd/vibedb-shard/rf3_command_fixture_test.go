package main

import (
	"encoding/asn1"
	"fmt"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

const rf3CommandMembers = 3

var rf3CommandIdentityOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}

func rf3CommandPolicy(nodes [rf3CommandMembers]rafttransport.NodeID) []byte {
	return []byte(fmt.Sprintf(
		`{"generation":5,"principals":[{"node":"%x","capabilities":["delegate","membership","topology"]},{"node":"%x","capabilities":["delegate","membership","topology"]},{"node":"%x","capabilities":["delegate","membership","topology"]}]}`,
		nodes[0], nodes[1], nodes[2],
	))
}

func rf3CommandNodes() (nodes [rf3CommandMembers]rafttransport.NodeID) {
	for member := range nodes {
		for index := range nodes[member] {
			nodes[member][index] = byte((member+1)*17 + index)
		}
	}
	return nodes
}

func rf3CommandStoreIdentity(member uint64) raftstore.Identity {
	identity := raftstore.Identity{
		Distribution:         string(gateway.ReplicatedCatalogDistribution),
		Shard:                string(gateway.ReplicatedCatalogShard),
		AllocationGeneration: 23,
		MemberID:             member,
	}
	for index := range identity.ClusterID {
		identity.ClusterID[index] = byte(index + 1)
		identity.ClusterIncarnation[index] = byte(index + 21)
		identity.ShardIncarnation[index] = byte(index + 141)
		identity.GroupID[index] = byte(index + 161)
		identity.StoreID[index] = byte(index+181) ^ byte(member)
	}
	return identity
}

func rf3CommandGroup() raftmember.GroupKey {
	identity := rf3CommandStoreIdentity(1)
	return raftmember.GroupKey{
		ClusterID:             identity.ClusterID,
		ClusterIncarnation:    identity.ClusterIncarnation,
		TopologyRecoveryEpoch: 3,
		ShardIncarnation:      identity.ShardIncarnation,
		GroupID:               identity.GroupID,
	}
}

func rf3CommandAuthority() sqldriver.ReplicatedAuthorityProfile {
	return sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 5,
		ProtectionEpoch:        7,
		OwnershipEpoch:         11,
		SchemaGeneration:       13,
		RoutingVersion:         17,
		RouteGeneration:        19,
	}
}
