package rf3testfixture

import (
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func durableCatalogTestRoute(ordinal byte) gateway.ReplicatedRoute {
	group := raftmember.GroupKey{TopologyRecoveryEpoch: 3}
	for index := range group.ClusterID {
		group.ClusterID[index] = byte(index + 1)
		group.ClusterIncarnation[index] = byte(index + 21)
		group.ShardIncarnation[index] = byte(index + 41 + int(ordinal)*17)
		group.GroupID[index] = byte(index + 61 + int(ordinal)*17)
	}
	route := gateway.ReplicatedRoute{
		Distribution: distribution.DistributionName("fixture-" + string(rune('a'+ordinal))),
		Shard:        "all", Group: group, AllocationGeneration: uint64(ordinal) + 1,
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 2, ProtectionEpoch: 3,
			OwnershipEpoch: 4, SchemaGeneration: 5,
			RelationManifestDigest: [32]byte{6, ordinal + 1},
			RoutingVersion:         7, RouteGeneration: 8,
		},
		RangeIdentity:        replication.Digest{0x71, ordinal + 1},
		LineageDigest:        replication.Digest{0x72, ordinal + 1},
		ForwardingRuleDigest: replication.Digest{0x73, ordinal + 1},
		Replicas:             make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount),
	}
	for member := uint64(1); member <= gateway.ServingReplicaCount; member++ {
		var node rafttransport.NodeID
		node[0] = byte(member)
		var store [16]byte
		store[0], store[1] = ordinal+1, byte(member)
		address := "127.0.0.1:" + string(rune('1'+ordinal)) + string(rune('0'+member)) + "00"
		route.Replicas = append(route.Replicas, gateway.ReplicatedEndpoint{
			Member: member, Node: node, StoreID: store, NodeIncarnation: member,
			Endpoint: address, NativeEndpoint: address, Address: address,
			ControlEndpoint: address,
		})
	}
	return route
}

func TestDurableCatalogBindsTwoDataGroupsLedgerAndSharedAckAuthority(t *testing.T) {
	first, second, ledger := durableCatalogTestRoute(0), durableCatalogTestRoute(1), durableCatalogTestRoute(2)
	target := gateway.ReplicatedEndpoint{
		Member: 4, Node: rafttransport.NodeID{4}, StoreID: [16]byte{4}, NodeIncarnation: 4,
		Endpoint: "127.0.0.1:1400", NativeEndpoint: "127.0.0.1:1400",
		Address: "127.0.0.1:1400", ControlEndpoint: "127.0.0.1:1400",
	}
	ackKey := gateway.DurableRequestAckDerivationKey{0xa1}
	built, err := NewDurableCatalog(DurableCatalogOptions{
		Generation: 11, AckKey: ackKey,
		Groups: []DurableCatalogGroup{
			{Route: first, Table: "orders", PrimaryKey: "/id", Relation: 1,
				MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20, EnrolledTarget: &target},
			{Route: second, Table: "customers", PrimaryKey: "/id", Relation: 1,
				MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20},
			{Route: ledger, Table: "request_ledger", PrimaryKey: "/id",
				LedgerRanges: []gateway.DurableRequestLedgerRangeDescriptor{{
					Identity: replication.Digest{0x91},
				}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if built.Snapshot == nil || built.AckKey != ackKey {
		t.Fatalf("fixture = %+v", built)
	}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	var scratch [replication.MaxMutationKeyBytes + 16]byte
	key, ok := orderedkey.AppendString(nil, []byte("primary-1"), orderedkey.Ascending)
	if !ok {
		t.Fatal("ordered key")
	}
	for _, candidate := range []struct {
		table string
		route gateway.ReplicatedRoute
	}{{"orders", first}, {"customers", second}} {
		resolved, found := built.Snapshot.ResolveReplicatedTableKey(
			[]byte(candidate.table), key, scratch[:0], replicas[:0],
		)
		if !found || resolved.Route.Group != candidate.route.Group ||
			resolved.Route.RangeIdentity != candidate.route.RangeIdentity ||
			len(resolved.Route.Replicas) != gateway.ServingReplicaCount {
			t.Fatalf("resolve %s = %+v,%v", candidate.table, resolved, found)
		}
	}
	membership, found := built.Snapshot.ResolveReplicatedMembershipRoute(
		first.Distribution, first.Shard, replicas[:0],
	)
	if !found || !membership.HasEnrolledTarget || membership.EnrolledTarget.Member != target.Member ||
		membership.EnrolledTarget.Address != target.NativeEndpoint {
		t.Fatalf("membership target = %+v,%v", membership, found)
	}
	topology, err := gateway.NewCatalogDurableRequestLedgerTopologyHolder(
		gateway.NewCatalogHolder(built.Snapshot),
	)
	if err != nil {
		t.Fatal(err)
	}
	home, generation, found := topology.Lookup(requestledger.LedgerHome{0x55})
	if !found || generation != 11 || home.Identity != (replication.Digest{0x91}) ||
		home.ReplicatedRoute().Group != ledger.Group {
		t.Fatalf("ledger home = %+v generation=%d found=%v", home, generation, found)
	}
}

func TestDurableCatalogFailsClosedOnAuthorityAndRoleAmbiguity(t *testing.T) {
	data, ledger := durableCatalogTestRoute(0), durableCatalogTestRoute(1)
	valid := DurableCatalogOptions{Generation: 1,
		AckKey: gateway.DurableRequestAckDerivationKey{1},
		Groups: []DurableCatalogGroup{
			{Route: data, Table: "docs", PrimaryKey: "/id", Relation: 1,
				MaxKeyBytes: 64, MaxDocumentBytes: 1024},
			{Route: ledger, Table: "request_ledger", PrimaryKey: "/id",
				LedgerRanges: []gateway.DurableRequestLedgerRangeDescriptor{{Identity: replication.Digest{1}}}},
		}}
	for name, mutate := range map[string]func(*DurableCatalogOptions){
		"zero ack": func(options *DurableCatalogOptions) {
			options.AckKey = gateway.DurableRequestAckDerivationKey{}
		},
		"no ledger": func(options *DurableCatalogOptions) {
			options.Groups[1].LedgerRanges = nil
			options.Groups[1].Relation = 1
			options.Groups[1].MaxKeyBytes = 64
			options.Groups[1].MaxDocumentBytes = 1024
		},
		"two ledgers": func(options *DurableCatalogOptions) {
			options.Groups[0].Relation = 0
			options.Groups[0].LedgerRanges = []gateway.DurableRequestLedgerRangeDescriptor{{Identity: replication.Digest{2}}}
		},
		"missing lineage": func(options *DurableCatalogOptions) {
			options.Groups[0].Route.LineageDigest = replication.Digest{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			candidate.Groups = append([]DurableCatalogGroup(nil), valid.Groups...)
			mutate(&candidate)
			if _, err := NewDurableCatalog(candidate); err == nil {
				t.Fatal("accepted invalid fixture")
			}
		})
	}
}
