package rf3testfixture

import (
	"crypto/sha256"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftstore"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	DurableGatewayCatalogGroup = iota
	DurableGatewayLedgerGroup
	DurableGatewayDataAGroup
	DurableGatewayDataBGroup
	DurableGatewayGroups
)

// DurableGatewayMemberProfiles is the shared schema/apply input for the shipped
// gateway RF3 process gates and the shard command's strict-manifest preflight.
// Each data group also hosts the other table's exact global-index relation.
func DurableGatewayMemberProfiles() [DurableGatewayGroups]MemberOptions {
	authority := sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: 5,
		ProtectionEpoch: 7, OwnershipEpoch: 11, SchemaGeneration: 13,
		RoutingVersion: 17, RouteGeneration: 19}
	apply := sqldriver.ReplicatedApplyOptions{MaxSessions: 96, RetryWindow: 16,
		TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 2048, MaxBytes: 256 << 20},
		Placement: sqldriver.ReplicatedPlacementProfile{
			Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "/id",
			TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
			Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		}}
	profiles := [DurableGatewayGroups]MemberOptions{
		{Table: "controlplane", CreateTable: "CREATE TABLE controlplane (PRIMARY KEY (id))"},
		{Table: "request_ledger_home", CreateTable: "CREATE TABLE request_ledger_home (PRIMARY KEY (home))"},
		{Table: "orders_a", CreateTable: "CREATE TABLE orders_a (PRIMARY KEY (id))",
			SchemaStatements: []string{`CREATE INDEX by_kind_a ON orders_a (kind)`,
				`CREATE TABLE orders_b_email (PRIMARY KEY (key))`},
			GlobalIndexes: []sqldriver.ReplicatedGlobalIndexRelation{{
				Relation: 2, Table: "orders_b_email", IndexID: 42, Incarnation: 1,
				LocatorCount: 1, Unique: true,
				KeyEncoding: sqldriver.ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
				TupleVersion:  distribution.CurrentTupleVersion,
				MapperVersion: distribution.NativeMapperVersion,
				BucketBits:    distribution.DefaultVirtualBucketBits}}},
		{Table: "orders_b", CreateTable: "CREATE TABLE orders_b (PRIMARY KEY (id))",
			SchemaStatements: []string{`CREATE INDEX by_kind_b ON orders_b (kind)`,
				`CREATE TABLE orders_a_email (PRIMARY KEY (key))`},
			GlobalIndexes: []sqldriver.ReplicatedGlobalIndexRelation{{
				Relation: 2, Table: "orders_a_email", IndexID: 41, Incarnation: 1,
				LocatorCount: 1, Unique: true,
				KeyEncoding: sqldriver.ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
				TupleVersion:  distribution.CurrentTupleVersion,
				MapperVersion: distribution.NativeMapperVersion,
				BucketBits:    distribution.DefaultVirtualBucketBits}}},
	}
	for group := range profiles {
		profiles[group].Authority = authority
		profiles[group].Apply = apply
	}
	ledger := &profiles[DurableGatewayLedgerGroup].Apply
	ledger.Placement.ShardKey = "/home"
	ledger.RequestLedgerCapacityBytes = 64 << 20
	ledger.RequestLedgerCleanupReserveBytes = 8 << 20
	ledger.RequestLedgerRangeIdentity = sha256.Sum256([]byte("vibedb/external-process/request-ledger/range\x00"))
	return profiles
}

func DurableGatewayWALOptions() raftstore.Options {
	// The maximum Ready is an admission reserve, not useful live-log capacity.
	// Leave a bounded 8 MiB/16K-entry workload suffix in addition to that
	// reserve. A profile with only two records and one Ready of live bytes
	// can be created, but cannot survive its first election and proposal.
	const suffixBytes = 8 << 20
	const suffixEntries = 16 << 10
	return raftstore.Options{
		MaxFileBytes: int64(raftstore.HeaderBytes+raftstore.MaxSnapshotBaseRecordBytes+
			raftstore.MinimumReadyRecordBytes) + raftstore.MinimumReadyLiveBytes + suffixBytes,
		MaxRecordBytes: raftstore.MinimumReadyRecordBytes, MaxRecords: suffixEntries,
		MaxEntries:   raftstore.MaxReadyEntries + suffixEntries,
		MaxLiveBytes: raftstore.MinimumReadyLiveBytes + suffixBytes,
	}
}
