//go:build darwin || linux

package main

import (
	"crypto/sha256"
	"errors"
	"runtime"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/storeio"
	vibesql "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	durableRF3CatalogGroup = iota
	durableRF3LedgerGroup
	durableRF3DataAGroup
	durableRF3DataBGroup
	durableRF3ExternalGroups
)

var durableRF3ExternalRoleNames = [...]string{"catalog", "request-ledger", "data-a", "data-b"}

// The external process gates and the host preflight consume the same complete
// schema/apply profile. Keep canonical JSON pointers here: the ledger has a
// different primary key from the catalog and user-data relations.
func durableRF3ExternalMemberProfiles() [durableRF3ExternalGroups]rf3testfixture.MemberOptions {
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
	profiles := [durableRF3ExternalGroups]rf3testfixture.MemberOptions{
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
	ledger := &profiles[durableRF3LedgerGroup].Apply
	ledger.Placement.ShardKey = "/home"
	ledger.RequestLedgerCapacityBytes = 64 << 20
	ledger.RequestLedgerCleanupReserveBytes = 8 << 20
	ledger.RequestLedgerRangeIdentity = sha256.Sum256([]byte("vibedb/external-process/request-ledger/range\x00"))
	return profiles
}

func durableRF3ExternalWALOptions() raftstore.Options {
	return raftstore.Options{
		MaxFileBytes: int64(raftstore.HeaderBytes+raftstore.MaxSnapshotBaseRecordBytes+
			raftstore.MinimumReadyRecordBytes) + raftstore.MinimumReadyLiveBytes,
		MaxRecordBytes: raftstore.MinimumReadyRecordBytes, MaxRecords: 2,
		MaxEntries: raftstore.MaxReadyEntries, MaxLiveBytes: raftstore.MinimumReadyLiveBytes,
	}
}

// This is deliberately not Linux-only or opt-in: compile-only checks cannot
// validate placement/schema equality or open a real multi-relation apply bundle.
func TestDurableRF3FixturePreparedProfilesPreflight(t *testing.T) {
	profiles := durableRF3ExternalMemberProfiles()
	for group := range profiles {
		t.Run(durableRF3ExternalRoleNames[group], func(t *testing.T) {
			options := profiles[group]
			// Grammar validation is production code and runs before any platform-
			// specific strict allocation. Compare it to the actual parsed schema,
			// so either the old "id" spelling or a ledger /id mismatch fails here.
			if _, err := vibejson.Marshal(&options.Apply.Placement); err != nil {
				t.Fatalf("canonical placement grammar: %v", err)
			}
			statement, err := vibesql.ParseStatement(options.CreateTable)
			if err != nil || statement.CreateTable == nil || len(statement.CreateTable.PrimaryKey) != 1 {
				t.Fatalf("fixture primary schema: %v", err)
			}
			segments := statement.CreateTable.PrimaryKey[0].Segments
			if len(segments) != 1 || segments[0].IsIndex ||
				options.Apply.Placement.ShardKey != "/"+segments[0].Key {
				t.Fatalf("schema primary=%+v placement=%q", segments, options.Apply.Placement.ShardKey)
			}
			if len(options.SeedDocuments) != 0 {
				t.Fatal("replicated bundle must be unmaterialized; seed through shipped gateway")
			}
			options.Root = t.TempDir()
			options.Identity = replicaProcessIdentities()[0]
			options.Identity.GroupID[0] += byte(group)
			options.Key = raftstore.Key{ID: "fixture-preflight-key", Wrapped: []byte("wrapped-key")}
			options.Key.Material[0] = 1
			options.WAL = durableRF3ExternalWALOptions()
			options.Bootstrap = rf3testfixture.InitialBootstrap([]uint64{1, 2, 3})
			prepared, err := rf3testfixture.PrepareMember(options)
			if runtime.GOOS != "linux" && errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
				// This is an asserted platform contract, not a skipped preflight.
				// All schema/bundle validation precedes the strict allocation gate.
				t.Log("canonical schema/bundle accepted; strict sidecars correctly refused off Linux")
				return
			}
			if err != nil {
				t.Fatalf("prepare canonical schema/apply bundle: %v", err)
			}
			defer func() {
				if err := prepared.Close(); err != nil {
					t.Fatal(err)
				}
			}()
			if prepared.Base.UserPrimaryKey != options.Apply.Placement.ShardKey {
				t.Fatalf("prepared primary key=%q placement=%q", prepared.Base.UserPrimaryKey,
					options.Apply.Placement.ShardKey)
			}
			if len(prepared.Base.Relations) != 1+len(options.GlobalIndexes) {
				t.Fatalf("prepared relations=%d want=%d", len(prepared.Base.Relations), 1+len(options.GlobalIndexes))
			}
			if _, err := vibejson.Marshal(&prepared.ApplyIdentity); err != nil {
				t.Fatalf("persist canonical apply identity: %v", err)
			}
			if err := prepared.Apply.Close(); err != nil {
				t.Fatal(err)
			}
			prepared.Apply = nil
			if err := prepared.Database.Close(); err != nil {
				t.Fatal(err)
			}
			prepared.Database = nil
			prepared.Database, prepared.Apply, err = raftmember.OpenBoundSQLWithApply(
				prepared.SQLPath, prepared.WAL, options.Authority, prepared.Base, prepared.ApplyIdentity)
			if err != nil {
				t.Fatalf("reopen exact prepared schema/apply bundle: %v", err)
			}
		})
	}
}
