//go:build darwin || linux

package main

import (
	"errors"
	"runtime"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/storeio"
	vibesql "github.com/thesyncim/vibedb/sql"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	durableRF3CatalogGroup   = rf3testfixture.DurableGatewayCatalogGroup
	durableRF3LedgerGroup    = rf3testfixture.DurableGatewayLedgerGroup
	durableRF3DataAGroup     = rf3testfixture.DurableGatewayDataAGroup
	durableRF3DataBGroup     = rf3testfixture.DurableGatewayDataBGroup
	durableRF3ExternalGroups = rf3testfixture.DurableGatewayGroups
)

var durableRF3ExternalRoleNames = [...]string{"catalog", "request-ledger", "data-a", "data-b"}

// The external process gates and the host preflight consume the same complete
// schema/apply profile. Keep canonical JSON pointers here: the ledger has a
// different primary key from the catalog and user-data relations.
func durableRF3ExternalMemberProfiles() [durableRF3ExternalGroups]rf3testfixture.MemberOptions {
	return rf3testfixture.DurableGatewayMemberProfiles()
}

func durableRF3ExternalWALOptions() raftstore.Options {
	return rf3testfixture.DurableGatewayWALOptions()
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
