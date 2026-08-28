package replicatedstate

import (
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func schemaAuditFixture(t *testing.T, rows int) (machineFixture, Binding, []RelationCollection, Options) {
	t.Helper()
	f := newMachineFixture(t)
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, f.machine, 2, commandValue(f.binding, 1))
	index := uint64(3)
	for first := 0; first < rows; first += 64 {
		mutations := make([]replication.Mutation, 0, 64)
		for i := first; i < min(first+64, rows); i++ {
			key := fmt.Sprintf("employee-%04d", i)
			mutations = append(mutations, replication.Mutation{Kind: replication.MutationPut,
				Key: []byte(key), Value: []byte(fmt.Sprintf(`{"id":%q,"city":"Lisbon"}`, key))})
		}
		if _, err := f.machine.ApplyNormal(normalMeta(index), testCommand(f.binding, index-2, mutations...)); err != nil {
			t.Fatal(err)
		}
		index++
	}
	to := f.binding
	to.SchemaGeneration++
	specs := []RelationCollection{{Relation: 1, Kind: RelationJSON, Name: "docs", Target: f.user}}
	// A materialized target has no pending journal fold. These generic-write
	// fixtures otherwise fold on Snapshot and change their physical identity.
	if err := f.user.Collection.Flush(); err != nil {
		t.Fatal(err)
	}
	audit, err := AuditSchemaImages(to, specs)
	if err != nil || audit.Certificate().TotalRows != uint64(rows) {
		t.Fatalf("audit rows=%d: %v", audit.Certificate().TotalRows, err)
	}
	proof := audit.Certificate()
	contract, err := RelationBundleApplyContractDigest(to, specs, BundleApplyContractOptions{
		MaxSessions: f.machine.options.MaxSessions, RetryWindow: f.machine.options.RetryWindow})
	if err != nil {
		t.Fatal(err)
	}
	transition := testSchemaTransition(f.binding, f.machine.manifestDigest, f.machine.applyContract,
		proof.ManifestDigest, contract, f.machine.state.ReplicaSetVersion)
	transition.FromPlacementDigest = f.machine.state.RelationPlacementDigest
	transition.ToPlacementDigest = proof.PlacementDigest
	command, err := AppendSchemaTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.machine.ApplyNormal(normalMeta(index), command); err != nil {
		t.Fatal(err)
	}
	options := f.machine.options
	options.SchemaTransition = command
	options.SchemaMembershipWitness = durable.CheckpointMembershipWitness{Sequence: transition.MembershipSequence,
		Source: transition.MembershipSource, Target: transition.MembershipTarget}
	options.SchemaAuthorizationDigest = transition.AuthorizationDigest
	options.SchemaCatalogCASDigest = transition.CatalogCASDigest
	options.SchemaImageAudit = audit
	return f, to, specs, options
}

func TestSchemaImageAuditActivatesThousandRowsWithoutRescanning(t *testing.T) {
	f, to, specs, options := schemaAuditFixture(t, 1000)
	plainOptions := options
	plainOptions.SchemaImageAudit = nil
	plain, err := OpenBundle(to, f.bootstrap, f.system, specs, f.log, plainOptions)
	if err != nil {
		t.Fatal(err)
	}
	before := f.user.Collection.Stats().SnapshotFullScanCalls
	systemBefore := f.system.Collection.Stats().SnapshotFullScanCalls
	active, err := OpenBundle(to, f.bootstrap, f.system, specs, f.log, options)
	if err != nil {
		t.Fatal(err)
	}
	if f.user.Collection.Stats().SnapshotFullScanCalls != before {
		t.Fatal("audited activation scanned target rows")
	}
	if f.system.Collection.Stats().SnapshotFullScanCalls <= systemBefore {
		t.Fatal("audit bypassed system/session validation")
	}
	if active.openedImageDigest != plain.openedImageDigest || active.state.DataChainDigest != plain.state.DataChainDigest ||
		active.state.SessionCount != plain.state.SessionCount || active.state.SessionSlotCount != plain.state.SessionSlotCount ||
		active.state.SessionEpochHighWater != plain.state.SessionEpochHighWater || active.state.SessionCount != 1 {
		t.Fatal("audit changed canonical image or durable session identity")
	}
	if _, err := active.ApplyNormal(normalMeta(active.Applied()+1), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenBundle(to, f.bootstrap, f.system, specs, f.log, options); !errors.Is(err, ErrSchemaTransition) {
		t.Fatalf("historical audit accepted beyond first activation: %v", err)
	}
}

func TestSchemaImageAuditRefusesStaleOrSubstitutedProofWithoutRescanning(t *testing.T) {
	for _, name := range []string{"zero", "binding", "target", "authority", "no-transition", "system"} {
		t.Run(name, func(t *testing.T) {
			f, to, specs, options := schemaAuditFixture(t, 1)
			switch name {
			case "zero":
				options.SchemaImageAudit = &SchemaImageAudit{}
			case "binding":
				to.ShardIncarnation[0]++
			case "target":
				if _, err := f.user.Collection.Put([]byte("employee-0000"), []byte(`{"id":"employee-0000","city":"Porto"}`)); err != nil {
					t.Fatal(err)
				}
			case "authority":
				options.SchemaAuthorizationDigest[0]++
			case "no-transition":
				options.SchemaTransition = nil
			case "system":
				if _, err := f.system.Collection.Put(stateKey, []byte("corrupt state")); err != nil {
					t.Fatal(err)
				}
			}
			before := f.user.Collection.Stats().SnapshotFullScanCalls
			if _, err := OpenBundle(to, f.bootstrap, f.system, specs, f.log, options); err == nil {
				t.Fatal("invalid proof authorized activation")
			}
			if f.user.Collection.Stats().SnapshotFullScanCalls != before {
				t.Fatal("invalid proof fell back to a row scan")
			}
		})
	}
}
