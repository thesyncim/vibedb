package main

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestRF3DynamicDonorsRecoverJournalAndWithdrawExactReplica(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	group := serveRF3TestGroup()
	node := rafttransport.NodeID{1}
	registry, err := rafttransport.NewStaticRegistry(node, []rafttransport.Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: node, Role: rafttransport.MemberVoter},
	}, rafttransport.Limits{MaxGroups: 1, MaxMembers: 1})
	if err != nil {
		t.Fatal(err)
	}
	identity := raftmember.RuntimeIdentity{Group: group, AllocationGeneration: 1,
		MemberID: 1, StoreID: [16]byte{2}, NodeIncarnation: 1, RelationManifestDigest: [32]byte{3}}
	// Service construction never needs a SQL read; the certified installer
	// supplies that separately. Every attempt to export this unopened source
	// remains fenced by ReplicatedApply's own state checks.
	state := &rf3SchemaGeneration{identity: identity, apply: new(sqldriver.ReplicatedApply)}
	schemas := &rf3SchemaActivator{groups: make(map[raftmember.GroupKey]*rf3SchemaGeneration)}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{Node: node, Capabilities: serviceauthz.CapabilityMembership}})
	if err != nil {
		t.Fatal(err)
	}
	budget, err := migrationbudget.New(migrationbudget.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	manifest := rf3Manifest{ReplicaControl: rf3ManifestReplicaControl{
		SourceDataRoot: root, SourceJournalPath: filepath.Join(root, "source-journal"),
		SourceRepositoryPath: filepath.Join(root, "source-artifacts"), MaxSourceRecords: 8,
		MaxSourceArtifacts: 4, MaxSourceArtifactBytes: 1 << 20, MaxSourceDiskBytes: 4 << 20,
		SourceChunkBytes: snapshottransfer.MinChunkBytes, MaxSourceConcurrent: 1}}
	open := func() *rf3DynamicDonorServices {
		donors, err := newRF3DynamicDonorServices(schemas, registry, policy, manifest, budget,
			func() time.Time { return time.Now().Add(time.Second) })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = donors.Close() })
		return donors
	}
	donors := open()
	if err := donors.Register(group); !errors.Is(err, snapshottransfer.ErrSourceUnauthorized) {
		t.Fatalf("unknown schema group registered: %v", err)
	}
	if donors.controlService(group) != nil || donors.dataService(group) != nil {
		t.Fatal("empty source inventory became serving")
	}
	schemas.groups[group] = state
	if err := donors.Register(group); err != nil {
		t.Fatal(err)
	}
	if donors.controlService(group) == nil || donors.dataService(group) == nil {
		t.Fatal("adopted source was absent from the live routers")
	}
	record := snapshottransfer.SourceControlRecord{Request: snapshottransfer.SourceControlRequest{
		Operation: [32]byte{4}, Step: [32]byte{5}, Group: group, SourceMember: 1,
		TargetMember: 2, TargetStore: [16]byte{6}, TargetIncarnation: 1, ReplicaSetVersion: 2, SourceNode: node,
	}, Revision: 1, State: snapshottransfer.SourceControlRunning}
	if err := donors.groups[group].journal.PublishSourceExport(t.Context(), 0, record); err != nil {
		t.Fatal(err)
	}
	foreign := identity
	foreign.StoreID[0]++
	if err := donors.Unregister(foreign); !errors.Is(err, snapshottransfer.ErrSourceConflict) || donors.controlService(group) == nil {
		t.Fatalf("foreign replica withdrew live source: %v", err)
	}
	if err := donors.Close(); err != nil {
		t.Fatal(err)
	}
	if donors.controlService(group) != nil || donors.dataService(group) != nil {
		t.Fatal("closed donor still resolves")
	}
	restarted := open()
	if err := restarted.Register(group); err != nil {
		t.Fatal(err)
	}
	if got, err := restarted.groups[group].journal.ReadSourceExport(t.Context(), record.Request.Operation); err != nil || got != record {
		t.Fatalf("restart lost exact source operation: record=%+v err=%v", got, err)
	}
	if err := restarted.Unregister(identity); err != nil {
		t.Fatal(err)
	}
	if restarted.controlService(group) != nil || restarted.dataService(group) != nil {
		t.Fatal("withdrawn group still resolves")
	}
}

func TestRF3DynamicDonorCutFailsClosedWithoutActiveSQL(t *testing.T) {
	for _, cut := range []rf3DynamicDonorCut{{}, {state: &rf3SchemaGeneration{}},
		{state: &rf3SchemaGeneration{apply: new(sqldriver.ReplicatedApply), quiesced: true}}} {
		if snapshot, err := cut.SnapshotArtifactCut(); err == nil || snapshot != nil {
			t.Fatalf("inactive SQL produced a cut: %v", err)
		}
		if _, err := cut.SnapshotAuthorizationFence(); err == nil {
			t.Fatal("inactive SQL authorized snapshot data")
		}
	}
}
