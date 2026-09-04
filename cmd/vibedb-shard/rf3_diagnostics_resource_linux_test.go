//go:build linux

package main

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Recover a genuinely committed schema generation using the production node
// opener. The prepared list and adopted runtime retain the closed predecessor;
// sampling must instead reach the current schema handle. A second real bundle
// exercises the native-union/provider wiring used for dynamically loaded groups.
func TestRF3DiagnosticRealApplyCurrentSchemaAndDynamicCoverage(t *testing.T) {
	f := newRF3NodeRecoveryFixture(t)
	wal, _ := f.store.GroupByID(f.boots[0].Descriptor.GroupID)
	_, _, sourceDB, source, err := openRF3RetainedApply(f.paths[0], wal, f.bases[0], f.applies[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close(); _ = sourceDB.Close() })
	member := &rf3testfixture.PreparedMember{SQLPath: f.paths[0], Base: f.bases[0],
		ApplyIdentity: f.applies[0], Apply: source}
	raw := prepareSchemaStartupTarget(t, member, rf3testfixture.MemberOptions{
		Table: "docs", CreateTable: `CREATE TABLE docs (PRIMARY KEY (id))`, Apply: f.applyOptions,
	})
	proof, err := source.PrepareReplicatedSchemaTarget(raw, source.Applied(), [32]byte{41})
	if err != nil {
		t.Fatal(err)
	}
	cas, err := source.ReplicatedSchemaCatalogCASDigest(proof, [32]byte{41}, [32]byte{42})
	if err != nil {
		t.Fatal(err)
	}
	command, err := source.AppendReplicatedSchemaTransition(nil, proof, sqldriver.ReplicatedSchemaTransitionAuthority{
		RequestDigest: [32]byte{41}, AuthorizationDigest: [32]byte{42}, CatalogCASDigest: cas,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.PersistReplicatedSchemaTransition(command); err != nil {
		t.Fatal(err)
	}
	descriptor, err := wal.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	incarnations, err := f.store.BeginIncarnations([]uint64{descriptor.LogKey})
	if err != nil {
		t.Fatal(err)
	}
	index, term, typ := proof.SourceApplied+1, uint64(2), pb.EntryNormal
	if err := wal.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnations[0].Incarnation, ReadyID: 1, MustSync: true,
		HardState: &pb.HardState{Term: &term, Commit: &index},
		Entries:   []*pb.Entry{{Index: &index, Term: &term, Type: &typ, Data: command}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ApplyNormal(raftmodel.ApplyMeta{Index: index, Term: term, Type: typ}, command); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(source.Close(), sourceDB.Close()); err != nil {
		t.Fatal(err)
	}
	base, applyID, currentDB, current, err := openRF3RetainedApply(f.paths[0], wal, f.bases[0], f.applies[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = current.Close(); _ = currentDB.Close() })
	if base.Binding.Authority.SchemaGeneration != 2 || applyID == f.applies[0] {
		t.Fatal("fixture did not recover the committed successor schema")
	}
	if _, err := source.ResourceStats(); !errors.Is(err, sqldriver.ErrReplicatedApplyClosed) {
		t.Fatalf("prepared predecessor remains live: %v", err)
	}
	neighborWAL, _ := f.store.GroupByID(f.boots[1].Descriptor.GroupID)
	neighborBase, neighborID, neighborDB, neighbor, err := openRF3RetainedApply(
		f.paths[1], neighborWAL, f.bases[1], f.applies[1])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = neighbor.Close(); _ = neighborDB.Close() })
	first, second := groupFromBinding(base.Binding), groupFromBinding(neighborBase.Binding)
	manifest := rf3Manifest{Groups: []rf3ManifestGroup{{Route: rf3ManifestGroupRoute{Group: first}}}}
	prepared := []preparedRF3Group{{base: f.bases[0], apply: source}}
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	inventory := &rf3AdoptedGroupInventory{root: root,
		runtimes: map[raftmember.GroupKey]rf3AdoptedRuntime{first: {apply: source}}}
	t.Cleanup(func() { _ = inventory.Close() })
	inventory.mu.Lock()
	inventory.publishNativeChild(raftmember.RuntimeIdentity{Group: first})
	inventory.publishNativeChild(raftmember.RuntimeIdentity{Group: second})
	inventory.mu.Unlock()
	schemas := &rf3SchemaActivator{groups: map[raftmember.GroupKey]*rf3SchemaGeneration{
		first: {base: base, applyID: applyID, apply: current},
	}}
	missing := collectRF3DiagnosticResources(manifest, prepared, inventory, schemas)
	if missing.available || missing.covered != 1 || missing.failures != 1 || len(missing.expected) != 2 {
		t.Fatalf("native group without installed provider did not fail closed: %+v", missing)
	}
	// Reload installs its apply in schemas, without extending prepared or the
	// certified split-runtime map. Use the same publication/selection boundary.
	schemas.mu.Lock()
	schemas.groups[second] = &rf3SchemaGeneration{base: neighborBase, applyID: neighborID, apply: neighbor}
	schemas.mu.Unlock()
	currentBefore, err := current.ResourceStats()
	if err != nil {
		t.Fatal(err)
	}
	neighborBefore, err := neighbor.ResourceStats()
	if err != nil {
		t.Fatal(err)
	}
	durabilityBefore, err := current.DurabilityStats()
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		totals := collectRF3DiagnosticResources(manifest, prepared, inventory, schemas)
		if !totals.available || totals.covered != 2 || totals.failures != 0 || len(totals.expected) != 2 ||
			totals.groups[first] != current || totals.groups[second] != neighbor {
			t.Fatalf("current/live provider selection: %+v", totals)
		}
	}
	if after, err := current.ResourceStats(); err != nil || after != currentBefore {
		t.Fatalf("current schema sampling changed storage counters: err=%v before=%+v after=%+v", err, currentBefore, after)
	}
	if after, err := neighbor.ResourceStats(); err != nil || after != neighborBefore {
		t.Fatalf("dynamic provider sampling changed storage counters: err=%v before=%+v after=%+v", err, neighborBefore, after)
	}
	if after, err := current.DurabilityStats(); err != nil || after != durabilityBefore {
		t.Fatalf("sampling changed checkpoint-group I/O: err=%v before=%+v after=%+v", err, durabilityBefore, after)
	}
	if err := inventory.Close(); err != nil {
		t.Fatal(err)
	}
	closedInventory := collectRF3DiagnosticResources(manifest, prepared, inventory, schemas)
	if closedInventory.available || closedInventory.failures == 0 {
		t.Fatal("closed inventory advertised complete resource coverage")
	}
	// The collector deliberately releases schema/inventory locks before entering
	// the apply/database lock. Retirement can race sampling, but it must be safe
	// and all subsequent observations must refuse the closed generation.
	done := make(chan error, 1)
	go func() { done <- current.Close() }()
	for range 32 {
		_ = collectRF3DiagnosticResources(manifest, prepared, nil, schemas)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	closedApply := collectRF3DiagnosticResources(manifest, prepared, nil, schemas)
	if closedApply.available || closedApply.covered != 0 || closedApply.failures != 1 {
		t.Fatalf("closed current apply fell back to predecessor: %+v", closedApply)
	}
}
