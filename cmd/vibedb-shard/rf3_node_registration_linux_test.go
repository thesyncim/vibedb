//go:build linux

package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/servicetls"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRF3NodeRegistrationAuthenticatesPreparationAndPreservesRetry(t *testing.T) {
	input := prepareRF3NodeTestInput(t)
	pending := input.Groups[1]
	input.Groups = input.Groups[:1]
	if err := provisionRF3Node(input); err != nil {
		t.Fatal(err)
	}
	initial, err := loadRF3Manifest(filepath.Join(input.Root, "serve-rf3.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := servicetls.LoadProfile(initial.TLS.Certificate, initial.TLS.Key, initial.TLS.Roots, initial.TLS.IdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := openRF3NodeOwner(initial, profile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	// This succeeds while the owner holds the node writer lock.
	if err := provisionRF3MemberInto(pending, pending.Root, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := provisionRF3MemberInto(pending, pending.Root, nil, true); err != nil {
		t.Fatal("identical preparation retry", err)
	}
	manifest, err := loadRF3Manifest(filepath.Join(pending.Root, "serve-rf3.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	base, applyID, err := loadRF3RetainedIdentities(manifest)
	if err != nil {
		t.Fatal(err)
	}
	foreign := manifest
	foreign.SQL.Path = initial.Groups[0].SQL.Path
	if _, err := owner.ensurePreparedGroup(foreign, base, applyID, sqldriver.ReplicatedOpenOptions{}); err == nil {
		t.Fatal("registered another SQL root")
	}
	if _, found := owner.store.GroupByID(base.Binding.GroupID); found {
		t.Fatal("invalid SQL registered a group")
	}
	bootstrapPath := filepath.Join(pending.Root, "node-bootstrap.pb")
	raw, err := os.ReadFile(bootstrapPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := new(pb.Snapshot)
	if err := proto.Unmarshal(raw, snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Data = []byte("foreign bootstrap")
	corrupt, err := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bootstrapPath, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.ensurePreparedGroup(manifest, base, applyID, sqldriver.ReplicatedOpenOptions{}); err == nil {
		t.Fatal("registered foreign bootstrap")
	}
	if _, found := owner.store.GroupByID(base.Binding.GroupID); found {
		t.Fatal("foreign bootstrap registered a group")
	}
	if err := os.WriteFile(bootstrapPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	group, err := owner.ensurePreparedGroup(manifest, base, applyID, sqldriver.ReplicatedOpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := group.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	db, claim, err := openRF3SelectedLog(manifest.SQL.Path, group, base, applyID)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := owner.adopt(group, db, claim)
	if err != nil {
		t.Fatal(err)
	}
	incarnation := runtime.Identity().NodeIncarnation
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	index, term, typ := uint64(2), uint64(2), pb.EntryNormal
	var appendEntry raftstore.Submission
	if err := appendEntry.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err := appendEntry.Prepare(raftstore.NodeReady{GroupID: descriptor.LogKey, Batch: raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, MustSync: true, HardState: &pb.HardState{Term: &term, Commit: &index}, Entries: []*pb.Entry{{Index: &index, Term: &term, Type: &typ}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.sequencer.TrySubmit(&appendEntry); err != nil {
		t.Fatal(err)
	}
	if _, err := appendEntry.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	owner, err = openRF3NodeOwner(initial, profile)
	if err != nil {
		t.Fatal(err)
	}
	group, err = owner.ensurePreparedGroup(manifest, base, applyID, sqldriver.ReplicatedOpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if last, err := group.LastIndex(); err != nil || last != index {
		t.Fatalf("retry reset newer log: %d %v", last, err)
	}
	if got, err := group.NodeIncarnation(); err != nil || got != incarnation {
		t.Fatalf("retry minted incarnation: %d %v", got, err)
	}
	wrong := base
	wrong.Binding.StoreID[0] ^= 1
	if _, err := owner.ensurePreparedGroup(manifest, wrong, applyID, sqldriver.ReplicatedOpenOptions{}); !errors.Is(err, raftmember.ErrBindingMismatch) {
		t.Fatalf("existing descriptor conflict accepted: %v", err)
	}
}

func TestRF3NodeRecoveryAcrossSegmentEntryCapacity(t *testing.T) {
	fixture := newRF3NodeRecoveryFixture(t)
	group, _ := fixture.store.GroupByID(fixture.boots[0].Descriptor.GroupID)
	descriptor, err := group.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	incarnations, err := fixture.store.BeginIncarnations([]uint64{descriptor.LogKey})
	if err != nil {
		t.Fatal(err)
	}
	// The fixture permits 64 resident entries per group. Retain twice that
	// history without a checkpoint: rotation must preserve the logical suffix.
	term, typ := uint64(2), pb.EntryNormal
	last := uint64(2*fixture.options.MaxEntriesPerGroup + 1)
	for index := uint64(2); index <= last; index++ {
		if err := group.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnations[0].Incarnation, ReadyID: index - 1, MustSync: true, HardState: &pb.HardState{Term: &term, Commit: &index}, Entries: []*pb.Entry{{Index: &index, Term: &term, Type: &typ}}}); err != nil {
			t.Fatal(err)
		}
	}
	fixture.reopen(t)
	group, _ = fixture.store.GroupByID(fixture.boots[0].Descriptor.GroupID)
	_, _, db, claim, err := openRF3RetainedApply(fixture.paths[0], group, fixture.bases[0], fixture.applies[0])
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer claim.Close()
	if err := raftmember.ValidateNodeApplyCapacity(group, claim); err != nil {
		t.Fatal("segment capacity capped retained history", err)
	}
	for _, index := range []uint64{2, last} {
		entries, err := group.Entries(index, index+1, 1024)
		if err != nil || len(entries) != 1 || entries[0].GetIndex() != index {
			t.Fatalf("lost retained entry %d: %v", index, err)
		}
	}
}
