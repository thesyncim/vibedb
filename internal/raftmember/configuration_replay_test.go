package raftmember

import (
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeConfigurationReplayUsesExactRetainedWALAcrossRestart(t *testing.T) {
	fixture := newRuntimeFixture(t, 231, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	replay, err := fixture.runtime.ConfigurationReplay()
	if err != nil {
		t.Fatal(err)
	}
	again, err := fixture.runtime.ConfigurationReplay()
	if err != nil || replay != again {
		t.Fatalf("capability was not reused: %v", err)
	}
	var retained []*pb.Entry
	var through uint64
	for move := range 4 {
		target := fixture.runtime.identity.MemberID + uint64(move) + 1
		digest := MembershipTransitionDigest(fixture.runtime.identity.Group,
			[16]byte{byte(move + 1)}, 2, uint64(move+3), fixture.runtime.identity.MemberID, target)
		if err := fixture.runtime.ProposeConfChange(&pb.ConfChange{
			Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: proto.Uint64(target), Context: digest[:],
		}); err != nil {
			t.Fatal(err)
		}
		drainRuntime(t, fixture.runtime, nil)
		publication, err := fixture.runtime.Publication()
		if err != nil {
			t.Fatal(err)
		}
		through = publication.ReplicaSetVersion
		entries, err := fixture.wal.Entries(through, through+1, raftmodel.MaxInboundMessageBytes)
		if err != nil || len(entries) != 1 {
			t.Fatalf("retained configuration %d: entries=%d err=%v", through, len(entries), err)
		}
		retained = append(retained, proto.Clone(entries[0]).(*pb.Entry))
	}
	for _, entry := range retained {
		if !replay.MatchesCommittedConfiguration(entry, through) {
			t.Fatalf("exact retained configuration %d rejected", entry.GetIndex())
		}
		if replay.MatchesCommittedConfiguration(entry, entry.GetIndex()-1) {
			t.Fatalf("configuration beyond committed cut %d accepted", entry.GetIndex())
		}
		for _, mutate := range []func(*pb.Entry){
			func(entry *pb.Entry) { entry.Term = proto.Uint64(entry.GetTerm() + 1) },
			func(entry *pb.Entry) { entry.Index = proto.Uint64(entry.GetIndex() + 1) },
			func(entry *pb.Entry) { entry.Type = pb.EntryNormal.Enum() },
			func(entry *pb.Entry) { entry.Data[len(entry.Data)-1] ^= 1 },
		} {
			foreign := proto.Clone(entry).(*pb.Entry)
			mutate(foreign)
			if replay.MatchesCommittedConfiguration(foreign, through) {
				t.Fatalf("changed configuration accepted: %+v", foreign)
			}
		}
	}
	compacted := proto.Clone(retained[0]).(*pb.Entry)
	compacted.Index = proto.Uint64(1) // The fixture's immutable snapshot base.
	if replay.MatchesCommittedConfiguration(compacted, through) {
		t.Fatal("compacted configuration accepted without exact WAL evidence")
	}
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 100 {
				_ = replay.MatchesCommittedConfiguration(retained[0], through)
			}
		})
	}
	if err = fixture.runtime.Close(); err != nil {
		t.Fatal(err)
	}
	readers.Wait()
	if replay.MatchesCommittedConfiguration(retained[0], through) {
		t.Fatal("closed runtime capability remained usable")
	}
	wal, err := raftstore.Open(fixture.walPath, fixture.walID,
		testTopologyRecoveryEpoch, fixture.walKey, fixture.options)
	if err != nil {
		t.Fatal(err)
	}
	database, apply, err := OpenBoundSQLWithApply(fixture.sqlPath, wal,
		testAuthorityProfile(), fixture.base, fixture.applyID)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	restarted, err := AdoptRuntime(wal, database, apply)
	if err != nil {
		if restarted != nil {
			_ = restarted.Close()
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	recovered, err := restarted.ConfigurationReplay()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range retained {
		if !recovered.MatchesCommittedConfiguration(entry, through) {
			t.Fatalf("restart lost exact retained configuration %d", entry.GetIndex())
		}
	}
}
