package rafttransport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type retainedConfigurationLog struct {
	store  *raft.MemoryStorage
	closed atomic.Bool
}

type nonComparableConfigurationReplay []byte

func (nonComparableConfigurationReplay) MatchesCommittedConfiguration(*pb.Entry, uint64) bool {
	return false
}

func (log *retainedConfigurationLog) MatchesCommittedConfiguration(entry *pb.Entry, through uint64) bool {
	if log.closed.Load() || entry.GetIndex() == 0 || entry.GetIndex() > through {
		return false
	}
	entries, err := log.store.Entries(entry.GetIndex(), entry.GetIndex()+1, 1<<20)
	return err == nil && len(entries) == 1 && proto.Equal(entries[0], entry)
}

func replayGrant(group raftmember.GroupKey, version, ordinal, source, target uint64, voters []uint64) membershipgrant.Grant {
	grant := authorityTestGrant(group)
	grant.InitialReplicaSetVersion = version
	grant.TransitionID[0] = byte(ordinal)
	grant.CatalogGeneration += ordinal
	grant.SourceMember, grant.TargetMember, grant.TargetNode = source, target, [16]byte(testNode(byte(target)))
	copy(grant.InitialVoters[:], voters)
	var roster [3]membershipgrant.RosterMember
	for i, member := range voters {
		roster[i] = membershipgrant.RosterMember{Member: member, Node: [16]byte(testNode(byte(member)))}
	}
	grant.InitialRosterDigest = membershipgrant.CertifiedRosterDigest(group, version, roster)
	return grant
}

func replayConfigurationEntry(t *testing.T, grant membershipgrant.Grant, index uint64, kind pb.ConfChangeType, member uint64) *pb.Entry {
	t.Helper()
	digest := grant.Digest()
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(&pb.ConfChange{Type: kind.Enum(), NodeId: proto.Uint64(member), Context: digest[:]})
	if err != nil {
		t.Fatal(err)
	}
	return &pb.Entry{Type: pb.EntryConfChange.Enum(), Index: proto.Uint64(index), Term: proto.Uint64(5), Data: data}
}

// Each omitted AppResp leaves a sender free to reframe the same retained
// configuration prefix after another grant has replaced its original grant.
// Three completed moves and reconstruction rule out a one-prior-grant cache.
func TestConfigurationReplayAcrossThreeMovesReconnectAndRestart(t *testing.T) {
	group := testGroup(103)
	store := raft.NewMemoryStorage()
	if err := store.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(5), Term: proto.Uint64(5), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 4}}}}); err != nil {
		t.Fatal(err)
	}
	log := &retainedConfigurationLog{store: store}
	voters := []uint64{1, 2, 4}
	version := uint64(5)
	open := func(local byte, replay *retainedConfigurationLog) *StaticRegistry {
		members := make([]Member, 5)
		for i := range members {
			id := uint64(i + 1)
			role := MemberEnrolled
			if slices.Contains(voters, id) {
				role = MemberVoter
			}
			members[i] = Member{Group: group, ReplicaSetVersion: version, MemberID: id, Node: testNode(byte(id)), Role: role}
		}
		registry, err := NewStaticRegistry(testNode(local), members, Limits{MaxGroups: 1, MaxMembers: 5})
		if err != nil {
			t.Fatal(err)
		}
		if replay != nil {
			if err := registry.PublishCommittedAuthorityWithReplay(group, version, &pb.ConfState{Voters: voters}, replay); err != nil {
				t.Fatal(err)
			}
		}
		return registry
	}
	sender, receiver := open(2, log), open(4, log)
	grant := replayGrant(group, version, 1, 1, 3, voters)
	for _, r := range []*StaticRegistry{sender, receiver} {
		if err := r.InstallTransitionGrant(grant); err != nil {
			t.Fatal(err)
		}
	}
	var history []*pb.Entry
	var replay *pb.Message
	var frame []byte
	for move := range 3 {
		for _, kind := range []pb.ConfChangeType{pb.ConfChangeAddLearnerNode, pb.ConfChangeAddNode, pb.ConfChangeRemoveNode} {
			version++
			member := grant.TargetMember
			if kind == pb.ConfChangeRemoveNode {
				member = grant.SourceMember
			}
			entry := replayConfigurationEntry(t, grant, version, kind, member)
			history = append(history, entry)
			if err := store.Append([]*pb.Entry{entry}); err != nil {
				t.Fatal(err)
			}
			conf := &pb.ConfState{Voters: slices.Clone(voters)}
			switch kind {
			case pb.ConfChangeAddLearnerNode:
				conf.Learners = []uint64{grant.TargetMember}
			case pb.ConfChangeAddNode:
				voters = append(voters, grant.TargetMember)
				slices.Sort(voters)
				conf.Voters = slices.Clone(voters)
			case pb.ConfChangeRemoveNode:
				voters = slices.DeleteFunc(voters, func(id uint64) bool { return id == grant.SourceMember })
				conf.Voters = slices.Clone(voters)
			}
			for _, r := range []*StaticRegistry{sender, receiver} {
				if err := r.PublishCommittedAuthorityWithReplay(group, version, conf, log); err != nil {
					t.Fatal(err)
				}
			}
		}
		nextTarget := uint64(5)
		if move == 1 {
			nextTarget = 1
		}
		if move == 2 {
			nextTarget = 3
		}
		next := replayGrant(group, version, uint64(move+2), grant.TargetMember, nextTarget, voters)
		for _, r := range []*StaticRegistry{sender, receiver} {
			if err := r.ReplaceTransitionGrantWithCommit(grant, next, nil); err != nil {
				t.Fatal(err)
			}
		}
		grant = next
		replay = frameBaseMessage(pb.MsgApp, 2, 4)
		replay.Index = proto.Uint64(5)
		replay.LogTerm = proto.Uint64(5)
		replay.Commit = proto.Uint64(version)
		replay.Entries = history
		frame = frameTestEncode(t, sender, group, replay)
		decoded, err := receiver.DecodeInbound(testPeerIdentity(receiver, testNode(2)), frame)
		if err != nil || len(decoded.Message.GetEntries()) != 0 {
			t.Fatalf("move %d reconnect=%+v err=%v", move, decoded, err)
		}
	}
	// The oldest exact entries remain usable after every grant changed. A forged
	// term or bytes must not be framed merely because its index was committed.
	forged := proto.Clone(replay).(*pb.Message)
	forged.LogTerm = proto.Uint64(4)
	for _, entry := range forged.Entries {
		entry.Term = proto.Uint64(4)
	}
	if _, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{Group: group, From: 2, To: 4, Message: forged}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("forged retained entry=%v", err)
	}
	// Restart publishes only the current grant, while the bounded WAL restores
	// all retained replay evidence independently of previous authority views.
	log.closed.Store(true)
	fresh := &retainedConfigurationLog{store: store}
	sender, receiver = open(2, fresh), open(4, fresh)
	for _, r := range []*StaticRegistry{sender, receiver} {
		if err := r.InstallTransitionGrant(grant); err != nil {
			t.Fatal(err)
		}
	}
	frame = frameTestEncode(t, sender, group, replay)
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, testNode(2)), frame); err != nil {
		t.Fatalf("restart replay: %v", err)
	}
	// Compaction actually removes proof, rather than retaining old grants forever.
	if err := store.Compact(version); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{Group: group, From: 2, To: 4, Message: replay}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("compacted sender proof=%v", err)
	}
	// A previously emitted frame can still reach the compacted receiver. It is
	// stripped to an empty probe and cannot reapply even a forged future suffix.
	for index := version + 1; index <= version+2; index++ {
		forged.Entries = append(forged.Entries, replayConfigurationEntry(t, grant, index, pb.ConfChangeAddNode, 99))
	}
	decoded, err := receiver.DecodeInbound(testPeerIdentity(receiver, testNode(2)), frameTestReplacePayload(t, frame, forged))
	if err != nil || len(decoded.Message.GetEntries()) != 0 || decoded.Message.GetCommit() != version {
		t.Fatalf("compacted probe=%+v err=%v", decoded, err)
	}
	duplicateTerm := frameTestReplaceRawPayload(frame, wireVarint(bytes.Clone(frame[FrameHeaderBytes:]), 4, 5))
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, testNode(2)), duplicateTerm); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("compacted noncanonical probe=%v", err)
	}
	stale := bytes.Clone(frame)
	binary.BigEndian.PutUint64(stale[112:120], version-1)
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, testNode(2)), stale); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("compacted stale generation=%v", err)
	}
	// The exact boundary can append and must retain normal grant authorization.
	boundary := frameBaseMessage(pb.MsgApp, 2, 4)
	boundary.Index = proto.Uint64(version)
	boundary.Entries = []*pb.Entry{replayConfigurationEntry(t, grant, version+1, pb.ConfChangeAddNode, 99)}
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, testNode(2)), frameTestReplacePayload(t, frame, boundary)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("boundary unauthorized suffix=%v", err)
	}
	// Removed source 5 has no current role, even on an otherwise old probe.
	forged.From = proto.Uint64(5)
	removed := frameTestReplacePayload(t, frame, forged)
	binary.BigEndian.PutUint64(removed[frameTestFromOffset:frameTestToOffset], 5)
	if _, err := receiver.DecodeInbound(testPeerIdentity(receiver, testNode(5)), removed); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("removed sender=%v", err)
	}
	// A bootstrap roster alone is never a local durable membership floor.
	unpublished := open(4, nil)
	if _, err := unpublished.DecodeInbound(testPeerIdentity(unpublished, testNode(2)), frame); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unpublished bootstrap floor=%v", err)
	}
}

func TestConfigurationReplayRebindsSamePublishedVersion(t *testing.T) {
	group := testGroup(104)
	members := []Member{{Group: group, ReplicaSetVersion: 8, MemberID: 2, Node: testNode(2), Role: MemberVoter}, {Group: group, ReplicaSetVersion: 8, MemberID: 3, Node: testNode(3), Role: MemberVoter}, {Group: group, ReplicaSetVersion: 8, MemberID: 4, Node: testNode(4), Role: MemberVoter}}
	registry, err := NewStaticRegistry(testNode(2), members, Limits{MaxGroups: 1, MaxMembers: 3})
	if err != nil {
		t.Fatal(err)
	}
	store := raft.NewMemoryStorage()
	entry := replayConfigurationEntry(t, authorityTestGrant(group), 8, pb.ConfChangeRemoveNode, 1)
	if err := store.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(7), Term: proto.Uint64(5), ConfState: &pb.ConfState{Voters: []uint64{2, 3, 4}}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append([]*pb.Entry{entry}); err != nil {
		t.Fatal(err)
	}
	old := &retainedConfigurationLog{store: store}
	old.closed.Store(true)
	fresh := &retainedConfigurationLog{store: store}
	for _, reader := range []*retainedConfigurationLog{old, fresh} {
		if err := registry.PublishCommittedAuthorityWithReplay(group, 8, &pb.ConfState{Voters: []uint64{2, 3, 4}}, reader); err != nil {
			t.Fatal(err)
		}
	}
	message := frameBaseMessage(pb.MsgApp, 2, 4)
	message.Entries = []*pb.Entry{entry}
	frameTestEncode(t, registry, group, message)
	conf := &pb.ConfState{Voters: []uint64{2, 3, 4}}
	if allocations := testing.AllocsPerRun(100, func() {
		if err := registry.PublishCommittedAuthorityWithReplay(group, 8, conf, fresh); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("unchanged published capability allocations=%v", allocations)
	}
	// The interface permits alternative implementations. Idempotence must not
	// panic when one contains a slice or other non-comparable dynamic value.
	other := nonComparableConfigurationReplay{1}
	for range 2 {
		if err := registry.PublishCommittedAuthorityWithReplay(group, 8, conf, other); err != nil {
			t.Fatal(err)
		}
	}
}
