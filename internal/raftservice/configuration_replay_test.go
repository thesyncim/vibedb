package raftservice

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
)

type testConfigurationReplay struct{}

func (*testConfigurationReplay) MatchesCommittedConfiguration(*pb.Entry, uint64) bool {
	return false
}

type replayProgressHost struct {
	*membershipProgressHost
	replay      raftmember.CommittedConfigurationReplay
	replayErr   error
	replayCalls int
}

func (h *replayProgressHost) ConfigurationReplay(raftmember.GroupKey) (raftmember.CommittedConfigurationReplay, error) {
	h.replayCalls++
	return h.replay, h.replayErr
}

type replayProgressAuthority struct {
	*progressAuthority
	replay        raftmember.CommittedConfigurationReplay
	replayErr     error
	replayGroup   raftmember.GroupKey
	replayVersion uint64
	replayConf    *pb.ConfState
	replayCalls   int
}

func (a *replayProgressAuthority) PublishCommittedAuthorityWithReplay(
	group raftmember.GroupKey, version uint64, conf *pb.ConfState, replay raftmember.CommittedConfigurationReplay,
) error {
	a.replayCalls++
	a.replay, a.replayGroup, a.replayVersion, a.replayConf = replay, group, version, conf
	return a.replayErr
}

func TestOwnerPublishesConfigurationReplayAndRetainsFailures(t *testing.T) {
	group := peerServerTestGroup()
	replay := &testConfigurationReplay{}
	host := &replayProgressHost{
		membershipProgressHost: &membershipProgressHost{publicationCalls: make(map[raftmember.GroupKey]int)},
		replay:                 replay,
	}
	authority := &replayProgressAuthority{progressAuthority: &progressAuthority{publications: make(map[raftmember.GroupKey]int)}}
	owner := &Owner{host: host, authority: authority}
	if err := owner.syncMembershipAuthority(group); err != nil {
		t.Fatal(err)
	}
	if authority.replay != replay || authority.replayGroup != group || authority.replayVersion != 1 ||
		authority.replayConf == nil || len(authority.replayConf.Voters) != 3 || authority.publications[group] != 0 {
		t.Fatalf("replay publication: %+v", authority)
	}
	host.replayErr = errors.New("durable store unavailable")
	if err := owner.syncMembershipAuthority(group); !errors.Is(err, host.replayErr) {
		t.Fatalf("acquisition error lost: %v", err)
	}
	if authority.replayCalls != 1 || authority.publications[group] != 0 {
		t.Fatalf("failed acquisition published authority: %+v", authority)
	}
	host.replayErr = nil
	authority.replayErr = errors.New("invalid committed replay")
	if err := owner.syncMembershipAuthority(group); !errors.Is(err, authority.replayErr) {
		t.Fatalf("publication error lost: %v", err)
	}
	if authority.publications[group] != 0 {
		t.Fatal("failed replay publication downgraded to legacy authority")
	}
}

func TestOwnerConfigurationReplayRetainsLegacyImplementations(t *testing.T) {
	for _, mode := range []string{"legacy-host", "legacy-authority", "nil-replay"} {
		t.Run(mode, func(t *testing.T) {
			group := peerServerTestGroup()
			host := &replayProgressHost{
				membershipProgressHost: &membershipProgressHost{publicationCalls: make(map[raftmember.GroupKey]int)},
				replay:                 &testConfigurationReplay{},
			}
			authority := &replayProgressAuthority{progressAuthority: &progressAuthority{publications: make(map[raftmember.GroupKey]int)}}
			owner := &Owner{host: host, authority: authority}
			switch mode {
			case "legacy-host":
				owner.host = host.membershipProgressHost
			case "legacy-authority":
				owner.authority = authority.progressAuthority
			case "nil-replay":
				host.replay = nil
			}
			if err := owner.syncMembershipAuthority(group); err != nil {
				t.Fatal(err)
			}
			if authority.publications[group] != 1 || authority.replayCalls != 0 {
				t.Fatalf("legacy publication: %+v", authority)
			}
			if mode == "legacy-authority" && host.replayCalls != 0 {
				t.Fatal("legacy authority acquired unused replay capability")
			}
		})
	}
}
