package multiraft

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

type configurationReplayTestRuntime struct {
	*fakeRuntime
	replay raftmember.CommittedConfigurationReplay
	err    error
}

func (runtime *configurationReplayTestRuntime) ConfigurationReplay() (raftmember.CommittedConfigurationReplay, error) {
	return runtime.replay, runtime.err
}

func TestConfigurationReplayUsesOwningLaneAndPreservesAcquisitionErrors(t *testing.T) {
	set, err := NewExecutionLanes(2, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	replay := &testConfigurationReplay{}
	member := &configurationReplayTestRuntime{fakeRuntime: runtimeForLane(t, set, 1, 37), replay: replay}
	if err := set.addRuntime(member); err != nil {
		t.Fatal(err)
	}
	owner, err := set.OwnerLane(1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := owner.ConfigurationReplay(member.identity.Group)
	if err != nil || got != replay {
		t.Fatalf("replay=%v err=%v", got, err)
	}
	other, err := set.OwnerLane(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.ConfigurationReplay(member.identity.Group); !errors.Is(err, ErrExecutionLane) {
		t.Fatalf("cross-lane replay acquisition: %v", err)
	}
	member.err = errors.New("durable store unavailable")
	if _, err := owner.ConfigurationReplay(member.identity.Group); !errors.Is(err, member.err) {
		t.Fatalf("replay acquisition error: %v", err)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.ConfigurationReplay(member.identity.Group); !errors.Is(err, ErrHostClosed) {
		t.Fatalf("closed replay acquisition: %v", err)
	}
}

func TestHostConfigurationReplaySupportsLegacyRuntime(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	member := newFakeRuntime(37)
	if err := host.addRuntime(member); err != nil {
		t.Fatal(err)
	}
	if replay, err := host.ConfigurationReplay(member.identity.Group); replay != nil || err != nil {
		t.Fatalf("legacy replay=%v err=%v", replay, err)
	}
}
