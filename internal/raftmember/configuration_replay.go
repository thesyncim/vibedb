package raftmember

import (
	"math"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// CommittedConfigurationReplay proves an exact historical configuration from
// one runtime's retained durable log. It grants no membership role: the caller
// must authenticate current peers and supply its published configuration cut.
// Implementations must be safe for concurrent transport readers.
type CommittedConfigurationReplay interface {
	MatchesCommittedConfiguration(entry *pb.Entry, through uint64) bool
}

type runtimeConfigurationReplay struct {
	stable runtimeStableStore
	closed atomic.Bool
}

// ConfigurationReplay captures this runtime's already-bound synchronized store
// while on the owner lane. Verification never reads mutable Runtime or Node
// state, and the same capability is reused across publication updates.
func (runtime *Runtime) ConfigurationReplay() (CommittedConfigurationReplay, error) {
	if err := runtime.checkUsable(); err != nil {
		return nil, err
	}
	if runtime.configurationReplay == nil {
		runtime.configurationReplay = &runtimeConfigurationReplay{
			stable: runtime.stableStore(),
		}
	}
	return runtime.configurationReplay, nil
}

func (replay *runtimeConfigurationReplay) MatchesCommittedConfiguration(entry *pb.Entry, through uint64) bool {
	if replay == nil || replay.closed.Load() || replay.stable == nil || entry == nil ||
		entry.GetIndex() == 0 || entry.GetIndex() > through || entry.GetIndex() == math.MaxUint64 ||
		entry.GetTerm() == 0 || entry.GetType() != pb.EntryConfChange && entry.GetType() != pb.EntryConfChangeV2 {
		return false
	}
	entries, err := replay.stable.Entries(entry.GetIndex(), entry.GetIndex()+1, raftmodel.MaxInboundMessageBytes)
	return err == nil && len(entries) == 1 && proto.Equal(entries[0], entry) && !replay.closed.Load()
}
