package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type rf3RosterTestDialer struct{}

func (rf3RosterTestDialer) DialOrdinary(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
	return nil, rafttransport.ErrNodeNotFound
}

func TestRF3TransportBoundsCoverEnrolledAndMaximumGroupRosters(t *testing.T) {
	for _, count := range []int{2, 3, maxRF3ManifestGroups * rf3ManifestMembers} {
		t.Run(fmt.Sprintf("remote_nodes_%d", count), func(t *testing.T) {
			local := rafttransport.NodeID{0xff, 0xff}
			var members []rafttransport.Member
			var peers []rafttransport.NodeID
			groupCount := (count + rf3ManifestMembers - 1) / rf3ManifestMembers
			for index := 0; index < groupCount; index++ {
				group := raftmember.GroupKey{
					ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
					TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3},
					GroupID: [16]byte{byte(index + 1)},
				}
				members = append(members, rafttransport.Member{
					Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: local,
					Role: rafttransport.MemberVoter,
				})
				for ordinal := 0; ordinal < rf3ManifestMembers && len(peers) < count; ordinal++ {
					node := rafttransport.NodeID{byte(index + 1), byte(ordinal + 1)}
					role := rafttransport.MemberVoter
					if ordinal == rf3ManifestMembers-1 {
						role = rafttransport.MemberEnrolled
					}
					members = append(members, rafttransport.Member{
						Group: group, ReplicaSetVersion: 1, MemberID: uint64(ordinal + 2),
						Node: node, Role: role,
					})
					peers = append(peers, node)
				}
			}
			registry, err := rafttransport.NewStaticRegistry(local, members,
				rafttransport.Limits{MaxGroups: groupCount, MaxMembers: len(members)})
			if err != nil {
				t.Fatal(err)
			}
			options := rf3TransportOptions(peers, func() time.Time { return time.Now().Add(time.Second) })
			options.Registry, options.Dialer = registry, rf3RosterTestDialer{}
			transport, err := rafttransport.NewOrdinaryTransport(options)
			if err != nil {
				t.Fatalf("valid %d-node roster rejected: %v", count, err)
			}
			t.Cleanup(func() { _ = transport.Close() })
			if options.Queue.PerPeerFrames != 32 || options.Queue.GlobalFrames != max(64, count*32) ||
				options.Queue.PerPeerBytes != 32<<20 || options.Queue.GlobalBytes != 64<<20 {
				t.Fatalf("roster changed byte admission or underfunded frame slots: %+v", options.Queue)
			}
			if options.Queue.GlobalFrames > rafttransport.AbsoluteMaxQueuedFrames ||
				options.Queue.GlobalBytes > rafttransport.AbsoluteMaxQueuedBytes ||
				options.RetainedFrameBytes != rafttransport.DefaultRetainedFrameBytes ||
				options.Coalesce.RetainedBytes != rafttransport.DefaultRetainedFrameBytes {
				t.Fatal("roster exceeded global or retained scratch bounds")
			}
		})
	}
}
