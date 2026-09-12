package main

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestRF3EmptyTransportRegistrySupportsEveryTransitioningGroup(t *testing.T) {
	for _, stage := range []struct {
		name string
		role rafttransport.MemberRole
	}{
		{name: "enrolled", role: rafttransport.MemberEnrolled},
		{name: "learner", role: rafttransport.MemberLearner},
		{name: "promoted", role: rafttransport.MemberVoter},
	} {
		t.Run(stage.name, func(t *testing.T) {
			local := rafttransport.NodeID{1}
			domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
			registry, err := rafttransport.NewEmptyRegistry(local, domain, rf3TransportRegistryLimits())
			if err != nil {
				t.Fatal(err)
			}
			for node := byte(2); node <= rf3ManifestMembers+1; node++ {
				intent := rafttransport.EnrollmentIntent{
					Digest: [32]byte{node}, Domain: domain,
					DirectoryRevision: registry.PeerDirectoryRevision(),
					Peer: rafttransport.PhysicalPeer{
						NodeID: rafttransport.NodeID{node}, TrustDomain: domain,
						Incarnation: 1, Revision: 1, ServiceKeyDigest: [32]byte{node},
						State: rafttransport.PeerEnrolled,
					},
				}
				if err := registry.EnrollPeer(intent, rafttransport.EnrollmentVerifierFunc(func(rafttransport.EnrollmentIntent) error { return nil })); err != nil {
					t.Fatalf("enroll node %d: %v", node, err)
				}
			}
			for index := 0; index <= maxRF3ManifestGroups; index++ {
				group := raftmember.GroupKey{
					ClusterID: domain.ClusterID, ClusterIncarnation: domain.ClusterIncarnation,
					TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{byte(index + 1)},
				}
				roster := make([]rafttransport.Member, rf3ManifestMembers+1)
				for member := range roster {
					roster[member] = rafttransport.Member{
						Group: group, ReplicaSetVersion: 1, MemberID: uint64(member + 1),
						Node: rafttransport.NodeID{byte(member + 1)}, Role: rafttransport.MemberVoter,
					}
				}
				roster[rf3ManifestMembers].Role = stage.role
				published := false
				err := registry.InstallGroup(roster, func(publish func()) error {
					publish()
					published = true
					return nil
				})
				if index == maxRF3ManifestGroups {
					if !errors.Is(err, rafttransport.ErrRegistryBound) || published {
						t.Fatalf("group beyond capacity: published=%t err=%v", published, err)
					}
					continue
				}
				if err != nil || !published {
					t.Fatalf("supported group %d: published=%t err=%v", index+1, published, err)
				}
				if node, err := registry.Node(group, rf3ManifestMembers+1); err != nil || node != (rafttransport.NodeID{rf3ManifestMembers + 1}) {
					t.Fatalf("group %d retained transition mapping: node=%x err=%v", index+1, node, err)
				}
			}
		})
	}
}
