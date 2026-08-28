package rebalance

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardservice"
)

var ErrMembershipGrantInstall = errors.New("rebalance: membership grant installation failed")

const MembershipGrantFanout = gateway.ServingReplicaCount + 1

type MembershipGrantInstaller interface {
	InstallMembershipGrant(context.Context, rafttransport.NodeID, membershipgrant.Grant) error
}

type MembershipApplier interface {
	ApplyMembership(
		context.Context,
		gateway.ReplicatedMembershipRoute,
		shardservice.ReplicatedMembershipRequest,
	) (gateway.ReplicatedMembershipResult, error)
}

// MembershipGrantClient binds one certified move grant to its native
// membership control. Every ApplyMembership first installs or confirms the
// exact grant on the three current voters and the enrolled target.
type MembershipGrantClient struct {
	grant     membershipgrant.Grant
	installer MembershipGrantInstaller
	applier   MembershipApplier
}

func NewMembershipGrantClient(
	grant membershipgrant.Grant,
	installer MembershipGrantInstaller,
	applier MembershipApplier,
) (*MembershipGrantClient, error) {
	if !grant.Valid() || installer == nil || applier == nil {
		return nil, ErrMembershipGrantInstall
	}
	return &MembershipGrantClient{grant: grant, installer: installer, applier: applier}, nil
}

func (client *MembershipGrantClient) ApplyMembership(
	ctx context.Context,
	route gateway.ReplicatedMembershipRoute,
	request shardservice.ReplicatedMembershipRequest,
) (gateway.ReplicatedMembershipResult, error) {
	if client == nil || ctx == nil || !membershipRequestMatchesGrant(request, client.grant) {
		return gateway.ReplicatedMembershipResult{}, ErrMembershipGrantInstall
	}
	nodes, err := membershipGrantFanoutNodes(route, client.grant)
	if err != nil {
		return gateway.ReplicatedMembershipResult{}, err
	}
	errorsByNode := make(chan error, len(nodes))
	var workers sync.WaitGroup
	workers.Add(len(nodes))
	for _, node := range nodes {
		node := node
		go func() {
			defer workers.Done()
			if err := client.installer.InstallMembershipGrant(ctx, node, client.grant); err != nil {
				errorsByNode <- err
			}
		}()
	}
	workers.Wait()
	close(errorsByNode)
	var installErr error
	for err := range errorsByNode {
		installErr = errors.Join(installErr, err)
	}
	if installErr != nil {
		return gateway.ReplicatedMembershipResult{}, errors.Join(ErrMembershipGrantInstall, installErr)
	}
	return client.applier.ApplyMembership(ctx, route, request)
}

func membershipRequestMatchesGrant(
	request shardservice.ReplicatedMembershipRequest,
	grant membershipgrant.Grant,
) bool {
	return request.TransitionID == grant.TransitionID &&
		request.MetadataEpoch == grant.MetadataEpoch &&
		request.CatalogGeneration == grant.CatalogGeneration &&
		request.SourceMember == grant.SourceMember && request.TargetMember == grant.TargetMember
}

func membershipGrantFanoutNodes(
	route gateway.ReplicatedMembershipRoute,
	grant membershipgrant.Grant,
) ([MembershipGrantFanout]rafttransport.NodeID, error) {
	var result [MembershipGrantFanout]rafttransport.NodeID
	if route.Serving.Group != grant.Group ||
		len(route.Serving.Replicas) != gateway.ServingReplicaCount ||
		!route.HasEnrolledTarget || route.EnrolledTarget.Member != grant.TargetMember ||
		[16]byte(route.EnrolledTarget.Node) != grant.TargetNode {
		return result, ErrMembershipGrantInstall
	}
	roster := make([]membershipgrant.RosterMember, gateway.ServingReplicaCount)
	for index, replica := range route.Serving.Replicas {
		if replica.Member == 0 || replica.Node == (rafttransport.NodeID{}) ||
			replica.Member == grant.TargetMember {
			return result, ErrMembershipGrantInstall
		}
		roster[index] = membershipgrant.RosterMember{Member: replica.Member, Node: [16]byte(replica.Node)}
		result[index] = replica.Node
	}
	slices.SortFunc(roster, func(left, right membershipgrant.RosterMember) int {
		if left.Member < right.Member {
			return -1
		}
		if left.Member > right.Member {
			return 1
		}
		return 0
	})
	if [3]uint64{roster[0].Member, roster[1].Member, roster[2].Member} != grant.InitialVoters ||
		membershipgrant.CertifiedRosterDigest(
			grant.Group, grant.InitialReplicaSetVersion,
			[3]membershipgrant.RosterMember{roster[0], roster[1], roster[2]},
		) != grant.InitialRosterDigest {
		return result, ErrMembershipGrantInstall
	}
	result[gateway.ServingReplicaCount] = route.EnrolledTarget.Node
	for index := range result {
		for prior := 0; prior < index; prior++ {
			if result[index] == result[prior] {
				return [MembershipGrantFanout]rafttransport.NodeID{}, ErrMembershipGrantInstall
			}
		}
	}
	return result, nil
}
