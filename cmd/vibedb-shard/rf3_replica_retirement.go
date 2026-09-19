package main

import (
	"context"
	"errors"
	"net"
	"slices"
	"time"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// A removed source has no controller capability. Its one read-only exception
// is the exact final membership cut of its retained transition, authenticated
// against the physical peer's current key. This never restores Raft traffic
// authority or permits health discovery for unrelated groups.
func rf3AuthenticatedReplicaObservationAuthorizer(registry *rafttransport.StaticRegistry, policy *serviceauthz.Policy) replicacontrol.AuthenticatedAuthorizeFunc {
	controller := rf3ReplicaObservationAuthorizer(registry, policy)
	return func(peer rafttransport.PeerBinding, request replicacontrol.Request) bool {
		if controller(peer.Identity, request) {
			return true
		}
		if registry == nil || peer.Identity.TrustDomain != registry.TrustDomain() ||
			peer.ServiceKeyDigest == ([32]byte{}) || request.HealthOnly || request.ExpectedReplicaSetVersion == 0 ||
			request.Operation == ([32]byte{}) || request.Step == ([32]byte{}) {
			return false
		}
		grant, found, err := registry.CurrentTransitionGrant(request.Group)
		if err != nil || !found || !grant.Valid() || request.TargetMember != grant.TargetMember ||
			request.ExpectedReplicaSetVersion <= grant.InitialReplicaSetVersion {
			return false
		}
		node, err := registry.Node(request.Group, grant.SourceMember)
		if err != nil || node != peer.Identity.Node {
			return false
		}
		physical, err := registry.PhysicalPeer(node)
		if err != nil || physical.Incarnation == 0 || physical.ServiceKeyDigest != peer.ServiceKeyDigest ||
			(physical.State != rafttransport.PeerEnrolled && physical.State != rafttransport.PeerRetiring) {
			return false
		}
		version, found := registry.ReplicaSetVersion(request.Group)
		if !found || version != request.ExpectedReplicaSetVersion {
			return false
		}
		if _, err = registry.Role(request.Group, grant.SourceMember); !errors.Is(err, rafttransport.ErrMemberNotFound) {
			return false
		}
		local, err := registry.LocalMember(request.Group)
		if err != nil || local == grant.SourceMember {
			return false
		}
		role, err := registry.Role(request.Group, local)
		return err == nil && role == rafttransport.MemberVoter
	}
}

// Discovery addresses never carry authority: both the eligible members and the
// current TLS key are resolved from the source's locally retained grant/roster.
// A single deadline bounds every attempted surviving-member observation.
func rf3ReplicaRetirementProof(registry *rafttransport.StaticRegistry, profile *rafttransport.PeerTLS,
	deadline rafttransport.DeadlineFunc,
) func(context.Context, replicaaction.Request) (raftservice.ReplicaRetirementProof, error) {
	return func(ctx context.Context, request replicaaction.Request) (raftservice.ReplicaRetirementProof, error) {
		if registry == nil || profile == nil || deadline == nil {
			return raftservice.ReplicaRetirementProof{}, replicaaction.ErrControl
		}
		bound := deadline()
		if bound.IsZero() {
			return raftservice.ReplicaRetirementProof{}, replicaaction.ErrControl
		}
		ctx, cancel := context.WithDeadline(ctx, bound)
		defer cancel()
		locators, err := replicaaction.OpenRetirementLocators(request.Command)
		if err != nil {
			return raftservice.ReplicaRetirementProof{}, err
		}
		grant, found, err := registry.CurrentTransitionGrant(request.Fence.Group)
		if err != nil || !found || grant.SourceMember != request.SourceMember || grant.TargetMember != request.TargetMember {
			return raftservice.ReplicaRetirementProof{}, errors.Join(raftservice.ErrMembershipUnauthorized, err)
		}
		survivors := grant.InitialVoters
		for index := range survivors {
			if survivors[index] == grant.SourceMember {
				survivors[index] = grant.TargetMember
			}
		}
		var failures error
		for _, locator := range locators {
			if !slices.Contains(survivors[:], locator.Member) || locator.Member == request.SourceMember {
				return raftservice.ReplicaRetirementProof{}, replicaaction.ErrUnauthorized
			}
			node, nodeErr := registry.Node(request.Fence.Group, locator.Member)
			if nodeErr != nil {
				failures = errors.Join(failures, nodeErr)
				continue
			}
			peer, peerErr := registry.PhysicalPeer(node)
			if peerErr != nil || peer.Incarnation == 0 || peer.ServiceKeyDigest == ([32]byte{}) {
				failures = errors.Join(failures, replicaaction.ErrUnauthorized, peerErr)
				continue
			}
			opener := &rf3RetirementStreamOpener{registry: registry, profile: profile, node: node, address: locator.Address,
				peer: peer, deadline: func() time.Time { return bound }, dial: func(ctx context.Context, address string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "tcp", address)
				}}
			client, clientErr := replicacontrol.NewClient(replicacontrol.ClientOptions{Opener: opener, ReadDeadline: opener.deadline, WriteDeadline: opener.deadline})
			if clientErr != nil {
				return raftservice.ReplicaRetirementProof{}, clientErr
			}
			observation, observeErr := client.Observe(ctx, node, replicacontrol.Request{Operation: request.Operation, Step: request.Step,
				Group: request.Fence.Group, TargetMember: request.TargetMember, ExpectedReplicaSetVersion: request.Fence.Command.ReplicaSetVersion})
			if observeErr != nil {
				failures = errors.Join(failures, observeErr)
				continue
			}
			// Revalidate the same physical incarnation/key after the bounded read, so
			// an address refresh or roster rotation cannot lend stale authority.
			current, currentErr := registry.PhysicalPeer(node)
			if currentErr != nil || current.Incarnation != peer.Incarnation || current.ServiceKeyDigest != peer.ServiceKeyDigest || observation.Status.MemberID != locator.Member {
				failures = errors.Join(failures, replicaaction.ErrUnauthorized, currentErr)
				continue
			}
			proof, proofErr := raftservice.NewReplicaRetirementProof(raftservice.ReplicaRetirementRequest{
				Operation: request.Operation, Step: request.Step, Fence: request.Fence, SourceMember: request.SourceMember, TargetMember: request.TargetMember}, grant,
				raftservice.ReplicaObservation{Publication: observation.Publication, Status: observation.Status, State: observation.State})
			if proofErr == nil {
				return proof, nil
			}
			failures = errors.Join(failures, proofErr)
		}
		return raftservice.ReplicaRetirementProof{}, errors.Join(replicaaction.ErrOutcomeUnknown, failures)
	}
}

type rf3RetirementStreamOpener struct {
	registry *rafttransport.StaticRegistry
	profile  *rafttransport.PeerTLS
	node     rafttransport.NodeID
	peer     rafttransport.PhysicalPeer
	address  string
	deadline rafttransport.DeadlineFunc
	dial     func(context.Context, string) (net.Conn, error)
}

func (opener *rf3RetirementStreamOpener) OpenShardControl(ctx context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
	if node != opener.node {
		return nil, replicaaction.ErrUnauthorized
	}
	raw, err := opener.dial(ctx, opener.address)
	if err != nil {
		return nil, err
	}
	connection, err := opener.profile.Client(ctx, raw, node, rafttransport.TrafficShardControl, opener.deadline)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	if err = opener.registry.VerifyPeerConnectionBinding(connection); err != nil {
		_ = connection.Close()
		return nil, err
	}
	current, err := opener.registry.PhysicalPeer(node)
	if err != nil || current.Incarnation != opener.peer.Incarnation || current.ServiceKeyDigest != opener.peer.ServiceKeyDigest {
		_ = connection.Close()
		return nil, errors.Join(replicaaction.ErrUnauthorized, err)
	}
	return connection, nil
}
