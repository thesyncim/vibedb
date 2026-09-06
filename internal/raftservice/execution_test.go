package raftservice

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type executionTestOutbound struct{}

func (executionTestOutbound) Send(raftmember.OutboundMessage) error { return nil }

func TestExecutionOwnersRouteByGroupAndStartEveryLane(t *testing.T) {
	registry, err := raftserve.NewRegistry(raftserve.Limits{
		MaxGroups: 2, MaxOutstandingIdentities: 2, MaxOutstandingAttempts: 2,
		MaxWaiters: 2, MaxAttemptsPerIdentity: 1,
		MaxRetainedCompletionBytes: 2 * int64(replicatedstate.MaxCompletionEnvelopeBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	hostLimits := multiraft.Limits{
		MaxGroups: 2, MaxQueueItems: 2, MaxQueueBytes: raftmodel.MaxInboundMessageBytes,
		MaxGroupItems: 2, MaxGroupBytes: raftmodel.MaxInboundMessageBytes,
		MaxOutboxItems: 2, MaxOutboxBytes: raftmodel.MaxInboundMessageBytes, MaxPendingTicks: 1,
	}
	lanes, err := registry.NewExecutionLanes(2, hostLimits)
	if err != nil {
		t.Fatal(err)
	}
	groups := make([]raftmember.GroupKey, 0, 2)
	for seed := byte(1); len(groups) < 2; seed++ {
		group := peerServerTestGroup()
		group.GroupID[0] = seed
		lane, laneErr := lanes.Lane(group)
		if laneErr != nil {
			t.Fatal(laneErr)
		}
		if lane == len(groups) {
			groups = append(groups, group)
		}
	}
	members := make([]raftmember.RuntimeIdentity, 2)
	commands := make([]CommandFence, 2)
	for index, group := range groups {
		members[index] = raftmember.RuntimeIdentity{Group: group, AllocationGeneration: 1,
			MemberID: uint64(index + 1), StoreID: [16]byte{byte(index + 1)},
			NodeIncarnation: 1, RelationManifestDigest: [32]byte{1}}
		commands[index] = CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1,
			ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1,
			RelationManifestDigest: [32]byte{1}, RoutingVersion: 1, RouteGeneration: 1}
	}
	outbound := executionTestOutbound{}
	owners, err := NewExecutionOwners(ExecutionOptions{
		Registry: registry, Lanes: lanes, Members: members, CommandFences: commands,
		Outbound: outbound, Limits: Limits{MaxIngressItems: 2, MaxIngressBytes: 1024,
			MaxPendingProposalItems: 2, MaxPendingProposalBytes: 1024,
			MaxPendingReadItems: 2, MaxPendingReadBytes: 1024, MaxPendingOutboundBytes: 1024},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, group := range groups {
		owner, routeErr := owners.owner(group)
		if routeErr != nil || owner != owners.owners[index] || owner.outbound != outbound {
			t.Fatalf("group %d route owner=%p want=%p err=%v", index, owner, owners.owners[index], routeErr)
		}
	}
	unknown := groups[0]
	unknown.GroupID[15] ^= 0xff
	if _, err := owners.Probe(context.Background(), unknown); !errors.Is(err, ErrExecutionGroup) {
		t.Fatalf("unknown group error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- owners.Run(ctx) }()
	<-owners.Started()
	if !owners.Running() || !owners.owners[0].Running() || !owners.owners[1].Running() {
		t.Fatal("not every lane owner published running")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run shutdown = %v", err)
	}
	select {
	case <-owners.Done():
	default:
		t.Fatal("Done did not close after joined shutdown")
	}
	if owners.Running() {
		t.Fatal("owners remained running after shutdown")
	}
}

func TestAuthenticatedExecutionPeerTwoGroupsProgressWithTransportPerPeer(t *testing.T) {
	preflightExecutionFixtureLanes(t)
	const voters = 3
	var runtimes [voters][2]*raftmember.Runtime
	var bases [voters][2]sqldriver.ReplicatedShardStoreIdentity
	var reads [voters][2]*sqldriver.ReplicatedApply
	var groups [2]raftmember.GroupKey
	for member := 0; member < voters; member++ {
		for group := 0; group < 2; group++ {
			runtimes[member][group], bases[member][group], reads[member][group] = newRF3RuntimeForTestGroup(t, uint64(member+1), group, false)
			groups[group] = runtimes[member][group].Identity().Group
		}
	}
	var nodes [voters]rafttransport.NodeID
	members := make([]rafttransport.Member, 0, voters*2)
	for member := range nodes {
		nodes[member][0] = byte(member + 1)
		for _, group := range groups {
			members = append(members, rafttransport.Member{Group: group, ReplicaSetVersion: 1, MemberID: uint64(member + 1), Node: nodes[member], Role: rafttransport.MemberVoter})
		}
	}
	authority := newPeerServerTestAuthority(t)
	listeners := make([]net.Listener, voters)
	addresses := make([]string, voters)
	profiles := make([]*rafttransport.PeerTLS, voters)
	registries := make([]*rafttransport.StaticRegistry, voters)
	for member := 0; member < voters; member++ {
		registry, err := rafttransport.NewStaticRegistry(nodes[member], members, rafttransport.Limits{MaxGroups: 2, MaxMembers: len(members)})
		if err != nil {
			t.Fatal(err)
		}
		registries[member] = registry
		profiles[member] = newPeerServerTestTLS(t, authority, rafttransport.PeerIdentity{TrustDomain: registry.TrustDomain(), Node: nodes[member]})
		listeners[member], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addresses[member] = listeners[member].Addr().String()
	}
	for member := range nodes {
		registries[member] = pinnedPeerTestRegistry(t, nodes[member], members, rafttransport.Limits{MaxGroups: 2, MaxMembers: len(members)}, profiles)
	}

	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	peers := make([]*AuthenticatedExecutionPeerRuntime, voters)
	owners := make([]*ExecutionOwners, voters)
	pulses := make([]chan struct{}, voters)
	contexts := make([]context.Context, voters)
	cancels := make([]context.CancelFunc, voters)
	done := make([]chan error, voters)
	for member := 0; member < voters; member++ {
		serving, err := raftserve.NewRegistry(raftserve.Limits{MaxGroups: 2, MaxOutstandingIdentities: 64, MaxOutstandingAttempts: 128, MaxWaiters: 128, MaxAttemptsPerIdentity: 4, MaxRetainedCompletionBytes: 64 * int64(replicatedstate.MaxCompletionEnvelopeBytes)})
		if err != nil {
			t.Fatal(err)
		}
		lanes, err := serving.NewExecutionLanes(2, multiraft.Limits{MaxGroups: 2, MaxQueueItems: 256, MaxQueueBytes: 128 << 20, MaxGroupItems: 256, MaxGroupBytes: 128 << 20, MaxOutboxItems: 256, MaxOutboxBytes: 128 << 20, MaxPendingTicks: 16})
		if err != nil {
			t.Fatal(err)
		}
		identities := make([]raftmember.RuntimeIdentity, 0, 2)
		fences := make([]CommandFence, 0, 2)
		localReads := make([]ReadSource, 0, 2)
		recovery := make([]TransactionRecoverySource, 0, 2)
		for group := 0; group < 2; group++ {
			if err := lanes.Add(runtimes[member][group]); err != nil {
				t.Fatal(err)
			}
			identity := runtimes[member][group].Identity()
			identities = append(identities, identity)
			fences = append(fences, rf3CommandFence(identity, bases[member][group]))
			localReads = append(localReads, reads[member][group])
			recovery = append(recovery, reads[member][group])
		}
		if left, _ := lanes.Lane(groups[0]); left == func() int { right, _ := lanes.Lane(groups[1]); return right }() {
			t.Fatal("fixture groups did not span execution lanes")
		}
		remote := make([]rafttransport.NodeID, 0, voters-1)
		for index := range nodes {
			if index != member {
				remote = append(remote, nodes[index])
			}
		}
		pulses[member] = make(chan struct{}, 1)
		peer, err := NewAuthenticatedExecutionPeerRuntime(AuthenticatedExecutionPeerOptions{
			Registry: registries[member], TLS: profiles[member], Listener: listeners[member], HandshakeDeadline: deadline, MaxInboundStreams: 8,
			Dial: func(ctx context.Context, node rafttransport.NodeID) (net.Conn, error) {
				for index := range nodes {
					if nodes[index] == node {
						var dialer net.Dialer
						return dialer.DialContext(ctx, "tcp", addresses[index])
					}
				}
				return nil, rafttransport.ErrNodeNotFound
			},
			Execution: ExecutionOptions{Registry: serving, Lanes: lanes, Members: identities, CommandFences: fences, ReadSources: localReads, TransactionRecoverySources: recovery, Pulse: pulses[member], Limits: Limits{MaxIngressItems: 256, MaxIngressBytes: 128 << 20, MaxPendingProposalItems: 128, MaxPendingProposalBytes: 128 << 20, MaxPendingReadItems: 128, MaxPendingReadBytes: 128 << 20, MaxPendingOutboundBytes: 128 << 20}},
			Transport: rafttransport.OrdinaryTransportOptions{Peers: remote, Queue: rafttransport.QueueLimits{PerPeerFrames: 64, PerPeerBytes: 8 << 20, GlobalFrames: 128, GlobalBytes: 16 << 20}, Coalesce: rafttransport.CoalesceLimits{MaxFrames: 8, MaxBytes: 1 << 20, RetainedBytes: rafttransport.DefaultRetainedFrameBytes}, Wait: rafttransport.WaitWithTimer, Backoff: func(uint32) time.Duration { return time.Millisecond }, MaxReconnectDelay: time.Second, WriteDeadline: deadline, RetainedFrameBytes: rafttransport.DefaultRetainedFrameBytes},
			Receiver:  rafttransport.OrdinaryReceiverOptions{ReadDeadline: deadline, RetainedFrameBytes: rafttransport.DefaultRetainedFrameBytes},
		})
		if err != nil {
			t.Fatal(err)
		}
		peers[member], owners[member] = peer, peer.Owners()
		contexts[member], cancels[member] = context.WithCancel(context.Background())
		done[member] = make(chan error, 1)
		go func(member int) { done[member] <- peers[member].Run(contexts[member]) }(member)
	}
	for _, peer := range peers {
		<-peer.Started()
		if !peer.Running() {
			t.Fatal("peer did not start")
		}
	}
	stopTicks := make(chan struct{})
	defer close(stopTicks)
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTicks:
				return
			case <-ticker.C:
				for _, pulse := range pulses {
					select {
					case pulse <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := owners[0].Campaign(ctx, groups[0]); err != nil {
		t.Fatal(err)
	}
	if err := owners[1].Campaign(ctx, groups[1]); err != nil {
		t.Fatal(err)
	}
	for _, group := range groups {
		waitExecutionGroupLeader(t, ctx, owners, group)
	}
	for member, peer := range peers {
		for remote := range nodes {
			if remote != member {
				if _, err := peer.transport.Stats(nodes[remote]); err != nil {
					t.Fatalf("peer %d missing shared transport for node %d: %v", member, remote, err)
				}
			}
		}
		if _, err := peer.transport.Stats(rafttransport.NodeID{0xff}); !errors.Is(err, rafttransport.ErrNodeNotFound) {
			t.Fatalf("unknown transport peer error = %v", err)
		}
	}
	for _, cancel := range cancels {
		cancel()
	}
	for member := range done {
		if err := <-done[member]; !errors.Is(err, context.Canceled) {
			t.Fatalf("peer %d shutdown: %v", member, err)
		}
	}
}

func TestExecutionOwnersInstallAndRemoveDynamicGroupAtomically(t *testing.T) {
	preflightExecutionFixtureLanes(t)
	initialRuntime, initialBase, initialRead := newRF3RuntimeForTestGroup(t, 1, 0, false)
	dynamicRuntime, dynamicBase, dynamicRead := newRF3RuntimeForTestGroup(t, 1, 1, false)
	initialIdentity := initialRuntime.Identity()
	dynamicIdentity := dynamicRuntime.Identity()

	serving, err := raftserve.NewRegistry(raftserve.Limits{
		MaxGroups: 2, MaxOutstandingIdentities: 16, MaxOutstandingAttempts: 16,
		MaxWaiters: 16, MaxAttemptsPerIdentity: 2,
		MaxRetainedCompletionBytes: 16 * int64(replicatedstate.MaxCompletionEnvelopeBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer serving.Close()
	lanes, err := serving.NewExecutionLanes(2, executionDynamicHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err = lanes.Add(initialRuntime); err != nil {
		t.Fatal(err)
	}
	local := rafttransport.NodeID{1}
	initialRoster := executionTestRoster(initialIdentity.Group, local)
	transportRegistry, err := rafttransport.NewStaticRegistry(local, initialRoster,
		rafttransport.Limits{MaxGroups: 2, MaxMembers: 6})
	if err != nil {
		t.Fatal(err)
	}
	owners, err := NewExecutionOwners(ExecutionOptions{
		Registry: serving, Lanes: lanes,
		Members:                    []raftmember.RuntimeIdentity{initialIdentity},
		CommandFences:              []CommandFence{rf3CommandFence(initialIdentity, initialBase)},
		ReadSources:                []ReadSource{initialRead},
		TransactionRecoverySources: []TransactionRecoverySource{initialRead},
		MembershipAuthority:        transportRegistry, Outbound: executionTestOutbound{},
		Limits: Limits{MaxIngressItems: 64, MaxIngressBytes: 16 << 20,
			MaxPendingProposalItems: 16, MaxPendingProposalBytes: 8 << 20,
			MaxPendingReadItems: 16, MaxPendingReadBytes: 8 << 20,
			MaxPendingOutboundBytes: 8 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- owners.Run(ctx) }()
	<-owners.Started()

	dynamicGroup := ExecutionGroup{Runtime: dynamicRuntime, Identity: dynamicIdentity,
		Command: rf3CommandFence(dynamicIdentity, dynamicBase), Read: dynamicRead,
		Recovery: dynamicRead}
	lane, err := lanes.Lane(dynamicIdentity.Group)
	if err != nil {
		t.Fatal(err)
	}
	var installedGeneration *ownerGeneration
	if err = transportRegistry.InstallGroup(
		executionTestRoster(dynamicIdentity.Group, local),
		func(publish func()) error {
			return owners.installGroup(dynamicGroup, func() {
				installedGeneration = owners.owners[lane].members[dynamicIdentity.Group].generation
				if _, routeErr := owners.owner(dynamicIdentity.Group); !errors.Is(routeErr, ErrExecutionGroup) {
					t.Fatalf("owner visible before atomic transport commit: %v", routeErr)
				}
				publish()
				if _, ok := transportRegistry.ReplicaSetVersion(dynamicIdentity.Group); !ok {
					t.Fatal("transport absent during commit")
				}
				if _, routeErr := owners.owner(dynamicIdentity.Group); !errors.Is(routeErr, ErrExecutionGroup) {
					t.Fatalf("owner gate opened before commit returned: %v", routeErr)
				}
			})
		},
	); err != nil {
		t.Fatalf("install dynamic group: %v", err)
	}
	if installedGeneration == nil || !installedGeneration.acquire() {
		t.Error("adopted group cannot pin its read generation")
	} else {
		installedGeneration.release()
		if !installedGeneration.quiesce() || installedGeneration.acquire() {
			t.Error("adopted group read generation does not fence quiescence")
		}
		installedGeneration.resume()
	}
	if _, err = owners.Probe(context.Background(), dynamicIdentity.Group); err != nil {
		t.Fatalf("dynamic owner route: %v", err)
	}
	if member, lookupErr := transportRegistry.LocalMember(dynamicIdentity.Group); lookupErr != nil || member != dynamicIdentity.MemberID {
		t.Fatalf("dynamic transport member=%d err=%v", member, lookupErr)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		err = transportRegistry.RemoveGroup(dynamicIdentity.Group, func(withdraw func()) error {
			return owners.removeGroup(dynamicIdentity, withdraw)
		})
		if err == nil || time.Now().After(deadline) {
			break
		}
		if !errors.Is(err, multiraft.ErrGroupBusy) {
			t.Fatalf("remove dynamic group: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatalf("remove dynamic group did not quiesce: %v", err)
	}
	if _, err = owners.Probe(context.Background(), dynamicIdentity.Group); !errors.Is(err, ErrExecutionGroup) {
		t.Fatalf("removed owner route: %v", err)
	}
	if _, err = transportRegistry.LocalMember(dynamicIdentity.Group); !errors.Is(err, rafttransport.ErrGroupNotFound) {
		t.Fatalf("removed transport route: %v", err)
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown: %v", err)
	}
}

func executionDynamicHostLimits() multiraft.Limits {
	// Each lane must admit one maximum legal inbound frame. Item budgets stay
	// unchanged; the old 8/16 MiB byte caps could not admit the 17 MiB envelope.
	return multiraft.Limits{
		MaxGroups: 2, MaxQueueItems: 64, MaxQueueBytes: raftmodel.MaxInboundMessageBytes,
		MaxGroupItems: 32, MaxGroupBytes: raftmodel.MaxInboundMessageBytes,
		MaxOutboxItems: 64, MaxOutboxBytes: raftmodel.MaxInboundMessageBytes, MaxPendingTicks: 8,
	}
}

func preflightExecutionFixtureLanes(t *testing.T) {
	t.Helper()
	lanes, err := multiraft.NewExecutionLanes(2, executionDynamicHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer lanes.Close()
	var assignments [2]int
	for group := range assignments {
		identity := rf3RuntimeTestIdentity(1, group)
		key := raftmember.GroupKey{ClusterID: identity.ClusterID, ClusterIncarnation: identity.ClusterIncarnation,
			TopologyRecoveryEpoch: 3, ShardIncarnation: identity.ShardIncarnation, GroupID: identity.GroupID}
		assignments[group], err = lanes.Lane(key)
		if err != nil {
			t.Fatal(err)
		}
	}
	if assignments[0] == assignments[1] {
		t.Fatal("runtime fixture groups must span both owner lanes before opening stores")
	}
}

func TestExecutionFixtureHostLimitsAndLaneRouting(t *testing.T) {
	preflightExecutionFixtureLanes(t)
}

func executionTestRoster(group raftmember.GroupKey, local rafttransport.NodeID) []rafttransport.Member {
	return []rafttransport.Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: local, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: rafttransport.NodeID{2}, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 3, Node: rafttransport.NodeID{3}, Role: rafttransport.MemberVoter},
	}
}

func waitExecutionGroupLeader(t testing.TB, ctx context.Context, owners []*ExecutionOwners, group raftmember.GroupKey) {
	t.Helper()
	for ctx.Err() == nil {
		leader, term, complete := uint64(0), uint64(0), true
		for _, owner := range owners {
			state, err := owner.Probe(ctx, group)
			if err != nil || state.Status.LeaderID == 0 {
				complete = false
				break
			}
			if leader == 0 {
				leader, term = state.Status.LeaderID, state.Status.Term
			} else if leader != state.Status.LeaderID || term != state.Status.Term {
				complete = false
				break
			}
		}
		if complete {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("group leader: %v", context.Cause(ctx))
}
