package gateway

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestAuthenticatedReplicatedProbeCarriesDrainingDataProofAndSelfOwnerScope(t *testing.T) {
	route, _, _ := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[0]
	domain := rafttransport.TrustDomain{ClusterID: route.Group.ClusterID, ClusterIncarnation: route.Group.ClusterIncarnation}
	authority := newGatewayTLSAuthority(t)
	storageIdentity := rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{1}}
	gatewayIdentity := rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{92}}
	storageProfile := authority.profile(t, storageIdentity)
	gatewayProfile := authority.profile(t, gatewayIdentity)
	state := raftservice.ServingState{
		Identity: raftmember.RuntimeIdentity{Group: route.Group,
			AllocationGeneration: route.AllocationGeneration, MemberID: endpoint.Member,
			StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation,
			RelationManifestDigest: route.Command.RelationManifestDigest},
		Command: route.Command,
		Status: raftmember.RuntimeStatus{MemberID: endpoint.Member, LeaderID: endpoint.Member,
			Term: 7, Commit: 8, Applied: 8, CheckpointApplied: 8},
	}
	owner := &semanticGatewayOwner{state: state}
	server, err := shardservice.NewReplicatedServer(owner, shardservice.DefaultReplicatedInFlightFrameBytes, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	actor := serviceauthz.Authority{Node: rafttransport.NodeID{93}, Generation: 1}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{
		{Node: actor.Node, Capabilities: serviceauthz.CapabilityDataRead},
		{Node: gatewayIdentity.Node, Capabilities: serviceauthz.CapabilityDelegate | serviceauthz.CapabilityTopology},
	})
	if err != nil {
		t.Fatal(err)
	}
	policyGate, err := serviceauthz.NewGate(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.BindAuthorization(policyGate, nil); err != nil {
		t.Fatal(err)
	}

	var relation [16]byte
	copy(relation[:], route.Command.RelationManifestDigest[:])
	internalFence := serviceauthz.ServiceFence{
		Action:          serviceauthz.ServiceActionGatewayCatalogRead,
		Operation:       serviceauthz.ServiceOperationCatalogRead,
		Group:           route.Group,
		Relation:        relation,
		SessionID:       [16]byte{10},
		SessionRevision: 11,
		IntentID:        route.Command.RelationManifestDigest,
		FenceDigest:     route.Command.RelationManifestDigest,
	}
	binding := serviceauthz.ServiceBinding{
		Principal:           gatewayIdentity.Node,
		PhysicalNode:        storageIdentity.Node,
		PhysicalIncarnation: endpoint.NodeIncarnation,
		KeyDigest:           gatewayProfile.LocalPeerKeyDigest(),
		Roles:               serviceauthz.ServiceRoleGateway,
		Lifecycle:           serviceauthz.ServiceDraining,
		GatewayIncarnation:  1,
		SessionID:           internalFence.SessionID,
		SessionRevision:     internalFence.SessionRevision,
		ParticipantDigest:   [32]byte{12},
		DrainFenceDigest:    internalFence.FenceDigest,
		DrainFence:          internalFence,
		InternalFences:      []serviceauthz.ServiceFence{internalFence},
	}
	token := serviceauthz.FrontendConnToken{13}
	scope := serviceauthz.FrontendContinuationScopeRecord{
		Protocol: serviceauthz.FrontendScopeNative, Action: serviceauthz.FrontendActionForwardedData,
		Capability: serviceauthz.CapabilityDataRead, Operation: serviceauthz.ServiceOperationForwardedRead,
		Group: route.Group,
	}
	grant, err := serviceauthz.NewCommittedFrontendContinuationGrant(serviceauthz.CommittedFrontendContinuationGrant{
		TrustDomain: domain, PhysicalNode: storageIdentity.Node,
		PhysicalIncarnation: endpoint.NodeIncarnation, PeerKeyDigest: gatewayProfile.LocalPeerKeyDigest(),
		GatewayServiceID: gatewayIdentity.Node, GatewaySessionID: binding.SessionID,
		GatewaySessionRevision: binding.SessionRevision, DrainID: [32]byte{14}, AdmissionEpoch: 1,
		AcceptedConnectionTokens:    []serviceauthz.FrontendConnToken{token},
		AcceptedConnectionProtocols: []serviceauthz.FrontendContinuationScope{serviceauthz.FrontendScopeNative},
		AdmissionClosedProofDigest:  [32]byte{15}, Revision: 1,
		State: serviceauthz.ContinuationGrantEnforcing,
	})
	if err != nil {
		t.Fatal(err)
	}
	directory, err := serviceauthz.NewServiceDirectoryGate(serviceauthz.ServiceDirectoryCut{
		Revision: 1, TrustDomain: domain, PolicyGeneration: 1,
		Bindings:           []serviceauthz.ServiceBinding{binding},
		ForwardedScopes:    []serviceauthz.FrontendContinuationScopeRecord{scope},
		ContinuationGrants: []serviceauthz.CommittedFrontendContinuationGrant{grant},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.BindServiceDirectoryGate(directory); err != nil {
		t.Fatal(err)
	}
	capability, err := shardservice.NewReplicatedServerTLS(storageProfile, []rafttransport.NodeID{gatewayIdentity.Node})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	served := make(chan error, 1)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	go func() {
		served <- server.ServeAuthenticated(ctx, listener, capability, deadline, 2, 1)
	}()
	defer func() {
		cancel()
		select {
		case <-served:
		case <-time.After(time.Second):
			t.Error("replicated server did not stop")
		}
	}()

	newClient := func(profile *rafttransport.PeerTLS) *AuthenticatedReplicatedClient {
		client, clientErr := NewAuthenticatedReplicatedClient(AuthenticatedReplicatedClientOptions{
			TLS: profile,
			Dial: func(ctx context.Context, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", address)
			},
			HandshakeDeadline: deadline,
			MaxConnections:    1, MaxPerEndpoint: 1, MaxIdlePerEndpoint: 1,
			MaxHandshakes: 1, MaxWaiters: 1, MaxIdleAge: time.Minute, MaxLifetime: time.Hour,
		})
		if clientErr != nil {
			t.Fatal(clientErr)
		}
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	endpoint.Node = storageIdentity.Node
	endpoint.Address = listener.Addr().String()
	client := newClient(gatewayProfile)
	requestRoute := route
	requestRoute.Replicas = []ReplicatedEndpoint{endpoint}
	dataContext, err := serviceauthz.WithAuthority(ctx, actor)
	if err != nil {
		t.Fatal(err)
	}
	dataContext, err = serviceauthz.WithFrontendContinuationCredential(dataContext,
		serviceauthz.FrontendContinuationCredential{GrantDigest: grant.GrantDigest, ConnToken: token,
			Protocol: serviceauthz.FrontendScopeNative})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.ProbeReplicated(dataContext, requestRoute, endpoint, serviceauthz.CapabilityDataRead)
	if err != nil || response == nil || response.Kind != shardservice.ReplicatedHandshake {
		t.Fatalf("held data probe response=%+v err=%v", response, err)
	}

	// A held frontend credential must not be copied onto an internal owner
	// probe. The self authority is retained, and the exact manifest-derived
	// InternalFence authorizes the call while the service is Draining. If the
	// envelope were present, the data-only grant above would reject it.
	selfContext, err := serviceauthz.WithAuthority(ctx, serviceauthz.Authority{
		Node: gatewayIdentity.Node, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	selfContext, err = serviceauthz.WithFrontendContinuationCredential(selfContext,
		serviceauthz.FrontendContinuationCredential{GrantDigest: grant.GrantDigest, ConnToken: token,
			Protocol: serviceauthz.FrontendScopeNative})
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.ProbeReplicated(selfContext, requestRoute, endpoint, serviceauthz.CapabilityTopology)
	if err != nil || response == nil || response.Kind != shardservice.ReplicatedHandshake {
		t.Fatalf("held owner probe response=%+v err=%v", response, err)
	}

	bareSelfContext, err := serviceauthz.WithAuthority(ctx, serviceauthz.Authority{
		Node: gatewayIdentity.Node, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response, err = client.ProbeReplicated(bareSelfContext, requestRoute, endpoint, serviceauthz.CapabilityTopology); err != nil ||
		response == nil || response.Kind != shardservice.ReplicatedHandshake {
		t.Fatalf("background owner probe response=%+v err=%v", response, err)
	}

	for _, test := range []struct {
		name       string
		credential serviceauthz.FrontendContinuationCredential
	}{
		{name: "unknown token", credential: serviceauthz.FrontendContinuationCredential{
			GrantDigest: grant.GrantDigest, ConnToken: serviceauthz.FrontendConnToken{16},
			Protocol: serviceauthz.FrontendScopeNative,
		}},
		{name: "wrong protocol", credential: serviceauthz.FrontendContinuationCredential{
			GrantDigest: grant.GrantDigest, ConnToken: token,
			Protocol: serviceauthz.FrontendScopePostgreSQL,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			badContext, contextErr := serviceauthz.WithAuthority(ctx, actor)
			if contextErr != nil {
				t.Fatal(contextErr)
			}
			badContext, contextErr = serviceauthz.WithFrontendContinuationCredential(badContext, test.credential)
			if contextErr != nil {
				t.Fatal(contextErr)
			}
			response, callErr := client.ProbeReplicated(badContext, requestRoute, endpoint, serviceauthz.CapabilityDataRead)
			if callErr != nil || response == nil || response.Kind != shardservice.ReplicatedRefusal ||
				response.Refusal != shardservice.ReplicatedRefusalUnauthorized {
				t.Fatalf("invalid data proof response=%+v err=%v", response, callErr)
			}
		})
	}

	rogueProfile := authority.profile(t, gatewayIdentity)
	rogue := newClient(rogueProfile)
	response, err = rogue.ProbeReplicated(dataContext, requestRoute, endpoint, serviceauthz.CapabilityDataRead)
	if err != nil || response == nil || response.Kind != shardservice.ReplicatedRefusal ||
		response.Refusal != shardservice.ReplicatedRefusalUnauthorized {
		t.Fatalf("wrong leaf key response=%+v err=%v", response, err)
	}
	wrongIdentity := authority.profile(t, rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{94}})
	wrongPrincipal := newClient(wrongIdentity)
	if _, err := wrongPrincipal.ProbeReplicated(dataContext, requestRoute, endpoint, serviceauthz.CapabilityDataRead); err == nil {
		t.Fatal("wrong TLS principal completed a probe")
	} else if errors.Is(err, shardservice.ErrReplicatedWire) {
		t.Fatalf("wrong TLS principal reached native wire: %v", err)
	}
}
