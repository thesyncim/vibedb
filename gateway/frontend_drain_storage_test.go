package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type frontendDrainStorageScanner struct {
	evidence GatewayParticipantEvidence
}

func TestReadFrontendDrainRuntimeCutAllowsCatalogOnlyGenerationAdvance(t *testing.T) {
	ctx := context.Background()
	authority, _, current := newCatalogAuthorityFixture(t)
	node := scalingTestNodeRecord(rafttransport.NodeID{0xde}, 1, NodeJoining, 1)
	node.CatalogGeneration = current.Generation()
	if err := authority.PutNode(ctx, node, 0); err != nil {
		t.Fatalf("seed unchanged node row: %v", err)
	}
	node.Lifecycle = NodeActive
	node.Revision++
	if err := authority.PutNode(ctx, node, node.Revision-1); err != nil {
		t.Fatalf("activate unchanged node row: %v", err)
	}
	next := testCatalogAuthoritySnapshot(t, current.Generation()+1)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0xdf)
	if err := peer.Publish(ctx, current.Generation(), next); err != nil {
		t.Fatalf("publish catalog-only generation: %v", err)
	}
	cut, err := authority.ReadFrontendDrainRuntimeCut(ctx)
	if err != nil {
		t.Fatalf("read catalog-only runtime cut: %v", err)
	}
	var catalogGeneration uint64
	if cut.Catalog != nil {
		catalogGeneration = cut.Catalog.Generation()
	}
	if cut.Catalog == nil || cut.Catalog.Generation() != next.Generation() ||
		cut.Nodes.CatalogGeneration != next.Generation() || len(cut.Nodes.Nodes) != 1 ||
		cut.Nodes.Nodes[0].CatalogGeneration != current.Generation() {
		t.Fatalf("catalog-only cut lost raw/effective generations: catalog=%d nodes=%+v",
			catalogGeneration, cut.Nodes)
	}
}

// TestReadFrontendDrainRuntimeCutRetriesAcrossPreparedAndCatalogNodeChange
// exercises the production read/verify loop, rather than a synthetic reader.
// The writer publishes a Prepared child after the first node/catalog fragments
// have been observed, and also advances the catalog head plus the owning node
// to the matching catalog generation. The reader must discard that straddled
// attempt: returning the old node/head beside the new child would manufacture
// a directory revision no committed authority ever held.
func TestReadFrontendDrainRuntimeCutRetriesAcrossPreparedAndCatalogNodeChange(t *testing.T) {
	ctx := context.Background()
	authority, client, current := newCatalogAuthorityFixture(t)
	writer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0xd3)

	nodeID := rafttransport.NodeID{0xd4}
	joining := scalingTestNodeRecord(nodeID, 1, NodeJoining, 1)
	joining.Roles = NodeRoleStorage | NodeRoleGateway
	joining.GatewayEndpoint = distribution.EndpointID("gateway-d4")
	joining.GatewayAddress = "127.0.0.1:8434"
	joining.Gateway = GatewayIdentity{
		NodeID: joining.NodeID, Incarnation: joining.Incarnation,
		ServiceKeyDigest: joining.ServiceKeyDigest, ServiceID: [16]byte{0xd5},
		SessionID: [16]byte{0xd6}, SessionRevision: 1, ParticipantDigest: replication.Digest{0xd7},
	}
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle, active.Revision = NodeActive, joining.Revision+1
	if err := authority.PutNode(ctx, active, joining.Revision); err != nil {
		t.Fatal(err)
	}

	request := ScalingIntentRequest{Kind: ScalingDecommission, RequestID: [32]byte{0xd8},
		Drain:    NodeReference{NodeID: active.NodeID, Incarnation: active.Incarnation},
		MaxMoves: 1, MaxMigrationBytes: 1 << 20}
	intent := ScalingIntent{ID: request.ID(), Request: request, CatalogGeneration: current.Generation(),
		Revision: 1, DirectoryRevision: 1, State: ScalingReserved}
	if !intent.Valid() {
		t.Fatal("drain intent fixture is invalid")
	}
	if err := authority.PutScalingIntent(ctx, intent, 0); err != nil {
		t.Fatal(err)
	}

	nextCatalog := testCatalogAuthoritySnapshot(t, current.Generation()+1)
	nextNode := active
	nextNode.Revision++
	nextNode.CatalogGeneration = nextCatalog.Generation()
	drainID := NewFrontendDrainID(intent.ID, request.Drain)
	grant, err := serviceauthz.NewCommittedFrontendContinuationGrant(serviceauthz.CommittedFrontendContinuationGrant{
		TrustDomain:  rafttransport.TrustDomain{ClusterID: [16]byte{0xd9}, ClusterIncarnation: [16]byte{0xda}},
		PhysicalNode: nextNode.NodeID, PhysicalIncarnation: nextNode.Incarnation,
		PeerKeyDigest: [32]byte(nextNode.Gateway.ServiceKeyDigest), GatewayServiceID: nextNode.Gateway.NodeID,
		GatewaySessionID: nextNode.Gateway.SessionID, GatewaySessionRevision: nextNode.Gateway.SessionRevision,
		DrainID: drainID, AdmissionEpoch: 1, AcceptedConnectionTokens: []serviceauthz.FrontendConnToken{{0xdb}},
		AcceptedConnectionProtocols: []serviceauthz.FrontendContinuationScope{serviceauthz.FrontendScopeNative},
		AdmissionClosedProofDigest:  [32]byte{0xdc}, Revision: nextNode.Revision,
		State: serviceauthz.ContinuationGrantPrepared,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := FrontendDrainRecord{
		IntentID: intent.ID, DrainID: drainID, TrustDomain: grant.TrustDomain,
		PhysicalNode: nextNode.NodeID, PhysicalIncarnation: nextNode.Incarnation,
		GatewayServiceID: nextNode.Gateway.NodeID, GatewayIncarnation: nextNode.Gateway.Incarnation,
		PeerKeyDigest: nextNode.Gateway.ServiceKeyDigest, GatewayIdentityServiceID: nextNode.Gateway.ServiceID,
		GatewaySessionID: nextNode.Gateway.SessionID, GatewaySessionRevision: nextNode.Gateway.SessionRevision,
		NodeRevision: nextNode.Revision, AdmissionEpoch: grant.AdmissionEpoch,
		AdmissionClosedProofDigest: replication.Digest(grant.AdmissionClosedProofDigest),
		DrainFence: serviceauthz.ServiceFence{Action: serviceauthz.ServiceActionGatewayCatalogRead,
			Operation: serviceauthz.ServiceOperationCatalogRead, Group: nextCatalog.ReplicatedShardDescriptors()[0].Group,
			SessionID: nextNode.Gateway.SessionID, SessionRevision: nextNode.Gateway.SessionRevision,
			IntentID: intent.ID, FenceDigest: replication.Digest{0xdd}},
		ContinuationGrant: &grant, Lifecycle: FrontendDrainPrepared, Revision: 1,
	}
	if !record.Valid() {
		t.Fatal("prepared drain record fixture is invalid")
	}
	if err := authority.ReserveFrontendDrainCapacity(ctx, drainID); err != nil {
		t.Fatalf("reserve prepared child: %v", err)
	}
	// The aggregate reader must bind its own authority before calling private
	// helpers such as catalogServiceFences. This wrapper makes an anonymous
	// helper call fail, while the public authority entry point is intentionally
	// invoked with a bare context.
	authority.executor.client = authorityContextCheckingClient{inner: client}

	var publishErr, nodeErr, preparedErr error
	client.mu.Lock()
	client.onRead = func(key []byte) {
		if !bytes.Equal(key, replicatedCatalogHeadKey) {
			return
		}
		// The injected writes read their own authority rows. Clear first so the
		// hook cannot recurse through those reads.
		client.mu.Lock()
		if client.onRead == nil {
			client.mu.Unlock()
			return
		}
		client.onRead = nil
		client.mu.Unlock()
		publishErr = writer.Publish(ctx, current.Generation(), nextCatalog)
		if publishErr != nil {
			return
		}
		nodeErr = writer.PutNode(ctx, nextNode, active.Revision)
		if nodeErr != nil {
			return
		}
		preparedErr = writer.PutFrontendDrainRecord(ctx, record, 0)
	}
	client.mu.Unlock()

	cut, err := authority.ReadFrontendDrainRuntimeCut(ctx)
	if err != nil || publishErr != nil || nodeErr != nil || preparedErr != nil {
		t.Fatalf("straddled runtime cut read=%v publish=%v node=%v prepared=%v", err, publishErr, nodeErr, preparedErr)
	}
	if cut.Nodes.CatalogGeneration != nextCatalog.Generation() || cut.Catalog == nil ||
		cut.Catalog.Generation() != nextCatalog.Generation() || len(cut.Nodes.Nodes) != 1 ||
		cut.Nodes.Nodes[0].Revision != nextNode.Revision || len(cut.ContinuationGrants) != 1 ||
		cut.ContinuationGrants[0].GrantDigest != grant.GrantDigest || len(cut.DrainRecords) != 1 ||
		cut.DrainRecords[0].DrainID != drainID {
		t.Fatalf("reader installed a mixed runtime cut: %+v", cut)
	}

	// No unrelated write is needed to repair a cut that raced the Prepared
	// publication. The immediate reread must return the same coherent source.
	following, err := authority.ReadFrontendDrainRuntimeCut(ctx)
	if err != nil || following.Nodes.Digest != cut.Nodes.Digest ||
		following.CatalogHeadDigest != cut.CatalogHeadDigest ||
		following.ServiceDirectoryRevision != cut.ServiceDirectoryRevision ||
		len(following.ContinuationGrants) != 1 ||
		following.ContinuationGrants[0].GrantDigest != grant.GrantDigest {
		t.Fatalf("coherent reread after race=%+v err=%v", following, err)
	}
}

func TestReplicatedFrontendDrainPreparedCatalogOnlyAckThenEnforce(t *testing.T) {
	ctx := context.Background()
	authority, _, snapshot := newCatalogAuthorityFixture(t)
	nodeID := rafttransport.NodeID{0xae}
	joining := scalingTestNodeRecord(nodeID, 1, NodeJoining, 1)
	joining.Roles = NodeRoleStorage | NodeRoleGateway
	joining.GatewayEndpoint = distribution.EndpointID("gateway-ae")
	joining.GatewayAddress = "127.0.0.1:8314"
	joining.Gateway = GatewayIdentity{
		NodeID: joining.NodeID, Incarnation: joining.Incarnation,
		ServiceKeyDigest: joining.ServiceKeyDigest, ServiceID: [16]byte{0xaf},
		SessionID: [16]byte{0xb0}, SessionRevision: 1, ParticipantDigest: replication.Digest{0xb1},
	}
	if !joining.Valid() {
		t.Fatal("gateway drain node fixture is invalid")
	}
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle, active.Revision = NodeActive, joining.Revision+1
	if err := authority.PutNode(ctx, active, joining.Revision); err != nil {
		t.Fatal(err)
	}

	request := ScalingIntentRequest{Kind: ScalingDecommission, RequestID: [32]byte{0xb2},
		Drain:    NodeReference{NodeID: active.NodeID, Incarnation: active.Incarnation},
		MaxMoves: 1, MaxMigrationBytes: 1 << 20}
	intent := ScalingIntent{ID: request.ID(), Request: request, CatalogGeneration: snapshot.Generation(),
		Revision: 1, DirectoryRevision: 1, State: ScalingReserved}
	if !intent.Valid() {
		t.Fatal("decommission intent fixture is invalid")
	}
	if err := authority.PutScalingIntent(ctx, intent, 0); err != nil {
		t.Fatal(err)
	}

	drainID := NewFrontendDrainID(intent.ID, request.Drain)
	record := FrontendDrainRecord{
		IntentID: intent.ID, DecommissionIntentID: intent.ID, DrainID: drainID,
		TrustDomain:  rafttransport.TrustDomain{ClusterID: [16]byte{0xb3}, ClusterIncarnation: [16]byte{0xb4}},
		PhysicalNode: active.NodeID, PhysicalIncarnation: active.Incarnation,
		GatewayServiceID: active.Gateway.NodeID, GatewayIncarnation: active.Gateway.Incarnation,
		PeerKeyDigest: active.Gateway.ServiceKeyDigest, GatewayIdentityServiceID: active.Gateway.ServiceID,
		GatewaySessionID: active.Gateway.SessionID, GatewaySessionRevision: active.Gateway.SessionRevision,
		NodeRevision: active.Revision, AdmissionEpoch: 1, AdmissionClosedProofDigest: replication.Digest{0xb5},
		DrainFence: serviceauthz.ServiceFence{
			Action: serviceauthz.ServiceActionGatewayCatalogRead, Operation: serviceauthz.ServiceOperationCatalogRead,
			Group: snapshot.ReplicatedShardDescriptors()[0].Group, Relation: [16]byte{0xb6},
			SessionID: active.Gateway.SessionID, SessionRevision: active.Gateway.SessionRevision,
			IntentID: [32]byte{0xb7}, FenceDigest: [32]byte{0xb8},
		},
		Lifecycle: FrontendDrainPrepared, Revision: 1,
	}
	if !record.Valid() || !record.ValidForNode(active) {
		t.Fatalf("prepared drain fixture is invalid: valid=%t for-node=%t", record.Valid(), record.ValidForNode(active))
	}
	if err := authority.ReserveFrontendDrainCapacity(ctx, drainID); err != nil {
		t.Fatalf("reserve prepared drain capacity: %v", err)
	}
	if err := authority.PutFrontendDrainRecord(ctx, record, 0); err != nil {
		t.Fatalf("persist prepared drain: %v", err)
	}
	if err := authority.EnforceFrontendDrain(ctx, drainID, active.NodeID, active.Incarnation, active.Revision); !errors.Is(err, ErrScalingRevision) {
		t.Fatalf("unacknowledged receiver cut accepted: %v", err)
	}

	prepared, err := authority.ReadFrontendDrainRuntimeCut(ctx)
	if err != nil || prepared.Catalog == nil || len(prepared.DrainFences) != 1 ||
		prepared.DrainFences[0].DrainID != drainID {
		t.Fatalf("prepared canonical ACK source=%+v err=%v", prepared, err)
	}
	// Publish only the catalog head. The physical directory and child proof
	// remain byte-identical; the ACK must use the newer effective generation
	// before the atomic Enforce CAS reads that same full cut.
	nextCatalog := testCatalogAuthoritySnapshot(t, snapshot.Generation()+1)
	publisher := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(snapshot), 0xb9)
	if err := publisher.Publish(ctx, snapshot.Generation(), nextCatalog); err != nil {
		t.Fatalf("publish catalog-only generation: %v", err)
	}
	ackCut, err := authority.ReadFrontendDrainRuntimeCut(ctx)
	if err != nil || ackCut.Catalog == nil || ackCut.Catalog.Generation() != nextCatalog.Generation() ||
		ackCut.Nodes.CatalogGeneration != nextCatalog.Generation() || ackCut.Nodes.Revision != prepared.Nodes.Revision ||
		ackCut.Nodes.Digest != prepared.Nodes.Digest || len(ackCut.DrainFences) != 1 ||
		ackCut.DrainFences[0].DrainID != drainID {
		t.Fatalf("catalog-only ACK source=%+v err=%v", ackCut, err)
	}
	acked := record
	acked.ReceiverDirectoryRevision, acked.ReceiverDirectoryDigest = ackCut.Nodes.Revision, ackCut.Nodes.Digest
	acked.ReceiverCatalogGeneration, acked.ReceiverCatalogHeadDigest = ackCut.Nodes.CatalogGeneration, ackCut.CatalogHeadDigest
	acked.Revision++
	if err := authority.PutFrontendDrainRecord(ctx, acked, record.Revision); err != nil {
		t.Fatalf("persist catalog-only ACK fence: %v", err)
	}
	for _, change := range []struct {
		name  string
		apply func(*FrontendDrainRecord)
	}{
		{"directory revision", func(r *FrontendDrainRecord) { r.ReceiverDirectoryRevision++ }},
		{"directory digest", func(r *FrontendDrainRecord) { r.ReceiverDirectoryDigest[0]++ }},
		{"catalog generation", func(r *FrontendDrainRecord) { r.ReceiverCatalogGeneration++ }},
		{"catalog digest", func(r *FrontendDrainRecord) { r.ReceiverCatalogHeadDigest[0]++ }},
	} {
		stale := acked
		stale.Revision++
		change.apply(&stale)
		if err := authority.PutFrontendDrainRecord(ctx, stale, acked.Revision); err != nil {
			t.Fatal(err)
		}
		if err := authority.EnforceFrontendDrain(ctx, drainID, active.NodeID, active.Incarnation, active.Revision); !errors.Is(err, ErrScalingRevision) {
			t.Fatalf("changed %s accepted: %v", change.name, err)
		}
		acked.Revision = stale.Revision + 1
		if err := authority.PutFrontendDrainRecord(ctx, acked, stale.Revision); err != nil {
			t.Fatal(err)
		}
	}
	// Production authorities attest reads through the durable route tracker.
	// Enforce already owns authority.mu: rebuilding a runtime cut under that
	// lock used to deadlock when attestation tried to acquire it again.
	authority.routeSeed.Store(&replicatedCatalogRouteSeedTracker{
		immutable: testCatalogAuthoritySnapshot(t, 1), active: ackCut.Catalog,
		activeExists: true, shutdown: make(chan struct{}),
	})
	done := make(chan error, 1)
	go func() {
		done <- authority.EnforceFrontendDrain(ctx, drainID, active.NodeID, active.Incarnation, active.Revision)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("catalog-only Prepared ACK then Enforce: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Enforce reentered the catalog authority lock")
	}
	draining, err := authority.ReadNode(ctx, active.NodeID, active.Incarnation)
	if err != nil || draining.Lifecycle != NodeDraining || draining.Revision != active.Revision+1 {
		t.Fatalf("catalog-only Enforce node=%+v err=%v", draining, err)
	}
	enforcing, err := authority.ReadFrontendDrainRecord(ctx, drainID)
	if err != nil || enforcing.Lifecycle != FrontendDrainEnforcing || enforcing.ReceiverCatalogGeneration != nextCatalog.Generation() {
		t.Fatalf("catalog-only Enforce child=%+v err=%v", enforcing, err)
	}
}

type authorityContextCheckingClient struct {
	inner *catalogAuthorityClient
}

func (client authorityContextCheckingClient) DoReplicated(
	ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	authority, ok := serviceauthz.FromContext(ctx)
	if !ok || client.inner == nil || authority != client.inner.wantAuthority {
		return nil, errors.New("catalog request lost bound authority context")
	}
	return client.inner.DoReplicated(ctx, endpoint, request)
}

func TestFrontendDrainCapacityReservationIsIdempotentAndBounded(t *testing.T) {
	ctx := context.Background()
	authority, client, _ := newCatalogAuthorityFixture(t)
	const callers = 16
	var drainID [32]byte
	drainID[0] = 0xd1
	var wait sync.WaitGroup
	errs := make(chan error, callers)
	for index := 0; index < callers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errs <- authority.ReserveFrontendDrainCapacity(ctx, drainID)
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("idempotent reservation=%v", err)
		}
	}
	directory, err := openReplicatedServiceDirectory(client.rows[string(replicatedServiceDirectoryKey)])
	if err != nil || len(directory.Reservations) != 1 || len(directory.Drains) != 0 {
		t.Fatalf("reservation directory drains=%d reservations=%d err=%v", len(directory.Drains), len(directory.Reservations), err)
	}

	full := replicatedServiceDirectory{Revision: 1,
		Reservations: make([]replicatedFrontendDrainReservation, maxReplicatedFrontendDrainEntries)}
	for index := range full.Reservations {
		id := make([]byte, 32)
		ordinal := index + 1
		id[30], id[31] = byte(ordinal>>8), byte(ordinal)
		full.Reservations[index] = replicatedFrontendDrainReservation{DrainID: id}
	}
	// Two distinct coordinators race for the final slot. Exactly one durable
	// reservation may win; the loser receives a pre-admission conflict rather
	// than closing a listener and discovering capacity afterward.
	almostFull := full
	almostFull.Reservations = append([]replicatedFrontendDrainReservation(nil), full.Reservations[:len(full.Reservations)-1]...)
	raw, err := appendReplicatedServiceDirectory(nil, almostFull)
	if err != nil {
		t.Fatal(err)
	}
	client.rows[string(replicatedServiceDirectoryKey)] = raw
	ids := [][32]byte{{0xec}, {0xed}}
	contention := make(chan error, len(ids))
	for _, id := range ids {
		wait.Add(1)
		go func(id [32]byte) {
			defer wait.Done()
			contention <- authority.ReserveFrontendDrainCapacity(ctx, id)
		}(id)
	}
	wait.Wait()
	close(contention)
	successes, conflicts := 0, 0
	for err := range contention {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrReplicatedCatalogConflict) {
			conflicts++
		} else {
			t.Fatalf("final-slot reservation=%v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("final-slot successes=%d conflicts=%d", successes, conflicts)
	}

	raw, err = appendReplicatedServiceDirectory(nil, full)
	if err != nil {
		t.Fatal(err)
	}
	client.rows[string(replicatedServiceDirectoryKey)] = raw
	var blocked [32]byte
	blocked[0] = 0xee
	if err := authority.ReserveFrontendDrainCapacity(ctx, blocked); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("full directory reservation=%v, want conflict before admission closes", err)
	}
}

func TestReplicatedScalingRejectsConcurrentGatewayDrainsBeforeReservation(t *testing.T) {
	ctx := context.Background()
	authority, _, snapshot := newCatalogAuthorityFixture(t)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(snapshot), 0xf1)

	makeGateway := func(seed byte) NodeRecord {
		node := scalingTestNodeRecord(rafttransport.NodeID{seed}, 1, NodeJoining, 1)
		node.Roles = NodeRoleStorage | NodeRoleGateway
		node.GatewayEndpoint = distribution.EndpointID(fmt.Sprintf("gateway-%02x", seed))
		node.GatewayAddress = fmt.Sprintf("127.0.0.1:%d", 8400+int(seed))
		node.Gateway = GatewayIdentity{
			NodeID: node.NodeID, Incarnation: node.Incarnation,
			ServiceKeyDigest: node.ServiceKeyDigest, ServiceID: [16]byte{seed, 1},
			SessionID: [16]byte{seed, 2}, SessionRevision: 1,
			ParticipantDigest: replication.Digest{seed, 3},
		}
		return node
	}
	activate := func(node NodeRecord) NodeRecord {
		if err := authority.PutNode(ctx, node, 0); err != nil {
			t.Fatal(err)
		}
		node.Lifecycle = NodeActive
		node.Revision++
		if err := authority.PutNode(ctx, node, node.Revision-1); err != nil {
			t.Fatal(err)
		}
		return node
	}
	firstNode := activate(makeGateway(0xf2))
	secondNode := activate(makeGateway(0xf3))
	makeIntent := func(node NodeRecord, requestByte byte) ScalingIntent {
		request := ScalingIntentRequest{Kind: ScalingDecommission, RequestID: [32]byte{requestByte},
			Drain:    NodeReference{NodeID: node.NodeID, Incarnation: node.Incarnation},
			MaxMoves: 1, MaxMigrationBytes: 1 << 20}
		return ScalingIntent{ID: request.ID(), Request: request, CatalogGeneration: snapshot.Generation(),
			Revision: 1, DirectoryRevision: 1, State: ScalingReserved}
	}
	first, second := makeIntent(firstNode, 0xf4), makeIntent(secondNode, 0xf5)
	if !first.Valid() || !second.Valid() || first.ID == second.ID {
		t.Fatalf("gateway drain intents invalid or collided: first=%+v second=%+v", first, second)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		results <- authority.PutScalingIntent(ctx, first, 0)
	}()
	go func() {
		<-start
		results <- peer.PutScalingIntent(ctx, second, 0)
	}()
	close(start)
	var successes, conflicts int
	for range 2 {
		err := <-results
		if err == nil {
			successes++
			continue
		}
		if errors.Is(err, ErrConcurrentFrontendDrain) || errors.Is(err, ErrReplicatedCatalogConflict) {
			conflicts++
			continue
		}
		t.Fatalf("concurrent gateway drain admission=%v", err)
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent gateway drains successes=%d conflicts=%d", successes, conflicts)
	}

	intents, err := authority.ListScalingIntents(ctx)
	if err != nil || len(intents) != 1 {
		t.Fatalf("durable concurrent drain intents=%d err=%v", len(intents), err)
	}
	winner := intents[0]
	if err := authority.PutScalingIntent(ctx, winner, 0); err != nil {
		t.Fatalf("same gateway drain retry=%v", err)
	}
	if err := authority.ReserveFrontendDrainCapacity(ctx, NewFrontendDrainID(winner.ID, winner.Request.Drain)); err != nil {
		t.Fatalf("winner reservation after guarded admission=%v", err)
	}
	if retry, err := authority.ReadScalingIntent(ctx, winner.ID); err != nil || retry.ID != winner.ID {
		t.Fatalf("same drain retry did not retain durable intent=%+v err=%v", retry, err)
	}
}

func TestReplicatedGatewayRetirementRequiresCanonicalChild(t *testing.T) {
	newDrainingGateway := func(t *testing.T, seed byte) (*ReplicatedCatalogAuthority, *catalogAuthorityClient, *Snapshot, NodeRecord, NodeRecord, NodeReferenceEvidence) {
		t.Helper()
		authority, client, current := newCatalogAuthorityFixture(t)
		joining := scalingTestNodeRecord(rafttransport.NodeID{seed}, 1, NodeJoining, 1)
		joining.Roles = NodeRoleStorage | NodeRoleGateway
		joining.GatewayEndpoint = distribution.EndpointID(fmt.Sprintf("gateway-%02x", seed))
		joining.GatewayAddress = fmt.Sprintf("127.0.0.1:%d", 8500+int(seed))
		joining.Gateway = GatewayIdentity{
			NodeID: joining.NodeID, Incarnation: joining.Incarnation,
			ServiceKeyDigest: joining.ServiceKeyDigest, ServiceID: [16]byte{seed, 1},
			SessionID: [16]byte{seed, 2}, SessionRevision: 1,
			ParticipantDigest: replication.Digest{seed, 3},
		}
		if err := authority.PutNode(t.Context(), joining, 0); err != nil {
			t.Fatal(err)
		}
		active := joining
		active.Lifecycle, active.Revision = NodeActive, 2
		if err := authority.PutNode(t.Context(), active, joining.Revision); err != nil {
			t.Fatal(err)
		}
		draining := active
		draining.Lifecycle, draining.Revision = NodeDraining, active.Revision+1
		// This unexported fixture seam deliberately manufactures the durable
		// invalid state that a legacy/broken writer could leave behind. The
		// production PutNode path rejects the same gateway transition.
		if err := authority.putNodeWithExtra(t.Context(), draining, active.Revision, nil, nil,
			func(context.Context, NodeRecord, NodeRecord, uint64, replication.Digest) ([]NativeMutation, error) {
				return nil, nil
			}); err != nil {
			t.Fatal(err)
		}
		scanner := frontendDrainStorageScanner{evidence: GatewayParticipantEvidence{
			NodeID: draining.NodeID, Incarnation: draining.Incarnation, ServiceKeyDigest: draining.ServiceKeyDigest,
			NodeRevision: draining.Revision, CatalogGeneration: draining.CatalogGeneration,
			GatewayNodeID: draining.Gateway.NodeID, GatewayIncarnation: draining.Gateway.Incarnation,
			GatewayServiceKeyDigest: draining.Gateway.ServiceKeyDigest, ServiceID: draining.Gateway.ServiceID,
			SessionID: draining.Gateway.SessionID, SessionRevision: draining.Gateway.SessionRevision,
			ParticipantDigest: draining.Gateway.ParticipantDigest, DirectoryRevision: 1,
			Digest: replication.Digest{seed, 4}, Active: false,
		}}
		authority.gatewayParticipants = scanner
		evidence, err := authority.ScanNodeReferences(t.Context(), draining.NodeID, draining.Incarnation)
		if err != nil || !evidence.ZeroAllReferences() {
			t.Fatalf("invalid gateway fixture evidence=%+v err=%v", evidence, err)
		}
		return authority, client, current, active, draining, evidence
	}

	t.Run("missing-child", func(t *testing.T) {
		authority, _, _, active, draining, evidence := newDrainingGateway(t, 0xc1)
		if err := authority.RetireNode(t.Context(), active.NodeID, active.Incarnation, draining.Revision, evidence); !errors.Is(err, ErrScalingState) {
			t.Fatalf("gateway retirement without canonical child=%v, want scaling-state rejection", err)
		}
		stored, err := authority.ReadNode(t.Context(), active.NodeID, active.Incarnation)
		if err != nil || stored.Lifecycle != NodeDraining {
			t.Fatalf("missing-child retirement crossed lifecycle stored=%+v err=%v", stored, err)
		}
	})

	t.Run("mismatched-child", func(t *testing.T) {
		authority, client, current, active, draining, evidence := newDrainingGateway(t, 0xc2)
		request := ScalingIntentRequest{Kind: ScalingDecommission, RequestID: [32]byte{0xc3},
			Drain:    NodeReference{NodeID: active.NodeID, Incarnation: active.Incarnation},
			MaxMoves: 1, MaxMigrationBytes: 1 << 20}
		wrongSession := [16]byte{0xc4}
		fence := serviceauthz.ServiceFence{
			Action: serviceauthz.ServiceActionGatewayCatalogRead, Operation: serviceauthz.ServiceOperationCatalogRead,
			Group: current.ReplicatedShardDescriptors()[0].Group, Relation: [16]byte{0xc5},
			SessionID: wrongSession, SessionRevision: 1,
			IntentID: [32]byte{0xc6}, FenceDigest: [32]byte{0xc7},
		}
		foreign := FrontendDrainRecord{
			IntentID: request.ID(), DrainID: NewFrontendDrainID(request.ID(), request.Drain),
			TrustDomain:  rafttransport.TrustDomain{ClusterID: [16]byte{0xc8}, ClusterIncarnation: [16]byte{0xc9}},
			PhysicalNode: active.NodeID, PhysicalIncarnation: active.Incarnation,
			GatewayServiceID: active.Gateway.NodeID, GatewayIncarnation: active.Gateway.Incarnation,
			PeerKeyDigest: active.Gateway.ServiceKeyDigest, GatewayIdentityServiceID: active.Gateway.ServiceID,
			GatewaySessionID: wrongSession, GatewaySessionRevision: 1,
			NodeRevision: draining.Revision, AdmissionEpoch: 1, AdmissionClosedProofDigest: replication.Digest{0xca},
			DrainFence: fence, Lifecycle: FrontendDrainEnforcing, Revision: 1,
		}
		childRaw, err := appendReplicatedFrontendDrainDocument(nil, foreign)
		if err != nil {
			t.Fatal(err)
		}
		directory := replicatedServiceDirectory{Revision: 1, Drains: []replicatedFrontendDrainEntry{frontendDrainEntry(foreign, childRaw)}}
		directoryRaw, err := appendReplicatedServiceDirectory(nil, directory)
		if err != nil {
			t.Fatal(err)
		}
		client.mu.Lock()
		client.rows[string(frontendDrainDocumentKey(foreign.DrainID))] = childRaw
		client.rows[string(replicatedServiceDirectoryKey)] = directoryRaw
		client.mu.Unlock()
		fresh, freshErr := authority.ScanNodeReferences(t.Context(), active.NodeID, active.Incarnation)
		if freshErr != nil || !sameNodeReferenceEvidence(fresh, evidence) {
			t.Fatalf("mismatched-child fixture changed reference cut fresh=%+v prior=%+v err=%v", fresh, evidence, freshErr)
		}
		if err := authority.RetireNode(t.Context(), active.NodeID, active.Incarnation, draining.Revision, evidence); !errors.Is(err, ErrScalingIdentity) {
			t.Fatalf("gateway retirement with mismatched canonical child=%v, want identity rejection", err)
		}
		stored, err := authority.ReadNode(t.Context(), active.NodeID, active.Incarnation)
		if err != nil || stored.Lifecycle != NodeDraining {
			t.Fatalf("mismatched-child retirement crossed lifecycle stored=%+v err=%v", stored, err)
		}
	})
}

func (scanner frontendDrainStorageScanner) ScanGatewayParticipant(
	context.Context, NodeRecord,
) (GatewayParticipantEvidence, error) {
	return scanner.evidence, nil
}

func TestReplicatedFrontendDrainAtomicEnforceAndRetire(t *testing.T) {
	ctx := context.Background()
	authority, client, snapshot := newCatalogAuthorityFixture(t)
	nodeID := rafttransport.NodeID{0xa1}
	joining := scalingTestNodeRecord(nodeID, 1, NodeJoining, 1)
	joining.Roles = NodeRoleStorage | NodeRoleGateway
	joining.GatewayEndpoint = distribution.EndpointID("gateway-a1")
	joining.GatewayAddress = "127.0.0.1:8301"
	joining.Gateway = GatewayIdentity{
		NodeID: joining.NodeID, Incarnation: joining.Incarnation,
		ServiceKeyDigest: joining.ServiceKeyDigest, ServiceID: [16]byte{0xa2},
		SessionID: [16]byte{0xa3}, SessionRevision: 1,
		ParticipantDigest: replication.Digest{0xa4},
	}
	if !joining.Valid() {
		t.Fatal("gateway drain node fixture is invalid")
	}
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}

	request := ScalingIntentRequest{Kind: ScalingDecommission, RequestID: [32]byte{0xb1},
		Drain:    NodeReference{NodeID: active.NodeID, Incarnation: active.Incarnation},
		MaxMoves: 1, MaxMigrationBytes: 1 << 20}
	intent := ScalingIntent{ID: request.ID(), Request: request, CatalogGeneration: snapshot.Generation(),
		Revision: 1, DirectoryRevision: 1, State: ScalingReserved}
	if !intent.Valid() {
		t.Fatal("decommission intent fixture is invalid")
	}
	if err := authority.PutScalingIntent(ctx, intent, 0); err != nil {
		t.Fatal(err)
	}

	group := snapshot.ReplicatedShardDescriptors()[0].Group
	drainID := NewFrontendDrainID(intent.ID, request.Drain)
	fence := serviceauthz.ServiceFence{
		Action: serviceauthz.ServiceActionGatewayCatalogRead, Operation: serviceauthz.ServiceOperationCatalogRead,
		Group: group, Relation: [16]byte{0xb2}, SessionID: active.Gateway.SessionID,
		SessionRevision: active.Gateway.SessionRevision, IntentID: [32]byte{0xb3}, FenceDigest: [32]byte{0xb4},
	}
	record := FrontendDrainRecord{
		IntentID: intent.ID, DecommissionIntentID: intent.ID, DrainID: drainID,
		TrustDomain:  rafttransport.TrustDomain{ClusterID: [16]byte{0xb5}, ClusterIncarnation: [16]byte{0xb6}},
		PhysicalNode: active.NodeID, PhysicalIncarnation: active.Incarnation,
		GatewayServiceID: active.Gateway.NodeID, GatewayIncarnation: active.Gateway.Incarnation,
		PeerKeyDigest: active.Gateway.ServiceKeyDigest, GatewayIdentityServiceID: active.Gateway.ServiceID,
		GatewaySessionID: active.Gateway.SessionID, GatewaySessionRevision: active.Gateway.SessionRevision,
		NodeRevision: active.Revision, AdmissionEpoch: 1, AdmissionClosedProofDigest: replication.Digest{0xb7},
		DrainFence: fence, Lifecycle: FrontendDrainPrepared, Revision: 1,
	}
	if !record.Valid() || !record.ValidForNode(active) {
		t.Fatalf("prepared drain fixture is invalid: valid=%t for-node=%t", record.Valid(), record.ValidForNode(active))
	}
	if err := authority.ReserveFrontendDrainCapacity(ctx, drainID); err != nil {
		t.Fatalf("reserve prepared drain capacity: %v", err)
	}
	// Each lifecycle mutation is one atomic native batch. Simulate the client
	// losing its response after the batch was applied, then recover through the
	// session's retained byte-identical command before reading the phase. This
	// keeps replay coverage distinct from scaling-intent publication recovery.
	recoverAppliedUnknown := func(label string, mutate func() error) {
		t.Helper()
		client.mu.Lock()
		client.unknownNext = true
		client.mu.Unlock()
		if err := mutate(); !errors.Is(err, ErrReplicatedCatalogPending) {
			t.Fatalf("%s applied response loss=%v, want pending outcome", label, err)
		}
		client.mu.Lock()
		client.holdUnknown = false
		client.mu.Unlock()
		if err := authority.RetryPending(ctx); err != nil {
			t.Fatalf("%s exact pending replay=%v", label, err)
		}
	}
	recoverAppliedUnknown("Prepared", func() error {
		return authority.PutFrontendDrainRecord(ctx, record, 0)
	})
	preparedAfterReplay, err := authority.ReadFrontendDrainRecord(ctx, drainID)
	if err != nil || preparedAfterReplay.Lifecycle != FrontendDrainPrepared {
		t.Fatalf("prepared phase after response-loss replay=%+v err=%v", preparedAfterReplay, err)
	}
	marker := serviceauthz.CommittedFrontendDrainFence{
		TrustDomain: record.TrustDomain, PhysicalNode: record.PhysicalNode,
		PhysicalIncarnation: record.PhysicalIncarnation, PeerKeyDigest: [32]byte(record.keyDigest()),
		GatewayServiceID: record.GatewayServiceID, GatewaySessionID: record.GatewaySessionID,
		GatewaySessionRevision: record.GatewaySessionRevision, DrainID: record.DrainID,
		Revision: record.NodeRevision, Fence: record.DrainFence,
	}
	if !marker.Valid() {
		t.Fatal("prepared empty drain fence fixture is invalid")
	}
	// A receiver roster change after Prepared is recoverable only by a CAS that
	// refreshes the global directory/head fence while preserving the immutable
	// admission proof. This models a retry that must recollect every receiver
	// rather than enforcing against the stale roster.
	directoryCut, err := authority.ReadNodeDirectoryCut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, headDigest, err := authority.ReadReplicatedCatalogHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	refreshed := record
	refreshed.ReceiverDirectoryRevision, refreshed.ReceiverDirectoryDigest = directoryCut.Revision, directoryCut.Digest
	refreshed.ReceiverCatalogGeneration, refreshed.ReceiverCatalogHeadDigest = directoryCut.CatalogGeneration, headDigest
	refreshed.Revision++
	if err := authority.PutFrontendDrainRecord(ctx, refreshed, record.Revision); err != nil {
		t.Fatalf("refresh prepared receiver roster fence: %v", err)
	}
	stale := refreshed
	stale.Revision++
	stale.ReceiverDirectoryRevision++
	if err := authority.PutFrontendDrainRecord(ctx, stale, record.Revision); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("stale prepared roster CAS = %v, want conflict", err)
	}
	reloaded, err := authority.ReadFrontendDrainRecord(ctx, drainID)
	if err != nil || reloaded.Revision != refreshed.Revision || reloaded.ReceiverDirectoryDigest != directoryCut.Digest ||
		reloaded.AdmissionClosedProofDigest != record.AdmissionClosedProofDigest || reloaded.DrainFence != record.DrainFence {
		t.Fatalf("refreshed prepared record=%+v err=%v", reloaded, err)
	}
	record = refreshed
	recoverAppliedUnknown("Enforcing", func() error {
		return authority.EnforceFrontendDrain(ctx, drainID, active.NodeID, active.Incarnation, active.Revision)
	})
	draining, err := authority.ReadNode(ctx, active.NodeID, active.Incarnation)
	if err != nil || draining.Lifecycle != NodeDraining || draining.Revision != active.Revision+1 {
		t.Fatalf("enforced node=%+v err=%v", draining, err)
	}
	enforcing, err := authority.ReadFrontendDrainRecord(ctx, drainID)
	if err != nil || enforcing.Lifecycle != FrontendDrainEnforcing || enforcing.NodeRevision != draining.Revision || enforcing.Revision != record.Revision+1 {
		t.Fatalf("enforcing child=%+v err=%v", enforcing, err)
	}
	markersRevision, markers, err := authority.ReadServiceDirectoryDrainFenceCut(ctx)
	if err != nil || markersRevision == 0 || len(markers) != 1 || markers[0].DrainID != drainID || markers[0].Revision != draining.Revision {
		t.Fatalf("enforcing service fence revision=%d markers=%+v err=%v", markersRevision, markers, err)
	}
	// The aggregate row is now a bounded child index. Its authorization proof
	// must be derived from the verified drain child, never stored as a second
	// full fence/grant payload that can exhaust the aggregate before a legal
	// later drain begins.
	directoryRaw := client.rows[string(replicatedServiceDirectoryKey)]
	if len(directoryRaw) == 0 {
		t.Fatal("canonical drain index is missing")
	}
	directory, err := openReplicatedServiceDirectory(directoryRaw)
	if err != nil || len(directory.Drains) != 1 || len(directory.Reservations) != 0 {
		t.Fatalf("canonical drain index drains=%d reservations=%d err=%v", len(directory.Drains), len(directory.Reservations), err)
	}

	scanner := frontendDrainStorageScanner{evidence: GatewayParticipantEvidence{
		NodeID: draining.NodeID, Incarnation: draining.Incarnation, ServiceKeyDigest: draining.ServiceKeyDigest,
		NodeRevision: draining.Revision, CatalogGeneration: draining.CatalogGeneration,
		GatewayNodeID: draining.Gateway.NodeID, GatewayIncarnation: draining.Gateway.Incarnation,
		GatewayServiceKeyDigest: draining.Gateway.ServiceKeyDigest, ServiceID: draining.Gateway.ServiceID,
		SessionID: draining.Gateway.SessionID, SessionRevision: draining.Gateway.SessionRevision,
		ParticipantDigest: draining.Gateway.ParticipantDigest, DirectoryRevision: 1,
		Digest: replication.Digest{0xb8},
	}}
	authority.gatewayParticipants = scanner
	evidence, err := authority.ScanNodeReferences(ctx, draining.NodeID, draining.Incarnation)
	if err != nil || !evidence.ZeroAllReferences() {
		t.Fatalf("zero retirement reference cut=%+v err=%v", evidence, err)
	}
	recoverAppliedUnknown("Retired", func() error {
		return authority.RetireNode(ctx, draining.NodeID, draining.Incarnation, draining.Revision, evidence)
	})
	terminal, err := authority.ReadNode(ctx, draining.NodeID, draining.Incarnation)
	if err != nil || terminal.Lifecycle != NodeDecommissioned || !terminal.HasRetirementProof() {
		t.Fatalf("terminal node=%+v err=%v", terminal, err)
	}
	retired, err := authority.ReadFrontendDrainRecord(ctx, drainID)
	if err != nil || retired.Lifecycle != FrontendDrainRetired || retired.NodeRevision != draining.Revision || retired.Revision != enforcing.Revision+1 {
		t.Fatalf("retired child=%+v err=%v", retired, err)
	}
	terminalEvidence, err := authority.ScanDecommissionedNodeReferences(ctx, terminal.NodeID, terminal.Incarnation)
	if err != nil || !terminalEvidence.ZeroAllReferences() {
		t.Fatalf("terminal reference cut=%+v err=%v", terminalEvidence, err)
	}
	// The first gateway intent still owns admission after the node tombstone.
	// A second distinct gateway drain must therefore remain blocked while the
	// terminal ACK and ScalingComplete transition are outstanding.
	secondJoining := scalingTestNodeRecord([16]byte{0xbe}, 1, NodeJoining, 1)
	secondJoining.Roles = NodeRoleStorage | NodeRoleGateway
	secondJoining.GatewayEndpoint = distribution.EndpointID("gateway-be")
	secondJoining.GatewayAddress = "127.0.0.1:8399"
	secondJoining.Gateway = GatewayIdentity{
		NodeID: secondJoining.NodeID, Incarnation: secondJoining.Incarnation,
		ServiceKeyDigest: secondJoining.ServiceKeyDigest, ServiceID: [16]byte{0xbf},
		SessionID: [16]byte{0xc0}, SessionRevision: 1, ParticipantDigest: replication.Digest{0xc1},
	}
	if err := authority.PutNode(ctx, secondJoining, 0); err != nil {
		t.Fatalf("seed second gateway node: %v", err)
	}
	secondActive := secondJoining
	secondActive.Lifecycle, secondActive.Revision = NodeActive, 2
	if err := authority.PutNode(ctx, secondActive, secondJoining.Revision); err != nil {
		t.Fatalf("activate second gateway node: %v", err)
	}
	secondRequest := ScalingIntentRequest{Kind: ScalingDecommission, RequestID: [32]byte{0xc2},
		Drain: NodeReference{NodeID: secondActive.NodeID, Incarnation: secondActive.Incarnation}, MaxMoves: 1, MaxMigrationBytes: 1 << 20}
	secondIntent := ScalingIntent{ID: secondRequest.ID(), Request: secondRequest,
		CatalogGeneration: snapshot.Generation(), Revision: 1, DirectoryRevision: 1, State: ScalingReserved}
	if err := authority.PutScalingIntent(ctx, secondIntent, 0); !errors.Is(err, ErrConcurrentFrontendDrain) {
		t.Fatalf("second gateway drain crossed unfinished terminal ACK: %v", err)
	}
	if err := authority.EnforceFrontendDrain(ctx, drainID, draining.NodeID, draining.Incarnation, draining.Revision); !errors.Is(err, ErrScalingState) {
		t.Fatalf("enforce replay crossed terminal lifecycle: %v", err)
	}
	loadedRevision, loadedMarkers, err := authority.ReadServiceDirectoryDrainFenceCut(ctx)
	if err != nil || loadedRevision != markersRevision+1 || len(loadedMarkers) != 1 || !sameServiceDrainFenceImmutable(loadedMarkers[0], markers[0]) {
		t.Fatalf("terminal fence publication mismatch revision=%d markers=%+v err=%v", loadedRevision, loadedMarkers, err)
	}
	// Fill the remaining compact index slots, then reserve one new drain. The
	// terminal child is compacted atomically; no restarted reader can recover a
	// continuation grant from it, while existing gates retain deny-only state.
	directory, _, err = authority.readFrontendDrainDirectory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	directory.Reservations = make([]replicatedFrontendDrainReservation, maxReplicatedFrontendDrainEntries-1)
	for index := range directory.Reservations {
		id := make([]byte, 32)
		ordinal := index + 1
		id[30], id[31] = byte(ordinal>>8), byte(ordinal)
		directory.Reservations[index] = replicatedFrontendDrainReservation{DrainID: id}
	}
	directory.Revision++
	directoryRaw, err = appendReplicatedServiceDirectory(nil, directory)
	if err != nil {
		t.Fatal(err)
	}
	client.rows[string(replicatedServiceDirectoryKey)] = directoryRaw
	var replacement [32]byte
	replacement[0] = 0xfa
	if err := authority.ReserveFrontendDrainCapacity(ctx, replacement); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("unacknowledged terminal child was compacted: %v", err)
	}
	ackIntent, err := authority.ReadScalingIntent(ctx, intent.ID)
	if err != nil {
		t.Fatal(err)
	}
	// RetireNode's witness is captured before the terminal lifecycle CAS. The
	// completion ACK uses the fresh post-retirement cut, which is the evidence
	// that the authority will validate when the intent reaches Complete.
	ackIntent.Evidence = SafeToStopEvidenceFromReference(terminalEvidence)
	ackIntent.Evidence.DrainAcknowledged = true
	ackIntent.Evidence.RetiredAcknowledged = true
	ackIntent.Evidence.CatalogControlMigrated = true
	// The incomplete intent above is the lost terminal-ACK state: GC remains
	// fenced until the acknowledgement and completion are committed together.
	if retained, readErr := authority.ReadFrontendDrainRecord(ctx, drainID); readErr != nil || retained.Lifecycle != FrontendDrainRetired {
		t.Fatalf("lost terminal ACK did not retain retired child: record=%+v err=%v", retained, readErr)
	}
	ackIntent.State = ScalingRunning
	ackIntent.Revision++
	ackIntent.DirectoryRevision = ackIntent.Revision
	if err := authority.PutScalingIntent(ctx, ackIntent, ackIntent.Revision-1); err != nil {
		t.Fatalf("persist incomplete terminal intent: %v", err)
	}
	if err := authority.ReserveFrontendDrainCapacity(ctx, replacement); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("retired child compacted before scaling intent completion: %v", err)
	}
	terminalEvidence, err = authority.ScanDecommissionedNodeReferences(ctx, terminal.NodeID, terminal.Incarnation)
	if err != nil || !terminalEvidence.ZeroAllReferences() {
		t.Fatalf("refresh terminal reference cut after running intent=%+v err=%v", terminalEvidence, err)
	}
	ackIntent.Evidence = SafeToStopEvidenceFromReference(terminalEvidence)
	ackIntent.Evidence.DrainAcknowledged = true
	ackIntent.Evidence.RetiredAcknowledged = true
	ackIntent.Evidence.CatalogControlMigrated = true
	ackIntent.State = ScalingComplete
	ackIntent.Revision++
	ackIntent.DirectoryRevision = ackIntent.Revision
	if err := authority.PutScalingIntent(ctx, ackIntent, ackIntent.Revision-1); err != nil {
		t.Fatalf("complete terminal frontend acknowledgement: %v", err)
	}
	terminalIntents, err := authority.ListScalingTerminalIntents(ctx)
	if err != nil || len(terminalIntents) != 1 || terminalIntents[0].ID != intent.ID ||
		terminalIntents[0].State != ScalingComplete || !terminalIntents[0].Evidence.SafeToStop() {
		t.Fatalf("completed terminal intent history=%+v err=%v", terminalIntents, err)
	}
	if err := authority.ReserveFrontendDrainCapacity(ctx, replacement); err != nil {
		t.Fatalf("compact terminal child for replacement reservation: %v", err)
	}
	_, records, err := authority.ReadFrontendDrainRecordCut(ctx)
	if err != nil || len(records) != 0 {
		t.Fatalf("compacted terminal records=%d err=%v", len(records), err)
	}
	_, grants, fences, _, err := authority.ReadFrontendDrainServiceCut(ctx)
	if err != nil || len(grants) != 0 || len(fences) != 0 {
		t.Fatalf("restart-visible compacted service material grants=%d fences=%d err=%v", len(grants), len(fences), err)
	}
}
