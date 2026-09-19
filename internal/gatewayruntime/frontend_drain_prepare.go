package gatewayruntime

import (
	"context"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// PrepareFrontendDrain is the local gateway side of Active -> Draining. It
// closes admission, captures the exact accepted-token cut, and persists that
// cut as Prepared before the node lifecycle CAS is attempted. A gateway
// process never manufactures a token or a zero-admission proof when the
// catalog has not supplied the corresponding internal fence.
func (runtime *Runtime) PrepareFrontendDrain(ctx context.Context, node gateway.NodeRecord) error {
	if runtime == nil || ctx == nil || !node.Valid() {
		return gateway.ErrInvalidScalingMetadata
	}
	if node.Roles&gateway.NodeRoleGateway == 0 {
		return nil
	}
	if runtime.authority == nil || runtime.frontend == nil || runtime.config.TLSProfile == nil ||
		runtime.config.Authorization == nil {
		return errors.Join(ErrScalingControllerBlocked, gateway.ErrScalingState)
	}
	profile := runtime.config.TLSProfile
	localIdentity := profile.LocalIdentity()
	localKey := replication.Digest(profile.LocalServiceKeyDigest())
	intent, err := runtime.decommissionIntentForNode(ctx, node)
	if err != nil {
		return err
	}
	// A controller may observe a gateway participant hosted by another
	// process. Ask that process to close its own listener over the mutually
	// authenticated gateway-control stream; the catalog CAS remains in this
	// authority and is still the only lifecycle transition.
	if node.Gateway.NodeID != localIdentity.Node || node.Gateway.ServiceKeyDigest != localKey {
		return runtime.prepareRemoteFrontendDrain(ctx, node, intent.ID)
	}

	drainID := gateway.NewFrontendDrainID(intent.ID, gateway.NodeReference{
		NodeID: node.NodeID, Incarnation: node.Incarnation,
	})
	if drainID == ([32]byte{}) {
		return gateway.ErrScalingIdentity
	}

	// A retry after a lost response must reuse the durable child exactly. It
	// still verifies the local identity before touching admission, so a copied
	// process cannot revive a different gateway session under the old intent.
	if reader, ok := any(runtime.authority).(gateway.FrontendDrainRecordReader); ok {
		if prior, readErr := reader.ReadFrontendDrainRecord(ctx, drainID); readErr == nil {
			if prior.PhysicalNode != node.NodeID || prior.PhysicalIncarnation != node.Incarnation ||
				prior.GatewayServiceID != node.Gateway.NodeID || prior.GatewayIncarnation != node.Gateway.Incarnation ||
				prior.GatewaySessionID != node.Gateway.SessionID ||
				prior.GatewaySessionRevision != node.Gateway.SessionRevision {
				return gateway.ErrScalingIdentity
			}
			if prior.Lifecycle == gateway.FrontendDrainPrepared {
				cut, cutErr := runtime.authority.ReadNodeDirectoryCut(ctx)
				if cutErr != nil {
					return cutErr
				}
				if !runtime.syncFrontendDrainFromDirectoryWithAdmission(cut.CurrentNodes(), cut.Revision, true) {
					return gateway.ErrScalingIdentity
				}
				// A crash can leave the durable child at Prepared while the
				// process-local admission object is fresh. The directory cut is
				// still Active in this case, so syncFrontendDrain... intentionally
				// binds identity without starting the fence. Reassert the local
				// admission boundary before replaying the exact Prepared proof;
				// BeginFrontendDrain is idempotent and does not mint or alter the
				// accepted-token set.
				ack := runtime.BeginFrontendDrain()
				if !ack.Identity.Valid() || ack.Identity.NodeID != prior.PhysicalNode ||
					ack.Identity.Incarnation != prior.PhysicalIncarnation ||
					ack.Identity.GatewayNodeID != prior.GatewayServiceID ||
					ack.Identity.GatewayIncarnation != prior.GatewayIncarnation ||
					ack.Identity.SessionID != prior.GatewaySessionID ||
					ack.Identity.SessionRevision != prior.GatewaySessionRevision ||
					!ack.NativeAdmissionDrained || ack.Revision == 0 {
					return gateway.ErrScalingIdentity
				}
				if err := runtime.ensurePreparedFrontendProof(ctx, prior); err != nil {
					return err
				}
				prior, err = runtime.refreshPreparedFrontendDrainRosterFence(ctx, node, prior)
				if err != nil {
					return err
				}
				return runtime.publishPreparedFrontendDrainRoster(ctx, node, prior)
			}
			if prior.Lifecycle == gateway.FrontendDrainEnforcing || prior.Lifecycle == gateway.FrontendDrainRetired {
				return gateway.ErrScalingState
			}
			return gateway.ErrScalingState
		} else if !errors.Is(readErr, gateway.ErrReplicatedCatalogMissing) {
			return readErr
		}
	}

	identity := FrontendDrainIdentity{
		NodeID: node.NodeID, Incarnation: node.Incarnation,
		GatewayNodeID: node.Gateway.NodeID, GatewayIncarnation: node.Gateway.Incarnation,
		GatewayServiceKeyDigest: node.Gateway.ServiceKeyDigest,
		SessionID:               node.Gateway.SessionID, SessionRevision: node.Gateway.SessionRevision,
		NodeRevision: node.Revision, CatalogGeneration: node.CatalogGeneration,
	}
	cut, snapshot, catalogHeadDigest, preflightFence, source, err := runtime.preflightFrontendDrainProof(ctx, node)
	if err != nil {
		return err
	}
	identity.DirectoryRevision = cut.Revision
	identity.CatalogGeneration = snapshot.Generation()
	runtime.frontend.mu.Lock()
	priorIdentity := runtime.frontend.identity
	if priorIdentity.GatewayNodeID != (rafttransport.NodeID{}) &&
		(priorIdentity.NodeID != identity.NodeID || priorIdentity.Incarnation != identity.Incarnation ||
			priorIdentity.GatewayNodeID != identity.GatewayNodeID ||
			priorIdentity.GatewayIncarnation != identity.GatewayIncarnation ||
			priorIdentity.GatewayServiceKeyDigest != identity.GatewayServiceKeyDigest ||
			priorIdentity.SessionID != identity.SessionID ||
			priorIdentity.SessionRevision != identity.SessionRevision) {
		runtime.frontend.mu.Unlock()
		return gateway.ErrScalingIdentity
	}
	runtime.frontend.identity = identity
	runtime.frontend.mu.Unlock()

	// Validate every component that can make the durable proof too large or
	// impossible to project before setting the admission bit. BeginFrontendDrain
	// is intentionally irreversible for this process lifetime: failing a later
	// serialization or catalog-fence read would otherwise strand an Active node
	// with its public listener closed. The captured snapshot is also reused
	// below, so scope coordinates cannot drift between preflight and the exact
	// accepted-token cut.
	if !frontendDrainWorstCaseRecordFits(node, intent.ID, drainID, localIdentity.TrustDomain,
		cut, catalogHeadDigest, snapshot, preflightFence.Fence) ||
		!frontendDrainWorstCaseProjectedCutFits(ctx, source, node, intent.ID, drainID,
			localIdentity.TrustDomain, profile, runtime.config.Authorization.Generation()) {
		return gateway.ErrScalingState
	}
	reserver, ok := any(runtime.authority).(gateway.FrontendDrainCapacityReserver)
	if !ok {
		return gateway.ErrScalingState
	}
	// Reserve the durable index slot while this node is still Active. A full
	// directory or competing prepare is therefore reported before Begin closes
	// public admission; the idempotent drainID reservation survives a lost reply.
	if err := reserver.ReserveFrontendDrainCapacity(ctx, drainID); err != nil {
		return err
	}
	ack := runtime.BeginFrontendDrain()
	if !ack.Identity.Valid() || !ack.AdmissionDrained || ack.Identity != identity || ack.Revision == 0 {
		return gateway.ErrScalingState
	}

	record := gateway.FrontendDrainRecord{
		IntentID: intent.ID, DecommissionIntentID: intent.ID, DrainID: drainID,
		TrustDomain:  localIdentity.TrustDomain,
		PhysicalNode: node.NodeID, PhysicalIncarnation: node.Incarnation,
		GatewayServiceID: node.Gateway.NodeID, GatewayIncarnation: node.Gateway.Incarnation,
		PeerKeyDigest:            node.Gateway.ServiceKeyDigest,
		GatewayServiceKeyDigest:  node.Gateway.ServiceKeyDigest,
		GatewayIdentityServiceID: node.Gateway.ServiceID,
		GatewaySessionID:         node.Gateway.SessionID, GatewaySessionRevision: node.Gateway.SessionRevision,
		NodeRevision: node.Revision, AdmissionEpoch: ack.Revision,
		AdmissionClosedProofDigest: replication.Digest(frontendDrainDigest(ack)),
		ReceiverDirectoryRevision:  cut.Revision, ReceiverDirectoryDigest: cut.Digest,
		ReceiverCatalogGeneration: cut.CatalogGeneration, ReceiverCatalogHeadDigest: catalogHeadDigest,
		Lifecycle: gateway.FrontendDrainPrepared, Revision: 1,
	}
	if record.AdmissionClosedProofDigest == (replication.Digest{}) {
		return gateway.ErrScalingState
	}
	if len(frontendAcceptedTokens(runtime.frontend)) != 0 {
		grant, ok := runtime.buildFrontendContinuationGrant(node)
		if !ok {
			return gateway.ErrScalingState
		}
		grant.DrainID = drainID
		grant.State = serviceauthz.ContinuationGrantPrepared
		grant.GrantDigest = grant.Digest()
		if !grant.Valid() {
			return gateway.ErrScalingState
		}
		record.ContinuationGrant = &grant
		// Keep the durable catalog drain fence alongside the token proof. The
		// continuation grant digest deliberately excludes resource scopes; the
		// projection later derives its admission marker from this committed fence.
		record.DrainFence = preflightFence.Fence
	} else {
		fence := preflightFence
		fence.DrainID = drainID
		fence.Revision = node.Revision
		if !fence.Valid() {
			return gateway.ErrScalingState
		}
		record.DrainFence = fence.Fence
	}
	if !record.Valid() || !record.ValidForNode(node) {
		return gateway.ErrScalingIdentity
	}
	writer, ok := any(runtime.authority).(gateway.FrontendDrainRecordWriter)
	if !ok {
		return gateway.ErrScalingState
	}
	if err = writer.PutFrontendDrainRecord(ctx, record, 0); err != nil {
		return err
	}
	if err = runtime.publishPreparedFrontendProof(ctx, record); err != nil {
		return err
	}
	return runtime.publishPreparedFrontendDrainRoster(ctx, node, record)
}

// frontendDrainWorstCaseRecordFits proves the maximum admission-token shape
// allowed by admitNative/admitPG is serializable before BeginFrontendDrain.
// Tokens are fixed-width in the child encoding, so the actual captured proof
// can only be smaller; scopes are taken from the exact preflight snapshot.
func frontendDrainWorstCaseRecordFits(
	node gateway.NodeRecord, intentID, drainID [32]byte, trust rafttransport.TrustDomain,
	cut gateway.NodeDirectoryCut, catalogHead replication.Digest, snapshot *gateway.Snapshot,
	drainFence serviceauthz.ServiceFence,
) bool {
	if !node.Valid() || intentID == ([32]byte{}) || drainID == ([32]byte{}) ||
		trust.ClusterID == ([16]byte{}) || trust.ClusterIncarnation == ([16]byte{}) ||
		cut.Revision == 0 || catalogHead == (replication.Digest{}) || snapshot == nil ||
		!drainFence.Valid() || drainFence.SessionID != node.Gateway.SessionID ||
		drainFence.SessionRevision != node.Gateway.SessionRevision {
		return false
	}
	if !frontendDrainScopesFitRecord(continuationScopesForSnapshot(snapshot)) {
		return false
	}
	grant, ok := frontendDrainWorstCaseGrant(node, drainID, trust)
	if !ok {
		return false
	}
	record := gateway.FrontendDrainRecord{
		IntentID: intentID, DecommissionIntentID: intentID, DrainID: drainID, TrustDomain: trust,
		PhysicalNode: node.NodeID, PhysicalIncarnation: node.Incarnation,
		GatewayServiceID: node.Gateway.NodeID, GatewayIncarnation: node.Gateway.Incarnation,
		PeerKeyDigest: node.Gateway.ServiceKeyDigest, GatewayServiceKeyDigest: node.Gateway.ServiceKeyDigest,
		GatewayIdentityServiceID: node.Gateway.ServiceID, GatewaySessionID: node.Gateway.SessionID,
		GatewaySessionRevision: node.Gateway.SessionRevision, NodeRevision: node.Revision,
		AdmissionEpoch: 1, AdmissionClosedProofDigest: replication.Digest{1},
		ReceiverDirectoryRevision: cut.Revision, ReceiverDirectoryDigest: cut.Digest,
		ReceiverCatalogGeneration: cut.CatalogGeneration, ReceiverCatalogHeadDigest: catalogHead,
		DrainFence:        drainFence,
		ContinuationGrant: &grant, Lifecycle: gateway.FrontendDrainPrepared, Revision: 1,
	}
	return record.Valid() && gateway.FrontendDrainRecordFitsStorage(record)
}

func frontendDrainWorstCaseGrant(
	node gateway.NodeRecord, drainID [32]byte, trust rafttransport.TrustDomain,
) (serviceauthz.CommittedFrontendContinuationGrant, bool) {
	if !node.Valid() || drainID == ([32]byte{}) ||
		trust.ClusterID == ([16]byte{}) || trust.ClusterIncarnation == ([16]byte{}) {
		return serviceauthz.CommittedFrontendContinuationGrant{}, false
	}
	tokens := make([]serviceauthz.FrontendConnToken, maxFrontendContinuationTokens)
	protocols := make([]serviceauthz.FrontendContinuationScope, len(tokens))
	for index := range tokens {
		// Big-endian ordinal makes the fixed-width tokens strictly canonical.
		ordinal := index + 1
		tokens[index][30], tokens[index][31] = byte(ordinal>>8), byte(ordinal)
		protocols[index] = serviceauthz.FrontendScopeNative
	}
	grant := serviceauthz.CommittedFrontendContinuationGrant{
		TrustDomain: trust, PhysicalNode: node.NodeID, PhysicalIncarnation: node.Incarnation,
		PeerKeyDigest: [32]byte(node.Gateway.ServiceKeyDigest), GatewayServiceID: node.Gateway.NodeID,
		GatewaySessionID: node.Gateway.SessionID, GatewaySessionRevision: node.Gateway.SessionRevision,
		DrainID: drainID, AdmissionEpoch: 1, AdmissionClosedProofDigest: [32]byte{1},
		Revision: node.Revision, State: serviceauthz.ContinuationGrantPrepared,
		AcceptedConnectionTokens: tokens, AcceptedConnectionProtocols: protocols,
	}
	grant.GrantDigest = grant.Digest()
	return grant, grant.Valid()
}

// frontendDrainWorstCaseProjectedCutFits checks the independently bounded
// source-cut wire before BeginFrontendDrain. The child record and its compact
// index are checked separately; this second check includes the complete
// service-directory projection that every physical receiver must decode.
func frontendDrainWorstCaseProjectedCutFits(
	ctx context.Context, source gateway.FrontendDrainRuntimeCut, node gateway.NodeRecord,
	intentID, drainID [32]byte, trust rafttransport.TrustDomain,
	profile *rafttransport.PeerTLS, policyGeneration uint64,
) bool {
	if ctx == nil || profile == nil || policyGeneration == 0 || !node.Valid() ||
		intentID == ([32]byte{}) || drainID == ([32]byte{}) {
		return false
	}
	grant, ok := frontendDrainWorstCaseGrant(node, drainID, trust)
	if !ok {
		return false
	}
	projected := source
	projected.ContinuationGrants = append(slices.Clone(source.ContinuationGrants), grant)
	serviceCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
		ctx, projected, profile, policyGeneration)
	if err != nil || !serviceCut.Valid() {
		return false
	}
	cut := frontenddrain.PreparedAckCut{
		DirectoryRevision:        source.Nodes.Revision,
		DirectoryDigest:          source.Nodes.Digest,
		CatalogGeneration:        source.Nodes.CatalogGeneration,
		CatalogHeadDigest:        source.CatalogHeadDigest,
		ServiceDirectoryRevision: serviceCut.Revision,
		ServiceDirectory:         serviceCut,
		SourceRoster:             frontendDrainPreparedAckSourceRoster(source),
	}
	request := frontenddrain.PreparedAckRequest{
		Nonce:                    [16]byte{1},
		DrainID:                  drainID,
		GrantDigest:              grant.GrantDigest,
		SourcePrincipal:          node.Gateway.NodeID,
		SourcePrincipalKeyDigest: [32]byte(node.Gateway.ServiceKeyDigest),
		ReceiverNode:             node.NodeID,
		ReceiverIncarnation:      node.Incarnation,
		ReceiverServiceKeyDigest: [32]byte(node.ServiceKeyDigest),
		ReceiverNodeRevision:     node.Revision,
		SourceCut:                cut,
	}
	return frontendDrainProjectedCutFitsStorage(request)
}

func frontendDrainProjectedCutFitsStorage(request frontenddrain.PreparedAckRequest) bool {
	raw, err := request.Marshal()
	return err == nil && len(raw) <= frontenddrain.MaxPreparedAckFrameBytes
}

// preflightFrontendDrainProof performs every fallible proof construction that
// is independent of the accepted-token cut. It must run before BeginFrontendDrain:
// a rejected snapshot, an oversized scope set, or a missing catalog fence must
// leave the public listener admitting the existing Active session.
func (runtime *Runtime) preflightFrontendDrainProof(
	ctx context.Context, node gateway.NodeRecord,
) (gateway.NodeDirectoryCut, *gateway.Snapshot, replication.Digest,
	serviceauthz.CommittedFrontendDrainFence, gateway.FrontendDrainRuntimeCut, error) {
	if runtime == nil || ctx == nil || runtime.authority == nil || !node.Valid() {
		return gateway.NodeDirectoryCut{}, nil, replication.Digest{}, serviceauthz.CommittedFrontendDrainFence{}, gateway.FrontendDrainRuntimeCut{}, gateway.ErrScalingState
	}
	reader, ok := any(runtime.authority).(gateway.FrontendDrainRuntimeCutReader)
	if !ok {
		return gateway.NodeDirectoryCut{}, nil, replication.Digest{}, serviceauthz.CommittedFrontendDrainFence{}, gateway.FrontendDrainRuntimeCut{}, gateway.ErrScalingState
	}
	source, err := reader.ReadFrontendDrainRuntimeCut(ctx)
	if err != nil {
		return gateway.NodeDirectoryCut{}, nil, replication.Digest{}, serviceauthz.CommittedFrontendDrainFence{}, gateway.FrontendDrainRuntimeCut{}, err
	}
	if !source.Nodes.Valid() || source.Catalog == nil || source.CatalogHeadDigest == (replication.Digest{}) ||
		source.Catalog.Generation() != source.Nodes.CatalogGeneration {
		return gateway.NodeDirectoryCut{}, nil, replication.Digest{}, serviceauthz.CommittedFrontendDrainFence{}, gateway.FrontendDrainRuntimeCut{}, gateway.ErrScalingRevision
	}
	var sourceNode gateway.NodeRecord
	found := false
	for _, candidate := range source.Nodes.Nodes {
		if candidate.NodeID != node.NodeID || candidate.Incarnation != node.Incarnation {
			continue
		}
		if candidate != node {
			return gateway.NodeDirectoryCut{}, nil, replication.Digest{}, serviceauthz.CommittedFrontendDrainFence{}, gateway.FrontendDrainRuntimeCut{}, gateway.ErrScalingRevision
		}
		sourceNode, found = candidate, true
		break
	}
	if !found || !sourceNode.Valid() || !frontendDrainScopesFitRecord(continuationScopesForSnapshot(source.Catalog)) {
		return gateway.NodeDirectoryCut{}, nil, replication.Digest{}, serviceauthz.CommittedFrontendDrainFence{}, gateway.FrontendDrainRuntimeCut{}, gateway.ErrScalingState
	}
	fence, ok := runtime.buildFrontendDrainFence(node, source.CatalogFences)
	if !ok {
		return gateway.NodeDirectoryCut{}, nil, replication.Digest{}, serviceauthz.CommittedFrontendDrainFence{}, gateway.FrontendDrainRuntimeCut{}, gateway.ErrScalingState
	}
	return source.Nodes, source.Catalog, source.CatalogHeadDigest, fence, source, nil
}

func frontendDrainScopesFitRecord(scopes []serviceauthz.FrontendContinuationScopeRecord) bool {
	return len(scopes) != 0 && len(scopes) <= maxFrontendContinuationScopes
}

// refreshPreparedFrontendDrainRosterFence updates only the captured receiver
// roster after a Prepared retry. It is the recovery path for a legitimate
// directory/head change between the first ACK round and EnforceFrontendDrain:
// the immutable accepted-token proof remains untouched while the next round
// acknowledges every receiver in the newer cut.
func (runtime *Runtime) refreshPreparedFrontendDrainRosterFence(
	ctx context.Context, node gateway.NodeRecord, record gateway.FrontendDrainRecord,
) (gateway.FrontendDrainRecord, error) {
	if runtime == nil || runtime.authority == nil || ctx == nil || record.Lifecycle != gateway.FrontendDrainPrepared {
		return gateway.FrontendDrainRecord{}, gateway.ErrScalingState
	}
	reader, ok := any(runtime.authority).(gateway.FrontendDrainRuntimeCutReader)
	if !ok {
		return gateway.FrontendDrainRecord{}, gateway.ErrScalingState
	}
	source, err := reader.ReadFrontendDrainRuntimeCut(ctx)
	if err != nil || !source.Nodes.Valid() || source.Catalog == nil || source.CatalogHeadDigest == (replication.Digest{}) ||
		source.Catalog.Generation() != source.Nodes.CatalogGeneration {
		if err != nil {
			return gateway.FrontendDrainRecord{}, err
		}
		return gateway.FrontendDrainRecord{}, gateway.ErrScalingRevision
	}
	cut, headDigest := source.Nodes, source.CatalogHeadDigest
	if record.ReceiverDirectoryRevision == cut.Revision && record.ReceiverDirectoryDigest == cut.Digest &&
		record.ReceiverCatalogGeneration == cut.CatalogGeneration && record.ReceiverCatalogHeadDigest == headDigest {
		return record, nil
	}
	if record.Revision == ^uint64(0) {
		return gateway.FrontendDrainRecord{}, gateway.ErrScalingRevision
	}
	next := record
	next.ReceiverDirectoryRevision, next.ReceiverDirectoryDigest = cut.Revision, cut.Digest
	next.ReceiverCatalogGeneration, next.ReceiverCatalogHeadDigest = cut.CatalogGeneration, headDigest
	next.Revision++
	writer, ok := any(runtime.authority).(gateway.FrontendDrainRecordWriter)
	if !ok {
		return gateway.FrontendDrainRecord{}, gateway.ErrScalingState
	}
	if err := writer.PutFrontendDrainRecord(ctx, next, record.Revision); err != nil {
		return gateway.FrontendDrainRecord{}, err
	}
	return next, nil
}

// publishPreparedFrontendDrainRoster is the local half of the receiver
// barrier. It is idempotent across a lost RPC response: every receiver is
// asked to apply the same digest-bound Prepared cut again before this frontend
// may attach a continuation envelope to an already accepted socket.
func (runtime *Runtime) publishPreparedFrontendDrainRoster(
	ctx context.Context, node gateway.NodeRecord, record gateway.FrontendDrainRecord,
) error {
	if err := runtime.acknowledgePreparedFrontendDrainRoster(ctx, node, record); err != nil {
		return err
	}
	if record.ContinuationGrant == nil {
		return nil
	}
	for _, protocol := range record.ContinuationGrant.AcceptedConnectionProtocols {
		if !runtime.InstallFrontendContinuationGrant(record.ContinuationGrant.GrantDigest, protocol) {
			return gateway.ErrScalingState
		}
	}
	return nil
}

// RefreshFrontendDrainIdentity updates the in-memory acknowledgement's node
// revision after the catalog commits Active -> Draining. The physical and
// gateway session identities remain immutable; only the CAS-bound node and
// directory revisions move forward.
func (runtime *Runtime) RefreshFrontendDrainIdentity(ctx context.Context, node gateway.NodeRecord) error {
	if runtime == nil || ctx == nil || !node.Valid() || runtime.authority == nil {
		return gateway.ErrScalingState
	}
	if runtime.config.TLSProfile != nil {
		local := runtime.config.TLSProfile.LocalIdentity()
		localKey := replication.Digest(runtime.config.TLSProfile.LocalServiceKeyDigest())
		if node.Gateway.NodeID != local.Node || node.Gateway.ServiceKeyDigest != localKey {
			// A controller can prepare a remote gateway over its authenticated
			// participant stream. That owner already refreshed its local
			// admission identity; the controller must not bind its own frontend
			// to the remote physical node as a side effect of the catalog CAS.
			return nil
		}
	}
	cut, err := runtime.authority.ReadNodeDirectoryCut(ctx)
	if err != nil {
		return err
	}
	for _, candidate := range cut.CurrentNodes() {
		if candidate.NodeID == node.NodeID && candidate.Incarnation == node.Incarnation {
			if candidate.Revision != node.Revision || candidate.Lifecycle != node.Lifecycle ||
				candidate.Gateway != node.Gateway || candidate.ServiceKeyDigest != node.ServiceKeyDigest {
				return gateway.ErrScalingRevision
			}
			if !runtime.syncFrontendDrainFromDirectoryWithAdmission(cut.CurrentNodes(), cut.Revision, false) {
				return gateway.ErrScalingIdentity
			}
			return nil
		}
	}
	return gateway.ErrScalingNodeMissing
}

func (runtime *Runtime) decommissionIntentForNode(
	ctx context.Context, node gateway.NodeRecord,
) (gateway.ScalingIntent, error) {
	intents, err := runtime.authority.ListScalingIntents(ctx)
	if err != nil {
		return gateway.ScalingIntent{}, err
	}
	var match gateway.ScalingIntent
	found := false
	for _, intent := range intents {
		if intent.Request.Kind != gateway.ScalingDecommission ||
			(intent.State != gateway.ScalingReserved && intent.State != gateway.ScalingRunning) ||
			intent.Request.Drain.NodeID != node.NodeID || intent.Request.Drain.Incarnation != node.Incarnation {
			continue
		}
		if found && intent.ID != match.ID {
			return gateway.ScalingIntent{}, gateway.ErrScalingState
		}
		match, found = intent, true
	}
	if !found {
		return gateway.ScalingIntent{}, gateway.ErrScalingIntentMissing
	}
	return match, nil
}

func frontendAcceptedTokens(frontend *frontendAdmission) []serviceauthz.FrontendConnToken {
	if frontend == nil {
		return nil
	}
	frontend.mu.Lock()
	accepted := make([]serviceauthz.FrontendConnToken, 0, len(frontend.tokens))
	for token, state := range frontend.tokens {
		if state.eligible {
			accepted = append(accepted, token)
		}
	}
	frontend.mu.Unlock()
	return accepted
}

func (runtime *Runtime) ensurePreparedFrontendProof(
	ctx context.Context, record gateway.FrontendDrainRecord,
) error {
	return runtime.publishPreparedFrontendProof(ctx, record)
}

func (runtime *Runtime) publishPreparedFrontendProof(
	ctx context.Context, record gateway.FrontendDrainRecord,
) error {
	if runtime == nil || runtime.authority == nil || ctx == nil || !record.Valid() {
		return gateway.ErrScalingState
	}
	reader, ok := any(runtime.authority).(gateway.FrontendDrainRecordReader)
	if !ok {
		return gateway.ErrScalingState
	}
	stored, err := reader.ReadFrontendDrainRecord(ctx, record.DrainID)
	if err != nil || stored.Lifecycle != gateway.FrontendDrainPrepared ||
		stored.DrainID != record.DrainID || stored.IntentID != record.IntentID ||
		stored.AdmissionClosedProofDigest != record.AdmissionClosedProofDigest ||
		stored.GatewayServiceID != record.GatewayServiceID || stored.GatewaySessionID != record.GatewaySessionID ||
		stored.GatewaySessionRevision != record.GatewaySessionRevision {
		return gateway.ErrReplicatedCatalogConflict
	}
	return nil
}

var _ ScalingFrontendDrainPreparer = (*Runtime)(nil)
