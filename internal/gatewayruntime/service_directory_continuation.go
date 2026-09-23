package gatewayruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"slices"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// publishFrontendContinuationGrant performs the post-CAS half of a frontend
// drain.  Admission has already been closed by syncFrontendDrainFromDirectory;
// this call captures the exact still-open sockets and commits their immutable
// grant before applyLiveControlDirectory installs the enforcing gate.  A
// concurrent runtime can safely race this call because the catalog row is
// updated with a digest-fenced CAS and the next directory tick re-reads it.
func (runtime *Runtime) publishFrontendContinuationGrant(
	ctx context.Context, cut gateway.ReplicatedControlDirectorySnapshot,
) error {
	if runtime == nil || ctx == nil || !cut.Valid() || runtime.frontend == nil {
		return nil
	}
	reader := runtime.config.ControlDirectory
	if reader == nil {
		reader = runtime.authority
	}
	drains, ok := reader.(gateway.FrontendDrainRecordReader)
	if !ok {
		return gateway.ErrScalingState
	}
	profile := runtime.config.TLSProfile
	if profile == nil {
		return gateway.ErrScalingState
	}
	local, key := profile.LocalIdentity(), replication.Digest(profile.LocalServiceKeyDigest())
	for _, node := range cut.Nodes {
		if node.Lifecycle != gateway.NodeDraining || node.Gateway.NodeID != local.Node || node.Gateway.ServiceKeyDigest != key {
			continue
		}
		_, records, err := drains.ReadFrontendDrainRecordCut(ctx)
		if err != nil {
			return err
		}
		for _, record := range records {
			if record.PhysicalNode == node.NodeID && record.PhysicalIncarnation == node.Incarnation &&
				record.GatewayServiceID == node.Gateway.NodeID && record.GatewaySessionID == node.Gateway.SessionID &&
				record.GatewaySessionRevision == node.Gateway.SessionRevision &&
				(record.Lifecycle == gateway.FrontendDrainEnforcing || record.Lifecycle == gateway.FrontendDrainRetired) {
				return nil
			}
		}
		return gateway.ErrScalingState
	}
	return nil
}

func (runtime *Runtime) buildFrontendDrainFence(
	record gateway.NodeRecord, catalogFences []serviceauthz.ServiceFence,
) (serviceauthz.CommittedFrontendDrainFence, bool) {
	if runtime == nil || runtime.config.TLSProfile == nil {
		return serviceauthz.CommittedFrontendDrainFence{}, false
	}
	identity := runtime.config.TLSProfile.LocalIdentity()
	for _, candidate := range catalogFences {
		if candidate.Action != serviceauthz.ServiceActionGatewayCatalogRead ||
			candidate.Operation != serviceauthz.ServiceOperationCatalogRead ||
			candidate.Group == (raftmember.GroupKey{}) || candidate.IntentID == ([32]byte{}) ||
			candidate.FenceDigest == ([32]byte{}) {
			continue
		}
		candidate.SessionID = record.Gateway.SessionID
		candidate.SessionRevision = record.Gateway.SessionRevision
		fence := serviceauthz.CommittedFrontendDrainFence{
			TrustDomain: identity.TrustDomain, PhysicalNode: record.NodeID, PhysicalIncarnation: record.Incarnation,
			PeerKeyDigest: [32]byte(record.Gateway.ServiceKeyDigest), GatewayServiceID: record.Gateway.NodeID,
			GatewaySessionID: record.Gateway.SessionID, GatewaySessionRevision: record.Gateway.SessionRevision,
			DrainID: frontendDrainID(FrontendDrainIdentity{NodeID: record.NodeID, Incarnation: record.Incarnation,
				GatewayNodeID: record.Gateway.NodeID, GatewayIncarnation: record.Gateway.Incarnation,
				SessionID: record.Gateway.SessionID, SessionRevision: record.Gateway.SessionRevision}),
			Revision: record.Revision, Fence: candidate,
		}
		if fence.Valid() {
			return fence, true
		}
	}
	return serviceauthz.CommittedFrontendDrainFence{}, false
}

func (runtime *Runtime) buildFrontendContinuationGrant(
	record gateway.NodeRecord,
) (serviceauthz.CommittedFrontendContinuationGrant, bool) {
	if runtime == nil || runtime.frontend == nil || runtime.config.TLSProfile == nil {
		return serviceauthz.CommittedFrontendContinuationGrant{}, false
	}
	runtime.frontend.mu.Lock()
	identity := runtime.frontend.identity
	if !runtime.frontend.draining {
		runtime.frontend.mu.Unlock()
		return serviceauthz.CommittedFrontendContinuationGrant{}, false
	}
	type tokenScope struct {
		token serviceauthz.FrontendConnToken
		scope serviceauthz.FrontendContinuationScope
	}
	accepted := make([]tokenScope, 0, len(runtime.frontend.tokens))
	for token, state := range runtime.frontend.tokens {
		if state.eligible {
			accepted = append(accepted, tokenScope{token: token, scope: state.scope})
		}
	}
	runtime.frontend.mu.Unlock()
	if identity.NodeID != record.NodeID || identity.Incarnation != record.Incarnation ||
		identity.GatewayNodeID != record.Gateway.NodeID || identity.GatewayIncarnation != record.Gateway.Incarnation ||
		identity.GatewayServiceKeyDigest != record.Gateway.ServiceKeyDigest || identity.SessionID != record.Gateway.SessionID ||
		identity.SessionRevision != record.Gateway.SessionRevision || identity.NodeRevision != record.Revision ||
		identity.CatalogGeneration != record.CatalogGeneration || identity.DirectoryRevision == 0 || len(accepted) == 0 {
		return serviceauthz.CommittedFrontendContinuationGrant{}, false
	}
	slices.SortFunc(accepted, func(left, right tokenScope) int {
		return bytes.Compare(left.token[:], right.token[:])
	})
	for index := 1; index < len(accepted); index++ {
		if accepted[index-1].token == accepted[index].token {
			return serviceauthz.CommittedFrontendContinuationGrant{}, false
		}
	}
	ack := runtime.FrontendDrainStatus()
	drainID := frontendDrainID(identity)
	grant := serviceauthz.CommittedFrontendContinuationGrant{
		TrustDomain:                 runtime.config.TLSProfile.LocalIdentity().TrustDomain,
		PhysicalNode:                record.NodeID,
		PhysicalIncarnation:         record.Incarnation,
		PeerKeyDigest:               [32]byte(record.Gateway.ServiceKeyDigest),
		GatewayServiceID:            record.Gateway.NodeID,
		GatewaySessionID:            record.Gateway.SessionID,
		GatewaySessionRevision:      record.Gateway.SessionRevision,
		DrainID:                     drainID,
		AdmissionEpoch:              ack.Revision,
		AdmissionClosedProofDigest:  frontendDrainDigest(ack),
		Revision:                    record.Revision,
		State:                       serviceauthz.ContinuationGrantEnforcing,
		AcceptedConnectionTokens:    make([]serviceauthz.FrontendConnToken, len(accepted)),
		AcceptedConnectionProtocols: make([]serviceauthz.FrontendContinuationScope, len(accepted)),
	}
	for index, item := range accepted {
		grant.AcceptedConnectionTokens[index] = item.token
		grant.AcceptedConnectionProtocols[index] = item.scope
	}
	grant.GrantDigest = grant.Digest()
	return grant, grant.Valid()
}

func frontendDrainID(identity FrontendDrainIdentity) (id [32]byte) {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/frontend-drain/id/v1\x00"))
	_, _ = hash.Write(identity.NodeID[:])
	var scalar [8]byte
	binary.BigEndian.PutUint64(scalar[:], identity.Incarnation)
	_, _ = hash.Write(scalar[:])
	_, _ = hash.Write(identity.GatewayNodeID[:])
	binary.BigEndian.PutUint64(scalar[:], identity.GatewayIncarnation)
	_, _ = hash.Write(scalar[:])
	_, _ = hash.Write(identity.SessionID[:])
	binary.BigEndian.PutUint64(scalar[:], identity.SessionRevision)
	_, _ = hash.Write(scalar[:])
	return sha256.Sum256(hash.Sum(nil))
}

func continuationScopesForSnapshot(
	snapshot *gateway.Snapshot,
) []serviceauthz.FrontendContinuationScopeRecord {
	if snapshot == nil {
		return nil
	}
	descriptors := snapshot.ReplicatedShardDescriptors()
	result := make([]serviceauthz.FrontendContinuationScopeRecord, 0, len(descriptors)*4)
	for _, descriptor := range descriptors {
		for _, protocol := range []serviceauthz.FrontendContinuationScope{
			serviceauthz.FrontendScopeNative, serviceauthz.FrontendScopePostgreSQL,
		} {
			appendScope := func(relation [16]byte) {
				result = append(result,
					serviceauthz.FrontendContinuationScopeRecord{Protocol: protocol,
						Action: serviceauthz.FrontendActionForwardedData, Capability: serviceauthz.CapabilityDataRead,
						Operation: serviceauthz.ServiceOperationForwardedRead, Group: descriptor.Group, Relation: relation},
					serviceauthz.FrontendContinuationScopeRecord{Protocol: protocol,
						Action: serviceauthz.FrontendActionForwardedData, Capability: serviceauthz.CapabilityDataWrite,
						Operation: serviceauthz.ServiceOperationForwardedWrite, Group: descriptor.Group, Relation: relation},
				)
			}
			appendScope([16]byte{})
			for _, profile := range snapshot.ReplicatedTableProfiles() {
				placement, placed := snapshot.Placement(profile.Table)
				if !placed || placement.Distribution != descriptor.Distribution {
					continue
				}
				var relation [16]byte
				binary.BigEndian.PutUint16(relation[len(relation)-2:], uint16(profile.Relation))
				appendScope(relation)
			}
		}
	}
	// Every valid catalog has at least one physical record, but duplicate
	// groups are possible in a node cut. Canonicalize the scope set before the
	// grant digest is computed.
	slices.SortFunc(result, serviceauthz.CompareContinuationScopes)
	result = slices.CompactFunc(result, func(left, right serviceauthz.FrontendContinuationScopeRecord) bool {
		return serviceauthz.CompareContinuationScopes(left, right) == 0
	})
	return result
}
