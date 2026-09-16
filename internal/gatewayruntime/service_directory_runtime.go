package gatewayruntime

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// serviceDirectoryCutReader is intentionally optional. An adapter that owns
// the complete service cut may return it directly. The production catalog
// authority uses FrontendDrainRuntimeCutReader below so node, catalog, and
// drain rows are read from one verified source epoch.
type serviceDirectoryCutReader interface {
	ReadServiceDirectoryCut(context.Context) (serviceauthz.ServiceDirectoryCut, error)
}

type frontendDrainRuntimeCutReader interface {
	ReadFrontendDrainRuntimeCut(context.Context) (gateway.FrontendDrainRuntimeCut, error)
}

// FrontendDrainRuntimeCutSource is the authenticated physical recovery source
// used by an embedded gateway whose local catalog owner is a follower. The
// source returns one authority-backed PreparedAckCut; the runtime installs
// that proof before using the local semantic transport and then re-reads its
// full runtime cut from the catalog authority. A source cannot supply rows,
// endpoint identities, or a static policy shortcut.
type FrontendDrainRuntimeCutSource interface {
	ReadLatestFrontendDrainCut(context.Context) (frontenddrain.PreparedAckCut, error)
}

// serviceDirectoryCompleteCutTestReader is intentionally a single-call test
// seam. It exists only for focused projection tests whose fixtures do not own
// a real catalog Snapshot. Production readers must implement
// FrontendDrainRuntimeCutReader or serviceDirectoryCutReader; no production
// path may assemble grants, fences, and scopes from independent row reads.
type serviceDirectoryCompleteCutTestReader interface {
	ReadCompleteServiceDirectoryCut(context.Context) (serviceDirectoryCompleteCut, error)
}

type serviceDirectoryCompleteCut struct {
	Revision           uint64
	CatalogGeneration  uint64
	ContinuationGrants []serviceauthz.CommittedFrontendContinuationGrant
	DrainFences        []serviceauthz.CommittedFrontendDrainFence
	DrainRecords       []gateway.FrontendDrainRecord
	ForwardedScopes    []serviceauthz.FrontendContinuationScopeRecord
	ScopesGeneration   uint64
	CatalogFences      []serviceauthz.ServiceFence
}

// frontendDrainRuntimeCutProjectionReader lets callers project one already
// verified runtime cut without refreshing any of its component rows.  The
// embedded DirectoryReader is intentionally unused by the projection path;
// runtimeServiceDirectoryCut only needs its complete-cut reader capability
// after the caller has supplied the matching snapshot.
type frontendDrainRuntimeCutProjectionReader struct {
	gateway.DirectoryReader
	cut gateway.FrontendDrainRuntimeCut
}

func (reader frontendDrainRuntimeCutProjectionReader) ReadFrontendDrainRuntimeCut(
	context.Context,
) (gateway.FrontendDrainRuntimeCut, error) {
	return reader.cut, nil
}

// runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut performs the same
// canonical projection as runtimeServiceDirectoryCut while retaining the
// exact source epoch returned by ReadFrontendDrainRuntimeCut.  It is used by
// authenticated source-cut RPCs so a response cannot combine a fresh service
// directory with a different node/catalog cut.
func runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
	ctx context.Context,
	source gateway.FrontendDrainRuntimeCut,
	profile *rafttransport.PeerTLS,
	policyGeneration uint64,
) (serviceauthz.ServiceDirectoryCut, error) {
	if ctx == nil || profile == nil || policyGeneration == 0 ||
		!source.Nodes.Valid() || source.Catalog == nil || source.CatalogHeadDigest == (replication.Digest{}) ||
		source.Catalog.Generation() != source.Nodes.CatalogGeneration || source.ServiceDirectoryRevision == 0 {
		return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
	}
	snapshot := gateway.ReplicatedControlDirectorySnapshot{
		Revision: source.Nodes.Revision, CatalogGeneration: source.Nodes.CatalogGeneration,
		Nodes: source.Nodes.CurrentNodes(),
	}
	if !snapshot.Valid() {
		return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
	}
	return runtimeServiceDirectoryCut(ctx, frontendDrainRuntimeCutProjectionReader{cut: source},
		snapshot, profile, policyGeneration)
}

// readCanonicalFrontendDrainRuntimeCut reads the complete authority cut used
// by a control-directory publication and verifies that its live node view is
// the same view that selected the transport endpoints.  The full cut is kept
// in the caller so the native receiver can advance its directory/catalog
// floor together with the projected service gate.
func readCanonicalFrontendDrainRuntimeCut(
	ctx context.Context, reader gateway.DirectoryReader,
	expected gateway.ReplicatedControlDirectorySnapshot,
) (gateway.FrontendDrainRuntimeCut, error) {
	if ctx == nil || reader == nil || !expected.Valid() {
		return gateway.FrontendDrainRuntimeCut{}, errGatewayControlDirectory
	}
	coherent, ok := reader.(frontendDrainRuntimeCutReader)
	if !ok {
		return gateway.FrontendDrainRuntimeCut{}, fmt.Errorf("%w: canonical frontend drain runtime cut reader is required", errGatewayControlDirectory)
	}
	source, err := coherent.ReadFrontendDrainRuntimeCut(ctx)
	if err != nil {
		return gateway.FrontendDrainRuntimeCut{}, err
	}
	if !source.Nodes.Valid() || source.Catalog == nil ||
		source.CatalogHeadDigest == (replication.Digest{}) ||
		source.Catalog.Generation() != source.Nodes.CatalogGeneration ||
		source.Nodes.Revision != expected.Revision ||
		source.Nodes.CatalogGeneration != expected.CatalogGeneration ||
		!reflect.DeepEqual(source.Nodes.CurrentNodes(), expected.Nodes) {
		return gateway.FrontendDrainRuntimeCut{}, errGatewayControlDirectory
	}
	return source, nil
}

// readCanonicalFrontendDrainRuntimeCutFromRows performs the same endpoint
// roster cross-check for a local catalog-owner row source. The local reader
// owns the serving fence and canonical cut; this helper only verifies that the
// resulting live node view is the one that selected the control endpoints.
func readCanonicalFrontendDrainRuntimeCutFromRows(
	ctx context.Context, reader gateway.FrontendDrainRuntimeCutRowReader,
	expected gateway.ReplicatedControlDirectorySnapshot,
) (gateway.FrontendDrainRuntimeCut, error) {
	if ctx == nil || reader == nil || !expected.Valid() {
		return gateway.FrontendDrainRuntimeCut{}, errGatewayControlDirectory
	}
	source, err := gateway.ReadFrontendDrainRuntimeCutFromRows(ctx, reader)
	if err != nil {
		return gateway.FrontendDrainRuntimeCut{}, err
	}
	if !source.Nodes.Valid() || source.Catalog == nil ||
		source.CatalogHeadDigest == (replication.Digest{}) ||
		source.Catalog.Generation() != source.Nodes.CatalogGeneration ||
		source.Nodes.Revision != expected.Revision ||
		source.Nodes.CatalogGeneration != expected.CatalogGeneration ||
		!reflect.DeepEqual(source.Nodes.CurrentNodes(), expected.Nodes) {
		return gateway.FrontendDrainRuntimeCut{}, errGatewayControlDirectory
	}
	return source, nil
}

// readCanonicalFrontendDrainRuntimeCutFromSourceProof verifies one physical
// source response against a fresh complete authority cut. The source proof
// is a floor: a concurrent catalog publication may produce a newer cut, but
// every unchanged coordinate and digest must still satisfy AtLeastFloor.
func readCanonicalFrontendDrainRuntimeCutFromSourceProof(
	ctx context.Context, proof frontenddrain.PreparedAckCut,
	authority *gateway.ReplicatedCatalogAuthority, profile *rafttransport.PeerTLS,
	policyGeneration uint64,
) (gateway.FrontendDrainRuntimeCut, error) {
	if ctx == nil || !proof.Valid() || authority == nil || profile == nil || policyGeneration == 0 {
		return gateway.FrontendDrainRuntimeCut{}, errGatewayControlDirectory
	}
	source, err := authority.ReadFrontendDrainRuntimeCut(ctx)
	if err != nil {
		return gateway.FrontendDrainRuntimeCut{}, err
	}
	serviceCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
		ctx, source, profile, policyGeneration,
	)
	if err != nil {
		return gateway.FrontendDrainRuntimeCut{}, err
	}
	full, err := frontendDrainPreparedAckCutFromRuntimeCut(source, serviceCut)
	if err != nil || !full.Valid() || !full.AtLeastFloor(proof.ReadFloor()) {
		return gateway.FrontendDrainRuntimeCut{}, errGatewayControlDirectory
	}
	return source, nil
}

// frontendDrainPreparedAckCutFromRuntimeCut binds the projected service
// directory to the exact node-directory digest and catalog head that produced
// it.  This is the only cut shape sent to a fused native receiver.
func frontendDrainPreparedAckCutFromRuntimeCut(
	source gateway.FrontendDrainRuntimeCut,
	serviceCut serviceauthz.ServiceDirectoryCut,
) (frontenddrain.PreparedAckCut, error) {
	if !source.Nodes.Valid() || source.Catalog == nil ||
		source.CatalogHeadDigest == (replication.Digest{}) ||
		source.Catalog.Generation() != source.Nodes.CatalogGeneration || !serviceCut.Valid() ||
		serviceCut.CatalogGeneration != source.Nodes.CatalogGeneration {
		return frontenddrain.PreparedAckCut{}, errGatewayControlDirectory
	}
	cut := frontenddrain.PreparedAckCut{
		DirectoryRevision: source.Nodes.Revision, DirectoryDigest: source.Nodes.Digest,
		CatalogGeneration: source.Nodes.CatalogGeneration, CatalogHeadDigest: source.CatalogHeadDigest,
		ServiceDirectoryRevision: serviceCut.Revision, ServiceDirectory: serviceCut,
		SourceRoster: frontendDrainPreparedAckSourceRoster(source),
		Subjects:     frontendDrainPreparedAckSubjects(source),
	}
	if !cut.Valid() {
		return frontenddrain.PreparedAckCut{}, errGatewayControlDirectory
	}
	return cut, nil
}

type serviceDirectoryCutBinder interface {
	InstallFrontendDrainServiceCut(context.Context, frontenddrain.PreparedAckCut) (uint64, error)
	ServiceDirectoryGate() *serviceauthz.ServiceDirectoryGate
	ServiceCutCoordinates() (frontenddrain.PreparedAckCutReadFloor, bool)
}

func bindRuntimeServiceDirectory(
	ctx context.Context,
	transport gateway.ReplicatedRoundTripper,
	cut frontenddrain.PreparedAckCut,
	required bool,
) (*serviceauthz.ServiceDirectoryGate, error) {
	if ctx == nil || transport == nil || !cut.Valid() {
		if required {
			return nil, fmt.Errorf("%w: local semantic transport and complete service-directory cut are required", errGatewayControlDirectory)
		}
		return nil, nil
	}
	binder, ok := transport.(serviceDirectoryCutBinder)
	if !ok {
		if required {
			return nil, fmt.Errorf("%w: local semantic transport does not install complete service-directory cuts", errGatewayControlDirectory)
		}
		return nil, nil
	}
	applied, err := binder.InstallFrontendDrainServiceCut(ctx, cut)
	if err != nil {
		return nil, fmt.Errorf("install complete service directory on semantic transport: %w", err)
	}
	if applied != cut.ServiceDirectoryRevision {
		return nil, fmt.Errorf("%w: semantic transport applied revision %d, cut revision %d", errGatewayControlDirectory, applied, cut.ServiceDirectoryRevision)
	}
	coordinates, ok := binder.ServiceCutCoordinates()
	if !ok || coordinates != cut.ReadFloor() {
		return nil, fmt.Errorf("%w: semantic transport retained incomplete service-directory floor", errGatewayControlDirectory)
	}
	directory := binder.ServiceDirectoryGate()
	if directory == nil {
		return nil, fmt.Errorf("%w: semantic transport did not retain service-directory gate", errGatewayControlDirectory)
	}
	return directory, nil
}

func runtimeServiceDirectoryCut(
	ctx context.Context,
	reader gateway.DirectoryReader,
	snapshot gateway.ReplicatedControlDirectorySnapshot,
	profile *rafttransport.PeerTLS,
	policyGeneration uint64,
) (serviceauthz.ServiceDirectoryCut, error) {
	if ctx == nil || reader == nil || profile == nil || policyGeneration == 0 ||
		!snapshot.Valid() {
		return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
	}
	if exact, ok := reader.(serviceDirectoryCutReader); ok {
		cut, err := exact.ReadServiceDirectoryCut(ctx)
		if err != nil {
			return serviceauthz.ServiceDirectoryCut{}, err
		}
		if !cut.Valid() || cut.TrustDomain != profile.LocalIdentity().TrustDomain ||
			cut.PolicyGeneration != policyGeneration || cut.CatalogGeneration != snapshot.CatalogGeneration {
			return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
		}
		return cut, nil
	}
	var continuationGrants []serviceauthz.CommittedFrontendContinuationGrant
	var drainFences []serviceauthz.CommittedFrontendDrainFence
	var drainRecords []gateway.FrontendDrainRecord
	var forwardedScopes []serviceauthz.FrontendContinuationScopeRecord
	var forwardedScopesGeneration uint64
	var serviceDirectoryRevision uint64
	var catalogFences []serviceauthz.ServiceFence
	var catalogGeneration uint64
	var source *gateway.FrontendDrainRuntimeCut
	if coherent, ok := reader.(frontendDrainRuntimeCutReader); ok {
		cut, err := coherent.ReadFrontendDrainRuntimeCut(ctx)
		if err != nil {
			return serviceauthz.ServiceDirectoryCut{}, err
		}
		if !cut.Nodes.Valid() || cut.Catalog == nil ||
			cut.Nodes.Revision != snapshot.Revision || cut.Nodes.CatalogGeneration != snapshot.CatalogGeneration ||
			cut.Catalog.Generation() != snapshot.CatalogGeneration {
			return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
		}
		source = &cut
		serviceDirectoryRevision = cut.ServiceDirectoryRevision
		continuationGrants = slices.Clone(cut.ContinuationGrants)
		drainFences = slices.Clone(cut.DrainFences)
		drainRecords = slices.Clone(cut.DrainRecords)
		forwardedScopes = continuationScopesForSnapshot(cut.Catalog)
		forwardedScopesGeneration = cut.Catalog.Generation()
	} else if complete, ok := reader.(serviceDirectoryCompleteCutTestReader); ok {
		var err error
		material, readErr := complete.ReadCompleteServiceDirectoryCut(ctx)
		err = readErr
		if err != nil {
			return serviceauthz.ServiceDirectoryCut{}, err
		}
		serviceDirectoryRevision = material.Revision
		continuationGrants = slices.Clone(material.ContinuationGrants)
		drainFences = slices.Clone(material.DrainFences)
		drainRecords = slices.Clone(material.DrainRecords)
		forwardedScopes = slices.Clone(material.ForwardedScopes)
		forwardedScopesGeneration = material.ScopesGeneration
		catalogFences = slices.Clone(material.CatalogFences)
		catalogGeneration = material.CatalogGeneration
		slices.SortFunc(continuationGrants, func(left, right serviceauthz.CommittedFrontendContinuationGrant) int {
			return bytes.Compare(left.GrantDigest[:], right.GrantDigest[:])
		})
		slices.SortFunc(drainFences, func(left, right serviceauthz.CommittedFrontendDrainFence) int {
			return bytes.Compare(left.DrainID[:], right.DrainID[:])
		})
		slices.SortFunc(forwardedScopes, serviceauthz.CompareContinuationScopes)
		for index := 1; index < len(forwardedScopes); index++ {
			if serviceauthz.CompareContinuationScopes(forwardedScopes[index-1], forwardedScopes[index]) == 0 {
				return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
			}
		}
	} else {
		return serviceauthz.ServiceDirectoryCut{}, fmt.Errorf("%w: complete service cut reader is required", errGatewayControlDirectory)
	}

	// The snapshot and every complete reader cut remain caller-owned. Build the
	// compact physical record set in fresh storage before sorting or merging so
	// a projection cannot rewrite a reader's backing slice.
	records := slices.Clone(snapshot.Nodes)
	if source != nil {
		records = slices.Clone(source.Nodes.Nodes)
	}
	latest := make(map[rafttransport.NodeID]gateway.NodeRecord, len(records))
	for _, record := range records {
		prior, found := latest[record.NodeID]
		if found && prior.Incarnation > record.Incarnation {
			continue
		}
		if found && prior.Incarnation == record.Incarnation && prior != record {
			return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
		}
		latest[record.NodeID] = record
	}
	records = make([]gateway.NodeRecord, 0, len(latest))
	for _, record := range latest {
		records = append(records, record)
	}
	slices.SortFunc(records, func(left, right gateway.NodeRecord) int {
		return bytes.Compare(left.NodeID[:], right.NodeID[:])
	})

	if source != nil {
		catalogFences, catalogGeneration = slices.Clone(source.CatalogFences), source.Catalog.Generation()
	}
	if catalogGeneration == 0 || catalogGeneration != snapshot.CatalogGeneration {
		return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
	}
	if forwardedScopesGeneration != 0 && forwardedScopesGeneration != catalogGeneration && catalogGeneration != 0 {
		return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
	}
	if len(continuationGrants) != 0 && len(forwardedScopes) == 0 {
		// A grant proves accepted connection/session state only. Without the
		// current catalog inventory it cannot authorize a forwarded resource.
		return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
	}
	bindings := make([]serviceauthz.ServiceBinding, 0, len(records)*2)
	selectedContinuationGrants := make([]serviceauthz.CommittedFrontendContinuationGrant, 0, len(continuationGrants))
	for _, record := range records {
		roles := serviceRoleMask(record)
		if roles == 0 {
			return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
		}
		lifecycle, ok := serviceLifecycle(record.Lifecycle)
		if !ok {
			return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
		}
		physicalRoles := roles &^ serviceauthz.ServiceRoleGateway
		if physicalRoles != 0 {
			bindings = append(bindings, serviceauthz.ServiceBinding{
				Principal: record.NodeID, PhysicalNode: record.NodeID,
				PhysicalIncarnation: record.Incarnation, KeyDigest: [32]byte(record.ServiceKeyDigest),
				Roles: physicalRoles, Lifecycle: lifecycle,
			})
		}
		if roles&serviceauthz.ServiceRoleGateway != 0 {
			if record.Gateway.NodeID == (rafttransport.NodeID{}) ||
				record.Gateway.Incarnation == 0 || record.Gateway.ServiceID == ([16]byte{}) ||
				record.Gateway.SessionID == ([16]byte{}) || record.Gateway.SessionRevision == 0 ||
				record.Gateway.ParticipantDigest == ([32]byte{}) {
				return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
			}
			gatewayBinding := serviceauthz.ServiceBinding{
				Principal: record.Gateway.NodeID, PhysicalNode: record.NodeID,
				PhysicalIncarnation: record.Incarnation,
				KeyDigest:           [32]byte(record.Gateway.ServiceKeyDigest),
				Roles:               serviceauthz.ServiceRoleGateway, Lifecycle: lifecycle,
				GatewayIncarnation: record.Gateway.Incarnation,
				SessionID:          record.Gateway.SessionID, SessionRevision: record.Gateway.SessionRevision,
				ParticipantDigest: [32]byte(record.Gateway.ParticipantDigest),
			}
			durableDrainID := [32]byte{}
			var durableDrainRevision uint64
			var durableCatalogFence serviceauthz.ServiceFence
			for _, drainRecord := range drainRecords {
				if drainRecord.PhysicalNode == gatewayBinding.PhysicalNode &&
					drainRecord.PhysicalIncarnation == gatewayBinding.PhysicalIncarnation &&
					drainRecord.GatewayServiceID == gatewayBinding.Principal &&
					drainRecord.GatewayIncarnation == gatewayBinding.GatewayIncarnation &&
					drainRecord.GatewaySessionID == gatewayBinding.SessionID &&
					drainRecord.GatewaySessionRevision == gatewayBinding.SessionRevision {
					if !drainRecord.Valid() || (drainRecord.Lifecycle != gateway.FrontendDrainRetired && drainRecord.NodeRevision != record.Revision) {
						return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
					}
					if durableDrainID != ([32]byte{}) && durableDrainID != drainRecord.DrainID {
						return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
					}
					durableDrainID, durableDrainRevision = drainRecord.DrainID, drainRecord.NodeRevision
					if drainRecord.DrainFence != (serviceauthz.ServiceFence{}) {
						durableCatalogFence = drainRecord.DrainFence
					}
				}
			}
			if durableCatalogFence == (serviceauthz.ServiceFence{}) {
				for _, fence := range drainFences {
					if fence.Valid() && fence.TrustDomain == profile.LocalIdentity().TrustDomain &&
						fence.GatewayServiceID == gatewayBinding.Principal && fence.PhysicalNode == gatewayBinding.PhysicalNode &&
						fence.PhysicalIncarnation == gatewayBinding.PhysicalIncarnation && fence.PeerKeyDigest == gatewayBinding.KeyDigest &&
						fence.GatewaySessionID == gatewayBinding.SessionID && fence.GatewaySessionRevision == gatewayBinding.SessionRevision &&
						(durableDrainID == ([32]byte{}) || fence.DrainID == durableDrainID) {
						durableCatalogFence = fence.Fence
						if durableDrainID == ([32]byte{}) {
							durableDrainID, durableDrainRevision = fence.DrainID, fence.Revision
						}
						break
					}
				}
			}
			for _, fence := range catalogFences {
				fence.SessionID, fence.SessionRevision = gatewayBinding.SessionID, gatewayBinding.SessionRevision
				gatewayBinding.InternalFences = append(gatewayBinding.InternalFences, fence)
			}
			matchedGrant := false
			for _, grant := range continuationGrants {
				if grant.GatewayServiceID != gatewayBinding.Principal ||
					grant.PhysicalNode != gatewayBinding.PhysicalNode ||
					grant.PhysicalIncarnation != gatewayBinding.PhysicalIncarnation ||
					grant.PeerKeyDigest != gatewayBinding.KeyDigest ||
					grant.GatewaySessionID != gatewayBinding.SessionID ||
					grant.GatewaySessionRevision != gatewayBinding.SessionRevision {
					continue
				}
				if !grant.Valid() {
					return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
				}
				if durableDrainID != ([32]byte{}) && grant.DrainID != durableDrainID {
					return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
				}
				switch lifecycle {
				case serviceauthz.ServiceActive:
					if grant.State != serviceauthz.ContinuationGrantPrepared {
						return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
					}
					if matchedGrant {
						return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
					}
					selectedContinuationGrants = append(selectedContinuationGrants, grant)
					matchedGrant = true
				case serviceauthz.ServiceDraining:
					if grant.State != serviceauthz.ContinuationGrantEnforcing &&
						grant.State != serviceauthz.ContinuationGrantRetired {
						return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
					}
					if matchedGrant {
						return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
					}
					gatewayBinding.DrainFenceDigest = grant.GrantDigest
					gatewayBinding.DrainFence = continuationDrainFence(grant, gatewayBinding, durableCatalogFence)
					if gatewayBinding.DrainFence.FenceDigest != gatewayBinding.DrainFenceDigest {
						return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
					}
					selectedContinuationGrants = append(selectedContinuationGrants, grant)
					matchedGrant = true
				case serviceauthz.ServiceDecommissioned:
					if grant.State != serviceauthz.ContinuationGrantRetired {
						return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
					}
					if matchedGrant {
						return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
					}
					selectedContinuationGrants = append(selectedContinuationGrants, grant)
					matchedGrant = true
				}
			}
			if lifecycle == serviceauthz.ServiceDraining && !matchedGrant {
				for _, fence := range drainFences {
					if !fence.Valid() || fence.TrustDomain != profile.LocalIdentity().TrustDomain ||
						fence.GatewayServiceID != gatewayBinding.Principal ||
						fence.PhysicalNode != gatewayBinding.PhysicalNode ||
						fence.PhysicalIncarnation != gatewayBinding.PhysicalIncarnation ||
						fence.PeerKeyDigest != gatewayBinding.KeyDigest ||
						fence.GatewaySessionID != gatewayBinding.SessionID ||
						fence.GatewaySessionRevision != gatewayBinding.SessionRevision ||
						fence.DrainID != func() [32]byte {
							if durableDrainID != ([32]byte{}) {
								return durableDrainID
							}
							return frontendDrainID(FrontendDrainIdentity{
								NodeID: gatewayBinding.PhysicalNode, Incarnation: gatewayBinding.PhysicalIncarnation,
								GatewayNodeID: gatewayBinding.Principal, GatewayIncarnation: gatewayBinding.GatewayIncarnation,
								SessionID: gatewayBinding.SessionID, SessionRevision: gatewayBinding.SessionRevision,
							})
						}() ||
						(fence.Revision != record.Revision && fence.Revision+1 != record.Revision &&
							(durableDrainRevision == 0 || fence.Revision != durableDrainRevision)) {
						continue
					}
					gatewayBinding.DrainFenceDigest = fence.Fence.FenceDigest
					gatewayBinding.DrainFence = fence.Fence
					matchedGrant = true
					break
				}
			}
			if lifecycle == serviceauthz.ServiceDraining && (!matchedGrant || gatewayBinding.DrainFenceDigest == ([32]byte{})) {
				// NodeRecord carries the participant identity but not the accepted
				// connection/token fence. Publishing it without a durable proof would
				// reopen new admissions, so require the exact owner commitment.
				return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
			}
			bindings = append(bindings, gatewayBinding)
		}
	}
	slices.SortFunc(bindings, func(left, right serviceauthz.ServiceBinding) int {
		return bytes.Compare(left.Principal[:], right.Principal[:])
	})
	merged := bindings[:0]
	for _, binding := range bindings {
		if len(merged) == 0 || merged[len(merged)-1].Principal != binding.Principal {
			merged = append(merged, binding)
			continue
		}
		prior := &merged[len(merged)-1]
		if prior.PhysicalNode != binding.PhysicalNode ||
			prior.PhysicalIncarnation != binding.PhysicalIncarnation || prior.KeyDigest != binding.KeyDigest {
			// A single NodeID cannot safely represent two simultaneously
			// authenticated certificates. An exact service cut is required for
			// that identity layout rather than silently choosing one key.
			return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
		}
		if prior.Roles&serviceauthz.ServiceRoleGateway != 0 && binding.Roles&serviceauthz.ServiceRoleGateway != 0 &&
			(prior.GatewayIncarnation != binding.GatewayIncarnation || prior.SessionID != binding.SessionID ||
				prior.SessionRevision != binding.SessionRevision || prior.ParticipantDigest != binding.ParticipantDigest) {
			return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
		}
		prior.Roles |= binding.Roles
		if binding.Roles&serviceauthz.ServiceRoleGateway != 0 {
			prior.GatewayIncarnation, prior.SessionID = binding.GatewayIncarnation, binding.SessionID
			prior.SessionRevision, prior.ParticipantDigest = binding.SessionRevision, binding.ParticipantDigest
			// The gateway's exact catalog grants belong to the same session
			// even when its TLS principal also owns the physical storage role.
			prior.InternalFences = binding.InternalFences
			prior.DrainFenceDigest = binding.DrainFenceDigest
			prior.DrainFence = binding.DrainFence
		}
	}
	bindings = merged
	replacementProofs, err := serviceBindingReplacements(records, bindings)
	if err != nil {
		return serviceauthz.ServiceDirectoryCut{}, err
	}
	// The receiver gate has one revision scalar, while the physical node cut
	// and the continuation row advance independently. Their bounded sum gives
	// every service-row publication a newer gate revision without allowing a
	// same-revision grant change to be mistaken for a stale replay.
	cutRevision := snapshot.Revision
	if serviceDirectoryRevision > ^uint64(0)-cutRevision {
		return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
	}
	cutRevision += serviceDirectoryRevision
	if cutRevision == 0 {
		return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
	}
	return serviceauthz.ServiceDirectoryCut{
		CatalogGeneration: catalogGeneration, Revision: cutRevision, TrustDomain: profile.LocalIdentity().TrustDomain,
		PolicyGeneration: policyGeneration, Bindings: bindings,
		Replacements:       replacementProofs,
		ForwardedScopes:    forwardedScopes,
		ContinuationGrants: uniqueContinuationGrantsForBindings(selectedContinuationGrants, bindings),
	}, nil
}

// serviceBindingReplacements translates each successor node marker into the
// exact principal binding pairs a retained receiver needs to validate.  The
// physical and gateway identities are emitted separately when their
// principals differ; colocated identities share one merged binding and one
// replacement edge.
func serviceBindingReplacements(
	records []gateway.NodeRecord, bindings []serviceauthz.ServiceBinding,
) ([]serviceauthz.ServiceBindingReplacement, error) {
	if len(records) == 0 || len(bindings) == 0 {
		return nil, nil
	}
	byPrincipal := make(map[rafttransport.NodeID]serviceauthz.ServiceBinding, len(bindings))
	for _, binding := range bindings {
		byPrincipal[binding.Principal] = binding
	}
	result := make([]serviceauthz.ServiceBindingReplacement, 0)
	for _, record := range records {
		if record.Replacement == nil {
			continue
		}
		marker := record.Replacement
		roles := serviceRoleMask(record)
		physicalRoles := roles &^ serviceauthz.ServiceRoleGateway
		currentPhysical, physicalFound := byPrincipal[record.NodeID]
		if physicalRoles != 0 && !physicalFound {
			return nil, fmt.Errorf("%w: replacement successor physical binding is missing", errGatewayControlDirectory)
		}
		priorPhysical := serviceauthz.ServiceBinding{
			Principal: record.NodeID, PhysicalNode: record.NodeID,
			PhysicalIncarnation: marker.PredecessorIncarnation,
			KeyDigest:           [32]byte(marker.PredecessorServiceKeyDigest), Roles: physicalRoles,
			Lifecycle: serviceauthz.ServiceDecommissioned,
		}
		if physicalRoles != 0 && record.Gateway.NodeID == record.NodeID {
			// A colocated gateway and physical process are merged into one
			// binding by the projection. Carry the predecessor gateway fields
			// into the same identity so the retained gate checks every key and
			// session coordinate in one replacement edge.
			priorPhysical.Roles |= serviceauthz.ServiceRoleGateway
			priorPhysical.KeyDigest = [32]byte(marker.PredecessorGateway.ServiceKeyDigest)
			priorPhysical.GatewayIncarnation = marker.PredecessorGateway.Incarnation
			priorPhysical.SessionID = marker.PredecessorGateway.SessionID
			priorPhysical.SessionRevision = marker.PredecessorGateway.SessionRevision
			priorPhysical.ParticipantDigest = [32]byte(marker.PredecessorGateway.ParticipantDigest)
			currentPhysical = byPrincipal[record.NodeID]
		}
		if physicalRoles != 0 {
			if record.Gateway.NodeID == record.NodeID {
				replacement, err := serviceauthz.NewServiceBindingReplacement(
					priorPhysical.Identity(), currentPhysical.Identity(), marker.IntentID, [32]byte(marker.ProofDigest))
				if err != nil {
					return nil, fmt.Errorf("%w: invalid colocated replacement: %v", errGatewayControlDirectory, err)
				}
				result = append(result, replacement)
			} else {
				replacement, err := serviceauthz.NewServiceBindingReplacement(
					priorPhysical.Identity(), currentPhysical.Identity(), marker.IntentID, [32]byte(marker.ProofDigest))
				if err != nil {
					return nil, fmt.Errorf("%w: invalid physical replacement: %v", errGatewayControlDirectory, err)
				}
				result = append(result, replacement)
			}
		}
		if roles&serviceauthz.ServiceRoleGateway == 0 {
			continue
		}
		currentGateway, gatewayFound := byPrincipal[record.Gateway.NodeID]
		if !gatewayFound {
			return nil, fmt.Errorf("%w: replacement successor gateway binding is missing", errGatewayControlDirectory)
		}
		if record.Gateway.NodeID == record.NodeID && physicalRoles != 0 {
			// Already emitted as the merged colocated binding above.
			continue
		}
		priorGateway := serviceauthz.ServiceBinding{
			Principal: marker.PredecessorGateway.NodeID, PhysicalNode: record.NodeID,
			PhysicalIncarnation: marker.PredecessorIncarnation,
			KeyDigest:           [32]byte(marker.PredecessorGateway.ServiceKeyDigest),
			Roles:               serviceauthz.ServiceRoleGateway, Lifecycle: serviceauthz.ServiceDecommissioned,
			GatewayIncarnation: marker.PredecessorGateway.Incarnation,
			SessionID:          marker.PredecessorGateway.SessionID,
			SessionRevision:    marker.PredecessorGateway.SessionRevision,
			ParticipantDigest:  [32]byte(marker.PredecessorGateway.ParticipantDigest),
		}
		replacement, err := serviceauthz.NewServiceBindingReplacement(
			priorGateway.Identity(), currentGateway.Identity(), marker.IntentID, [32]byte(marker.ProofDigest))
		if err != nil {
			return nil, fmt.Errorf("%w: invalid gateway replacement: %v", errGatewayControlDirectory, err)
		}
		result = append(result, replacement)
	}
	slices.SortFunc(result, func(left, right serviceauthz.ServiceBindingReplacement) int {
		if left.Next.Principal != right.Next.Principal {
			return bytes.Compare(left.Next.Principal[:], right.Next.Principal[:])
		}
		if left.Prior.Principal != right.Prior.Principal {
			return bytes.Compare(left.Prior.Principal[:], right.Prior.Principal[:])
		}
		return bytes.Compare(left.ProofDigest[:], right.ProofDigest[:])
	})
	return result, nil
}

func continuationDrainFence(
	grant serviceauthz.CommittedFrontendContinuationGrant,
	binding serviceauthz.ServiceBinding,
	catalogFence serviceauthz.ServiceFence,
) serviceauthz.ServiceFence {
	// The durable catalog fence supplies the admission marker's exact action,
	// operation, group, and relation. The stable grant digest then binds that
	// marker to this accepted connection proof. ForwardedScopes never supplies
	// this tuple and cannot be used to synthesize an admission fence.
	if !catalogFence.Valid() || catalogFence.SessionID != binding.SessionID ||
		catalogFence.SessionRevision != binding.SessionRevision {
		return serviceauthz.ServiceFence{}
	}
	fence := catalogFence
	fence.IntentID, fence.FenceDigest = grant.GrantDigest, grant.GrantDigest
	return fence
}

func uniqueContinuationGrantsForBindings(
	grants []serviceauthz.CommittedFrontendContinuationGrant,
	bindings []serviceauthz.ServiceBinding,
) []serviceauthz.CommittedFrontendContinuationGrant {
	allowed := make(map[[32]byte]struct{}, len(bindings))
	for _, binding := range bindings {
		if binding.DrainFenceDigest != ([32]byte{}) {
			allowed[binding.DrainFenceDigest] = struct{}{}
		}
	}
	result := make([]serviceauthz.CommittedFrontendContinuationGrant, 0, len(grants))
	for _, grant := range grants {
		if _, ok := allowed[grant.GrantDigest]; ok {
			result = append(result, grant)
			continue
		}
		if grant.State == serviceauthz.ContinuationGrantPrepared {
			// Prepared proofs are published while the physical binding is
			// still Active. Keep the exact proof in the receiver cut so it can
			// be acknowledged before the atomic Active -> Draining CAS; the
			// grant itself never authorizes a Draining binding.
			for _, binding := range bindings {
				if binding.Roles&serviceauthz.ServiceRoleGateway != 0 &&
					binding.Lifecycle == serviceauthz.ServiceActive &&
					grant.GatewayServiceID == binding.Principal &&
					grant.PhysicalNode == binding.PhysicalNode &&
					grant.PhysicalIncarnation == binding.PhysicalIncarnation &&
					grant.PeerKeyDigest == binding.KeyDigest &&
					grant.GatewaySessionID == binding.SessionID &&
					grant.GatewaySessionRevision == binding.SessionRevision {
					result = append(result, grant)
					break
				}
			}
			continue
		}
		if grant.State != serviceauthz.ContinuationGrantRetired {
			continue
		}
		for _, binding := range bindings {
			if binding.Roles&serviceauthz.ServiceRoleGateway != 0 &&
				grant.GatewayServiceID == binding.Principal && grant.PhysicalNode == binding.PhysicalNode &&
				grant.PhysicalIncarnation == binding.PhysicalIncarnation && grant.PeerKeyDigest == binding.KeyDigest &&
				grant.GatewaySessionID == binding.SessionID && grant.GatewaySessionRevision == binding.SessionRevision {
				result = append(result, grant)
				break
			}
		}
	}
	return result
}

func serviceRoleMask(record gateway.NodeRecord) serviceauthz.ServiceRoleMask {
	var roles serviceauthz.ServiceRoleMask
	if record.Roles&gateway.NodeRoleStorage != 0 {
		roles |= serviceauthz.ServiceRoleStorage
	}
	if record.Roles&gateway.NodeRoleGateway != 0 {
		roles |= serviceauthz.ServiceRoleGateway
	}
	if record.Roles&(gateway.NodeRoleCatalog|gateway.NodeRoleControl) != 0 {
		roles |= serviceauthz.ServiceRoleController
	}
	return roles
}

func serviceLifecycle(state gateway.NodeLifecycle) (serviceauthz.ServiceLifecycle, bool) {
	switch state {
	case gateway.NodeJoining:
		return serviceauthz.ServiceJoining, true
	case gateway.NodeActive:
		return serviceauthz.ServiceActive, true
	case gateway.NodeDraining:
		return serviceauthz.ServiceDraining, true
	case gateway.NodeDecommissioned:
		return serviceauthz.ServiceDecommissioned, true
	default:
		return 0, false
	}
}

// appendBootstrapControlNodes projects physical storage identities into the
// shared listener roster. Certificate-bound and request-level checks still
// authorize each bootstrap read against the committed directory and intent.
func appendBootstrapControlNodes(nodes []rafttransport.NodeID, cut serviceauthz.ServiceDirectoryCut) []rafttransport.NodeID {
	for _, binding := range cut.Bindings {
		if binding.Roles&serviceauthz.ServiceRoleStorage == 0 || binding.Principal != binding.PhysicalNode {
			continue
		}
		switch binding.Lifecycle {
		case serviceauthz.ServiceJoining, serviceauthz.ServiceActive, serviceauthz.ServiceDraining:
			if !slices.Contains(nodes, binding.Principal) {
				nodes = append(nodes, binding.Principal)
			}
		}
	}
	return nodes
}
