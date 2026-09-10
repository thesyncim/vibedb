package gatewayruntime

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// serviceDirectoryCutReader is intentionally optional. A catalog adapter that
// owns the full service grant rows can return the exact committed cut,
// including continuation grants. Older directory adapters are converted from
// the physical NodeRecord cut below, which is sufficient for joining/active
// identities and fails closed when a draining gateway has no grant proof.
type serviceDirectoryCutReader interface {
	ReadServiceDirectoryCut(context.Context) (serviceauthz.ServiceDirectoryCut, error)
}

type serviceDirectoryGateBinder interface {
	BindServiceDirectoryGate(*serviceauthz.ServiceDirectoryGate) error
}

func bindRuntimeServiceDirectory(
	transport gateway.ReplicatedRoundTripper,
	directory *serviceauthz.ServiceDirectoryGate,
	required bool,
) error {
	if transport == nil || directory == nil {
		if required {
			return fmt.Errorf("%w: local semantic transport and service-directory gate are required", errGatewayControlDirectory)
		}
		return nil
	}
	if binder, ok := transport.(serviceDirectoryGateBinder); ok {
		if err := binder.BindServiceDirectoryGate(directory); err != nil {
			return fmt.Errorf("bind service directory to semantic transport: %w", err)
		}
		return nil
	}
	if required {
		return fmt.Errorf("%w: local semantic transport does not bind service-directory gate", errGatewayControlDirectory)
	}
	return nil
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
			cut.PolicyGeneration != policyGeneration {
			return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
		}
		return cut, nil
	}

	records := snapshot.Nodes
	if full, ok := reader.(gateway.NodeDirectoryCutReader); ok {
		cut, err := full.ReadNodeDirectoryCut(ctx)
		if err != nil || !cut.Valid() || cut.Revision != snapshot.Revision ||
			cut.CatalogGeneration != snapshot.CatalogGeneration {
			return serviceauthz.ServiceDirectoryCut{}, errGatewayControlDirectory
		}
		// The service directory retains decommissioned tombstones even though
		// the transport projection deliberately admits only CurrentNodes.
		records = cut.Nodes
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
	records = records[:0]
	for _, record := range latest {
		records = append(records, record)
	}
	slices.SortFunc(records, func(left, right gateway.NodeRecord) int {
		return bytes.Compare(left.NodeID[:], right.NodeID[:])
	})

	var catalogFences []serviceauthz.ServiceFence
	var catalogGeneration uint64
	if owner, ok := reader.(interface {
		CatalogServiceFences(context.Context) ([]serviceauthz.ServiceFence, uint64, error)
	}); ok {
		var err error
		catalogFences, catalogGeneration, err = owner.CatalogServiceFences(ctx)
		if err != nil {
			return serviceauthz.ServiceDirectoryCut{}, err
		}
	}
	bindings := make([]serviceauthz.ServiceBinding, 0, len(records)*2)
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
			if lifecycle == serviceauthz.ServiceDraining {
				// NodeRecord carries the participant identity but not the accepted
				// connection/token fence. Publishing it as Active would reopen new
				// admissions, so require the exact service cut from the grant owner.
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
			for _, fence := range catalogFences {
				fence.SessionID, fence.SessionRevision = gatewayBinding.SessionID, gatewayBinding.SessionRevision
				gatewayBinding.InternalFences = append(gatewayBinding.InternalFences, fence)
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
		}
	}
	bindings = merged
	return serviceauthz.ServiceDirectoryCut{
		CatalogGeneration: catalogGeneration, Revision: snapshot.Revision, TrustDomain: profile.LocalIdentity().TrustDomain,
		PolicyGeneration: policyGeneration, Bindings: bindings,
	}, nil
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
