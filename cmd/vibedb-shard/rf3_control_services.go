package main

import (
	"context"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicemetrics"
	"github.com/thesyncim/vibedb/internal/shardcontrol"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	publicshardcontrol "github.com/thesyncim/vibedb/shardcontrol"
	"github.com/thesyncim/vibedb/shardservice"
)

// Both startup paths use this discriminator inventory. Handlers resolve live
// groups themselves, so starting without a group never removes a service needed
// after adoption. Optional deployment services remain explicit named fields.
type rf3ControlServices struct {
	membership, observation, metrics, capacity, preparation, enrollment      shardcontrol.Handler
	backup, source, action, split, schema, schemaBuild                       shardcontrol.Handler
	planObservation, admission, tail, terminal, childPrepare, restoreServing shardcontrol.Handler
	nodeInfo, nodeControl, bootstrap, preparedAck, canonicalSource           shardcontrol.Handler
}

func (services rf3ControlServices) mux() (*shardcontrol.Mux, error) {
	if services.membership == nil || services.observation == nil || services.metrics == nil {
		return nil, shardcontrol.ErrMux
	}
	routes := make([]shardcontrol.Route, 0, shardcontrol.MaxRoutes)
	add := func(discriminator [shardcontrol.DiscriminatorBytes]byte, handler shardcontrol.Handler) {
		if handler != nil {
			routes = append(routes, shardcontrol.Route{Discriminator: discriminator, Handler: handler})
		}
	}
	add(shardservice.MembershipGrantRequestDiscriminator(), services.membership)
	add(replicacontrol.RequestDiscriminator(), services.observation)
	add(servicemetrics.RequestDiscriminator(), services.metrics)
	add(replicacontrol.CapacityRequestDiscriminator(), services.capacity)
	add(nodecontrol.PreparationSourceRequestDiscriminator(), services.preparation)
	add(rafttransport.EnrollmentRequestDiscriminator(), services.enrollment)
	add(clusterbackup.LiveRequestDiscriminator(), services.backup)
	add(snapshottransfer.SourceControlRequestDiscriminator(), services.source)
	add(replicaaction.RequestDiscriminator(), services.action)
	add(publicshardcontrol.RequestDiscriminator(), services.split)
	add(schemainstall.RequestDiscriminator(), services.schema)
	add(schemainstall.BuildRequestDiscriminator(), services.schemaBuild)
	add(schemainstall.BuildResumeRequestDiscriminator(), services.schemaBuild)
	add(schemainstall.BuildShadowRequestDiscriminator(), services.schemaBuild)
	add(splitcontroller.PlanObservationRequestDiscriminator(), services.planObservation)
	add(splitcontroller.PlanAdmissionRequestDiscriminator(), services.admission)
	add(splitcontroller.TailStreamRequestDiscriminator(), services.tail)
	add(splitcontroller.TerminalRetirementRequestDiscriminator(), services.terminal)
	add(splitcontroller.ChildPrepareRequestDiscriminator(), services.childPrepare)
	add(shardservice.RestoreServingRequestDiscriminator(), services.restoreServing)
	add(nodecontrol.NodeInfoRequestDiscriminator(), services.nodeInfo)
	add(nodecontrol.RequestDiscriminator(), services.nodeControl)
	add(snapshottransfer.BootstrapRequestDiscriminator(), services.bootstrap)
	add(frontenddrain.PreparedAckDiscriminator, services.preparedAck)
	add(frontenddrain.PreparedAckCutReadDiscriminator, services.canonicalSource)
	return shardcontrol.New(routes...)
}

func newRF3ReplicaActionControl(journal replicaaction.Journal, owner replicaaction.Owner,
	registry *rafttransport.StaticRegistry, policy *serviceauthz.Policy, deadline rafttransport.DeadlineFunc,
	profile *rafttransport.PeerTLS, retired func(context.Context, replicaaction.Request) error,
) (*replicaaction.Service, error) {
	if registry == nil || policy == nil {
		return nil, replicaaction.ErrControl
	}
	return replicaaction.NewService(replicaaction.Options{
		Journal: journal, Owner: owner,
		RetirementProof: rf3ReplicaRetirementProof(registry, profile, deadline), Retired: retired,
		Authorize: func(identity rafttransport.PeerIdentity, request replicaaction.Request) bool {
			if identity.TrustDomain != registry.TrustDomain() || policy.Check(identity.Node, serviceauthz.CapabilityMembership) != serviceauthz.DecisionAllow {
				return false
			}
			local, err := registry.LocalMember(request.Fence.Group)
			if err == nil && request.Fence.MemberID == local {
				return true
			}
			// A fenced source is deliberately omitted from runtime/transport
			// admission on restart. Only its exact durable authority may retry.
			if request.Kind != replicaaction.SourceRetirement {
				return false
			}
			record, readErr := journal.ReadReplicaAction(context.Background(), request.Operation, request.Kind)
			return readErr == nil && (record.State == replicaaction.RetirementAuthorized || record.State == replicaaction.Complete) &&
				replicaaction.SameRequestAuthority(record.Request, request)
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 32,
	})
}
