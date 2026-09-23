package main

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

// Every physical node can receive a certified group, including one it hosted
// and retired earlier. Initial group count does not select a different wire
// protocol, journal or ownership path.
type rf3NodeEnrollment struct {
	runtime   *rf3NodeRuntime
	owner     *rf3NodeOwner
	control   *nodecontrol.Service
	info      *nodecontrol.NodeInfoService
	journal   *nodecontrol.FileJournal
	transport *servicetls.Client
}

func newRF3NodeEnrollment(manifest rf3Manifest, profile *rafttransport.PeerTLS, policy *serviceauthz.Policy,
	gate *serviceauthz.Gate, budget *migrationbudget.Budget, runtime *rf3NodeRuntime,
	owner *rf3NodeOwner, deadline rafttransport.DeadlineFunc,
) (_ *rf3NodeEnrollment, resultErr error) {
	if runtime == nil || owner == nil || profile == nil || policy == nil {
		return nil, nodecontrol.ErrControl
	}
	template, err := rf3NodePreparationTemplateFromManifest(manifest)
	if err != nil {
		return nil, err
	}
	// The authenticated physical log owns its key-provider metadata. Bootstrap
	// manifests for an existing node need not retain a second copy of it.
	key, err := loadRF3WALKey(manifest.NodeLog.KeyID, manifest.NodeLog.KeyMaterialPath)
	if err != nil {
		return nil, err
	}
	wrapped, err := owner.store.AuthenticatedWrappedKeyMetadata(key)
	clear(key.Material[:])
	if err != nil {
		return nil, err
	}
	template.WAL.WrappedKey = string(wrapped)
	result := &rf3NodeEnrollment{runtime: runtime, owner: owner}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, result.Close())
		}
	}()
	local := profile.LocalIdentity()
	runtime.reader = new(nodecontrol.IntentReaderSlot)
	result.transport, err = bindRF3NodeBootstrapIntentReader(runtime.reader, profile, manifest.GatewaySeeds,
		local.Node, manifest.NodeIncarnation, deadline)
	if err != nil {
		return nil, err
	}
	result.journal, err = nodecontrol.NewFileJournal(filepath.Join(manifest.ReplicaControl.SourceDataRoot, "node-control-journal"))
	if err != nil {
		return nil, err
	}
	runtime.receivers, err = newRF3DynamicBootstrapRegistry(local.TrustDomain, deadline, 32)
	if err != nil {
		return nil, err
	}
	preparer := &rf3NodeControlPreparer{NodeRoot: manifest.ReplicaControl.SourceDataRoot, Template: template}
	adopter := &rf3NodeControlAdopter{NodeRoot: manifest.ReplicaControl.SourceDataRoot, ActivateReceiver: runtime.receivers.Activate}
	result.control, err = nodecontrol.NewService(nodecontrol.ServiceOptions{
		Reader: runtime.reader, Journal: result.journal, Preparer: preparer, Adopter: adopter,
		Authorize: func(identity rafttransport.PeerIdentity, request nodecontrol.Request) bool {
			return identity.TrustDomain == local.TrustDomain && request.TargetNode == local.Node &&
				request.TargetNodeIncarnation == manifest.NodeIncarnation &&
				(policy.Check(identity.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow ||
					policy.Check(identity.Node, serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow)
		},
		ValidatePayload: func(ctx context.Context, intent gateway.GroupEnrollmentIntent, payload []byte) error {
			_, err := validateRF3EnrollmentPayload(ctx, intent, payload, manifest.ReplicaControl.SourceDataRoot, template)
			return err
		},
		LocalNode: local.Node, LocalIncarnation: manifest.NodeIncarnation,
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 32,
	})
	if err != nil {
		return nil, err
	}
	result.info, err = newRF3EmptyNodeInfo(owner.store, profile, manifest, policy, budget, runtime.servingGroups, preparer, deadline)
	if err != nil {
		return nil, err
	}
	runtime.learner, err = newRF3DynamicLearnerFactory(runtime, owner, manifest, profile, policy, gate, budget, deadline)
	if err != nil {
		return nil, err
	}
	if err = runtime.receivers.BindRegistrar(runtime.RegisterBootstrapService); err != nil {
		return nil, err
	}
	if err = owner.bindEmptyRuntime(runtime); err != nil {
		return nil, err
	}
	return result, nil
}

func (enrollment *rf3NodeEnrollment) Close() error {
	if enrollment == nil {
		return nil
	}
	if enrollment.owner != nil {
		enrollment.owner.unbindEmptyRuntime(enrollment.runtime)
	}
	var err error
	if enrollment.runtime != nil {
		err = enrollment.runtime.CloseBootstrapServices()
	}
	if enrollment.journal != nil {
		err = errors.Join(err, enrollment.journal.Close())
	}
	if enrollment.transport != nil {
		err = errors.Join(err, enrollment.transport.Close())
	}
	return err
}
