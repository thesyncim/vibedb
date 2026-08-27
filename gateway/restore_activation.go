package gateway

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var ErrRestoreActivation = errors.New("gateway: restore activation denied or failed")

// RestoreCatalog is the fresh target catalog RF3 boundary. Proposal must use
// the dedicated restore-activation capability; Observe must be a linearizable
// read of the settled witness, never a local echo of the proposal result.
type RestoreCatalog interface {
	clusterrestore.CatalogProposer
	ObserveRestoreActivation(context.Context, [32]byte) (clusterrestore.CatalogWitness, error)
}

type RestoreActivationOptions struct {
	Root      string
	Staging   *clusterbackup.RestoreStagingRoot
	Operation clusterrestore.Operation
	Installer clusterrestore.GroupInstaller
	Catalog   RestoreCatalog
	Gate      *serviceauthz.Gate
	Operator  serviceauthz.Authority
	Fault     func(clusterrestore.FaultPoint) error
}

// ActivateRestore drives verified staging through authority-free RF3 root
// construction, replicated catalog publication, and a separate linearizable
// catalog observation. Only the returned authority can open shard serving.
func ActivateRestore(ctx context.Context, options RestoreActivationOptions) (
	*clusterrestore.ServingAuthority, clusterrestore.ServingPermit, error,
) {
	if ctx == nil || options.Gate == nil || options.Catalog == nil || !options.Operator.Valid() ||
		options.Gate.CheckAuthority(options.Operator,
			serviceauthz.CapabilityRestoreActivate) != serviceauthz.DecisionAllow {
		return nil, clusterrestore.ServingPermit{}, ErrRestoreActivation
	}
	bound, err := serviceauthz.WithAuthority(ctx, options.Operator)
	if err != nil {
		return nil, clusterrestore.ServingPermit{}, errors.Join(ErrRestoreActivation, err)
	}
	activation, err := clusterrestore.ActivateComplete(bound, clusterrestore.Options{
		Root: options.Root, Staging: options.Staging, Operation: options.Operation,
		Installer: options.Installer,
		Catalog:   clusterrestore.ReplicatedCatalogPublisher{Proposer: options.Catalog},
		Fault:     options.Fault,
	})
	if err != nil {
		return nil, clusterrestore.ServingPermit{}, errors.Join(ErrRestoreActivation, err)
	}
	observed, err := options.Catalog.ObserveRestoreActivation(bound, options.Operation.Digest)
	if err != nil {
		return nil, clusterrestore.ServingPermit{}, errors.Join(ErrRestoreActivation, err)
	}
	authority, err := activation.AuthorizeServing(observed)
	if err != nil {
		return nil, clusterrestore.ServingPermit{}, errors.Join(ErrRestoreActivation, err)
	}
	return authority, activation.Permit, nil
}
