package splitcontroller

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardcontrol"
)

// ControlService is the durable fenced-action endpoint carried by a shipped
// shard-control listener. Catalog reconciliation is deliberately absent: the
// gateway that owns the replicated catalog runs ControllerService directly.
type ControlService struct {
	server                        *shardcontrol.Server
	journal                       *shardcontrol.JournalExecutor
	requests, completions, faults atomic.Uint64
}

type ControlMetrics struct{ Requests, Completions, Faults uint64 }

func (service *ControlService) Metrics() ControlMetrics {
	if service == nil {
		return ControlMetrics{}
	}
	return ControlMetrics{Requests: service.requests.Load(), Completions: service.completions.Load(), Faults: service.faults.Load()}
}

type controlActionDispatcher struct {
	remote shardcontrol.ActionExecutor
}

// OpenShardControlService opens the shard-local durable result boundary. A
// shard accepts only ordinary fenced actions after its plan-admission runtime
// has independently reconstructed the exact admitted intent.
func OpenShardControlService(
	path string,
	limits shardcontrol.JournalLimits,
	grants []shardcontrol.ActionGrant,
	remote shardcontrol.ActionExecutor,
) (*ControlService, error) {
	if remote == nil {
		return nil, ErrRemoteExecution
	}
	dispatcher := &controlActionDispatcher{remote: remote}
	journal, err := shardcontrol.OpenJournalExecutor(path, limits, dispatcher)
	if err != nil {
		return nil, err
	}
	authorizer, err := shardcontrol.NewAuthorizer(grants)
	if err != nil {
		return nil, errors.Join(err, journal.Close())
	}
	server, err := shardcontrol.NewServer(authorizer, journal)
	if err != nil {
		return nil, errors.Join(err, journal.Close())
	}
	return &ControlService{server: server, journal: journal}, nil
}

// OpenControlService is retained as a source-compatible construction guard
// for internal callers while composition migrates. ControllerService is not
// installed in the shard dispatcher; passing it only proves the caller has
// not silently omitted the gateway authority during the transition.
func OpenControlService(
	path string,
	limits shardcontrol.JournalLimits,
	grants []shardcontrol.ActionGrant,
	controller *ControllerService,
	remote shardcontrol.ActionExecutor,
) (*ControlService, error) {
	if controller == nil {
		return nil, ErrRemoteExecution
	}
	return OpenShardControlService(path, limits, grants, remote)
}

func (dispatcher *controlActionDispatcher) ExecuteAction(
	ctx context.Context,
	peer rafttransport.PeerIdentity,
	request shardcontrol.Request,
) (shardcontrol.Response, error) {
	if dispatcher == nil || dispatcher.remote == nil ||
		request.Action == shardcontrol.ActionReconcileSplit {
		return shardcontrol.Response{}, ErrRemoteExecution
	}
	return dispatcher.remote.ExecuteAction(ctx, peer, request)
}

func (service *ControlService) Serve(
	ctx context.Context,
	connection rafttransport.PeerConnection,
) (resultErr error) {
	if service == nil || service.server == nil {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrRemoteExecution
	}
	service.requests.Add(1)
	defer func() {
		if resultErr != nil {
			service.faults.Add(1)
		} else {
			service.completions.Add(1)
		}
	}()
	return service.server.Serve(ctx, connection)
}

func (service *ControlService) Close() error {
	if service == nil || service.journal == nil {
		return nil
	}
	return service.journal.Close()
}
