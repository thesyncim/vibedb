package splitcontroller

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardcontrol"
)

// ControlService is the complete durable split-control endpoint carried by a
// shipped shard-control listener. Controller triggers and fenced shard actions
// deliberately share one replay journal: an operation/step identity has one
// byte-identical result regardless of which peer retries it after a crash.
type ControlService struct {
	server  *shardcontrol.Server
	journal *shardcontrol.JournalExecutor
}

type controlActionDispatcher struct {
	controller *ControllerService
	remote     *RemoteActionService
}

// OpenControlService opens the local durable result boundary before exposing
// either split action class. Both runtimes are mandatory; registering a
// partial service would advertise steps this process cannot safely resume.
func OpenControlService(
	path string,
	limits shardcontrol.JournalLimits,
	grants []shardcontrol.ActionGrant,
	controller *ControllerService,
	remote *RemoteActionService,
) (*ControlService, error) {
	if controller == nil || remote == nil {
		return nil, ErrRemoteExecution
	}
	dispatcher := &controlActionDispatcher{controller: controller, remote: remote}
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

func (dispatcher *controlActionDispatcher) ExecuteAction(
	ctx context.Context,
	peer rafttransport.PeerIdentity,
	request shardcontrol.Request,
) (shardcontrol.Response, error) {
	if dispatcher == nil || dispatcher.controller == nil || dispatcher.remote == nil {
		return shardcontrol.Response{}, ErrRemoteExecution
	}
	if request.Action == shardcontrol.ActionReconcileSplit {
		return dispatcher.controller.ExecuteAction(ctx, peer, request)
	}
	return dispatcher.remote.ExecuteAction(ctx, peer, request)
}

func (service *ControlService) Serve(
	ctx context.Context,
	connection rafttransport.PeerConnection,
) error {
	if service == nil || service.server == nil {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrRemoteExecution
	}
	return service.server.Serve(ctx, connection)
}

func (service *ControlService) Close() error {
	if service == nil || service.journal == nil {
		return nil
	}
	return service.journal.Close()
}
