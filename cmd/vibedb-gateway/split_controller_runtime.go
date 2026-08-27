package main

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

const (
	gatewaySplitRouteCapacity        = 4096
	gatewaySplitLeaderHintCapacity   = 4096
	gatewaySplitAdmissionAttempts    = 3
	gatewaySplitObservationAttempts  = 3
	gatewaySplitAdmissionConcurrency = 8
)

// gatewayServingSplitRuntime is the shipped gateway-owned split controller.
// The replica-control manifest supplies only authenticated transport
// coordinates. Catalog RF3 and each admitted PlanIntent remain the route and
// topology authorities.
type gatewayServingSplitRuntime struct {
	controller *splitcontroller.ControllerService
	client     *splitcontroller.AuthenticatedShardControlClient
}

type gatewayServingSplitOptions struct {
	catalog     *gateway.ReplicatedCatalogAuthority
	drain       splitcontroller.ClusterCatalogDrainCertifier
	opener      splitcontroller.PlanObservationStreamOpener
	tls         *rafttransport.PeerTLS
	shards      []gateway.ReplicatedEndpoint
	dial        func(context.Context, string) (net.Conn, error)
	handshake   rafttransport.DeadlineFunc
	read        rafttransport.DeadlineFunc
	write       rafttransport.DeadlineFunc
	protocol    time.Duration
	connections int
	handshakes  int
}

func newGatewayServingSplitRuntime(
	options gatewayServingSplitOptions,
) (*gatewayServingSplitRuntime, error) {
	if options.catalog == nil || options.drain == nil || options.opener == nil ||
		options.tls == nil || len(options.shards) == 0 || options.dial == nil ||
		options.handshake == nil || options.read == nil || options.write == nil ||
		options.protocol <= 0 || options.connections <= 0 || options.handshakes <= 0 {
		return nil, splitcontroller.ErrControllerTrigger
	}

	directory, err := splitcontroller.NewPreparedPlanObservationPeerDirectory(options.catalog)
	if err != nil {
		return nil, err
	}
	observations, err := splitcontroller.NewNetworkPlanObservationClient(
		splitcontroller.NetworkPlanObservationClientOptions{
			Opener: options.opener, Directory: directory,
			ReadDeadline: options.read, WriteDeadline: options.write,
			MaxConcurrent:    autosplit.MaxSplitChildren,
			MaxResponseBytes: splitcontroller.MaxPlanObservationResponseBytes,
		},
	)
	if err != nil {
		return nil, err
	}
	observer, err := splitcontroller.NewCoherentPlanObserver(splitcontroller.CoherentPlanObserverOptions{
		Catalog: options.catalog, Observations: observations,
		CatalogDrain:  splitcontroller.ClusterPlanCatalogDrainAuthority{Certifier: options.drain},
		MaxConcurrent: autosplit.MaxSplitChildren, MaxAttempts: gatewaySplitObservationAttempts,
	})
	if err != nil {
		return nil, err
	}
	routes, err := splitcontroller.NewDynamicShardActionRoutes(gatewaySplitRouteCapacity)
	if err != nil {
		return nil, err
	}

	admissionConcurrency := min(options.connections, gatewaySplitAdmissionConcurrency)
	admission, err := splitcontroller.NewPlanAdmissionClient(splitcontroller.PlanAdmissionClientOptions{
		Opener: options.opener, ReadDeadline: options.read, WriteDeadline: options.write,
		MaxConcurrent:    admissionConcurrency,
		MaxInflightBytes: uint64(splitcontroller.MaxPlanAdmissionRequestBytes) * uint64(admissionConcurrency),
	})
	if err != nil {
		return nil, err
	}
	coordinator, err := splitcontroller.NewRF3PlanAdmissionCoordinator(
		splitcontroller.RF3PlanAdmissionCoordinatorOptions{
			Client: admission, Routes: routes, MaxConcurrent: admissionConcurrency,
			MaxAttempts: gatewaySplitAdmissionAttempts,
		},
	)
	if err != nil {
		return nil, err
	}
	terminalClient, err := splitcontroller.NewTerminalRetirementClient(
		options.opener, options.read, options.write,
	)
	if err != nil {
		return nil, err
	}
	terminal, err := splitcontroller.NewRF3TerminalRetirementCoordinator(
		terminalClient, gatewaySplitAdmissionAttempts,
	)
	if err != nil {
		return nil, err
	}

	client, err := splitcontroller.NewAuthenticatedShardControlClient(
		splitcontroller.AuthenticatedShardControlClientOptions{
			TLS: options.tls, Endpoints: options.shards, Dial: options.dial,
			HandshakeDeadline: options.handshake, ProtocolTimeout: options.protocol,
			MaxConnections: options.connections, MaxHandshakes: options.handshakes,
		},
	)
	if err != nil {
		return nil, err
	}
	router, err := splitcontroller.NewRoutedShardControl(splitcontroller.ShardControlRouterOptions{
		Resolver: routes, Client: client, MaxAttempts: gateway.ServingReplicaCount,
		AttemptTimeout: options.protocol, HintCapacity: gatewaySplitLeaderHintCapacity,
	})
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	controller, err := splitcontroller.NewServingControllerService(
		options.catalog, observer, router, coordinator,
		splitcontroller.CatalogGatewaySplitActions{Authority: options.catalog, Terminal: terminal},
	)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return &gatewayServingSplitRuntime{controller: controller, client: client}, nil
}

func (runtime *gatewayServingSplitRuntime) Close() error {
	if runtime == nil || runtime.client == nil {
		return nil
	}
	return runtime.client.Close()
}

func runServingSplitController(
	ctx context.Context,
	directory splitcontroller.ControllerDirectory,
	controller *splitcontroller.ControllerService,
	interval time.Duration,
	logf func(string, ...any),
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		pass, err := splitcontroller.RunDirectControllerPass(ctx, directory, controller)
		if err != nil && !errors.Is(err, context.Canceled) {
			logf("gateway: split controller: %v", err)
		} else if pass.Triggered != 0 {
			logf("gateway: split controller advanced %d/%d operation(s), completed %d",
				pass.Triggered, pass.Discovered, pass.Completed)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
