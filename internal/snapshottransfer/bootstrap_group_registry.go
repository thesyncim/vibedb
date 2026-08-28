package snapshottransfer

import (
	"context"
	"io"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type GroupBootstrapControlService struct {
	Group   raftmember.GroupKey
	Service *BootstrapControlService
}

type GroupBootstrapControlRegistryOptions struct {
	TrustDomain    rafttransport.TrustDomain
	Services       []GroupBootstrapControlService
	ReadDeadline   rafttransport.DeadlineFunc
	MaxConnections int
	Complete       func(raftmember.GroupKey)
}

// GroupBootstrapControlRegistry routes one shared cold-target control listener
// after opening the complete fixed request. Each selected service still owns
// exact source/target/store/incarnation authorization and durable idempotency;
// this registry contributes a process-wide connection ceiling.
type GroupBootstrapControlRegistry struct {
	trustDomain  rafttransport.TrustDomain
	services     map[raftmember.GroupKey]*BootstrapControlService
	readDeadline rafttransport.DeadlineFunc
	slots        chan struct{}
	complete     func(raftmember.GroupKey)
}

func NewGroupBootstrapControlRegistry(
	options GroupBootstrapControlRegistryOptions,
) (*GroupBootstrapControlRegistry, error) {
	if options.TrustDomain == (rafttransport.TrustDomain{}) || options.ReadDeadline == nil ||
		len(options.Services) == 0 || len(options.Services) > AbsoluteMaxGroupServices ||
		options.MaxConnections <= 0 || options.MaxConnections > AbsoluteMaxBootstrapConcurrency {
		return nil, ErrBootstrapControl
	}
	services := make(map[raftmember.GroupKey]*BootstrapControlService, len(options.Services))
	for _, entry := range options.Services {
		if entry.Group == (raftmember.GroupKey{}) || entry.Service == nil {
			return nil, ErrBootstrapControl
		}
		want := rafttransport.TrustDomain{ClusterID: entry.Group.ClusterID,
			ClusterIncarnation: entry.Group.ClusterIncarnation}
		if want != options.TrustDomain {
			return nil, ErrBootstrapUnauthorized
		}
		if _, duplicate := services[entry.Group]; duplicate {
			return nil, ErrBootstrapControl
		}
		services[entry.Group] = entry.Service
	}
	return &GroupBootstrapControlRegistry{trustDomain: options.TrustDomain,
		services: services, readDeadline: options.ReadDeadline,
		slots: make(chan struct{}, options.MaxConnections), complete: options.Complete}, nil
}

func (registry *GroupBootstrapControlRegistry) Serve(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if registry == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl ||
		connection.PeerIdentity().TrustDomain != registry.trustDomain {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrBootstrapUnauthorized
	}
	defer connection.Close()
	select {
	case registry.slots <- struct{}{}:
		defer func() { <-registry.slots }()
	default:
		return ErrBound
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := registry.readDeadline(); deadline.IsZero() {
		return ErrBootstrapControl
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	var raw [BootstrapRequestBytes]byte
	if _, err := io.ReadFull(connection, raw[:]); err != nil {
		return err
	}
	request, err := OpenBootstrapRequest(raw[:])
	if err != nil {
		return err
	}
	service := registry.services[request.Descriptor.Group]
	if service == nil {
		return ErrBootstrapUnauthorized
	}
	if err = service.serveRequest(ctx, connection, request); err != nil {
		return err
	}
	if registry.complete != nil {
		registry.complete(request.Descriptor.Group)
	}
	return nil
}
