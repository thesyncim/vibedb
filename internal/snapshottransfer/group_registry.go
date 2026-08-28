package snapshottransfer

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const AbsoluteMaxGroupServices = 64

type GroupDataService struct {
	Group   raftmember.GroupKey
	Service *Service
}

type GroupDataRegistryOptions struct {
	Registry         *rafttransport.StaticRegistry
	Services         []GroupDataService
	ReadDeadline     rafttransport.DeadlineFunc
	MaxConnections   int
	MaxInflightBytes int64
}

// GroupDataRegistry routes a shared authenticated snapshot listener only
// after opening the descriptor's complete group/incarnation identity. It owns
// a node-wide connection and byte budget in addition to each group's service
// budget; registering another group can never multiply the process ceiling.
type GroupDataRegistry struct {
	registry     *rafttransport.StaticRegistry
	services     map[raftmember.GroupKey]*Service
	readDeadline rafttransport.DeadlineFunc
	slots        chan struct{}
	maxInflight  int64
	inflight     atomic.Int64
}

func NewGroupDataRegistry(options GroupDataRegistryOptions) (*GroupDataRegistry, error) {
	if options.Registry == nil || options.ReadDeadline == nil || len(options.Services) == 0 ||
		len(options.Services) > AbsoluteMaxGroupServices || options.MaxConnections <= 0 ||
		options.MaxConnections > 4096 || options.MaxInflightBytes <= 0 ||
		options.MaxInflightBytes > int64(AbsoluteMaxChunkBytes)*int64(options.MaxConnections) {
		return nil, ErrBound
	}
	services := make(map[raftmember.GroupKey]*Service, len(options.Services))
	for _, entry := range options.Services {
		if entry.Service == nil || entry.Service.registry == nil ||
			entry.Service.registry.TrustDomain() != options.Registry.TrustDomain() {
			return nil, ErrBound
		}
		if _, err := options.Registry.LocalMember(entry.Group); err != nil {
			return nil, ErrStaleFence
		}
		if _, duplicate := services[entry.Group]; duplicate {
			return nil, ErrBound
		}
		services[entry.Group] = entry.Service
	}
	return &GroupDataRegistry{registry: options.Registry, services: services,
		readDeadline: options.ReadDeadline, slots: make(chan struct{}, options.MaxConnections),
		maxInflight: options.MaxInflightBytes}, nil
}

func (registry *GroupDataRegistry) Serve(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if registry == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficSnapshot ||
		connection.PeerIdentity().TrustDomain != registry.registry.TrustDomain() {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrStaleFence
	}
	select {
	case registry.slots <- struct{}{}:
		defer func() { <-registry.slots }()
	default:
		_ = connection.Close()
		return ErrBound
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := registry.readDeadline(); deadline.IsZero() {
		_ = connection.Close()
		return ErrBound
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		_ = connection.Close()
		return err
	}
	var request [requestBytes]byte
	if _, err := io.ReadFull(connection, request[:]); err != nil {
		_ = connection.Close()
		return err
	}
	if !bytes.Equal(request[:8], requestMagic[:]) {
		_ = connection.Close()
		return ErrDescriptor
	}
	descriptor, err := OpenDescriptor(request[8 : 8+DescriptorBytes])
	if err != nil {
		_ = connection.Close()
		return err
	}
	service := registry.services[descriptor.Group]
	if service == nil {
		_ = connection.Close()
		return ErrStaleFence
	}
	charge := int64(descriptor.ChunkBytes)
	for {
		current := registry.inflight.Load()
		if charge <= 0 || current > registry.maxInflight-charge {
			_ = connection.Close()
			return ErrBound
		}
		if registry.inflight.CompareAndSwap(current, current+charge) {
			break
		}
	}
	defer registry.inflight.Add(-charge)
	return service.serveRequest(ctx, connection, request)
}

type GroupSourceControlService struct {
	Group   raftmember.GroupKey
	Service *SourceControlService
}

type GroupSourceControlRegistryOptions struct {
	Registry       *rafttransport.StaticRegistry
	Services       []GroupSourceControlService
	ReadDeadline   rafttransport.DeadlineFunc
	MaxConnections int
}

// GroupSourceControlRegistry provides the matching control-plane dispatch.
// The complete fixed request is opened before routing, and the selected
// service independently authenticates source/target incarnation and policy.
type GroupSourceControlRegistry struct {
	registry     *rafttransport.StaticRegistry
	services     map[raftmember.GroupKey]*SourceControlService
	readDeadline rafttransport.DeadlineFunc
	slots        chan struct{}
}

func NewGroupSourceControlRegistry(
	options GroupSourceControlRegistryOptions,
) (*GroupSourceControlRegistry, error) {
	if options.Registry == nil || options.ReadDeadline == nil || len(options.Services) == 0 ||
		len(options.Services) > AbsoluteMaxGroupServices || options.MaxConnections <= 0 ||
		options.MaxConnections > AbsoluteMaxSourceConcurrency {
		return nil, ErrSourceControl
	}
	services := make(map[raftmember.GroupKey]*SourceControlService, len(options.Services))
	for _, entry := range options.Services {
		if entry.Service == nil {
			return nil, ErrSourceControl
		}
		if _, err := options.Registry.LocalMember(entry.Group); err != nil {
			return nil, ErrSourceUnauthorized
		}
		if _, duplicate := services[entry.Group]; duplicate {
			return nil, ErrSourceControl
		}
		services[entry.Group] = entry.Service
	}
	return &GroupSourceControlRegistry{registry: options.Registry, services: services,
		readDeadline: options.ReadDeadline, slots: make(chan struct{}, options.MaxConnections)}, nil
}

func (registry *GroupSourceControlRegistry) Serve(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if registry == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl ||
		connection.PeerIdentity().TrustDomain != registry.registry.TrustDomain() {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrSourceUnauthorized
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
		return ErrSourceControl
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	var raw [SourceControlRequestBytes]byte
	if _, err := io.ReadFull(connection, raw[:]); err != nil {
		return err
	}
	command, request, err := openSourceControlCommand(raw)
	if err != nil {
		return err
	}
	service := registry.services[request.Group]
	if service == nil {
		return ErrSourceUnauthorized
	}
	// The control actor may be a policy-authorized coordinator rather than
	// the learner receiving the artifact. The selected service authenticates
	// that actor and the exact source/target request before journal access.
	// Snapshot data access independently remains bound to the target member.
	localMember, err := registry.registry.LocalMember(request.Group)
	if err != nil || localMember != request.SourceMember {
		return ErrSourceUnauthorized
	}
	if command == sourceControlAbandon {
		var witnessRaw [AbandonmentWitnessBytes]byte
		if _, err = io.ReadFull(connection, witnessRaw[:]); err != nil {
			return err
		}
		witness, witnessErr := OpenAbandonmentWitness(witnessRaw[:])
		if witnessErr != nil {
			return witnessErr
		}
		return service.serveAbandonCommand(ctx, connection, request, witness)
	}
	return service.serveCommand(ctx, connection, command, request)
}
