package splitcontroller

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardcontrol"
)

var (
	ErrShardControlRoute       = errors.New("splitcontroller: invalid exact shard-control route")
	ErrShardControlNotLeader   = errors.New("splitcontroller: shard-control leader changed")
	ErrShardControlUnavailable = errors.New("splitcontroller: shard-control replicas unavailable")
)

const (
	DefaultShardControlAttempts        = gateway.ServingReplicaCount
	AbsoluteMaxShardControlAttempts    = 16
	AbsoluteMaxShardControlHintEntries = 65536
)

// ShardControlRouteResolver returns the catalog-authenticated route for the
// already reconciled action. It is the only target-selection authority: host
// names and DNS results are transport coordinates, never leader evidence.
type ShardControlRouteResolver interface {
	ResolveShardControl(
		context.Context, ShardActionTarget, Action, shardcontrol.Request,
	) (gateway.ReplicatedRoute, error)
}

// ShardControlRoundTripper performs one request against one exact catalog
// endpoint. Implementations must authenticate endpoint.Node at the transport
// boundary and must not reinterpret the request.
type ShardControlRoundTripper interface {
	DoShardControl(context.Context, gateway.ReplicatedEndpoint, shardcontrol.Request) (shardcontrol.Response, error)
}

type ShardControlRouterOptions struct {
	Resolver       ShardControlRouteResolver
	Client         ShardControlRoundTripper
	MaxAttempts    int
	AttemptTimeout time.Duration
	HintCapacity   int
}

type shardControlHintKey struct {
	group      raftmember.GroupKey
	allocation uint64
	command    [7]uint64
}

// RoutedShardControl is the production multi-group ShardControlRouter. It
// validates the complete catalog fence before I/O, scopes warm leader hints to
// an exact group generation, and retries only the byte-identical journaled
// operation. Memory and attempts are hard bounded.
type RoutedShardControl struct {
	resolver ShardControlRouteResolver
	client   ShardControlRoundTripper
	attempts int
	timeout  time.Duration

	hintsMu sync.Mutex
	hints   map[shardControlHintKey]uint64
	order   []shardControlHintKey
	limit   int
	next    int
}

func NewRoutedShardControl(options ShardControlRouterOptions) (*RoutedShardControl, error) {
	attempts := options.MaxAttempts
	if attempts == 0 {
		attempts = DefaultShardControlAttempts
	}
	hints := options.HintCapacity
	if hints == 0 {
		hints = 4096
	}
	if options.Resolver == nil || options.Client == nil || attempts <= 0 ||
		attempts > AbsoluteMaxShardControlAttempts || options.AttemptTimeout <= 0 ||
		options.AttemptTimeout > gateway.AbsoluteMaxReplicatedAttemptTimeout || hints <= 0 ||
		hints > AbsoluteMaxShardControlHintEntries {
		return nil, ErrShardControlRoute
	}
	return &RoutedShardControl{
		resolver: options.Resolver, client: options.Client, attempts: attempts,
		timeout: options.AttemptTimeout, hints: make(map[shardControlHintKey]uint64),
		order: make([]shardControlHintKey, 0, min(hints, 64)), limit: hints,
	}, nil
}

func (router *RoutedShardControl) ExecuteShardControl(
	ctx context.Context, action Action, request shardcontrol.Request,
) (shardcontrol.Response, error) {
	if router == nil || ctx == nil || shardcontrol.Action(action.Kind) != request.Action ||
		action.Child != request.Child {
		return shardcontrol.Response{}, ErrShardControlRoute
	}
	target, err := OpenRemoteActionTarget(request)
	if err != nil {
		return shardcontrol.Response{}, errors.Join(ErrShardControlRoute, err)
	}
	route, err := router.resolver.ResolveShardControl(ctx, target, action, request)
	if err != nil || !validShardControlRoute(route, target) {
		return shardcontrol.Response{}, errors.Join(ErrShardControlRoute, err)
	}
	key := shardControlRouteKey(route)
	start := router.hintedReplica(key, route.Replicas)
	attempts := router.attempts
	if remoteActionTargetsChild(action.Kind) && target.Member != 0 {
		found := false
		for index, replica := range route.Replicas {
			if replica.Member == target.Member {
				start, attempts, found = index, 1, true
				break
			}
		}
		if !found {
			return shardcontrol.Response{}, ErrShardControlRoute
		}
	}
	var joined error
	sawNotLeader := false
	for attempt := 0; attempt < attempts; attempt++ {
		if err = ctx.Err(); err != nil {
			return shardcontrol.Response{}, errors.Join(joined, err)
		}
		endpoint := route.Replicas[(start+attempt)%len(route.Replicas)]
		attemptContext, cancel := context.WithTimeout(ctx, router.timeout)
		response, roundTripErr := router.client.DoShardControl(attemptContext, endpoint, request)
		cancel()
		if roundTripErr != nil {
			joined = errors.Join(joined, roundTripErr)
			continue
		}
		if response.Operation != request.Operation || response.Step != request.Step {
			return shardcontrol.Response{}, errors.Join(ErrShardControlRoute, shardcontrol.ErrWire)
		}
		if response.Code == shardcontrol.ResultNotLeader || response.Code == shardcontrol.ResultRetry {
			if response.Code == shardcontrol.ResultNotLeader {
				sawNotLeader = true
			}
			continue
		}
		if response.Code == shardcontrol.ResultAccepted {
			router.rememberLeader(key, endpoint.Member)
		}
		return response, nil
	}
	if sawNotLeader {
		return shardcontrol.Response{}, errors.Join(ErrShardControlNotLeader, joined)
	}
	return shardcontrol.Response{}, errors.Join(ErrShardControlUnavailable, joined)
}

func validShardControlRoute(route gateway.ReplicatedRoute, target ShardActionTarget) bool {
	if route.Distribution == "" || route.Shard == "" || route.AllocationGeneration == 0 ||
		!route.Command.Valid() || !targetMatchesRoute(target, route) ||
		route.Group.ClusterID == ([16]byte{}) || route.Group.ClusterIncarnation == ([16]byte{}) ||
		route.Group.TopologyRecoveryEpoch == 0 || route.Group.ShardIncarnation == ([16]byte{}) ||
		route.Group.GroupID == ([16]byte{}) || len(route.Replicas) == 0 ||
		len(route.Replicas) > gateway.AbsoluteMaxReplicatedRouteMembers {
		return false
	}
	for index, endpoint := range route.Replicas {
		if endpoint.Member == 0 || endpoint.Node == (rafttransport.NodeID{}) ||
			endpoint.StoreID == ([16]byte{}) || endpoint.NodeIncarnation == 0 ||
			endpoint.ControlEndpoint == "" || endpoint.ControlAddress == "" {
			return false
		}
		for prior := 0; prior < index; prior++ {
			other := route.Replicas[prior]
			if endpoint.Member == other.Member || endpoint.Node == other.Node ||
				endpoint.StoreID == other.StoreID || endpoint.ControlEndpoint == other.ControlEndpoint ||
				endpoint.ControlAddress == other.ControlAddress {
				return false
			}
		}
	}
	return true
}

func shardControlRouteKey(route gateway.ReplicatedRoute) shardControlHintKey {
	return shardControlHintKey{group: route.Group, allocation: route.AllocationGeneration,
		command: [7]uint64{route.Command.ReplicaSetVersion, route.Command.ActivePolicyGeneration,
			route.Command.ProtectionEpoch, route.Command.OwnershipEpoch, route.Command.SchemaGeneration,
			route.Command.RoutingVersion, route.Command.RouteGeneration}}
}

func (router *RoutedShardControl) hintedReplica(
	key shardControlHintKey, replicas []gateway.ReplicatedEndpoint,
) int {
	router.hintsMu.Lock()
	member, ok := router.hints[key]
	router.hintsMu.Unlock()
	if ok {
		for index := range replicas {
			if replicas[index].Member == member {
				return index
			}
		}
	}
	return 0
}

func (router *RoutedShardControl) rememberLeader(key shardControlHintKey, member uint64) {
	router.hintsMu.Lock()
	defer router.hintsMu.Unlock()
	if _, exists := router.hints[key]; exists {
		router.hints[key] = member
		return
	}
	if len(router.hints) < router.limit {
		router.order = append(router.order, key)
		router.hints[key] = member
		return
	}
	delete(router.hints, router.order[router.next])
	router.order[router.next] = key
	router.hints[key] = member
	router.next++
	if router.next == router.limit {
		router.next = 0
	}
}

type AuthenticatedShardControlClientOptions struct {
	TLS               *rafttransport.PeerTLS
	Endpoints         []gateway.ReplicatedEndpoint
	Dial              func(context.Context, string) (net.Conn, error)
	HandshakeDeadline rafttransport.DeadlineFunc
	ProtocolTimeout   time.Duration
	MaxConnections    int
	MaxHandshakes     int
}

// AuthenticatedShardControlClient is one rotation-safe TLS 1.3 dial authority
// shared by every local Raft group. Each control address is pinned to the
// catalog's exact node certificate identity before any protocol bytes flow.
type AuthenticatedShardControlClient struct {
	mu      sync.RWMutex
	secure  *servicetls.Client
	timeout time.Duration
	nodes   map[string]rafttransport.NodeID
}

func NewAuthenticatedShardControlClient(
	options AuthenticatedShardControlClientOptions,
) (*AuthenticatedShardControlClient, error) {
	endpoints, err := controlTLSEndpoints(options.Endpoints)
	if err != nil || options.ProtocolTimeout <= 0 ||
		options.ProtocolTimeout > gateway.AbsoluteMaxReplicatedAttemptTimeout {
		return nil, errors.Join(ErrShardControlRoute, err)
	}
	secure, err := servicetls.NewClient(servicetls.ClientOptions{
		TLS: options.TLS, Class: rafttransport.TrafficShardControl, Endpoints: endpoints,
		Dial: options.Dial, HandshakeDeadline: options.HandshakeDeadline,
		MaxConnections: options.MaxConnections, MaxHandshakes: options.MaxHandshakes,
	})
	if err != nil {
		return nil, errors.Join(ErrShardControlRoute, err)
	}
	return &AuthenticatedShardControlClient{
		secure: secure, timeout: options.ProtocolTimeout, nodes: controlEndpointNodes(endpoints),
	}, nil
}

func (client *AuthenticatedShardControlClient) DoShardControl(
	ctx context.Context, endpoint gateway.ReplicatedEndpoint, request shardcontrol.Request,
) (shardcontrol.Response, error) {
	if client == nil || client.secure == nil || ctx == nil || endpoint.ControlAddress == "" ||
		endpoint.Node == (rafttransport.NodeID{}) {
		return shardcontrol.Response{}, ErrShardControlRoute
	}
	client.mu.RLock()
	defer client.mu.RUnlock()
	expected, configured := client.nodes[endpoint.ControlAddress]
	if !configured || expected != endpoint.Node {
		return shardcontrol.Response{}, ErrShardControlRoute
	}
	protocol, err := shardcontrol.NewClient(client.secure.Dial, endpoint.ControlAddress, client.timeout)
	if err != nil {
		return shardcontrol.Response{}, err
	}
	return protocol.Execute(ctx, request)
}

func (client *AuthenticatedShardControlClient) Rotate(
	profile *rafttransport.PeerTLS, endpoints []gateway.ReplicatedEndpoint,
) error {
	resolved, err := controlTLSEndpoints(endpoints)
	if client == nil || client.secure == nil || err != nil {
		return errors.Join(ErrShardControlRoute, err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if err = client.secure.Rotate(profile, resolved); err != nil {
		return err
	}
	client.nodes = controlEndpointNodes(resolved)
	return nil
}

func controlEndpointNodes(endpoints []servicetls.Endpoint) map[string]rafttransport.NodeID {
	result := make(map[string]rafttransport.NodeID, len(endpoints))
	for _, endpoint := range endpoints {
		result[endpoint.Address] = endpoint.Node
	}
	return result
}

func (client *AuthenticatedShardControlClient) Close() error {
	if client == nil || client.secure == nil {
		return ErrShardControlRoute
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.secure.Close()
}

func controlTLSEndpoints(replicas []gateway.ReplicatedEndpoint) ([]servicetls.Endpoint, error) {
	if len(replicas) == 0 || len(replicas) > servicetls.AbsoluteMaxIdentities {
		return nil, ErrShardControlRoute
	}
	result := make([]servicetls.Endpoint, len(replicas))
	for index, replica := range replicas {
		if replica.ControlAddress == "" || replica.Node == (rafttransport.NodeID{}) {
			return nil, ErrShardControlRoute
		}
		result[index] = servicetls.Endpoint{Address: replica.ControlAddress, Node: replica.Node}
	}
	return result, nil
}
