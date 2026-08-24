package gateway

import (
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

// NativeGatewayOptions composes an immutable-catalog route with the SQL-free
// replicated executor. Resolver is a schema-generation-matched dense relation
// resolver; this boundary never accepts SQL text or table names.
type NativeGatewayOptions struct {
	Catalog     *CatalogHolder
	Client      ReplicatedRoundTripper
	MaxAttempts int
	// AttemptTimeout bounds each individual probe or proposal exchange. The
	// caller context may impose a shorter end-to-end deadline.
	AttemptTimeout time.Duration
	Resolver       BundleResolver

	MaxRelationBatches  int
	MaxMutations        int
	InitialCommandBytes int
	MaxCommandBytes     int
}

// NativeGateway is the in-process construction primitive for byte-native RF3
// sessions. Each session pins one immutable catalog generation and exact route
// for its lifecycle; catalog movement cannot silently rewrite an unknown
// command or session identity.
type NativeGateway struct {
	catalog  *CatalogHolder
	executor *ReplicatedExecutor
	resolver BundleResolver

	maxRelationBatches  int
	maxMutations        int
	initialCommandBytes int
	maxCommandBytes     int
}

// NativeSessionRequest supplies only client identity and a catalog position.
// Distribution and Shard are cold catalog keys inherited by the existing
// replicated command grammar; relation/table names never cross this API.
type NativeSessionRequest struct {
	Distribution distribution.DistributionName
	Shard        distribution.ShardID
	Tenant       []byte
	ClientID     replication.ID128
	RetryHome    replication.RetryHome
}

func NewNativeGateway(options NativeGatewayOptions) (*NativeGateway, error) {
	if options.Catalog == nil || options.Catalog.Current() == nil || options.Resolver == nil {
		return nil, ErrNativeSession
	}
	executor, err := NewReplicatedExecutor(
		options.Client, options.MaxAttempts, options.AttemptTimeout,
	)
	if err != nil {
		return nil, err
	}
	return &NativeGateway{
		catalog: options.Catalog, executor: executor, resolver: options.Resolver,
		maxRelationBatches: options.MaxRelationBatches, maxMutations: options.MaxMutations,
		initialCommandBytes: options.InitialCommandBytes, maxCommandBytes: options.MaxCommandBytes,
	}, nil
}

// NewSession pins the current exact RF3 catalog route and returns its native
// Put/Delete/session command capability. A position without RF3 coordinates is
// refused; it never falls back to the local SQL write path.
func (gateway *NativeGateway) NewSession(request NativeSessionRequest) (*NativeSession, error) {
	distributionName := string(request.Distribution)
	shardID := string(request.Shard)
	if gateway == nil || gateway.catalog == nil ||
		!validNativeSessionIdentity(distributionName, shardID, request.Tenant) ||
		request.ClientID == (replication.ID128{}) {
		return nil, ErrNativeSession
	}
	snapshot := gateway.catalog.Current()
	if snapshot == nil {
		return nil, ErrNativeSession
	}
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute(
		request.Distribution, request.Shard, replicas[:0],
	)
	if !ok {
		return nil, ErrReplicatedRoute
	}
	return NewNativeSession(NativeSessionOptions{
		Executor: gateway.executor, Route: route,
		Distribution: distributionName, Shard: shardID,
		Tenant: request.Tenant, ClientID: request.ClientID, RetryHome: request.RetryHome,
		Resolver:            gateway.resolver,
		MaxRelationBatches:  gateway.maxRelationBatches,
		MaxMutations:        gateway.maxMutations,
		InitialCommandBytes: gateway.initialCommandBytes,
		MaxCommandBytes:     gateway.maxCommandBytes,
	})
}
