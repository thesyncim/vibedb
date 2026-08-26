package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	"github.com/thesyncim/vibedb/shardcontrol"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

// maxServeRequestBytes bounds one newline-delimited JSON envelope before JSON
// decoding or SQL parsing. Scanner grows only for an actually large request and
// releases the buffer with the connection.
const (
	maxServeRequestBytes              = 1 << 20
	defaultNativeResponseWriteTimeout = 5 * time.Second
	defaultRF3TransactionConcurrency  = 64
	defaultRF3TransactionInFlight     = uint64(64 << 20)
	defaultRF3TransactionRequests     = 65_536
	defaultRF3TransactionRecovery     = 24 * time.Hour
	defaultRF3TransactionMutations    = uint64(10_000_000)
	defaultRF3TransactionBytes        = uint64(1 << 30)
)

// The serve subcommand is a routing front-end. It loads an immutable
// catalog generation, refreshes the atomically replaced catalog file after a
// shard reports stale routing metadata, and accepts newline-delimited JSON
// requests over a connection. Each request routes and dispatches against the
// pinned generation: a bounded distributed read by default, one colocated
// single-shard write for exec, or an atomic fixed-participant write batch for
// exec_batch. The reply is the merged
// result. The wire form is a minimal JSON envelope; a request
// carries SQL, typed parameters, and an operational class. The pinned catalog
// and shared SQL planner derive placement, shard constraints, merge order, and
// the global limit. The envelope itself is decoded and emitted with vibejson.

// serveRequest is one query envelope a client sends. SQL and its typed
// parameters are the only semantic inputs; clients cannot override routing or
// merge metadata independently of the statement.
type serveRequest struct {
	// Op selects the gateway operation: the empty value and "query" are the
	// read path; "read_batch" is the RF3 exact-point SQL vector; "exec" is the
	// single-shard write path; "exec_batch" uses Statements and applies one
	// Class to the complete atomic batch.
	Op string `json:"op,omitempty"`
	// RequestID is the caller's fixed 128-bit hexadecimal idempotency key for
	// an RF3 exec_batch. It remains ingress metadata and never enters Raft as a
	// table or SQL string.
	RequestID string `json:"request_id,omitempty"`
	SQL       string `json:"sql"`
	Class     string `json:"class,omitempty"`
	// MaxResultBytes is required by read_batch and bounds its complete JSON
	// success response, including documents and the per-group observation vector.
	MaxResultBytes uint32           `json:"max_result_bytes,omitempty"`
	Params         []serveParam     `json:"params,omitempty"`
	Statements     []serveStatement `json:"statements,omitempty"`
}

type serveStatement struct {
	SQL    string       `json:"sql"`
	Params []serveParam `json:"params,omitempty"`
}

// serveParam is one typed bound parameter in placeholder order.
type serveParam struct {
	Kind string `json:"kind"`
	Bool bool   `json:"bool,omitempty"`
	Text string `json:"text,omitempty"`
}

// serveResponse is the merged reply plus the routing metadata a client reads for
// observability. Rows carries each cell as raw JSON (a null cell is the JSON
// literal null); Error is set instead when the operation failed.
type serveResponse struct {
	Kind           string            `json:"kind,omitempty"`
	Columns        []string          `json:"columns,omitempty"`
	Rows           [][]serveRawValue `json:"rows,omitempty"`
	RowsAffected   int64             `json:"rows_affected,omitempty"`
	Route          string            `json:"route,omitempty"`
	Generation     uint64            `json:"generation,omitempty"`
	ShardsFanned   int               `json:"shards_fanned,omitempty"`
	Retries        int               `json:"retries,omitempty"`
	TransactionID  replication.ID128 `json:"-"`
	Committed      bool              `json:"committed,omitempty"`
	OutcomeUnknown bool              `json:"outcome_unknown,omitempty"`
	Error          string            `json:"error,omitempty"`
}

var errServeResponseTransactionState = errors.New(
	"vibedb-gateway: invalid transaction response state",
)

// serveRawValue is one already-encoded JSON cell. The methods preserve test and
// client interoperability with encoding/json without using it in the server;
// production output writes the bytes directly through vibejson.Writer.
type serveRawValue []byte

func (r serveRawValue) MarshalJSON() ([]byte, error) { return r, nil }

func (r *serveRawValue) UnmarshalJSON(src []byte) error {
	*r = append((*r)[:0], src...)
	return nil
}

type repeatedFlag []string

func (values *repeatedFlag) String() string { return strings.Join(*values, ",") }
func (values *repeatedFlag) Set(value string) error {
	if value == "" || len(*values) >= servicetls.AbsoluteMaxIdentities {
		return servicetls.ErrInvalidProfile
	}
	*values = append(*values, value)
	return nil
}

// runServe loads the catalog, binds the listener, and serves until interrupted.
func runServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	catalog := fs.String("catalog", "", "path to the persisted catalog generation")
	devStaticCatalog := fs.Bool("dev-static-catalog", false, "explicitly use the local catalog file as development authority")
	catalogRelation := fs.Uint("catalog-relation", 0, "authenticated relation ID storing catalog and operation records")
	catalogAttempts := fs.Int("catalog-attempts", 8, "bounded leader-routing attempts for replicated catalog operations")
	catalogAttemptTimeout := fs.Duration("catalog-attempt-timeout", 5*time.Second, "per-endpoint replicated catalog attempt deadline")
	catalogSessionJournal := fs.String("catalog-session-journal", "", "durable native controller session journal base path")
	catalogClientID := fs.String("catalog-client-id", "", "stable 32-hex-character controller client identity")
	catalogRetryHome := fs.String("catalog-retry-home", "", "stable 16-hex-character controller retry-home identity")
	catalogSessionLease := fs.Duration("catalog-session-lease", 24*time.Hour, "monotonic controller session renewal interval")
	controllerInterval := fs.Duration("controller-interval", time.Second, "bounded replicated split reconciliation interval")
	hotShardCapacity := fs.String("hot-shard-capacity", "", "strict canonical vibejson provisioned pressure capacities")
	hotShardInterval := fs.Duration("hot-shard-interval", time.Second, "pressure-window publication cadence; not correctness authority")
	replicaControlManifestPath := fs.String("replica-control-manifest", "", "strict canonical vibejson replica-control topology and bounds")
	listen := fs.String("listen", "127.0.0.1:0", "host:port to serve on")
	devPlaintext := fs.Bool("dev-plaintext-loopback", false, "explicitly permit unauthenticated loopback development serving")
	tlsCertificate := fs.String("tls-certificate", "", "PEM gateway certificate chain")
	tlsKey := fs.String("tls-key", "", "PEM gateway private key")
	tlsRoots := fs.String("tls-roots", "", "PEM client trust roots")
	tlsIdentityOID := fs.String("tls-identity-oid", "", "operator VibeDB identity OID")
	tlsHandshakeTimeout := fs.Duration("tls-handshake-timeout", 5*time.Second, "hard TLS handshake deadline")
	maxConnections := fs.Int("max-client-connections", 1024, "hard authenticated client connection bound")
	maxHandshakes := fs.Int("max-client-handshakes", 64, "hard concurrent TLS handshake bound")
	authorizationPolicy := fs.String("authorization-policy", "", "bounded vibejson principal/capability policy")
	var shardPeers repeatedFlag
	fs.Var(&shardPeers, "shard-peer", "authenticated shard address=32-character-hex-NodeID; repeat for each endpoint")
	maxShardConnections := fs.Int("max-shard-connections-per-pool", 4096, "hard connection bound for each authenticated SQL and RF3 shard pool; transient control pools cap this at 8")
	maxShardHandshakes := fs.Int("max-shard-handshakes-per-pool", 64, "hard concurrent TLS handshake bound for each authenticated SQL and RF3 shard pool; transient control pools cap this at 8")
	maxNativeReadConcurrency := fs.Int("max-native-read-concurrency", gateway.DefaultReplicatedReadConcurrency, "hard concurrent public RF3 point-read bound")
	maxNativeReadBytes := fs.Uint64("max-native-read-bytes", gateway.DefaultReplicatedReadInFlight, "hard aggregate schema-bounded public RF3 response-byte reservation")
	maxNativeScatterConcurrency := fs.Int("max-native-scatter-concurrency", gateway.DefaultReplicatedScatterConcurrency, "hard concurrent RF3 shard-group reads; requests may contain more groups and drain through this bound")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *catalog == "" {
		usage()
		return 2
	}
	if *devStaticCatalog {
		if !*devPlaintext || *catalogRelation != 0 || *hotShardCapacity != "" {
			fmt.Fprintln(os.Stderr, "gateway: static catalog is an explicit plaintext development mode")
			return 2
		}
	} else if *catalogRelation == 0 || *catalogRelation > uint(replication.MaxRelationID) || *catalogAttempts <= 0 ||
		*catalogAttemptTimeout <= 0 || *catalogSessionJournal == "" || *controllerInterval <= 0 ||
		*catalogSessionLease <= 0 || *hotShardInterval <= 0 ||
		len(*catalogClientID) != 32 || len(*catalogRetryHome) != 16 {
		fmt.Fprintln(os.Stderr, "gateway: replicated catalog relation, identities, journal, and positive bounds are required")
		return 2
	}
	var clientTLS *gateway.ClientTLS
	var shardTLS *servicetls.Client
	var tlsProfile *rafttransport.PeerTLS
	var internalAuthority serviceauthz.Authority
	var authorization *serviceauthz.Policy
	var replicaControlManifest *gatewayReplicaControlManifest
	if *devPlaintext {
		if *tlsCertificate != "" || *tlsKey != "" || *tlsRoots != "" || *tlsIdentityOID != "" ||
			*authorizationPolicy != "" || len(shardPeers) != 0 {
			fmt.Fprintln(os.Stderr, "gateway: development plaintext and TLS configuration are mutually exclusive")
			return 2
		}
		if err := requireLoopbackListen(*listen); err != nil {
			fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
			return 2
		}
		// The loopback-only development transport skips policy enforcement, but
		// still emits the one canonical classified replicated request shape.
		internalAuthority.Node[0] = 1
		internalAuthority.Generation = 1
	} else {
		profile, err := servicetls.LoadProfile(*tlsCertificate, *tlsKey, *tlsRoots, *tlsIdentityOID, time.Now)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gateway: load TLS profile: %v\n", err)
			return 2
		}
		tlsProfile = profile
		policy, policyErr := serviceauthz.LoadFile(*authorizationPolicy)
		if policyErr != nil {
			fmt.Fprintf(os.Stderr, "gateway: load authorization policy: %v\n", policyErr)
			return 2
		}
		internalAuthority = serviceauthz.Authority{
			Node: profile.LocalIdentity().Node, Generation: policy.Generation(),
		}
		authorization = policy
		if policy.Check(internalAuthority.Node,
			serviceauthz.CapabilityDataRead|serviceauthz.CapabilityDataWrite|
				serviceauthz.CapabilityDelegate|serviceauthz.CapabilityTopology|
				serviceauthz.CapabilityTransactionRecovery|
				serviceauthz.CapabilityRequestLedger) != serviceauthz.DecisionAllow {
			fmt.Fprintln(os.Stderr, "gateway: local TLS identity lacks delegate, data_read, data_write, topology, transaction_recovery, and request_ledger authority")
			return 2
		}
		clientTLS, err = gateway.NewAuthorizedClientTLS(profile, policy)
		if err != nil || *tlsHandshakeTimeout <= 0 || *maxConnections <= 0 ||
			*maxConnections > servicetls.AbsoluteMaxConnections || *maxHandshakes <= 0 ||
			*maxHandshakes > *maxConnections {
			fmt.Fprintf(os.Stderr, "gateway: invalid authenticated listener profile: %v\n", err)
			return 2
		}
		endpoints := make([]servicetls.Endpoint, len(shardPeers))
		for index, encoded := range shardPeers {
			separator := strings.LastIndexByte(encoded, '=')
			if separator <= 0 || separator == len(encoded)-1 {
				fmt.Fprintf(os.Stderr, "gateway: shard peer %d is not address=node-id\n", index)
				return 2
			}
			endpoints[index].Address = encoded[:separator]
			endpoints[index].Node, err = servicetls.ParseNodeID(encoded[separator+1:])
			if err != nil {
				fmt.Fprintf(os.Stderr, "gateway: shard peer %d: %v\n", index, err)
				return 2
			}
		}
		shardTLS, err = servicetls.NewClient(servicetls.ClientOptions{
			TLS: profile, Class: rafttransport.TrafficShardSQL, Endpoints: endpoints,
			Dial: func(ctx context.Context, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", address)
			},
			HandshakeDeadline: servicetls.FixedDeadline(*tlsHandshakeTimeout),
			MaxConnections:    *maxShardConnections, MaxHandshakes: *maxShardHandshakes,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "gateway: invalid authenticated shard transport: %v\n", err)
			return 2
		}
		defer shardTLS.Close()
		if *replicaControlManifestPath != "" {
			manifest, manifestErr := loadGatewayReplicaControlManifest(
				*replicaControlManifestPath, profile.LocalIdentity().Node,
			)
			if manifestErr != nil || manifest.TLS != (gatewayReplicaTLSReferences{
				Certificate: *tlsCertificate, Key: *tlsKey, Roots: *tlsRoots,
				IdentityOID: *tlsIdentityOID, AuthorizationPolicy: *authorizationPolicy,
			}) {
				fmt.Fprintf(os.Stderr, "gateway: load replica control manifest: %v\n",
					errors.Join(manifestErr, errGatewayReplicaControlManifest))
				return 2
			}
			if policy.Check(profile.LocalIdentity().Node,
				serviceauthz.CapabilityTopology|serviceauthz.CapabilityMembership,
			) != serviceauthz.DecisionAllow {
				fmt.Fprintln(os.Stderr, "gateway: replica controller identity lacks topology and membership authority")
				return 2
			}
			for _, endpoint := range manifest.Gateways {
				if policy.Check(endpoint.Member.Node, serviceauthz.CapabilityTopology) !=
					serviceauthz.DecisionAllow {
					fmt.Fprintln(os.Stderr, "gateway: replica control roster contains a gateway without topology authority")
					return 2
				}
			}
			replicaControlManifest = &manifest
		}
	}

	var shardDial gateway.DialFunc
	if shardTLS != nil {
		shardDial = shardTLS.Dial
	}
	var exec *gateway.Executor
	var holder *gateway.CatalogHolder
	var catalogAuthority *gateway.ReplicatedCatalogAuthority
	var replicated *gateway.ReplicatedExecutor
	var replicatedPool *gateway.AuthenticatedReplicatedClient
	var err error
	if *devStaticCatalog {
		exec, holder, err = newGatewayWithDial(*catalog, shardDial, internalAuthority)
	} else {
		var clientID replication.ID128
		var retryHome replication.RetryHome
		if decodeFixedHex(*catalogClientID, clientID[:]) != nil ||
			decodeFixedHex(*catalogRetryHome, retryHome[:]) != nil {
			fmt.Fprintln(os.Stderr, "gateway: catalog client identity or retry home is not canonical hexadecimal")
			return 2
		}
		exec, holder, catalogAuthority, replicated, replicatedPool, err = newReplicatedCatalogGateway(
			context.Background(), *catalog, shardDial, tlsProfile, *devPlaintext,
			internalAuthority,
			replication.RelationID(*catalogRelation), *catalogAttempts, *catalogAttemptTimeout,
			*tlsHandshakeTimeout, *maxShardConnections, *maxShardHandshakes,
			*catalogSessionJournal, clientID, retryHome, *catalogSessionLease,
		)
	}
	if replicatedPool != nil {
		defer replicatedPool.Close()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: load catalog %q: %v\n", *catalog, err)
		return 1
	}
	var dataReader *gateway.ReplicatedDataReader
	if replicated != nil {
		dataReader, err = gateway.NewReplicatedDataReaderWithOptions(
			gateway.ReplicatedDataReaderOptions{
				Catalog: holder, Executor: replicated, Refresh: catalogAuthority.Refresh,
				MaxConcurrentReads:    *maxNativeReadConcurrency,
				MaxInFlightReadBytes:  *maxNativeReadBytes,
				MaxScatterConcurrency: *maxNativeScatterConcurrency,
			},
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gateway: initialize replicated data reader: %v\n", err)
			return 1
		}
	}
	if *replicaControlManifestPath != "" && (*devPlaintext || replicaControlManifest == nil) {
		fmt.Fprintln(os.Stderr, "gateway: replica control requires a complete authenticated manifest")
		return 2
	}
	if replicaControlManifest != nil {
		if err = replicaControlManifest.ValidateCatalog(holder.Current()); err != nil {
			fmt.Fprintf(os.Stderr, "gateway: replica control catalog endpoints: %v\n", err)
			return 2
		}
	}
	var hotShardRuntime *gatewayHotShardRuntime
	if *hotShardCapacity != "" {
		capacity, capacityErr := loadGatewayHotShardCapacity(*hotShardCapacity)
		if capacityErr != nil {
			fmt.Fprintf(os.Stderr, "gateway: load hot-shard capacity: %v\n", capacityErr)
			return 2
		}
		hotShardRuntime, err = newGatewayHotShardRuntime(
			context.Background(), holder, catalogAuthority, capacity,
		)
		if err != nil || !exec.InstallPressureObserver(hotShardRuntime) {
			fmt.Fprintf(os.Stderr, "gateway: initialize hot-shard pressure: %v\n", err)
			return 1
		}
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: listen %q: %v\n", *listen, err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "vibedb-gateway serving catalog generation %d on %s\n",
		holder.Current().Generation(), listener.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logf := func(format string, a ...any) { fmt.Fprintf(os.Stderr, format+"\n", a...) }
	var hotShardDone <-chan struct{}
	if hotShardRuntime != nil {
		hotShardDone = runGatewayHotShardPublisher(
			ctx, hotShardRuntime, *hotShardInterval, logf,
		)
	}
	var replicaControlDone <-chan error
	var replicaControllersDone <-chan struct{}
	if replicaControlManifest != nil {
		manifest := *replicaControlManifest
		readDeadline := servicetls.FixedDeadline(time.Duration(manifest.Bounds.ReadTimeout) * time.Millisecond)
		writeDeadline := servicetls.FixedDeadline(time.Duration(manifest.Bounds.WriteTimeout) * time.Millisecond)
		handshakeDeadline := servicetls.FixedDeadline(*tlsHandshakeTimeout)
		shardOpener, openErr := newGatewayShardControlOpener(
			tlsProfile, handshakeDeadline,
			func(ctx context.Context, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", address)
			}, manifest.Shards, int(manifest.Bounds.MaxConnections),
		)
		trust := tlsProfile.LocalIdentity().TrustDomain
		drainer, drainErr := newGatewayClusterDrainCertifier(
			trust, tlsProfile, handshakeDeadline, readDeadline, writeDeadline,
			func(ctx context.Context, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", address)
			}, manifest.Gateways, int(manifest.Bounds.MaxConcurrentDrains),
		)
		controls, controlsErr := newGatewayReplicaRemoteClients(gatewayReplicaRemoteClientOptions{
			Opener: shardOpener, ReadDeadline: readDeadline, WriteDeadline: writeDeadline,
			Authority: catalogAuthority, Replicated: replicated, Drainer: drainer,
		})
		moveController, controllerErr := newGatewayReplicaMoveController(
			catalogAuthority, replicated, controls,
		)
		healthController, healthErr := newGatewayReplicaHealthRuntime(
			catalogAuthority,
			rebalance.ReplicatedFailureAuthority{Source: catalogAuthority},
			controls.HealthObservations, manifest, moveController,
		)
		healthRevisions, revisionErr := newGatewayReplicaHealthRevisionController(
			catalogAuthority, controls.HealthObservations, catalogAuthority,
		)
		gatewayNodes := make([]rafttransport.NodeID, len(manifest.Gateways))
		gatewayRoster := make(map[rafttransport.NodeID]uint64, len(manifest.Gateways))
		for index, endpoint := range manifest.Gateways {
			gatewayNodes[index] = endpoint.Member.Node
			gatewayRoster[endpoint.Member.Node] = endpoint.Member.Incarnation
		}
		authorizer, authorizeErr := servicetls.NewNodeAuthorizer(gatewayNodes)
		controlTLS, tlsErr := servicetls.NewServer(
			tlsProfile, rafttransport.TrafficGatewayControl, authorizer,
		)
		controlService, serviceErr := gateway.NewClusterCatalogDrainControlService(
			gateway.ClusterCatalogDrainControlOptions{Holder: holder,
				Catalog: gatewayCatalogDigestVerifier{catalog: catalogAuthority},
				Member:  manifest.Local.Member,
				Authorize: func(identity rafttransport.PeerIdentity, _ gateway.ClusterCatalogDrainRequest) bool {
					incarnation, found := gatewayRoster[identity.Node]
					return found && incarnation != 0 && authorization.Check(
						identity.Node, serviceauthz.CapabilityTopology,
					) == serviceauthz.DecisionAllow
				}, ReadDeadline: readDeadline, WriteDeadline: writeDeadline},
		)
		controlListener, listenErr := net.Listen("tcp", manifest.Local.Address)
		if joined := errors.Join(openErr, drainErr, controlsErr, controllerErr, healthErr, revisionErr,
			authorizeErr, tlsErr, serviceErr, listenErr); joined != nil {
			if controlListener != nil {
				_ = controlListener.Close()
			}
			_ = listener.Close()
			fmt.Fprintf(os.Stderr, "gateway: initialize replica control: %v\n", joined)
			return 1
		}
		replicaControllersDone, err = startGatewayReplicaControllers(
			ctx, healthRevisions, moveController, healthController,
			time.Duration(manifest.Bounds.ControllerInterval)*time.Millisecond, logf,
		)
		if err != nil {
			_ = controlListener.Close()
			_ = listener.Close()
			fmt.Fprintf(os.Stderr, "gateway: start replica controllers: %v\n", err)
			return 1
		}
		controlDone := make(chan error, 1)
		replicaControlDone = controlDone
		go func() {
			serveErr := controlTLS.Serve(ctx, controlListener, servicetls.Limits{
				MaxConnections:    int(manifest.Bounds.MaxConnections),
				MaxHandshakes:     int(manifest.Bounds.MaxHandshakes),
				HandshakeDeadline: handshakeDeadline,
			}, func(connectionContext context.Context, connection rafttransport.PeerConnection) {
				if serveErr := controlService.Serve(connectionContext, connection); serveErr != nil &&
					!errors.Is(serveErr, context.Canceled) {
					logf("gateway: catalog drain control: %v", serveErr)
				}
			})
			controlDone <- serveErr
			stop()
		}()
	}
	if catalogAuthority != nil {
		trigger := &gatewayControllerTriggerClient{
			tls: tlsProfile, plaintext: *devPlaintext, handshake: *tlsHandshakeTimeout,
			maxConnections: *maxShardConnections, maxHandshakes: *maxShardHandshakes,
		}
		go runSplitController(ctx, catalogAuthority, trigger, *controllerInterval, logf)
	}
	if clientTLS != nil {
		err = serveAuthenticatedGatewayData(ctx, listener, exec, dataReader, clientTLS, gateway.ClientTLSLimits{
			MaxConnections: *maxConnections, MaxHandshakes: *maxHandshakes,
			HandshakeDeadline: servicetls.FixedDeadline(*tlsHandshakeTimeout),
		}, logf)
	} else {
		serveContext := gateway.WithLocalReplicatedTransactionRequestScope(ctx)
		if dataReader != nil {
			serveContext, err = serviceauthz.WithAuthority(serveContext, internalAuthority)
			if err != nil {
				fmt.Fprintf(os.Stderr, "gateway: establish development authority: %v\n", err)
				return 1
			}
		}
		err = serveGatewayData(serveContext, listener, exec, dataReader, logf)
	}
	var replicaControlErr error
	if replicaControlDone != nil {
		stop()
		if controlErr := <-replicaControlDone; controlErr != nil &&
			!errors.Is(controlErr, context.Canceled) {
			replicaControlErr = controlErr
		}
	}
	if replicaControllersDone != nil {
		stop()
		<-replicaControllersDone
	}
	if hotShardDone != nil {
		stop()
		<-hotShardDone
	}
	if serveErr := errors.Join(replicaControlErr, nonCanceledError(err)); serveErr != nil {
		fmt.Fprintf(os.Stderr, "gateway: serve: %v\n", serveErr)
		return 1
	}
	return 0
}

func nonCanceledError(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// requireLoopbackListen keeps the explicitly selected unauthenticated
// development protocol from becoming a remotely reachable query endpoint.
func requireLoopbackListen(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("listen address %q is invalid: %w", address, err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("listen address %q must be loopback; remote unauthenticated serving is refused", address)
	}
	return nil
}

// newGateway loads the initial catalog generation and returns an executor that
// dispatches leader-only strong reads over the default TCP client. A stale
// shard refusal reloads the same crash-safe catalog path, publishing only a
// strictly newer valid generation.
func newGateway(catalogPath string) (*gateway.Executor, *gateway.CatalogHolder, error) {
	return newGatewayWithDial(catalogPath, nil, serviceauthz.Authority{})
}

func newGatewayWithDial(catalogPath string, dial gateway.DialFunc,
	internalAuthority serviceauthz.Authority) (*gateway.Executor, *gateway.CatalogHolder, error) {
	snap, err := gateway.LoadSnapshot(catalogPath)
	if err != nil {
		return nil, nil, err
	}
	holder := gateway.NewCatalogHolder(snap)
	refresher := gateway.NewFileCatalogRefresher(catalogPath, holder)
	exec := gateway.NewExecutor(gateway.NewClient(dial), holder, gateway.Options{
		Refresh: refresher.Refresh, InternalAuthority: internalAuthority,
	})
	return exec, holder, nil
}

func newReplicatedCatalogGateway(
	ctx context.Context,
	bootstrapPath string,
	shardDial gateway.DialFunc,
	tlsProfile *rafttransport.PeerTLS,
	devPlaintext bool,
	internalAuthority serviceauthz.Authority,
	relation replication.RelationID,
	attempts int,
	attemptTimeout time.Duration,
	handshakeTimeout time.Duration,
	maxConnections int,
	maxHandshakes int,
	journalPath string,
	clientID replication.ID128,
	retryHome replication.RetryHome,
	lease time.Duration,
) (*gateway.Executor, *gateway.CatalogHolder, *gateway.ReplicatedCatalogAuthority,
	*gateway.ReplicatedExecutor, *gateway.AuthenticatedReplicatedClient, error) {
	if !devPlaintext && (maxConnections < 2 || maxHandshakes < 2) {
		// Catalog/topology traffic must retain one slot while public data is
		// saturated. A one-slot secure pool cannot provide that liveness fence.
		return nil, nil, nil, nil, nil, gateway.ErrReplicatedTLSProfile
	}
	distributionName := gateway.ReplicatedCatalogDistribution
	shardID := gateway.ReplicatedCatalogShard
	bootstrap, err := gateway.LoadSnapshot(bootstrapPath)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := bootstrap.ResolveReplicatedRoute(distributionName, shardID, replicas[:0])
	if !ok {
		return nil, nil, nil, nil, nil, gateway.ErrReplicatedCatalogMissing
	}
	var nativeClient gateway.ReplicatedRoundTripper
	var replicatedPool *gateway.AuthenticatedReplicatedClient
	if devPlaintext {
		nativeClient = gateway.TCPReplicatedClient{Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}}
	} else {
		perEndpoint := maxConnections
		replicatedPool, err = gateway.NewAuthenticatedReplicatedClient(
			gateway.AuthenticatedReplicatedClientOptions{
				TLS: tlsProfile,
				Dial: func(ctx context.Context, address string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "tcp", address)
				},
				HandshakeDeadline: servicetls.FixedDeadline(handshakeTimeout),
				MaxConnections:    maxConnections, MaxPerEndpoint: perEndpoint,
				MaxIdlePerEndpoint: min(perEndpoint, 8), MaxHandshakes: maxHandshakes,
				MaxWaiters: maxConnections, MaxIdleAge: 30 * time.Second,
				MaxLifetime: 15 * time.Minute,
			},
		)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		nativeClient = replicatedPool
	}
	replicated, err := gateway.NewReplicatedExecutor(nativeClient, attempts, attemptTimeout)
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	holder := gateway.NewCatalogHolder(nil)
	binding, err := gateway.NativeSessionJournalBinding(
		route, string(distributionName), string(shardID),
		[]byte{replicatedCatalogControllerTenant}, relation, serviceauthz.CapabilityTopology,
	)
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	journal, err := gateway.OpenNativeSessionJournal(gateway.NativeSessionJournalOptions{
		Path: journalPath, ClientID: clientID, RetryHome: retryHome,
		MaxCommandBytes: replication.MaxCommandBytes, Binding: binding,
	})
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	session, err := gateway.NewNativeSession(gateway.NativeSessionOptions{
		Executor: replicated, Route: route, Distribution: string(distributionName), Shard: string(shardID),
		Tenant: []byte{replicatedCatalogControllerTenant}, ClientID: clientID, RetryHome: retryHome,
		Resolver: gateway.BaseRelationResolver{Relation: relation}, Journal: journal,
		ProposalCapability: serviceauthz.CapabilityTopology,
		MaxRelationBatches: 1, MaxMutations: 4,
		InitialCommandBytes: 4 << 10, MaxCommandBytes: replication.MaxCommandBytes,
	})
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	authorizedContext, err := serviceauthz.WithAuthority(ctx, internalAuthority)
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	renewExisting := session.Status().Active
	if session.Status().Pending {
		_, err = session.RetryPending(authorizedContext)
	}
	if err == nil && !session.Status().Active {
		deadline := time.Now().Add(lease).UnixNano()
		if deadline <= 0 {
			err = gateway.ErrNativeSession
		} else {
			_, err = session.Open(authorizedContext, deadline)
		}
	}
	if err == nil && renewExisting && session.Status().Active {
		status := session.Status()
		next := time.Now().Add(lease).UnixNano()
		if next <= status.LeaseDeadline {
			if status.LeaseDeadline == math.MaxInt64 {
				err = gateway.ErrNativeSession
			} else {
				next = status.LeaseDeadline + 1
			}
		}
		if err == nil && next > 0 {
			_, err = session.Renew(authorizedContext, status.LeaseDeadline, next)
		}
	}
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	authority, err := gateway.NewReplicatedCatalogAuthority(gateway.ReplicatedCatalogAuthorityOptions{
		Executor: replicated, Route: route, Relation: relation, Holder: holder, Session: session,
		Authority: internalAuthority,
	})
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	if _, err = authority.Read(ctx); err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	transactionTenant, transactionRetryHome := replicatedDataTransactionIdentity(
		clientID, retryHome,
	)
	transactions, err := gateway.NewReplicatedTransactionOrchestrator(
		gateway.ReplicatedTransactionOrchestratorOptions{
			Executor: replicated, Tenant: transactionTenant, RetryHome: transactionRetryHome,
			MaxConcurrency:    defaultRF3TransactionConcurrency,
			MaxInFlightBytes:  defaultRF3TransactionInFlight,
			MaxMutations:      defaultRF3TransactionMutations,
			MaxMutationBytes:  defaultRF3TransactionBytes,
			RecoveryTimeout:   defaultRF3TransactionRecovery,
			RecoveryAuthority: internalAuthority,
		},
	)
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	transactionRequests, err := gateway.NewReplicatedTransactionRequestRegistry(
		gateway.ReplicatedTransactionRequestRegistryOptions{
			Orchestrator: transactions, MaxEntries: defaultRF3TransactionRequests,
		},
	)
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	executor := gateway.NewExecutor(
		gateway.NewClient(shardDial), holder, gateway.Options{
			Refresh: authority.Refresh, InternalAuthority: internalAuthority,
			ReplicatedTransactions:        transactions,
			ReplicatedTransactionRequests: transactionRequests,
		},
	)
	return executor, holder, authority, replicated, replicatedPool, nil
}

const replicatedCatalogControllerTenant = byte(1)

func replicatedDataTransactionIdentity(
	clientID replication.ID128,
	retryHome replication.RetryHome,
) ([]byte, replication.RetryHome) {
	var material [1 + len(clientID) + len(retryHome)]byte
	material[0] = 2
	copy(material[1:1+len(clientID)], clientID[:])
	copy(material[1+len(clientID):], retryHome[:])
	tenantDigest := sha256.Sum256(material[:])
	material[0] = 3
	retryDigest := sha256.Sum256(material[:])
	tenant := make([]byte, len(clientID))
	copy(tenant, tenantDigest[:len(tenant)])
	var dataRetryHome replication.RetryHome
	copy(dataRetryHome[:], retryDigest[:len(dataRetryHome)])
	return tenant, dataRetryHome
}

func decodeFixedHex(encoded string, destination []byte) error {
	if len(encoded) != hex.EncodedLen(len(destination)) {
		return gateway.ErrReplicatedCatalog
	}
	written, err := hex.Decode(destination, []byte(encoded))
	if err != nil || written != len(destination) {
		return gateway.ErrReplicatedCatalog
	}
	return nil
}

type gatewayControllerTriggerClient struct {
	tls            *rafttransport.PeerTLS
	plaintext      bool
	handshake      time.Duration
	maxConnections int
	maxHandshakes  int
}

func (client *gatewayControllerTriggerClient) TriggerSplitController(
	ctx context.Context, route gateway.ReplicatedRoute, request shardcontrol.Request,
) (shardcontrol.Response, error) {
	if client == nil || ctx == nil {
		return shardcontrol.Response{}, splitcontroller.ErrControllerTrigger
	}
	var last error
	for _, replica := range route.Replicas {
		var dial shardcontrol.Dial
		var secure *servicetls.Client
		if client.plaintext {
			dial = func(ctx context.Context, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", address)
			}
		} else {
			var err error
			secure, err = servicetls.NewClient(servicetls.ClientOptions{
				TLS: client.tls, Class: rafttransport.TrafficShardControl,
				Endpoints: []servicetls.Endpoint{{Address: replica.ControlAddress, Node: replica.Node}},
				Dial: func(ctx context.Context, address string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "tcp", address)
				},
				HandshakeDeadline: servicetls.FixedDeadline(client.handshake),
				MaxConnections:    min(client.maxConnections, 8),
				MaxHandshakes:     min(client.maxHandshakes, 8),
			})
			if err != nil {
				return shardcontrol.Response{}, err
			}
			dial = secure.Dial
		}
		protocol, err := shardcontrol.NewClient(dial, replica.ControlAddress, client.handshake)
		if err == nil {
			var response shardcontrol.Response
			response, err = protocol.Execute(ctx, request)
			if secure != nil {
				_ = secure.Close()
			}
			if err == nil {
				return response, nil
			}
		} else if secure != nil {
			_ = secure.Close()
		}
		last = errors.Join(last, err)
	}
	return shardcontrol.Response{}, errors.Join(last, splitcontroller.ErrControllerTrigger)
}

func runSplitController(
	ctx context.Context, directory splitcontroller.ControllerDirectory,
	client splitcontroller.ControllerTriggerClient, interval time.Duration,
	logf func(string, ...any),
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		pass, err := splitcontroller.RunControllerPass(ctx, directory, client)
		if err != nil && !errors.Is(err, context.Canceled) {
			logf("gateway: split controller: %v", err)
		} else if pass.Triggered != 0 {
			logf("gateway: split controller triggered %d/%d operation(s)", pass.Triggered, pass.Discovered)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// serveGateway accepts connections until ctx is canceled, then closes the
// listener and drains in-flight connections. It returns nil on a signaled
// shutdown and the accept error otherwise.
func serveGateway(ctx context.Context, listener net.Listener, exec *gateway.Executor, logf func(string, ...any)) error {
	return serveGatewayData(ctx, listener, exec, nil, logf)
}

func serveGatewayData(
	ctx context.Context,
	listener net.Listener,
	exec *gateway.Executor,
	data nativeDataReader,
	logf func(string, ...any),
) error {
	startGatewayRecovery(ctx, exec, logf)
	// Closing the listener when ctx is done unblocks a blocked Accept, so a
	// signal shuts the loop down without a poll.
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			wg.Wait()
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			handleConnData(ctx, conn, exec, data, logf)
		}()
	}
}

func startGatewayRecovery(ctx context.Context, exec *gateway.Executor, logf func(string, ...any)) {
	go exec.RunRecovery(ctx, 5*time.Second, func(results []gateway.RecoveryResult, err error) {
		if err != nil {
			logf("gateway: transaction recovery: %v", err)
		}
		if len(results) != 0 {
			logf("gateway: transaction recovery resolved %d coordinator(s)", len(results))
		}
	})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			resolved, err := exec.RecoverReplicatedTransactionRequests(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				logf("gateway: RF3 transaction recovery: %v", err)
			}
			if resolved != 0 {
				logf("gateway: RF3 transaction recovery attempted %d request(s)", resolved)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func serveAuthenticatedGateway(ctx context.Context, listener net.Listener, exec *gateway.Executor,
	capability *gateway.ClientTLS, limits gateway.ClientTLSLimits, logf func(string, ...any)) error {
	return serveAuthenticatedGatewayData(ctx, listener, exec, nil, capability, limits, logf)
}

func serveAuthenticatedGatewayData(
	ctx context.Context,
	listener net.Listener,
	exec *gateway.Executor,
	data nativeDataReader,
	capability *gateway.ClientTLS,
	limits gateway.ClientTLSLimits,
	logf func(string, ...any),
) error {
	startGatewayRecovery(ctx, exec, logf)
	return capability.ServeAuthorizedClients(ctx, listener, limits,
		func(ctx context.Context, connection net.Conn) {
			handleConnAuthorizedData(ctx, connection, exec, data, capability, logf)
		})
}

func handleConnAuthorized(ctx context.Context, conn net.Conn, exec *gateway.Executor,
	capability *gateway.ClientTLS, logf func(string, ...any)) {
	handleConnAuthorizedData(ctx, conn, exec, nil, capability, logf)
}

func handleConnAuthorizedData(ctx context.Context, conn net.Conn, exec *gateway.Executor,
	data nativeDataReader, capability *gateway.ClientTLS, logf func(string, ...any)) {
	handleConnPolicy(ctx, conn, exec, data, logf, func(required serviceauthz.Capability) bool {
		return capability.Authorize(ctx, required, nil) == serviceauthz.DecisionAllow
	})
}

// handleConn serves newline-delimited JSON requests on one connection until the
// peer disconnects or the server shuts down. Closing the connection when ctx is
// done unblocks a blocked decode so a signaled shutdown drains promptly.
func handleConn(ctx context.Context, conn net.Conn, exec *gateway.Executor, logf func(string, ...any)) {
	handleConnData(ctx, conn, exec, nil, logf)
}

func handleConnData(ctx context.Context, conn net.Conn, exec *gateway.Executor,
	data nativeDataReader, logf func(string, ...any)) {
	handleConnPolicy(ctx, conn, exec, data, logf, nil)
}

func handleConnPolicy(ctx context.Context, conn net.Conn, exec *gateway.Executor,
	data nativeDataReader, logf func(string, ...any), authorize func(serviceauthz.Capability) bool) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), maxServeRequestBytes)
	writer := vibejson.NewWriter(conn)
	var nativeRequest nativeDataWireRequest
	var nativeResponseScratch nativeDataResponseScratch
	for scanner.Scan() {
		line := scanner.Bytes()
		if nativeDataRequestCandidate(line) {
			if err := decodeNativeDataRequest(line, &nativeRequest); err != nil {
				response := nativeDataError(nativeDataResponseInvalidRequest, false)
				if writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
					return
				}
				continue
			}
			required := serviceauthz.CapabilityDataRead
			if nativeRequest.Operation != nativeDataOperationGet {
				required = serviceauthz.CapabilityDataWrite
			}
			if authorize != nil && !authorize(required) {
				response := nativeDataError(nativeDataResponseUnauthorized, false)
				if err := writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout); err != nil {
					return
				}
				continue
			}
			var response nativeDataWireResponse
			if data == nil {
				response = nativeDataError(nativeDataResponseUnavailable, true)
			} else {
				response = executeNativeDataRead(ctx, data, &nativeRequest)
			}
			writeErr := writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout)
			response.release()
			if writeErr != nil {
				if ctx.Err() == nil {
					logf("gateway: encode native response: %v", writeErr)
				}
				return
			}
			continue
		}
		var req serveRequest
		if err := vibejson.Unmarshal(line, &req); err != nil {
			if ctx.Err() == nil {
				logf("gateway: decode request: %v", err)
			}
			return
		}
		// get/put/delete belong exclusively to the canonical native namespace.
		// Reordered or escaped spellings may never fall through to legacy SQL
		// execution or its string error schema.
		if req.Op == "get" || req.Op == "put" || req.Op == "delete" {
			response := nativeDataError(nativeDataResponseInvalidRequest, false)
			if writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
				return
			}
			continue
		}
		if req.Op == "read_batch" {
			batchRequest, buildErr := buildNativeSQLBatchReadRequest(req)
			if buildErr != nil {
				response := nativeDataError(nativeDataResponseInvalidRequest, false)
				if writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
					return
				}
				continue
			}
			if authorize != nil && !authorize(serviceauthz.CapabilityDataRead) {
				response := nativeDataError(nativeDataResponseUnauthorized, false)
				if writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
					return
				}
				continue
			}
			batchReader, available := data.(nativeSQLBatchReader)
			if !available {
				response := nativeDataError(nativeDataResponseUnavailable, true)
				if writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
					return
				}
				continue
			}
			result, readErr := batchReader.ReadSQLBatch(ctx, batchRequest)
			if readErr != nil {
				response := nativeDataResponseForError(readErr)
				if writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
					return
				}
				continue
			}
			response := nativeSQLBatchWireResponse{
				Result: &result, Expected: len(batchRequest.Queries), Maximum: batchRequest.MaxResultBytes,
			}
			validationErr := validateNativeSQLBatchResponse(&response)
			if validationErr != nil {
				result.Release()
				encoded := nativeDataResponseForError(validationErr)
				if writeNativeDataConnResponse(conn, &encoded, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
					return
				}
				continue
			}
			writeErr := writeNativeSQLBatchConnResponse(
				conn, writer, &response, defaultNativeResponseWriteTimeout,
			)
			result.Release()
			if writeErr != nil {
				if ctx.Err() == nil {
					logf("gateway: encode replicated SQL batch response: %v", writeErr)
				}
				return
			}
			continue
		}
		if authorize != nil && !authorize(serveRequestCapability(&req)) {
			if err := writeServeResponse(writer, &serveResponse{Error: "authorization denied"}); err != nil {
				return
			}
			continue
		}
		if err := writeServeResponse(writer, execRequest(ctx, exec, req)); err != nil {
			if ctx.Err() == nil {
				logf("gateway: encode response: %v", err)
			}
			return
		}
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		logf("gateway: decode request: %v", err)
	}
}

func writeNativeDataConnResponse(
	connection net.Conn,
	response *nativeDataWireResponse,
	scratch *nativeDataResponseScratch,
	timeout time.Duration,
) error {
	if connection == nil || timeout <= 0 {
		return errInvalidNativeDataResponse
	}
	if err := connection.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	writeErr := writeNativeDataResponseDirect(connection, response, scratch)
	clearErr := connection.SetWriteDeadline(time.Time{})
	if writeErr != nil {
		return writeErr
	}
	return clearErr
}

func nativeDataRequestCandidate(source []byte) bool {
	index := skipNativeJSONSpace(source, 0)
	if index >= len(source) || source[index] != '{' {
		return false
	}
	index = skipNativeJSONSpace(source, index+1)
	if len(source)-index < len(`"op"`) ||
		!bytes.Equal(source[index:index+len(`"op"`)], []byte(`"op"`)) {
		return false
	}
	index = skipNativeJSONSpace(source, index+len(`"op"`))
	if index >= len(source) || source[index] != ':' {
		return false
	}
	index = skipNativeJSONSpace(source, index+1)
	for _, operation := range [...]string{`"get"`, `"put"`, `"delete"`} {
		if len(source)-index < len(operation) ||
			!bytes.Equal(source[index:index+len(operation)], []byte(operation)) {
			continue
		}
		next := index + len(operation)
		if next == len(source) {
			return true
		}
		switch source[next] {
		case ',', '}', ' ', '\t', '\r', '\n':
			return true
		default:
			return false
		}
	}
	return false
}

func skipNativeJSONSpace(source []byte, index int) int {
	for index < len(source) {
		switch source[index] {
		case ' ', '\t', '\r', '\n':
			index++
		default:
			return index
		}
	}
	return index
}

func serveRequestCapability(request *serveRequest) serviceauthz.Capability {
	if request == nil {
		return 0
	}
	var required serviceauthz.Capability
	if request.SQL != "" {
		required = serviceauthz.SQLCapability(request.SQL)
	}
	for index := range request.Statements {
		required |= serviceauthz.SQLCapability(request.Statements[index].SQL)
	}
	return required
}

// writeServeResponse emits one NDJSON response without converting raw result
// cells into strings or passing them through a generic JSON tree.
func writeServeResponse(w *vibejson.Writer, resp *serveResponse) error {
	if resp == nil {
		return errServeResponseTransactionState
	}
	hasTransaction := resp.TransactionID != (replication.ID128{})
	hasOutcome := resp.Committed || resp.OutcomeUnknown
	if hasTransaction != hasOutcome || resp.Committed && resp.OutcomeUnknown {
		return errServeResponseTransactionState
	}
	if err := w.BeginObject(); err != nil {
		return err
	}
	stringField := func(name, value string) error {
		if value == "" {
			return nil
		}
		if err := w.Key(name); err != nil {
			return err
		}
		return w.String(value)
	}
	if err := stringField("kind", resp.Kind); err != nil {
		return err
	}
	if len(resp.Columns) != 0 {
		if err := w.Key("columns"); err != nil {
			return err
		}
		if err := w.BeginArray(); err != nil {
			return err
		}
		for _, column := range resp.Columns {
			if err := w.String(column); err != nil {
				return err
			}
		}
		if err := w.EndArray(); err != nil {
			return err
		}
	}
	if len(resp.Rows) != 0 {
		if err := w.Key("rows"); err != nil {
			return err
		}
		if err := w.BeginArray(); err != nil {
			return err
		}
		for _, row := range resp.Rows {
			if err := w.BeginArray(); err != nil {
				return err
			}
			for _, cell := range row {
				if err := w.RawUnchecked(cell); err != nil {
					return err
				}
			}
			if err := w.EndArray(); err != nil {
				return err
			}
		}
		if err := w.EndArray(); err != nil {
			return err
		}
	}
	if resp.RowsAffected != 0 {
		if err := w.Key("rows_affected"); err != nil {
			return err
		}
		if err := w.Int(resp.RowsAffected); err != nil {
			return err
		}
	}
	if err := stringField("route", resp.Route); err != nil {
		return err
	}
	if resp.Generation != 0 {
		if err := w.Key("generation"); err != nil {
			return err
		}
		if err := w.Uint(resp.Generation); err != nil {
			return err
		}
	}
	if resp.ShardsFanned != 0 {
		if err := w.Key("shards_fanned"); err != nil {
			return err
		}
		if err := w.Int(int64(resp.ShardsFanned)); err != nil {
			return err
		}
	}
	if resp.Retries != 0 {
		if err := w.Key("retries"); err != nil {
			return err
		}
		if err := w.Int(int64(resp.Retries)); err != nil {
			return err
		}
	}
	if resp.TransactionID != (replication.ID128{}) {
		if err := w.Key("transaction_id"); err != nil {
			return err
		}
		if err := writeNativeHex(w, resp.TransactionID[:]); err != nil {
			return err
		}
	}
	if resp.Committed {
		if err := w.Key("committed"); err != nil {
			return err
		}
		if err := w.Bool(true); err != nil {
			return err
		}
	}
	if resp.OutcomeUnknown {
		if err := w.Key("outcome_unknown"); err != nil {
			return err
		}
		if err := w.Bool(true); err != nil {
			return err
		}
	}
	if err := stringField("error", resp.Error); err != nil {
		return err
	}
	if err := w.EndObject(); err != nil {
		return err
	}
	if err := w.Newline(); err != nil {
		return err
	}
	return w.Flush()
}

// execRequest translates one request and dispatches it, mapping any failure into
// an error reply rather than dropping the connection.
func execRequest(ctx context.Context, exec *gateway.Executor, req serveRequest) *serveResponse {
	var res *gateway.Result
	var err error
	switch req.Op {
	case "exec_batch":
		queries, buildErr := buildBatchQueries(req)
		if buildErr != nil {
			return &serveResponse{Error: buildErr.Error()}
		}
		var requestID replication.ID128
		if req.RequestID != "" {
			if decodeFixedHex(req.RequestID, requestID[:]) != nil ||
				requestID == (replication.ID128{}) {
				return &serveResponse{Error: gateway.ErrReplicatedTransactionRequestRegistry.Error()}
			}
		}
		res, err = exec.ExecBatchRequest(ctx, requestID, queries)
	case "exec":
		// The write path routes the statement to its single owning shard and
		// refuses every scatter before any dispatch.
		q, buildErr := buildQuery(req)
		if buildErr != nil {
			return &serveResponse{Error: buildErr.Error()}
		}
		res, err = exec.Exec(ctx, q)
	case "", "query":
		q, buildErr := buildQuery(req)
		if buildErr != nil {
			return &serveResponse{Error: buildErr.Error()}
		}
		res, err = exec.Query(ctx, q)
	default:
		return &serveResponse{Error: fmt.Sprintf("unknown operation %q", req.Op)}
	}
	if err != nil {
		response := &serveResponse{Error: err.Error()}
		var transactionErr *gateway.ReplicatedTransactionError
		if errors.As(err, &transactionErr) && transactionErr.ID != (distributedtxn.ID{}) {
			response.TransactionID = replication.ID128(transactionErr.ID)
			response.Committed = transactionErr.Committed
			response.OutcomeUnknown = !transactionErr.Committed
		}
		return response
	}
	return encodeResult(res)
}

func buildBatchQueries(req serveRequest) ([]gateway.Query, error) {
	if req.SQL != "" || len(req.Params) != 0 || req.MaxResultBytes != 0 {
		return nil, errors.New("exec_batch uses statements instead of top-level sql or params")
	}
	if len(req.Statements) == 0 {
		return nil, gateway.ErrBatchEmpty
	}
	class, err := parseClass(req.Class)
	if err != nil {
		return nil, err
	}
	queries := make([]gateway.Query, len(req.Statements))
	for i := range req.Statements {
		params, err := buildParams(req.Statements[i].Params)
		if err != nil {
			return nil, fmt.Errorf("statement %d: %w", i, err)
		}
		queries[i] = gateway.Query{SQL: req.Statements[i].SQL, Params: params, Class: class}
	}
	return queries, nil
}

// buildQuery turns a request envelope into a gateway query. Placement, routing,
// ordering, and limiting are deliberately absent here: the executor derives
// them from SQL against its pinned catalog generation.
func buildQuery(req serveRequest) (gateway.Query, error) {
	if req.MaxResultBytes != 0 {
		return gateway.Query{}, errors.New("max_result_bytes is only valid for read_batch")
	}
	params, err := buildParams(req.Params)
	if err != nil {
		return gateway.Query{}, err
	}
	class, err := parseClass(req.Class)
	if err != nil {
		return gateway.Query{}, err
	}
	return gateway.Query{
		SQL:    req.SQL,
		Params: params,
		Class:  class,
	}, nil
}

// buildParams maps the typed request parameters onto shard-service parameters in
// placeholder order.
func buildParams(in []serveParam) ([]shardservice.Param, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]shardservice.Param, len(in))
	for i, p := range in {
		switch p.Kind {
		case "null":
			out[i] = shardservice.NullParam()
		case "bool":
			out[i] = shardservice.BoolParam(p.Bool)
		case "number":
			out[i] = shardservice.NumberParam(p.Text)
		case "string":
			out[i] = shardservice.StringParam(p.Text)
		case "document":
			out[i] = shardservice.DocumentParam(p.Text)
		default:
			return nil, fmt.Errorf("unknown parameter kind %q", p.Kind)
		}
	}
	return out, nil
}

// parseClass maps the request's class name onto an operational class, defaulting
// to the interactive profile.
func parseClass(name string) (gateway.OperationClass, error) {
	switch name {
	case "", "interactive":
		return gateway.ClassInteractive, nil
	case "batch":
		return gateway.ClassBatch, nil
	case "admin":
		return gateway.ClassAdmin, nil
	default:
		return 0, fmt.Errorf("unknown class %q", name)
	}
}

// encodeResult renders a merged result as a reply envelope, carrying each cell as
// raw JSON so an already-encoded value is not re-encoded.
func encodeResult(res *gateway.Result) *serveResponse {
	resp := &serveResponse{
		Kind:          res.Kind.String(),
		RowsAffected:  res.RowsAffected,
		Route:         res.RouteKind.String(),
		Generation:    res.Generation,
		ShardsFanned:  res.ShardsFanned,
		Retries:       res.Retries,
		TransactionID: res.TransactionID,
		Committed:     res.TransactionID != (replication.ID128{}),
	}
	for _, col := range res.Columns {
		resp.Columns = append(resp.Columns, col.Name)
	}
	if len(res.Rows) > 0 {
		resp.Rows = make([][]serveRawValue, len(res.Rows))
		for i, row := range res.Rows {
			cells := make([]serveRawValue, len(row))
			for j, c := range row {
				if c.Null {
					cells[j] = serveRawValue("null")
				} else {
					cells[j] = serveRawValue(c.Bytes)
				}
			}
			resp.Rows[i] = cells
		}
	}
	return resp
}
