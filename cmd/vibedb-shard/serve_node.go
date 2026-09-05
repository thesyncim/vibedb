package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/gatewayruntime"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
)

// rf3EmbeddedGateway owns the resources that are shared by the embedded
// frontend and the node lifecycle. gatewayruntime intentionally does not own
// an injected semantic transport, so the remote pool is closed by this owner.
type rf3EmbeddedGateway struct {
	config  gatewayruntime.Config
	runtime *gatewayruntime.Runtime
	remote  *gateway.AuthenticatedReplicatedClient
	client  *gateway.ReplicatedNodeClient
}

// Entering Accept proves the service finished installing its cancellation,
// authentication and worker bounds. Binding a socket alone does not prove
// the service can receive the catalog calls made during frontend Open.
type rf3AcceptReadyListener struct {
	net.Listener
	accepting chan struct{}
	once      sync.Once
}

func newRF3AcceptReadyListener(listener net.Listener) *rf3AcceptReadyListener {
	return &rf3AcceptReadyListener{Listener: listener, accepting: make(chan struct{})}
}

func (listener *rf3AcceptReadyListener) Accept() (net.Conn, error) {
	listener.once.Do(func() { close(listener.accepting) })
	return listener.Listener.Accept()
}

func waitRF3ServiceAdmission(ctx context.Context, listeners ...*rf3AcceptReadyListener) error {
	for _, listener := range listeners {
		if listener == nil || listener.Listener == nil {
			return errRF3Serving
		}
		select {
		case <-listener.accepting:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return context.Cause(ctx)
}

func runServeNode(args []string) int {
	fs := flag.NewFlagSet("serve-node", flag.ContinueOnError)
	manifestPath := fs.String("manifest", "", "canonical prepared physical-node manifest")
	reload := fs.Bool("reload-prepared-groups", false, "allow SIGHUP to append durably prepared groups from the same manifest")
	executionLanes := fs.Int("execution-lanes", rf3DefaultExecutionLanes, "power-of-two Raft execution lanes shared by node groups")
	if err := fs.Parse(args); err != nil || *manifestPath == "" || fs.NArg() != 0 ||
		!validRF3ExecutionLanes(*executionLanes) {
		usage()
		return 2
	}
	manifest, err := loadRF3Manifest(*manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error RF3 node manifest: %v\n", err)
		return 2
	}
	if manifest.NodeLog == nil || len(manifest.Groups) == 0 || manifest.Gateway == nil {
		fmt.Fprintln(os.Stderr, "error RF3 node manifest: serve-node requires grouped node_log and explicit gateway configuration")
		return 2
	}
	stopReload := configureRF3ManifestReload(&manifest, *manifestPath, *reload)
	defer stopReload()
	diagnostics := make(chan os.Signal, 1)
	signal.Notify(diagnostics, syscall.SIGUSR1)
	defer signal.Stop(diagnostics)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	err = servePreparedRF3WithEmbeddedGatewayAndDiagnostics(ctx, manifest, *executionLanes, net.Listen, diagnostics)
	if err = componentShutdownError(err, context.Cause(ctx)); err != nil {
		fmt.Fprintf(os.Stderr, "error serve RF3 node: %v\n", err)
		return 1
	}
	return 0
}

// loadRF3EmbeddedGatewayCredentials validates the independent service
// principal before opening retained storage or constructing either service.
func loadRF3EmbeddedGatewayCredentials(
	manifest rf3Manifest,
	nodeProfile *rafttransport.PeerTLS,
	nodePolicy *serviceauthz.Policy,
) (*rafttransport.PeerTLS, *serviceauthz.Policy, error) {
	if manifest.Gateway == nil || nodeProfile == nil || nodePolicy == nil {
		return nil, nil, errRF3Serving
	}
	encoded := manifest.Gateway
	frontendProfile, err := servicetls.LoadProfile(
		encoded.TLS.Certificate, encoded.TLS.Key, encoded.TLS.Roots,
		encoded.TLS.IdentityOID, time.Now,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: embedded gateway TLS profile: %v", errRF3Serving, err)
	}
	nodeIdentity, frontendIdentity := nodeProfile.LocalIdentity(), frontendProfile.LocalIdentity()
	if nodeIdentity.TrustDomain != frontendIdentity.TrustDomain ||
		nodeIdentity.Node == frontendIdentity.Node {
		return nil, nil, fmt.Errorf("%w: embedded gateway must use a distinct identity in the node trust domain", errRF3Serving)
	}
	if err := nodeProfile.ValidatePeerProfile(frontendProfile); err != nil {
		return nil, nil, fmt.Errorf("%w: storage does not trust embedded gateway: %v", errRF3Serving, err)
	}
	if err := frontendProfile.ValidatePeerProfile(nodeProfile); err != nil {
		return nil, nil, fmt.Errorf("%w: embedded gateway does not trust storage: %v", errRF3Serving, err)
	}
	frontendPolicy, err := serviceauthz.LoadFile(encoded.AuthorizationPolicy)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: embedded gateway authorization policy: %v", errRF3Serving, err)
	}
	if frontendPolicy.Generation() != nodePolicy.Generation() ||
		nodePolicy.Check(frontendIdentity.Node, serviceauthz.CapabilityDelegate) != serviceauthz.DecisionAllow {
		return nil, nil, fmt.Errorf("%w: embedded gateway policy generation or delegation is not authorized by the storage policy", errRF3Serving)
	}
	return frontendProfile, frontendPolicy, nil
}

// prepareRF3EmbeddedGateway binds the authenticated local capability before
// native admission starts. Catalog/session Open is deferred until every node
// service is accepting connections, so quorum startup cannot deadlock.
func prepareRF3EmbeddedGateway(
	manifest rf3Manifest,
	frontendProfile *rafttransport.PeerTLS,
	frontendPolicy *serviceauthz.Policy,
	nativeTLS *shardservice.ReplicatedServerTLS,
	server *shardservice.ReplicatedServer,
) (*rf3EmbeddedGateway, error) {
	if manifest.Gateway == nil || frontendProfile == nil || frontendPolicy == nil || nativeTLS == nil || server == nil {
		return nil, errRF3Serving
	}
	encoded := manifest.Gateway
	frontendIdentity := frontendProfile.LocalIdentity()
	nodeIdentity := nativeTLS.LocalIdentity()
	peers, err := rf3EmbeddedGatewayPeers(manifest, encoded.ShardPeers)
	if err != nil {
		return nil, err
	}
	if err := validateRF3EmbeddedGatewayLocalNative(manifest, nodeIdentity.Node, peers); err != nil {
		return nil, err
	}
	maxShardConnections := rf3GatewayBound(encoded.MaxShardConnections, 4096)
	maxShardHandshakes := rf3GatewayBound(encoded.MaxShardHandshakes, 64)
	if maxShardHandshakes > maxShardConnections {
		return nil, fmt.Errorf("%w: embedded gateway handshake bound exceeds connection bound", errRF3Serving)
	}
	handshakeTimeout := rf3GatewayDuration(encoded.TLSHandshakeTimeoutMillis, 5*time.Second)
	remote, err := gateway.NewAuthenticatedReplicatedClient(gateway.AuthenticatedReplicatedClientOptions{
		TLS: frontendProfile,
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		},
		HandshakeDeadline: servicetls.FixedDeadline(handshakeTimeout),
		MaxConnections:    maxShardConnections, MaxPerEndpoint: maxShardConnections,
		MaxIdlePerEndpoint: min(maxShardConnections, 8), MaxHandshakes: maxShardHandshakes,
		MaxWaiters: maxShardConnections, MaxIdleAge: 30 * time.Second,
		MaxLifetime: 15 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: embedded gateway remote transport: %v", errRF3Serving, err)
	}
	client, err := gateway.NewReplicatedNodeClient(nativeTLS, frontendProfile, server, remote)
	if err != nil {
		_ = remote.Close()
		return nil, fmt.Errorf("%w: embedded gateway semantic transport: %v", errRF3Serving, err)
	}
	clientID, err := rf3GatewayID(encoded.CatalogClientID, 16, false)
	if err != nil {
		_ = remote.Close()
		return nil, fmt.Errorf("%w: embedded gateway catalog client identity: %v", errRF3Serving, err)
	}
	retryHome, err := rf3GatewayRetryHome(encoded.CatalogRetryHome)
	if err != nil {
		_ = remote.Close()
		return nil, fmt.Errorf("%w: embedded gateway catalog retry home: %v", errRF3Serving, err)
	}
	var ddlOwner rafttransport.NodeID
	if encoded.DDLOwnerNode != "" {
		var valid bool
		ddlOwner, valid = rf3GatewayNodeID(encoded.DDLOwnerNode)
		if !valid {
			_ = remote.Close()
			return nil, fmt.Errorf("%w: embedded gateway DDL owner identity", errRF3Serving)
		}
	}
	config := gatewayruntime.Config{
		CatalogPath: encoded.CatalogPath, CatalogRouteSeedPath: encoded.CatalogRouteSeedPath,
		CatalogBootstrapIfMissing: encoded.CatalogBootstrapIfMissing,
		CatalogRelation:           replication.RelationID(encoded.CatalogRelation),
		CatalogAttempts:           int(encoded.CatalogAttempts),
		CatalogAttemptTimeout:     rf3GatewayDuration(encoded.CatalogAttemptTimeoutMillis, 5*time.Second),
		CatalogSessionLease:       rf3GatewayDuration(encoded.CatalogSessionLeaseMillis, 24*time.Hour),
		CatalogSessionJournal:     encoded.CatalogSessionJournal, CatalogClientID: clientID,
		CatalogRetryHome: retryHome, DurableAckKeyPath: encoded.DurableAckKeyPath,
		Transport:      client,
		ListenAddress:  encoded.ListenAddress,
		TLSCertificate: encoded.TLS.Certificate, TLSKey: encoded.TLS.Key,
		TLSRoots: encoded.TLS.Roots, TLSIdentityOID: encoded.TLS.IdentityOID,
		TLSHandshakeTimeout: handshakeTimeout, TLSProfile: frontendProfile,
		AuthorizationPolicy: encoded.AuthorizationPolicy, InternalAuthority: serviceauthz.Authority{
			Node: frontendIdentity.Node, Generation: frontendPolicy.Generation(),
		}, Authorization: frontendPolicy,
		MaxConnections: int(encoded.MaxConnections), MaxHandshakes: int(encoded.MaxHandshakes),
		MaxShardConnections: maxShardConnections, MaxShardHandshakes: maxShardHandshakes,
		ShardPeers: peers, MaxNativeReadConcurrency: int(encoded.MaxNativeReadConcurrency),
		MaxNativeReadBytes:          encoded.MaxNativeReadBytes,
		MaxNativeScatterConcurrency: int(encoded.MaxNativeScatterConcurrency),
		PGListenAddress:             encoded.PGListenAddress, PGDDLSocket: encoded.PGDDLSocket,
		TableCatalogs: slices.Clone(encoded.TableCatalogs), TableCatalogsPath: encoded.TableCatalogsPath,
		HotShardCapacityPath:       encoded.HotShardCapacityPath,
		HotShardInterval:           rf3GatewayDuration(encoded.HotShardIntervalMillis, time.Second),
		ReplicaControlManifestPath: encoded.ReplicaControlManifestPath,
		ControlParticipantOnly:     encoded.ControlParticipantOnly,
		DDLOwnerAddress:            encoded.DDLOwnerAddress, DDLOwnerNode: ddlOwner,
		BackupRepositoryPath: encoded.BackupRepositoryPath,
		BackupMaxBackups:     int(encoded.BackupMaxBackups), BackupMaxArtifacts: int(encoded.BackupMaxArtifacts),
		BackupMaxArtifactBytes: encoded.BackupMaxArtifactBytes, BackupMaxDiskBytes: encoded.BackupMaxDiskBytes,
		ControllerInterval: rf3GatewayDuration(encoded.ControllerIntervalMillis, time.Second),
		SchemaRolloutPlan:  encoded.SchemaRolloutPlan, SchemaRolloutOnce: encoded.SchemaRolloutOnce,
		Logf: func(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) },
	}
	if config.MaxConnections == 0 {
		config.MaxConnections = 1024
	}
	if config.MaxHandshakes == 0 {
		config.MaxHandshakes = 64
	}
	return &rf3EmbeddedGateway{config: config, remote: remote, client: client}, nil
}

// validateRF3EmbeddedGatewayLocalNative binds the local semantic destination
// to the native service advertised by the prepared node.  The Raft member
// roster intentionally contains peer listener addresses, so comparing a
// gateway peer with that roster would reject valid deployments where the two
// services use distinct ports.  Remote destinations are authenticated by the
// mutual TLS NodeID handshake; only this local address needs the additional
// manifest identity check.
func validateRF3EmbeddedGatewayLocalNative(manifest rf3Manifest, localNode rafttransport.NodeID, peers []servicetls.Endpoint) error {
	if validateRF3Address(manifest.Listeners.Native, false) != nil {
		return fmt.Errorf("%w: embedded gateway local native listener is invalid", errRF3Serving)
	}
	for _, peer := range peers {
		if peer.Node != localNode {
			continue
		}
		if peer.Address != manifest.Listeners.Native {
			return fmt.Errorf("%w: embedded gateway local node must use its native listener", errRF3Serving)
		}
		return nil
	}
	return fmt.Errorf("%w: embedded gateway roster omits the local node", errRF3Serving)
}

func rf3EmbeddedGatewayPeers(manifest rf3Manifest, configured []rf3ManifestGatewayPeer) ([]servicetls.Endpoint, error) {
	// The member roster carries the Raft peer listener.  Gateway shard calls
	// use each node's native listener, which is a separate service and may use a
	// different port.  Treat the explicit gateway roster as the source of
	// native addresses and use the prepared member roster only to require that
	// every hosted identity is present.  TLS identity binding proves the remote
	// NodeID after dialing the configured native endpoint.
	expected := make(map[rafttransport.NodeID]struct{})
	for _, group := range manifest.groupBundles() {
		for _, member := range group.Members {
			expected[member.NodeID] = struct{}{}
		}
	}
	if len(expected) == 0 {
		return nil, fmt.Errorf("%w: embedded gateway has no shard roster", errRF3Serving)
	}
	if len(configured) == 0 {
		return nil, fmt.Errorf("%w: embedded gateway requires an explicit native shard peer roster", errRF3Serving)
	}
	if len(configured) < len(expected) {
		return nil, fmt.Errorf("%w: embedded gateway shard peer roster is incomplete", errRF3Serving)
	}
	peers := make([]servicetls.Endpoint, 0, len(configured))
	seen := make(map[rafttransport.NodeID]struct{}, len(configured))
	seenAddresses := make(map[string]struct{}, len(configured))
	for _, configuredPeer := range configured {
		node, ok := rf3GatewayNodeID(configuredPeer.NodeID)
		if !ok || validateRF3Address(configuredPeer.Address, false) != nil {
			return nil, fmt.Errorf("%w: embedded gateway shard peer is invalid", errRF3Serving)
		}
		if _, duplicate := seen[node]; duplicate {
			return nil, fmt.Errorf("%w: embedded gateway shard peer is duplicated", errRF3Serving)
		}
		if _, duplicate := seenAddresses[configuredPeer.Address]; duplicate {
			return nil, fmt.Errorf("%w: embedded gateway shard peer address is duplicated", errRF3Serving)
		}
		seen[node] = struct{}{}
		seenAddresses[configuredPeer.Address] = struct{}{}
		peers = append(peers, servicetls.Endpoint{Address: configuredPeer.Address, Node: node})
	}
	for node := range expected {
		if _, found := seen[node]; !found {
			return nil, fmt.Errorf("%w: embedded gateway shard peer roster omits a hosted group member", errRF3Serving)
		}
	}
	return peers, nil
}

func rf3GatewayNodeID(value string) (rafttransport.NodeID, bool) {
	var node rafttransport.NodeID
	if !decodeRF3FixedHex(value, node[:], false) {
		return rafttransport.NodeID{}, false
	}
	return node, true
}

func rf3GatewayID(value string, bytesCount int, allowZero bool) (replication.ID128, error) {
	var result replication.ID128
	if len(value) != bytesCount*2 || !decodeRF3FixedHex(value, result[:], allowZero) {
		return replication.ID128{}, errRF3Serving
	}
	return result, nil
}

func rf3GatewayRetryHome(value string) (replication.RetryHome, error) {
	var result replication.RetryHome
	if len(value) != len(result)*2 || !decodeRF3FixedHex(value, result[:], false) {
		return replication.RetryHome{}, errRF3Serving
	}
	return result, nil
}

func rf3GatewayBound(value uint64, fallback int) int {
	if value == 0 {
		return fallback
	}
	if value > uint64(^uint(0)>>1) {
		return fallback
	}
	return int(value)
}

func rf3GatewayDuration(millis uint64, fallback time.Duration) time.Duration {
	if millis == 0 {
		return fallback
	}
	return time.Duration(millis) * time.Millisecond
}

func stringsCompare(left, right string) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}
