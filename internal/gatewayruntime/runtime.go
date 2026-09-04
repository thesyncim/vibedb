package gatewayruntime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalanceexec"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

// SemanticTransport is the transport-neutral RF3 boundary used by an
// embedded frontend. Implementations authenticate and dispatch the complete
// semantic request at their own boundary; callers never encode an inner SQL
// frame or use a loopback socket to reach a local owner. The same transport is
// used for catalog NativeSession proposals, durable request and issuer
// recovery, and the public SQL executor.
//
// A standalone gateway leaves this nil and Open creates the existing bounded
// authenticated network transport from the configured TLS and shard peers.
type SemanticTransport = gateway.ReplicatedRoundTripper

// Config describes one persisted gateway frontend. Every frontend must have a
// distinct CatalogSessionJournal and ClientID/RetryHome pair. Journal-backed
// durable request pins and PostgreSQL issuer state are derived below that
// private base, so two embedded frontends cannot accidentally share recovery
// identity or command history. Open holds an exclusive local ownership lock on
// that base through Close. Operators must still provision unique identities:
// this filesystem lock cannot detect copied identities at other paths/hosts.
//
// The zero value is intentionally invalid: production startup must name every
// durable authority explicitly. DevStaticCatalog and DevPlaintext are the only
// exceptions and are restricted to loopback development serving.
type Config struct {
	CatalogPath               string
	CatalogRouteSeedPath      string
	DevStaticCatalog          bool
	DevPlaintext              bool
	CatalogBootstrapIfMissing bool

	CatalogRelation       replication.RelationID
	CatalogAttempts       int
	CatalogAttemptTimeout time.Duration
	CatalogSessionLease   time.Duration
	CatalogSessionJournal string
	CatalogClientID       replication.ID128
	CatalogRetryHome      replication.RetryHome

	// DurableAckKeyPath names the cluster-shared ACK derivation key. AckKey can
	// be supplied directly by a supervisor that already opened and validated the
	// key file; exactly one of the two is required in replicated mode.
	DurableAckKeyPath string
	AckKey            gateway.DurableRequestAckDerivationKey

	// Transport is shared by all semantic RF3 calls made by this frontend. It
	// may select the local physical node owner and authenticated remote nodes.
	// It is deliberately a ReplicatedRoundTripper so catalog and durable
	// NativeSession paths cannot bypass the semantic dispatcher. The runtime
	// borrows this transport and never closes it: the owner must keep it alive
	// until Drain and Close return, then close its local server and remote pool.
	// When Transport is nil, Open creates and owns the standalone authenticated
	// network pool from TLSProfile and ShardPeers.
	Transport SemanticTransport
	ShardDial gateway.DialFunc

	ListenAddress string
	Listener      net.Listener

	TLSCertificate      string
	TLSKey              string
	TLSRoots            string
	TLSIdentityOID      string
	TLSHandshakeTimeout time.Duration
	TLSProfile          *rafttransport.PeerTLS
	AuthorizationPolicy string
	InternalAuthority   serviceauthz.Authority
	Authorization       *serviceauthz.Policy
	MaxConnections      int
	MaxHandshakes       int
	MaxShardConnections int
	MaxShardHandshakes  int
	ShardPeers          []servicetls.Endpoint

	MaxNativeReadConcurrency    int
	MaxNativeReadBytes          uint64
	MaxNativeScatterConcurrency int

	// Optional local PostgreSQL frontend. Its journals are derived from
	// CatalogSessionJournal and therefore retain the same private frontend
	// identity while remaining separate from the catalog session journal.
	PGListenAddress string
	PGDDLSocket     string

	TableCatalogs              []string
	HotShardCapacityPath       string
	HotShardInterval           time.Duration
	ReplicaControlManifestPath string
	BackupRepositoryPath       string
	BackupMaxBackups           int
	BackupMaxArtifacts         int
	BackupMaxArtifactBytes     uint64
	BackupMaxDiskBytes         uint64
	ControllerInterval         time.Duration
	SchemaRolloutPlan          string
	SchemaRolloutOnce          bool

	Logf func(format string, args ...any)
}

// Runtime owns one complete gateway frontend and its durable recovery state.
// Open performs bounded validation and durable state recovery. Serve starts
// public admission; Drain cancels admission and waits for every accepted
// request and recovery obligation; Close then releases transport, journals,
// listeners, and optional controllers in dependency order.
type Runtime struct {
	config Config

	exec           *gateway.Executor
	holder         *gateway.CatalogHolder
	authority      *gateway.ReplicatedCatalogAuthority
	replicated     *gateway.ReplicatedExecutor
	replicatedPool *gateway.AuthenticatedReplicatedClient
	data           *gateway.ReplicatedDataReader
	durable        durableRequestService

	clientTLS   *gateway.ClientTLS
	shardTLS    *servicetls.Client
	listener    net.Listener
	journalLock *os.File

	routeSeedControl *gateway.ReplicatedCatalogRouteSeedControl
	pg               interface{ Close() error }
	pgWriter         interface{ Close() error }
	pgWriterDone     <-chan struct{}

	replicaControlManifest        *gatewayReplicaControlManifest
	controlListener               net.Listener
	controlTLS                    *servicetls.Server
	controlService                *gateway.ClusterCatalogDrainControlService
	controlOpener                 *gatewayShardControlOpener
	controlReadDeadline           rafttransport.DeadlineFunc
	controlWriteDeadline          rafttransport.DeadlineFunc
	controlHandshakeDeadline      rafttransport.DeadlineFunc
	controlDone                   <-chan error
	replicaControllersDone        <-chan struct{}
	splitControllerDone           <-chan struct{}
	hotShardDone                  <-chan struct{}
	metricsDone                   <-chan struct{}
	routeSeedDone                 <-chan struct{}
	splitRuntime                  *gatewayServingSplitRuntime
	hotShardRuntime               *gatewayHotShardRuntime
	backupOperator                gatewayBackupOperator
	backupRepository              *clusterbackup.BackupRepository
	distributedMetrics            *gateway.DistributedMetrics
	distributedMetricsConcurrency int
	schemaDDL                     *gatewaySchemaDDLRuntime
	controllerContext             context.Context
	controllerMetrics             *gatewayControllerMetrics
	healthController              *gatewayReplicaHealthController
	healthRevisions               *gatewayReplicaHealthRevisionController
	moveController                *rebalanceexec.Controller
	servingContext                context.Context

	ctx    context.Context
	cancel context.CancelFunc

	serveOnce    sync.Once
	serveDone    chan struct{}
	serveErr     error
	serveMu      sync.Mutex
	serveStarted bool
	drained      bool

	drainOnce sync.Once
	drainFin  sync.Once
	drainDone chan struct{}
	drainErr  error
	closeOnce sync.Once
	closeErr  error
}

var (
	ErrInvalidConfig = errors.New("gatewayruntime: invalid configuration")
	ErrAlreadyServed = errors.New("gatewayruntime: Serve called more than once")
)

// Open assembles one gateway frontend from a prepared configuration. The
// injected semantic transport is threaded through catalog, durable SQL, and
// public read paths. Open binds listeners but does not admit public requests
// or start controller loops. The node's peer/native services must already be
// reachable before Open recovers the replicated catalog session.
func Open(parent context.Context, config Config) (*Runtime, error) {
	if parent == nil {
		parent = context.Background()
	}
	config.withDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	runtimeCtx, cancel := context.WithCancel(parent)
	runtime := &Runtime{config: config, ctx: runtimeCtx, cancel: cancel,
		serveDone: make(chan struct{}), drainDone: make(chan struct{})}
	if runtime.config.Logf == nil {
		runtime.config.Logf = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, format+"\n", args...)
		}
	}
	if err := runtime.open(); err != nil {
		return nil, errors.Join(err, runtime.Close())
	}
	return runtime, nil
}

func (config *Config) withDefaults() {
	if config.ListenAddress == "" && config.Listener == nil {
		config.ListenAddress = "127.0.0.1:0"
	}
	if config.CatalogAttempts == 0 {
		config.CatalogAttempts = 8
	}
	if config.CatalogAttemptTimeout == 0 {
		config.CatalogAttemptTimeout = 5 * time.Second
	}
	if config.CatalogSessionLease == 0 {
		config.CatalogSessionLease = 24 * time.Hour
	}
	if config.TLSHandshakeTimeout == 0 {
		config.TLSHandshakeTimeout = 5 * time.Second
	}
	if config.MaxConnections == 0 {
		config.MaxConnections = 1024
	}
	if config.MaxHandshakes == 0 {
		config.MaxHandshakes = 64
	}
	if config.MaxShardConnections == 0 {
		config.MaxShardConnections = 4096
	}
	if config.MaxShardHandshakes == 0 {
		config.MaxShardHandshakes = 64
	}
	if config.MaxNativeReadConcurrency == 0 {
		config.MaxNativeReadConcurrency = gateway.DefaultReplicatedReadConcurrency
	}
	if config.MaxNativeReadBytes == 0 {
		config.MaxNativeReadBytes = gateway.DefaultReplicatedReadInFlight
	}
	if config.MaxNativeScatterConcurrency == 0 {
		config.MaxNativeScatterConcurrency = gateway.DefaultReplicatedScatterConcurrency
	}
	if config.HotShardInterval == 0 {
		config.HotShardInterval = time.Second
	}
	if config.ControllerInterval == 0 {
		config.ControllerInterval = time.Second
	}
	if config.BackupMaxBackups == 0 {
		config.BackupMaxBackups = 16
	}
	if config.BackupMaxArtifacts == 0 {
		config.BackupMaxArtifacts = 4096
	}
	if config.BackupMaxArtifactBytes == 0 {
		config.BackupMaxArtifactBytes = 64 << 30
	}
	if config.BackupMaxDiskBytes == 0 {
		config.BackupMaxDiskBytes = 256 << 30
	}
}

func (config Config) validate() error {
	if err := validateGatewayConfigPath("catalog path", config.CatalogPath, true); err != nil {
		return err
	}
	if config.DevStaticCatalog {
		if !config.DevPlaintext || config.CatalogRouteSeedPath != "" ||
			config.CatalogBootstrapIfMissing || config.CatalogRelation != 0 || config.CatalogSessionJournal != "" ||
			config.DurableAckKeyPath != "" || config.AckKey != (gateway.DurableRequestAckDerivationKey{}) ||
			config.Transport != nil {
			return fmt.Errorf("%w: static catalog requires explicit plaintext loopback mode", ErrInvalidConfig)
		}
	} else {
		if config.DevPlaintext && config.Transport != nil {
			// A local semantic transport can be used in plaintext development, but
			// it must still be explicitly loopback constrained by ListenAddress.
			if config.ListenAddress == "" {
				return fmt.Errorf("%w: plaintext semantic serving requires a loopback listener", ErrInvalidConfig)
			}
		}
		for label, path := range map[string]string{
			"catalog route seed path": config.CatalogRouteSeedPath,
			"catalog session journal": config.CatalogSessionJournal,
			"durable ACK key path":    config.DurableAckKeyPath,
		} {
			required := label != "durable ACK key path" || config.AckKey == (gateway.DurableRequestAckDerivationKey{})
			if err := validateGatewayConfigPath(label, path, required); err != nil {
				return err
			}
		}
		if config.CatalogRouteSeedPath == config.CatalogPath ||
			config.CatalogSessionJournal == config.CatalogPath ||
			config.CatalogRouteSeedPath == config.CatalogSessionJournal ||
			config.CatalogRouteSeedPath == config.DurableAckKeyPath ||
			config.CatalogSessionJournal == config.DurableAckKeyPath ||
			config.CatalogRelation == 0 || config.CatalogRelation > replication.MaxRelationID || config.CatalogAttempts <= 0 ||
			config.CatalogAttempts > gateway.AbsoluteMaxReplicatedAttempts ||
			config.CatalogAttemptTimeout <= 0 || config.CatalogSessionLease <= 0 ||
			config.CatalogSessionJournal == "" || config.CatalogClientID == (replication.ID128{}) ||
			config.CatalogRetryHome == (replication.RetryHome{}) ||
			config.CatalogAttemptTimeout > gateway.AbsoluteMaxReplicatedAttemptTimeout {
			return fmt.Errorf("%w: replicated catalog identity, route seed, relation, and bounds are required", ErrInvalidConfig)
		}
		if config.DurableAckKeyPath == "" && config.AckKey == (gateway.DurableRequestAckDerivationKey{}) {
			return fmt.Errorf("%w: durable ACK key is required", ErrInvalidConfig)
		}
		if config.DurableAckKeyPath != "" && config.AckKey != (gateway.DurableRequestAckDerivationKey{}) {
			return fmt.Errorf("%w: durable ACK key and key path are mutually exclusive", ErrInvalidConfig)
		}
	}
	for label, path := range map[string]string{
		"PostgreSQL DDL socket":    config.PGDDLSocket,
		"hot-shard capacity path":  config.HotShardCapacityPath,
		"replica-control manifest": config.ReplicaControlManifestPath,
		"backup repository":        config.BackupRepositoryPath,
		"schema rollout plan":      config.SchemaRolloutPlan,
	} {
		if path == "" {
			continue
		}
		if err := validateGatewayConfigPath(label, path, false); err != nil {
			return err
		}
	}
	for _, path := range config.TableCatalogs {
		if err := validateGatewayConfigPath("table catalog", path, true); err != nil {
			return err
		}
	}
	if config.PGDDLSocket != "" && (config.PGListenAddress == "" || config.DevStaticCatalog) {
		return fmt.Errorf("%w: PostgreSQL DDL socket requires an RF3 PostgreSQL listener", ErrInvalidConfig)
	}
	if config.SchemaRolloutOnce && config.SchemaRolloutPlan == "" {
		return fmt.Errorf("%w: schema rollout once requires a rollout plan", ErrInvalidConfig)
	}
	if config.SchemaRolloutPlan != "" &&
		(config.DevStaticCatalog || config.DevPlaintext || config.ReplicaControlManifestPath == "") {
		return fmt.Errorf("%w: schema rollout requires an authenticated replica-control manifest", ErrInvalidConfig)
	}
	if config.BackupRepositoryPath != "" &&
		(config.DevStaticCatalog || config.DevPlaintext || config.ReplicaControlManifestPath == "") {
		return fmt.Errorf("%w: backup repository requires an authenticated replica-control manifest", ErrInvalidConfig)
	}
	if config.ReplicaControlManifestPath != "" && config.DevPlaintext {
		return fmt.Errorf("%w: replica control requires authenticated TLS", ErrInvalidConfig)
	}
	if err := validateGatewayHotShardServeMode(config.HotShardCapacityPath,
		config.ReplicaControlManifestPath, config.DevStaticCatalog, config.DevPlaintext); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if config.ListenAddress == "" && config.Listener == nil {
		return fmt.Errorf("%w: listener or listen address is required", ErrInvalidConfig)
	}
	if config.Listener != nil && config.ListenAddress != "" {
		return fmt.Errorf("%w: listener and listen address are mutually exclusive", ErrInvalidConfig)
	}
	if config.DevPlaintext {
		if err := requireLoopbackListen(config.ListenAddress); err != nil && config.Listener == nil {
			return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
		}
		if config.Listener != nil {
			if config.Listener.Addr() == nil {
				return fmt.Errorf("%w: plaintext listener has no address", ErrInvalidConfig)
			}
			if err := requireLoopbackListen(config.Listener.Addr().String()); err != nil {
				return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
			}
		}
	}
	if config.MaxConnections <= 0 || config.MaxConnections > servicetls.AbsoluteMaxConnections ||
		config.MaxHandshakes <= 0 || config.MaxHandshakes > config.MaxConnections ||
		config.MaxShardConnections <= 0 || config.MaxShardConnections > gateway.AbsoluteMaxReplicatedPoolConnections ||
		config.MaxShardHandshakes <= 0 || config.MaxShardHandshakes > config.MaxShardConnections {
		return fmt.Errorf("%w: connection and handshake bounds are invalid", ErrInvalidConfig)
	}
	if config.MaxHandshakes > config.MaxConnections {
		return fmt.Errorf("%w: handshake bound exceeds connection bound", ErrInvalidConfig)
	}
	if config.TLSHandshakeTimeout <= 0 || config.ControllerInterval <= 0 || config.HotShardInterval <= 0 {
		return fmt.Errorf("%w: handshake and controller intervals must be positive", ErrInvalidConfig)
	}
	if config.MaxNativeReadConcurrency <= 0 || config.MaxNativeReadConcurrency > gateway.AbsoluteMaxReplicatedReadConcurrency ||
		config.MaxNativeReadBytes == 0 || config.MaxNativeReadBytes > gateway.AbsoluteMaxReplicatedReadInFlight ||
		config.MaxNativeScatterConcurrency <= 0 || config.MaxNativeScatterConcurrency > gateway.AbsoluteMaxReplicatedScatterConcurrency {
		return fmt.Errorf("%w: native read admission bounds are invalid", ErrInvalidConfig)
	}
	if config.PGListenAddress != "" {
		if err := requireLoopbackListen(config.PGListenAddress); err != nil {
			return fmt.Errorf("%w: PostgreSQL endpoint: %v", ErrInvalidConfig, err)
		}
		if config.DevStaticCatalog {
			return fmt.Errorf("%w: PostgreSQL requires RF3 catalog", ErrInvalidConfig)
		}
	}
	return nil
}

func validateGatewayConfigPath(label, path string, required bool) error {
	if path == "" {
		if required {
			return fmt.Errorf("%w: %s is required", ErrInvalidConfig, label)
		}
		return nil
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("%w: %s must be an absolute clean path", ErrInvalidConfig, label)
	}
	return nil
}

// Executor returns the catalog-pinning SQL executor after Open. It is useful
// to an embedded supervisor that owns a separate protocol endpoint.
func (runtime *Runtime) Executor() *gateway.Executor {
	if runtime == nil {
		return nil
	}
	return runtime.exec
}

// Catalog returns the current immutable catalog holder.
func (runtime *Runtime) Catalog() *gateway.CatalogHolder {
	if runtime == nil {
		return nil
	}
	return runtime.holder
}

// Listener returns the bound public listener, or nil before a successful Open.
func (runtime *Runtime) Listener() net.Listener {
	if runtime == nil {
		return nil
	}
	return runtime.listener
}

// Serve starts public and optional PostgreSQL admission and waits until the
// supplied context or Drain stops it. It is safe to call Drain concurrently.
func (runtime *Runtime) Serve(ctx context.Context) error {
	if runtime == nil {
		return ErrInvalidConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runtime.serveMu.Lock()
	if runtime.drained || runtime.serveStarted {
		runtime.serveMu.Unlock()
		return ErrAlreadyServed
	}
	runtime.serveStarted = true
	runtime.serveMu.Unlock()
	runtime.serveOnce.Do(func() { go runtime.runServe() })
	select {
	case <-ctx.Done():
		_ = runtime.Drain(context.Background())
	case <-runtime.serveDone:
	}
	<-runtime.serveDone
	runtime.serveMu.Lock()
	err := errors.Join(runtime.serveErr, runtime.drainErr)
	runtime.serveMu.Unlock()
	return err
}

// Drain stops public admission and waits for all serving goroutines and
// recovery work. A caller can use a shorter context to bound how long it waits;
// the runtime still closes listeners and cancels its internal context.
func (runtime *Runtime) Drain(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runtime.drainOnce.Do(func() {
		runtime.cancel()
		listenerErr := runtime.closeListeners()
		runtime.serveMu.Lock()
		runtime.drained = true
		runtime.drainErr = errors.Join(runtime.drainErr, listenerErr)
		started := runtime.serveStarted
		runtime.serveMu.Unlock()
		if !started {
			go runtime.finishDrain()
		}
	})
	select {
	case <-runtime.drainDone:
		runtime.serveMu.Lock()
		err := runtime.drainErr
		runtime.serveMu.Unlock()
		return err
	case <-ctx.Done():
		runtime.serveMu.Lock()
		err := errors.Join(runtime.drainErr, ctx.Err())
		runtime.serveMu.Unlock()
		return err
	}
}

func (runtime *Runtime) finishDrain() {
	if runtime == nil {
		return
	}
	runtime.drainFin.Do(func() {
		runtime.cancel()
		runtime.setDrainError(runtime.closeListeners())
		runtime.waitOptionalServices()
		close(runtime.drainDone)
		close(runtime.serveDone)
	})
}

func (runtime *Runtime) setDrainError(err error) {
	if err == nil {
		return
	}
	runtime.serveMu.Lock()
	runtime.drainErr = errors.Join(runtime.drainErr, err)
	runtime.serveMu.Unlock()
}

// Close drains the frontend and releases every owned resource. It is
// idempotent so startup failure and supervisor shutdown can share the same
// cleanup path.
func (runtime *Runtime) Close() error {
	if runtime == nil {
		return nil
	}
	runtime.closeOnce.Do(func() {
		runtime.closeErr = runtime.Drain(context.Background())
		runtime.closeErr = errors.Join(runtime.closeErr, runtime.closeResources())
	})
	return runtime.closeErr
}
