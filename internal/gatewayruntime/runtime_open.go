package gatewayruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

func (runtime *Runtime) open() error {
	config := runtime.config
	var (
		profile *rafttransport.PeerTLS
		policy  *serviceauthz.Policy
		err     error
	)
	if config.DevPlaintext {
		if config.TLSProfile != nil || config.TLSCertificate != "" || config.TLSKey != "" ||
			config.TLSRoots != "" || config.TLSIdentityOID != "" || config.AuthorizationPolicy != "" ||
			config.Authorization != nil || len(config.ShardPeers) != 0 {
			return fmt.Errorf("%w: plaintext and TLS configuration are mutually exclusive", ErrInvalidConfig)
		}
		if config.InternalAuthority.Valid() {
			runtime.config.InternalAuthority = config.InternalAuthority
		} else {
			authority := serviceauthz.Authority{}
			authority.Node[0] = 1
			authority.Generation = 1
			runtime.config.InternalAuthority = authority
		}
	} else {
		profile = config.TLSProfile
		if profile == nil {
			profile, err = servicetls.LoadProfile(
				config.TLSCertificate, config.TLSKey, config.TLSRoots, config.TLSIdentityOID, time.Now,
			)
			if err != nil {
				return fmt.Errorf("load TLS profile: %w", err)
			}
		}
		policy = config.Authorization
		if policy == nil {
			if config.AuthorizationPolicy == "" {
				return fmt.Errorf("%w: authorization policy is required", ErrInvalidConfig)
			}
			policy, err = serviceauthz.LoadFile(config.AuthorizationPolicy)
			if err != nil {
				return fmt.Errorf("load authorization policy: %w", err)
			}
		}
		authority := config.InternalAuthority
		if !authority.Valid() {
			authority = serviceauthz.Authority{Node: profile.LocalIdentity().Node, Generation: policy.Generation()}
		}
		if authority.Node != profile.LocalIdentity().Node || authority.Generation != policy.Generation() {
			return fmt.Errorf("%w: internal authority does not match TLS identity and policy generation", ErrInvalidConfig)
		}
		required := serviceauthz.CapabilityDataRead | serviceauthz.CapabilityDataWrite |
			serviceauthz.CapabilityDelegate | serviceauthz.CapabilityTopology |
			serviceauthz.CapabilityTransactionRecovery | serviceauthz.CapabilityRequestLedger |
			serviceauthz.CapabilityExecutionPin
		if config.SchemaRolloutPlan != "" || config.PGDDLSocket != "" || config.DDLOwnerAddress != "" {
			required |= serviceauthz.CapabilitySchema
		}
		if config.BackupRepositoryPath != "" {
			required |= serviceauthz.CapabilityBackup
		}
		if config.ReplicaControlManifestPath != "" && !config.ControlParticipantOnly {
			required |= serviceauthz.CapabilityMembership
		}
		if policy.Check(authority.Node, required) != serviceauthz.DecisionAllow {
			return fmt.Errorf("%w: local TLS identity lacks required gateway capabilities", ErrInvalidConfig)
		}
		runtime.config.InternalAuthority = authority
		runtime.config.Authorization = policy
		runtime.config.TLSProfile = profile
		clientTLS, tlsErr := gateway.NewAuthorizedClientTLS(profile, policy)
		if tlsErr != nil {
			return fmt.Errorf("create gateway client TLS: %w", tlsErr)
		}
		runtime.clientTLS = clientTLS
	}

	var shardDial gateway.DialFunc = config.ShardDial
	if shardDial == nil {
		shardDial = func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}
	}
	if !config.DevStaticCatalog {
		runtime.journalLock, err = openGatewayJournalLock(config.CatalogSessionJournal)
		if err != nil {
			return fmt.Errorf("own gateway recovery journals: %w", err)
		}
	}
	if config.DevStaticCatalog {
		runtime.exec, runtime.holder, err = newGatewayWithDial(
			config.CatalogPath, config.ShardDial, runtime.config.InternalAuthority,
		)
		if err != nil {
			return fmt.Errorf("load catalog: %w", err)
		}
	} else {
		if config.Transport == nil && !config.DevPlaintext {
			if profile == nil {
				return fmt.Errorf("%w: TLS profile is required for network transport", ErrInvalidConfig)
			}
			if len(config.ShardPeers) == 0 {
				return fmt.Errorf("%w: shard peers are required for network transport", ErrInvalidConfig)
			}
			runtime.shardTLS, err = servicetls.NewClient(servicetls.ClientOptions{
				TLS: profile, Class: rafttransport.TrafficShardSQL, Endpoints: config.ShardPeers,
				Dial: func(ctx context.Context, address string) (net.Conn, error) {
					return shardDial(ctx, address)
				}, HandshakeDeadline: servicetls.FixedDeadline(config.TLSHandshakeTimeout),
				MaxConnections: config.MaxShardConnections, MaxHandshakes: config.MaxShardHandshakes,
			})
			if err != nil {
				return fmt.Errorf("create authenticated shard transport: %w", err)
			}
		}
		var clientID replication.ID128 = config.CatalogClientID
		var retryHome replication.RetryHome = config.CatalogRetryHome
		var catalogDial gateway.DialFunc = shardDial
		if runtime.shardTLS != nil {
			catalogDial = runtime.shardTLS.Dial
		}
		runtime.exec, runtime.holder, runtime.authority, runtime.replicated, runtime.replicatedPool, err = newReplicatedCatalogGateway(
			runtime.ctx, config.CatalogPath, config.CatalogRouteSeedPath, catalogDial,
			profile, config.DevPlaintext, runtime.config.InternalAuthority,
			config.CatalogBootstrapIfMissing, config.CatalogRelation, config.CatalogAttempts,
			config.CatalogAttemptTimeout, config.TLSHandshakeTimeout, config.MaxShardConnections,
			config.MaxShardHandshakes, config.CatalogSessionJournal, clientID, retryHome,
			config.CatalogSessionLease, runtime, config.Transport,
		)
		if err != nil {
			return fmt.Errorf("open replicated catalog: %w", err)
		}
		runtime.routeSeedControl = runtime.authority.ReplicatedCatalogRouteSeedControl()
		if len(config.InitialNodeDirectory) != 0 {
			if err = runtime.authority.BootstrapNodeDirectory(runtime.ctx, config.InitialNodeDirectory); err != nil {
				return fmt.Errorf("bootstrap physical-node directory: %w", err)
			}
		}
	}
	tableCatalogs := config.TableCatalogs
	if config.TableCatalogsPath != "" {
		listed, loadErr := loadGatewayTableCatalogPaths(config.TableCatalogsPath)
		if loadErr != nil {
			return fmt.Errorf("load table catalogs: %w", loadErr)
		}
		tableCatalogs = append(append([]string(nil), tableCatalogs...), listed...)
	}
	for _, path := range tableCatalogs {
		if runtime.authority == nil {
			return fmt.Errorf("%w: table registration requires replicated catalog authority", ErrInvalidConfig)
		}
		addition, registerErr := openGatewayTableCatalog(path)
		if registerErr == nil {
			registerCtx, cancel := context.WithTimeout(runtime.ctx, time.Minute)
			registerErr = registerGatewayDevTable(registerCtx, func(ctx context.Context) error {
				return runtime.authority.RegisterProvisionedTable(ctx, addition)
			}, runtime.config.Logf)
			cancel()
		}
		if registerErr != nil {
			return fmt.Errorf("register table catalog %q: %w", path, registerErr)
		}
	}
	if runtime.replicated != nil {
		runtime.data, err = gateway.NewReplicatedDataReaderWithOptions(gateway.ReplicatedDataReaderOptions{
			Catalog: runtime.holder, Executor: runtime.replicated, Refresh: runtime.authority.Refresh,
			MaxConcurrentReads: config.MaxNativeReadConcurrency, MaxInFlightReadBytes: config.MaxNativeReadBytes,
			MaxScatterConcurrency: config.MaxNativeScatterConcurrency,
		})
		if err != nil {
			return fmt.Errorf("initialize replicated data reader: %w", err)
		}
		ackKey := config.AckKey
		if ackKey == (gateway.DurableRequestAckDerivationKey{}) {
			ackKey, err = loadDurableAckKey(config.DurableAckKeyPath)
			if err != nil {
				return fmt.Errorf("load durable ACK key: %w", err)
			}
		}
		runtime.durable, err = newReplicatedDurableRuntime(replicatedDurableRuntimeOptions{
			Planner: runtime.exec, Catalog: runtime.holder, CatalogControl: runtime.authority,
			Replicated: runtime.replicated, Authority: runtime.config.InternalAuthority,
			AckKey: ackKey, JournalBase: config.CatalogSessionJournal,
		})
		if err != nil {
			return fmt.Errorf("initialize durable request service: %w", err)
		}
	}
	if config.HotShardCapacityPath != "" {
		capacity, capacityErr := loadGatewayHotShardCapacity(config.HotShardCapacityPath)
		if capacityErr != nil {
			return fmt.Errorf("load hot-shard capacity: %w", capacityErr)
		}
		runtime.hotShardRuntime, err = newGatewayHotShardRuntime(
			runtime.ctx, runtime.holder, runtime.authority, capacity,
		)
		if err != nil || !runtime.exec.InstallPressureObserver(runtime.hotShardRuntime) ||
			runtime.data != nil && !runtime.data.InstallPressureObserver(runtime.hotShardRuntime) {
			return fmt.Errorf("initialize hot-shard pressure: %w", errors.Join(err, hotshard.ErrInvalidPressureCut))
		}
	}
	if err = runtime.openReplicaControl(); err != nil {
		return err
	}
	if err = runtime.openDDL(); err != nil {
		return err
	}
	if config.Listener != nil {
		runtime.listener = config.Listener
	} else {
		runtime.listener, err = net.Listen("tcp", config.ListenAddress)
		if err != nil {
			return fmt.Errorf("listen gateway %q: %w", config.ListenAddress, err)
		}
	}
	if err = runtime.restoreFrontendDrainFromDirectory(runtime.ctx); err != nil {
		return fmt.Errorf("restore frontend admission lifecycle: %w", err)
	}
	return nil
}

func openGatewayTableCatalog(path string) (*gateway.Snapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, (4<<20)+1))
	closeErr := file.Close()
	if err = errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	return gateway.OpenReplicatedTableProvision(raw)
}
