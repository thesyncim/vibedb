package gatewayruntime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

func (runtime *Runtime) openReplicaControl() error {
	config := runtime.config
	if config.ReplicaControlManifestPath == "" {
		return nil
	}
	if config.DevPlaintext || runtime.config.TLSProfile == nil || runtime.authority == nil ||
		runtime.config.Authorization == nil {
		return fmt.Errorf("%w: replica control requires authenticated catalog and TLS", ErrInvalidConfig)
	}
	profile := runtime.config.TLSProfile
	manifest, err := loadGatewayReplicaControlManifest(
		config.ReplicaControlManifestPath, profile.LocalIdentity().Node,
	)
	if err != nil {
		return fmt.Errorf("load replica control manifest: %w", err)
	}
	if manifest.TLS != (gatewayReplicaTLSReferences{
		Certificate: config.TLSCertificate, Key: config.TLSKey, Roots: config.TLSRoots,
		IdentityOID: config.TLSIdentityOID, AuthorizationPolicy: config.AuthorizationPolicy,
	}) {
		return fmt.Errorf("%w: replica control TLS references do not match frontend", errGatewayReplicaControlManifest)
	}
	if err = manifest.ValidateCatalog(runtime.holder.Current()); err != nil {
		return fmt.Errorf("replica control catalog endpoints: %w", err)
	}
	required := serviceauthz.CapabilityTopology
	if !config.ControlParticipantOnly {
		required |= serviceauthz.CapabilityMembership
	}
	if runtime.config.Authorization.Check(profile.LocalIdentity().Node, required) != serviceauthz.DecisionAllow {
		return fmt.Errorf("%w: gateway control identity lacks required authority", ErrInvalidConfig)
	}
	for _, endpoint := range manifest.Gateways {
		if runtime.config.Authorization.Check(endpoint.Member.Node, serviceauthz.CapabilityTopology) != serviceauthz.DecisionAllow {
			return fmt.Errorf("%w: replica control roster contains a gateway without topology authority", ErrInvalidConfig)
		}
	}
	if err := runtime.openCatalogDrainService(manifest, runtime.authority); err != nil {
		return err
	}
	if config.ControlParticipantOnly {
		return nil
	}

	handshakeDeadline := servicetls.FixedDeadline(config.TLSHandshakeTimeout)
	readDeadline := servicetls.FixedDeadline(time.Duration(manifest.Bounds.ReadTimeout) * time.Millisecond)
	writeDeadline := servicetls.FixedDeadline(time.Duration(manifest.Bounds.WriteTimeout) * time.Millisecond)
	dial := func(ctx context.Context, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	}
	shardOpener, err := newGatewayShardControlOpener(
		profile, handshakeDeadline, dial, manifest.Shards, int(manifest.Bounds.MaxConnections),
	)
	if err != nil {
		return fmt.Errorf("open shard control transport: %w", err)
	}
	runtime.controlOpener = shardOpener
	runtime.controlHandshakeDeadline = handshakeDeadline
	runtime.controlReadDeadline = readDeadline
	runtime.controlWriteDeadline = writeDeadline
	if config.PGDDLSocket != "" {
		schemaDeadline := servicetls.FixedDeadline(2 * time.Minute)
		runtime.schemaDDL, err = newGatewaySchemaDDLRuntime(
			runtime.authority, runtime.replicated, shardOpener, schemaDeadline, schemaDeadline,
			config.CatalogSessionJournal+".schema-ddl", runtime.config.InternalAuthority,
		)
		if err != nil {
			return fmt.Errorf("open schema DDL runtime: %w", err)
		}
	}
	runtime.distributedMetrics, err = newGatewayDistributedMetrics(runtime.holder.Current(), shardOpener)
	if err != nil {
		return fmt.Errorf("open distributed metrics: %w", err)
	}
	if runtime.distributedMetrics != nil {
		runtime.distributedMetricsConcurrency = min(runtime.distributedMetrics.Len(), int(manifest.Bounds.MaxConnections), 64)
	}
	trust := profile.LocalIdentity().TrustDomain
	drainer, err := newGatewayClusterDrainCertifier(
		trust, profile, handshakeDeadline, readDeadline, writeDeadline, dial,
		manifest.Gateways, int(manifest.Bounds.MaxConcurrentDrains),
	)
	if err != nil {
		return fmt.Errorf("open catalog drain certifier: %w", err)
	}
	runtime.splitRuntime, err = newGatewayServingSplitRuntime(gatewayServingSplitOptions{
		catalog: runtime.authority, drain: drainer, opener: shardOpener, tls: profile,
		shards: manifest.Shards, dial: dial, handshake: handshakeDeadline,
		read: readDeadline, write: writeDeadline,
		protocol: max(time.Duration(manifest.Bounds.ReadTimeout)*time.Millisecond,
			time.Duration(manifest.Bounds.WriteTimeout)*time.Millisecond),
		connections: int(manifest.Bounds.MaxConnections), handshakes: int(manifest.Bounds.MaxHandshakes),
	})
	if err != nil {
		return fmt.Errorf("open serving split runtime: %w", err)
	}
	controls, err := newGatewayReplicaRemoteClients(gatewayReplicaRemoteClientOptions{
		Opener: shardOpener, ReadDeadline: readDeadline, WriteDeadline: writeDeadline,
		Authority: runtime.authority, Replicated: runtime.replicated, Drainer: drainer,
	})
	if err != nil {
		return fmt.Errorf("open replica remote clients: %w", err)
	}
	if config.BackupRepositoryPath != "" {
		runtime.backupRepository, err = clusterbackup.OpenBackupRepository(config.BackupRepositoryPath,
			clusterbackup.RepositoryLimits{MaxBackups: config.BackupMaxBackups,
				MaxArtifacts: config.BackupMaxArtifacts, MaxArtifactBytes: config.BackupMaxArtifactBytes,
				MaxDiskBytes: config.BackupMaxDiskBytes})
		if err != nil {
			return fmt.Errorf("open backup repository: %w", err)
		}
		gate, gateErr := serviceauthz.NewGate(runtime.config.Authorization)
		if gateErr != nil {
			return fmt.Errorf("open backup authorization: %w", gateErr)
		}
		runtime.backupOperator = &gatewayBackupOperatorRuntime{
			authority: runtime.authority, gate: gate, principal: runtime.config.InternalAuthority,
			repository: runtime.backupRepository, opener: shardOpener,
			observer: controls.HealthObservations, read: readDeadline, write: writeDeadline,
		}
	}
	splitFactory, err := newGatewayHotSplitFactory(manifest, runtime.holder.Current())
	if err != nil {
		return fmt.Errorf("open hot split factory: %w", err)
	}
	moveController, err := newGatewayReplicaMoveController(runtime.authority, runtime.replicated, controls)
	if err != nil {
		return fmt.Errorf("open replica move controller: %w", err)
	}
	runtime.moveController = moveController
	hotAuthorities := gatewayHotShardOperationAuthorities{splits: splitFactory, journal: runtime.authority}
	if len(manifest.Candidates) != 0 {
		hotAuthorities.moves = gatewayHotReplicaMoveFactory{observations: controls.HealthObservations, grants: runtime.authority}
		hotAuthorities.moveRun = moveController
	}
	if runtime.hotShardRuntime != nil && !runtime.hotShardRuntime.InstallOperationAuthorities(hotAuthorities) {
		return fmt.Errorf("%w: hot-shard operation authorities", hotshard.ErrInvalidPressureCut)
	}
	healthController, err := newGatewayReplicaHealthRuntime(
		runtime.authority, rebalance.ReplicatedFailureAuthority{Source: runtime.authority},
		controls.HealthObservations, manifest, moveController, runtime.authority, controls.GrantInstaller,
	)
	if err != nil {
		return fmt.Errorf("open replica health controller: %w", err)
	}
	runtime.healthController = healthController
	healthRevisions, err := newGatewayReplicaHealthRevisionController(
		runtime.authority, controls.HealthObservations, runtime.authority,
	)
	if err != nil {
		return fmt.Errorf("open replica health revisions: %w", err)
	}
	runtime.healthRevisions = healthRevisions
	return nil
}

func (runtime *Runtime) startOptionalServices() error {
	if runtime == nil {
		return ErrInvalidConfig
	}
	if runtime.routeSeedControl != nil {
		// A binding-changing catalog head revokes this frontend's route seed.
		// Stop public and control admission before settling the old session; the
		// next startup will recover from the durable seed and native-session
		// journal. This preserves the standalone gateway's quiesced handoff
		// fence for embedded frontends too.
		done := make(chan struct{})
		runtime.routeSeedDone = done
		go func() {
			defer close(done)
			select {
			case <-runtime.routeSeedControl.ShutdownRequired():
				runtime.cancel()
			case <-runtime.ctx.Done():
			}
		}()
	}
	controllerCtx := runtime.controllerContext
	if controllerCtx == nil {
		var err error
		controllerCtx, err = serviceauthz.WithAuthority(runtime.ctx, runtime.config.InternalAuthority)
		if err != nil {
			return fmt.Errorf("establish controller authority: %w", err)
		}
		runtime.controllerContext = controllerCtx
	}
	if runtime.controllerMetrics == nil {
		runtime.controllerMetrics = new(gatewayControllerMetrics)
	}
	controllerCtx = withGatewayControllerMetrics(controllerCtx, runtime.controllerMetrics)
	runtime.controllerContext = controllerCtx
	if runtime.schemaDDL != nil {
		recoveryCtx, cancel := context.WithTimeout(controllerCtx, 2*time.Minute)
		err := runtime.schemaDDL.Recover(recoveryCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("recover schema DDL: %w", err)
		}
	}
	if runtime.config.SchemaRolloutPlan != "" {
		started := time.Now()
		result, err := executeGatewaySchemaRollout(runtime.ctx, runtime.config.SchemaRolloutPlan,
			runtime.authority, runtime.controlOpener, runtime.controlReadDeadline,
			runtime.controlWriteDeadline, min(int(runtime.replicaControlManifest.Bounds.MaxConnections), 64))
		if err != nil {
			return fmt.Errorf("schema rollout: %w", err)
		}
		printGatewaySchemaRolloutResult(result, time.Since(started))
		if runtime.config.SchemaRolloutOnce {
			runtime.cancel()
			return nil
		}
	}
	if runtime.replicaControlManifest != nil {
		manifest := *runtime.replicaControlManifest
		if !runtime.config.ControlParticipantOnly {
			var err error
			runtime.replicaControllersDone, err = startGatewayReplicaControllers(
				controllerCtx, runtime.healthRevisions, runtime.moveController, runtime.healthController,
				time.Duration(manifest.Bounds.ControllerInterval)*time.Millisecond, runtime.config.Logf,
			)
			if err != nil {
				return fmt.Errorf("start replica controllers: %w", err)
			}
		}
		controlDone := make(chan error, 1)
		runtime.controlDone = controlDone
		go func() {
			listener := &gatewayCancellationListener{Listener: runtime.controlListener,
				ctx: runtime.ctx, cancel: runtime.cancel}
			serveErr := runtime.controlTLS.Serve(runtime.ctx, listener, servicetls.Limits{
				MaxConnections:    int(manifest.Bounds.MaxConnections),
				MaxHandshakes:     int(manifest.Bounds.MaxHandshakes),
				HandshakeDeadline: runtime.controlHandshakeDeadline,
			}, func(connectionContext context.Context, connection rafttransport.PeerConnection) {
				if err := runtime.controlService.Serve(connectionContext, connection); err != nil &&
					!errors.Is(err, context.Canceled) {
					runtime.config.Logf("gatewayruntime: catalog drain control: %v", err)
				}
			})
			controlDone <- errors.Join(listener.acceptErr, nonCanceledError(serveErr, context.Cause(runtime.ctx)))
			runtime.cancel()
		}()
	}
	servingContext := runtime.ctx
	// Authenticated public connections receive only their certificate principal.
	// The explicitly selected plaintext development endpoint uses the service
	// authority for native requests, as in the standalone serving path.
	if runtime.clientTLS == nil && runtime.data != nil {
		var err error
		servingContext, err = serviceauthz.WithAuthority(servingContext, runtime.config.InternalAuthority)
		if err != nil {
			return fmt.Errorf("establish serving authority: %w", err)
		}
	}
	servingContext = withGatewayControllerMetrics(servingContext, runtime.controllerMetrics)
	if runtime.ddlForwardOwner != nil {
		servingContext = context.WithValue(servingContext, gatewayDDLForwardContextKey{}, runtime.ddlForwardOwner)
	}
	if runtime.backupOperator != nil {
		servingContext = withGatewayBackupOperator(servingContext, runtime.backupOperator)
	}
	if runtime.distributedMetrics != nil {
		servingContext = withGatewayDistributedMetrics(servingContext, runtime.distributedMetrics)
		done := make(chan struct{})
		runtime.metricsDone = done
		go func() {
			defer close(done)
			if err := runtime.distributedMetrics.RunRefresh(runtime.ctx, runtime.config.ControllerInterval,
				runtime.distributedMetricsConcurrency); err != nil && runtime.ctx.Err() == nil {
				runtime.config.Logf("gatewayruntime: distributed metrics refresh: %v", err)
				runtime.setServeError(err)
				runtime.cancel()
			}
		}()
	}
	if runtime.hotShardRuntime != nil {
		runtime.hotShardDone = runGatewayHotShardPublisher(
			controllerCtx, runtime.hotShardRuntime, runtime.config.HotShardInterval, runtime.config.Logf,
		)
	}
	if runtime.splitRuntime != nil {
		done := make(chan struct{})
		runtime.splitControllerDone = done
		go func() {
			defer close(done)
			runServingSplitController(controllerCtx, runtime.authority,
				runtime.splitRuntime.controller, runtime.config.ControllerInterval, runtime.config.Logf)
		}()
	}
	runtime.servingContext = servingContext
	return nil
}
