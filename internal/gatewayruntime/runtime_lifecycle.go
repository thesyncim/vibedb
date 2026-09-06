package gatewayruntime

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/storeio"
)

func (runtime *Runtime) runServe() {
	// Every exit, including partial optional-service startup, cancels and joins
	// all users before publishing drain completion or releasing their transport.
	defer runtime.finishDrain()
	// A durable NodeDraining cut may have been restored before Serve starts.
	// Close only the public listener now; optional controls and their contexts
	// remain live for the final decommission acknowledgement.
	if runtime.frontend != nil && runtime.frontend.isDraining() {
		runtime.BeginFrontendDrain()
	}
	logf := runtime.config.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if err := runtime.startOptionalServices(); err != nil {
		runtime.setServeError(err)
		return
	}
	if runtime.ctx.Err() != nil {
		return
	}

	// The caller may own the PostgreSQL listener separately in an embedded
	// composition. The command configuration uses the same lifecycle and keeps
	// this optional endpoint under the frontend's cancellation context.
	if runtime.config.PGListenAddress != "" {
		writeService, ok := runtime.durable.(postgresDurableService)
		if !ok {
			runtime.setServeError(fmt.Errorf("gatewayruntime: PostgreSQL requires durable write service"))
			return
		}
		writer, err := openPostgresTableWriters(
			runtime.config.CatalogSessionJournal+".pg-writes",
			runtime.config.InternalAuthority, writeService,
		)
		if err != nil {
			runtime.setServeError(err)
			return
		}
		runtime.pgWriter = writer
		writerDone := make(chan struct{})
		runtime.pgWriterDone = writerDone
		go func() {
			defer close(writerDone)
			writer.Run(runtime.ctx)
		}()
		pg, err := startGatewayPostgreSQLWithFrontend(runtime.ctx, runtime.config.PGListenAddress,
			runtime.exec, runtime.config.InternalAuthority, writer.Write, logf, runtime.frontend, runtime.pgDDL)
		if err != nil {
			runtime.setServeError(err)
			return
		}
		runtime.pg = pg
		if runtime.frontend != nil {
			runtime.frontend.bindPG(pg)
		}
	}

	if runtime.ctx.Err() != nil {
		return
	}
	close(runtime.ready)
	logf("vibedb-gateway serving catalog generation %d on %s",
		runtime.holder.Current().Generation(), runtime.listener.Addr())
	var err error
	frontendListener := &frontendAdmissionListener{Listener: runtime.listener, frontend: runtime.frontend}
	if runtime.clientTLS != nil {
		err = serveAuthenticatedGatewayDurableData(runtime.servingContext, frontendListener,
			runtime.exec, runtime.data, runtime.durable, runtime.clientTLS,
			gatewayClientTLSLimits(runtime.config), logf)
	} else {
		err = serveGatewayDurableData(runtime.servingContext, frontendListener,
			runtime.exec, runtime.data, runtime.durable, logf)
	}
	runtime.setServeError(nonCanceledError(err, context.Cause(runtime.ctx)))
}

func gatewayClientTLSLimits(config Config) gateway.ClientTLSLimits {
	return gateway.ClientTLSLimits{
		MaxConnections:    config.MaxConnections,
		MaxHandshakes:     config.MaxHandshakes,
		HandshakeDeadline: servicetls.FixedDeadline(config.TLSHandshakeTimeout),
	}
}

func (runtime *Runtime) setServeError(err error) {
	if err == nil {
		return
	}
	runtime.serveMu.Lock()
	runtime.serveErr = errors.Join(runtime.serveErr, err)
	runtime.serveMu.Unlock()
}

func (runtime *Runtime) closeListeners() error {
	if runtime == nil {
		return nil
	}
	var err error
	err = errors.Join(err, closeGatewayListener(runtime.listener))
	err = errors.Join(err, closeGatewayListener(runtime.controlListener))
	return err
}

func closeGatewayListener(listener net.Listener) error {
	if listener == nil {
		return nil
	}
	err := listener.Close()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (runtime *Runtime) waitOptionalServices() {
	if runtime == nil {
		return
	}
	// PostgreSQL sessions can retain reply leases and native write outboxes.
	// Join sessions before closing writers, then stop every controller/observer
	// before a live catalog session rollover or terminal seed handoff is retired.
	if runtime.pg != nil {
		runtime.setDrainError(runtime.pg.Close())
	}
	if runtime.pgWriter != nil {
		runtime.setDrainError(runtime.pgWriter.Close())
	}
	if runtime.pgWriterDone != nil {
		<-runtime.pgWriterDone
	}
	if runtime.controlDone != nil {
		runtime.setServeError(<-runtime.controlDone)
	}
	if runtime.replicaControllersDone != nil {
		<-runtime.replicaControllersDone
	}
	if runtime.scalingDone != nil {
		<-runtime.scalingDone
	}
	if runtime.splitControllerDone != nil {
		<-runtime.splitControllerDone
	}
	if runtime.hotShardDone != nil {
		<-runtime.hotShardDone
	}
	if runtime.metricsDone != nil {
		<-runtime.metricsDone
	}
	if runtime.controlDirectoryDone != nil {
		<-runtime.controlDirectoryDone
	}
	if runtime.routeSeedDone != nil {
		<-runtime.routeSeedDone
	}
	if runtime.routeSeedControl != nil {
		if required, err := completeReplicatedCatalogRouteSeedHandoff(
			runtime.routeSeedControl, runtime.config.CatalogAttempts, runtime.config.CatalogAttemptTimeout,
		); required && err != nil {
			runtime.setDrainError(err)
		}
	}
}

func (runtime *Runtime) closeResources() error {
	if runtime == nil {
		return nil
	}
	var err error
	err = errors.Join(err, closeGatewayListener(runtime.listener))
	err = errors.Join(err, closeGatewayListener(runtime.controlListener))
	if runtime.splitRuntime != nil {
		err = errors.Join(err, runtime.splitRuntime.Close())
	}
	if runtime.backupRepository != nil {
		err = errors.Join(err, runtime.backupRepository.Close())
	}
	if runtime.replicatedPool != nil {
		err = errors.Join(err, runtime.replicatedPool.Close())
	}
	if runtime.shardTLS != nil {
		err = errors.Join(err, runtime.shardTLS.Close())
	}
	if runtime.ddlForwardTLS != nil {
		err = errors.Join(err, runtime.ddlForwardTLS.Close())
	}
	// Recovery identities remain exclusively owned until every frontend user,
	// route handoff and owned transport has finished closing. Keep the lock
	// file itself: unlinking it would let a concurrent opener lock a new inode.
	if runtime.journalLock != nil {
		err = errors.Join(err, storeio.UnlockWriter(runtime.journalLock), runtime.journalLock.Close())
	}
	return err
}
