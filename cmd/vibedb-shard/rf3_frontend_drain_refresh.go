package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/gatewayruntime"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardservice"
)

var (
	errRF3FrontendDrainCutRefreshUnavailable = errors.New("vibedb-shard: canonical frontend drain cut refresh unavailable")
	errRF3FrontendDrainCutRefreshState       = errors.New("vibedb-shard: canonical frontend drain cut refresh state mismatch")
)

type rf3LatestServiceCutReader interface {
	ReadLatestServiceCut(context.Context, frontenddrain.ServiceCutReadLatestRequest) (frontenddrain.ServiceCut, error)
}

// rf3LocalCatalogServiceCutReader projects the locally owned catalog rows into
// a complete service cut. Catalog leaders install this proof without dialing a
// gateway that has not opened yet.
type rf3LocalCatalogServiceCutReader struct {
	rows             gateway.FrontendDrainRuntimeCutRowReader
	profile          *rafttransport.PeerTLS
	policyGeneration uint64
}

func (reader rf3LocalCatalogServiceCutReader) ReadLatestServiceCut(
	ctx context.Context, query frontenddrain.ServiceCutReadLatestRequest,
) (frontenddrain.ServiceCut, error) {
	if ctx == nil || reader.rows == nil || reader.profile == nil || reader.policyGeneration == 0 || !query.Valid() {
		return frontenddrain.ServiceCut{}, errRF3FrontendDrainCutRefreshUnavailable
	}
	source, err := gateway.ReadFrontendDrainRuntimeCutFromRows(ctx, reader.rows)
	if err != nil {
		return frontenddrain.ServiceCut{}, err
	}
	cut, err := gatewayruntime.PreparedAckCutFromFrontendDrainRuntimeCut(
		ctx, source, reader.profile, reader.policyGeneration)
	if err != nil {
		return frontenddrain.ServiceCut{}, err
	}
	if !cut.Valid() || !cut.AtLeastFloor(query.SourceFloor) {
		return frontenddrain.ServiceCut{}, errRF3FrontendDrainCutRefreshState
	}
	return cut, nil
}

// rf3PreferLocalServiceCutReader tries the local catalog owner first and
// falls back to the authenticated remote source. Followers keep using the
// remote path; a missing local source never disables it.
type rf3PreferLocalServiceCutReader struct {
	local  rf3LatestServiceCutReader
	remote rf3LatestServiceCutReader
}

func (reader rf3PreferLocalServiceCutReader) ReadLatestServiceCut(
	ctx context.Context, query frontenddrain.ServiceCutReadLatestRequest,
) (frontenddrain.ServiceCut, error) {
	if reader.local != nil {
		cut, err := reader.local.ReadLatestServiceCut(ctx, query)
		if err == nil {
			return cut, nil
		}
	}
	if reader.remote == nil {
		return frontenddrain.ServiceCut{}, errRF3FrontendDrainCutRefreshUnavailable
	}
	return reader.remote.ReadLatestServiceCut(ctx, query)
}

// refreshRF3FrontendDrainServiceCut performs one bounded authoritative
// recovery read and installs the returned complete cut before any request can
// use the native receiver. The source floor is read from the retained server
// state; caller-supplied coordinates never authorize a newer cut.
func refreshRF3FrontendDrainServiceCut(
	ctx context.Context,
	reader rf3LatestServiceCutReader,
	profile *rafttransport.PeerTLS,
	localIncarnation uint64,
	server *shardservice.ReplicatedServer,
) error {
	if ctx == nil || reader == nil || profile == nil || server == nil || localIncarnation == 0 {
		return errRF3FrontendDrainCutRefreshUnavailable
	}
	identity := profile.LocalIdentity()
	if identity.Node == (rafttransport.NodeID{}) || identity.TrustDomain.ClusterID == ([16]byte{}) ||
		identity.TrustDomain.ClusterIncarnation == ([16]byte{}) || profile.LocalServiceKeyDigest() == ([32]byte{}) {
		return errRF3FrontendDrainCutRefreshUnavailable
	}
	floor, haveFloor := server.ServiceCutCoordinates()
	if !haveFloor {
		floor = frontenddrain.PreparedAckCutReadFloor{}
	} else if !floor.Valid() || floor == (frontenddrain.PreparedAckCutReadFloor{}) {
		return errRF3FrontendDrainCutRefreshState
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return errors.Join(errRF3FrontendDrainCutRefreshUnavailable, err)
	}
	query := frontenddrain.ServiceCutReadLatestRequest{
		Operation: frontenddrain.ReadLatest, Nonce: nonce,
		ReceiverNode: identity.Node, ReceiverIncarnation: localIncarnation,
		ReceiverServiceKeyDigest: profile.LocalServiceKeyDigest(), SourceFloor: floor,
	}
	cut, err := reader.ReadLatestServiceCut(ctx, query)
	if err != nil {
		return errors.Join(errRF3FrontendDrainCutRefreshUnavailable, err)
	}
	if !cut.Valid() || !cut.AtLeastFloor(floor) {
		return errRF3FrontendDrainCutRefreshState
	}
	applied, err := server.InstallFrontendDrainServiceCut(ctx, cut)
	if err != nil {
		return errors.Join(errRF3FrontendDrainCutRefreshState, err)
	}
	if applied != cut.ServiceDirectoryRevision {
		return fmt.Errorf("%w: applied revision=%d cut revision=%d", errRF3FrontendDrainCutRefreshState, applied, cut.ServiceDirectoryRevision)
	}
	got, ok := server.ServiceCutCoordinates()
	if !ok || got != cut.ReadFloor() {
		return errRF3FrontendDrainCutRefreshState
	}
	return nil
}

// runRF3FrontendDrainServiceCutRefresh retries bounded source reads until the
// first complete cut is installed, then refreshes the same retained receiver
// on a fixed cadence. A missing source keeps readiness closed and never opens
// the static Policy fallback.
func runRF3FrontendDrainServiceCutRefresh(
	ctx context.Context,
	reader rf3LatestServiceCutReader,
	profile *rafttransport.PeerTLS,
	localIncarnation uint64,
	server *shardservice.ReplicatedServer,
	interval time.Duration,
	ready chan<- struct{},
) error {
	if ctx == nil || reader == nil || profile == nil || server == nil || ready == nil {
		return errRF3FrontendDrainCutRefreshUnavailable
	}
	if interval <= 0 {
		interval = time.Second
	}
	readyClosed := false
	lastError := ""
	lastErrorReport := time.Time{}
	closeReady := func() {
		if !readyClosed {
			close(ready)
			readyClosed = true
		}
	}
	reportError := func(err error) {
		if err == nil {
			return
		}
		now := time.Now()
		message := err.Error()
		if message == lastError && !lastErrorReport.IsZero() && now.Sub(lastErrorReport) < 30*time.Second {
			return
		}
		lastError, lastErrorReport = message, now
		fmt.Fprintf(os.Stderr, "RF3 frontend drain service cut refresh: %v\n", err)
	}
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, rf3NetworkTimeout)
		err := refreshRF3FrontendDrainServiceCut(attemptCtx, reader, profile, localIncarnation, server)
		cancel()
		if err == nil {
			closeReady()
		} else if !readyClosed {
			// Keep readiness closed while the canonical gate is unavailable. Emit
			// the first/current failure at a bounded rate so startup diagnostics can
			// distinguish a missing source from an installed-gate denial.
			reportError(err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			// Readiness is a success-only latch. Cancellation is delivered through
			// the shared context to the waiter; closing this channel here would
			// incorrectly release a gateway during supervisor teardown.
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			return nil
		case <-timer.C:
		}
	}
}

func waitRF3FrontendDrainServiceCutReady(ctx context.Context, ready <-chan struct{}) error {
	if ctx == nil || ready == nil {
		return errRF3FrontendDrainCutRefreshUnavailable
	}
	timer := time.NewTimer(rf3NetworkTimeout)
	defer timer.Stop()
	select {
	case <-ready:
		if serverReady := context.Cause(ctx); serverReady != nil {
			return serverReady
		}
		return nil
	case <-ctx.Done():
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return context.Canceled
	case <-timer.C:
		return errRF3FrontendDrainCutRefreshUnavailable
	}
}
