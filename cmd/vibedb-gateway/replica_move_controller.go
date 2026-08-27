package main

import (
	"context"
	"errors"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/rebalanceexec"
)

// gatewayReplicaMoveControls names only shard-control capabilities which do
// not yet have shipped clients. The catalog journal, membership proposal path,
// and catalog publication are concrete gateway services. Cluster catalog drain
// is an authenticated fanout capability and must be supplied explicitly; a
// local CatalogHolder is never sufficient move-completion authority.
type gatewayReplicaMoveControls struct {
	Observer rebalance.ReplicatedMoveObserver
	// HealthObservations is the authenticated shard-control client shared with
	// the failure scheduler. It only supplies current liveness/donor cuts.
	HealthObservations gatewayReplicaObservationClient
	GrantInstaller     gatewayMembershipGrantInstaller
	Routes             rebalanceexec.MoveRouteResolver
	Membership         rebalanceexec.MembershipClient
	Snapshots          rebalanceexec.SnapshotSource
	Bootstrap          rebalanceexec.SnapshotBootstrapClient
	Awaiter            rebalanceexec.MoveAwaiter
	Ownership          rebalanceexec.OwnershipProposer
	Drainer            rebalanceexec.CatalogDrainCertifier
	Retirement         rebalanceexec.SourceRetirer
}

func newGatewayReplicaMoveController(
	authority *gateway.ReplicatedCatalogAuthority,
	replicated *gateway.ReplicatedExecutor,
	controls gatewayReplicaMoveControls,
) (*rebalanceexec.Controller, error) {
	if authority == nil || replicated == nil || controls.Observer == nil ||
		controls.Membership == nil ||
		controls.Drainer == nil {
		return nil, rebalanceexec.ErrControllerConfig
	}
	executor, err := rebalanceexec.New(rebalanceexec.Options{
		Routes: controls.Routes, Grants: authority, Membership: controls.Membership,
		Snapshots: controls.Snapshots, Bootstrap: controls.Bootstrap,
		Awaiter: controls.Awaiter, Ownership: controls.Ownership,
		Catalog: authority, Drainer: controls.Drainer, Retirer: controls.Retirement,
	})
	if err != nil {
		return nil, err
	}
	controller, err := rebalanceexec.NewController(authority, authority, controls.Observer, executor)
	if err != nil {
		return nil, err
	}
	if source, ok := controls.Snapshots.(rebalanceexec.SnapshotAbandonmentClient); ok {
		controller.InstallAbandonmentScheduler(&rebalanceexec.AbandonmentScheduler{
			Directory: authority, Authority: rebalanceexec.CatalogAbandonmentAuthority{Journal: authority},
			Source: source, MaxRecords: 64, MaxBytes: 4 << 30,
		})
	}
	return controller, nil
}

type replicaMovePassRunner interface {
	RunPass(context.Context) (rebalanceexec.ControllerPass, error)
}

// runReplicaMoveController is the shipped gateway scheduling loop. It owns no
// queue and performs no speculative retries: each pass reopens the RF3 catalog
// directory, then advances each move through one journaled evidence boundary.
func runReplicaMoveController(
	ctx context.Context,
	controller replicaMovePassRunner,
	interval time.Duration,
	logf func(string, ...any),
) {
	if ctx == nil || controller == nil || interval <= 0 || logf == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		started := time.Now()
		pass, err := controller.RunPass(ctx)
		gatewayControllerMetricsFromContext(ctx).observeMove(pass, err, time.Since(started))
		if err != nil && !errors.Is(err, context.Canceled) {
			logf("gateway: replica move controller: %v", err)
		} else if pass.Advanced != 0 || pass.Completed != 0 || pass.AbandonmentDeleted != 0 {
			logf(
				"gateway: replica move controller advanced %d/%d move(s), completed %d; abandoned %d/%d witnessed (%d scanned, %d bytes)",
				pass.Advanced, pass.Moves, pass.Completed, pass.AbandonmentDeleted,
				pass.AbandonmentWitnessed, pass.AbandonmentScanned, pass.AbandonmentBytes,
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
