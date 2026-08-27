package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var errGatewayBackupOperator = errors.New("vibedb-gateway: backup operator unavailable")

type gatewayBackupOperatorRuntime struct {
	authority   *gateway.ReplicatedCatalogAuthority
	gate        *serviceauthz.Gate
	principal   serviceauthz.Authority
	repository  *clusterbackup.BackupRepository
	opener      *gatewayShardControlOpener
	observer    gatewayReplicaObservationClient
	read, write rafttransport.DeadlineFunc
}

type gatewayBackupCatalogAuthority struct {
	authority *gateway.ReplicatedCatalogAuthority
}

func (adapter gatewayBackupCatalogAuthority) ReadOperation(ctx context.Context, id [32]byte) (gateway.ReplicatedOperationRecord, error) {
	return adapter.authority.ReadOperation(ctx, id)
}
func (adapter gatewayBackupCatalogAuthority) SubmitOperation(ctx context.Context, record gateway.ReplicatedOperationRecord) error {
	return adapter.authority.SubmitOperation(ctx, record)
}
func (adapter gatewayBackupCatalogAuthority) AdvanceOperation(ctx context.Context, expected uint64,
	record gateway.ReplicatedOperationRecord,
) error {
	return adapter.authority.PublishOperation(ctx, expected, record)
}

func (runtime *gatewayBackupOperatorRuntime) Run(ctx context.Context, operation [sha256.Size]byte) (
	gateway.ReplicatedOperationRecord, clusterbackup.Certificate, error,
) {
	if runtime == nil || ctx == nil || operation == ([sha256.Size]byte{}) || runtime.authority == nil ||
		runtime.gate == nil || runtime.repository == nil || runtime.opener == nil || runtime.observer == nil {
		return gateway.ReplicatedOperationRecord{}, clusterbackup.Certificate{}, errGatewayBackupOperator
	}
	snapshot, err := runtime.authority.Read(ctx)
	if err != nil {
		return gateway.ReplicatedOperationRecord{}, clusterbackup.Certificate{}, err
	}
	cut, routes, err := gateway.BackupCatalogCut(snapshot)
	if err != nil {
		return gateway.ReplicatedOperationRecord{}, clusterbackup.Certificate{}, err
	}
	record, err := gateway.NewBackupOperation(operation, cut)
	if err != nil {
		return gateway.ReplicatedOperationRecord{}, clusterbackup.Certificate{}, err
	}
	adapter := gatewayBackupCatalogAuthority{authority: runtime.authority}
	lifecycle, err := gateway.NewBackupOperationController(adapter, runtime.gate, runtime.principal)
	if err != nil {
		return gateway.ReplicatedOperationRecord{}, clusterbackup.Certificate{}, err
	}
	coordinator, err := gateway.NewBackupRepositoryCoordinator(lifecycle, runtime.repository)
	if err != nil {
		return gateway.ReplicatedOperationRecord{}, clusterbackup.Certificate{}, err
	}
	if err = lifecycle.Submit(ctx, record); err != nil {
		stored, readErr := runtime.authority.ReadOperation(ctx, operation)
		if readErr != nil || stored.ID != record.ID || stored.Kind != record.Kind ||
			stored.CatalogGeneration != record.CatalogGeneration ||
			stored.IntentDigest != record.IntentDigest || !bytes.Equal(stored.Intent, record.Intent) {
			return gateway.ReplicatedOperationRecord{}, clusterbackup.Certificate{}, errors.Join(err, readErr)
		}
		if !stored.Equal(record) {
			return coordinator.ResumeExport(ctx, stored)
		}
		record = stored
	}
	resolver, err := newGatewayBackupLeaderResolver(operation, runtime.opener, runtime.observer,
		runtime.read, runtime.write, routes)
	if err != nil {
		return gateway.ReplicatedOperationRecord{}, clusterbackup.Certificate{}, err
	}
	return coordinator.CollectFromLeaders(ctx, record, cut, resolver)
}

func (runtime *gatewayBackupOperatorRuntime) Status(ctx context.Context, operation [sha256.Size]byte) (gateway.ReplicatedOperationRecord, error) {
	if runtime == nil || ctx == nil || operation == ([sha256.Size]byte{}) || runtime.authority == nil ||
		runtime.gate == nil ||
		runtime.gate.CheckAuthority(runtime.principal, serviceauthz.CapabilityBackup) != serviceauthz.DecisionAllow {
		return gateway.ReplicatedOperationRecord{}, errGatewayBackupOperator
	}
	return runtime.authority.ReadOperation(ctx, operation)
}
