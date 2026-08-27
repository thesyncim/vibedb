package main

import (
	"errors"
	"path/filepath"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var durableRequestServiceTenant = [...]byte{
	'v', 'i', 'b', 'e', 'd', 'b', '/', 's', 'y', 's', 't', 'e', 'm', '/',
	'r', 'e', 'q', 'u', 'e', 's', 't', '-', 'l', 'e', 'd', 'g', 'e', 'r', 0,
}

type replicatedDurableRuntimeOptions struct {
	Planner        *gateway.Executor
	Catalog        *gateway.CatalogHolder
	CatalogControl *gateway.ReplicatedCatalogAuthority
	Replicated     *gateway.ReplicatedExecutor
	Authority      serviceauthz.Authority
	AckKey         gateway.DurableRequestAckDerivationKey
	JournalBase    string
}

// newReplicatedDurableRuntime builds the single shipped RF3 request path. All
// components share one authenticated ledger topology and one journal-backed
// execution-pin authority; a missing input aborts startup instead of enabling
// the legacy in-memory transaction path.
func newReplicatedDurableRuntime(
	options replicatedDurableRuntimeOptions,
) (*replicatedDurableRequestService, error) {
	if options.Planner == nil || options.Catalog == nil || options.CatalogControl == nil ||
		options.Replicated == nil || !options.Authority.Valid() ||
		options.AckKey == (gateway.DurableRequestAckDerivationKey{}) || options.JournalBase == "" {
		return nil, errInvalidDurableRequestAdapter
	}
	topologyHolder, err := gateway.NewCatalogDurableRequestLedgerTopologyHolder(options.Catalog)
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	rf3, err := gateway.NewReplicatedRequestLedgerRF3(gateway.ReplicatedRequestLedgerRF3Options{
		Executor: options.Replicated, Service: options.Authority,
		ServiceTenant: durableRequestServiceTenant[:],
	})
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	ledger, err := gateway.NewDurableRequestLedgerRF3(rf3)
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	resolver, err := gateway.NewCatalogDurableRequestRouteResolver(options.Catalog)
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	waves, err := gateway.NewDurableRequestLifecycleRunner(ledger, resolver, options.Replicated, options.Authority)
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	payloads, err := gateway.NewDurableRequestDynamicPayloadStore(ledger)
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	sessions, err := gateway.NewJournaledDurableRequestExecutionPinSessionFactory(
		options.Replicated, filepath.Clean(options.JournalBase)+".durable-pins", options.Authority,
	)
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	pins, err := gateway.NewNativeDurableRequestExecutionPinAuthority(
		options.Replicated, sessions, gateway.DefaultDurableRequestExecutionPinSpan,
	)
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	terminal, err := gateway.NewDurableRequestTerminalCoordinatorWithSessionFactory(
		ledger, options.Replicated, sessions,
	)
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	terminalAuthority, err := gateway.NewNativeDurableRequestTerminalAuthorityProvider(
		options.AckKey, options.Authority,
	)
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	runner, err := gateway.NewDurableRequestDistributedRunner(
		ledger, resolver, waves, payloads, terminal, terminalAuthority,
	)
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	requests, err := gateway.NewDurableRequestService(topologyHolder, ledger, runner, pins)
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	issuers, err := gateway.NewReplicatedIssuerAuthority(
		options.CatalogControl, topologyHolder, ledger, authenticatedIssuerTenantResolver{},
	)
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	sql, err := gateway.NewDurableSQLRequestExecutor(gateway.DurableSQLRequestExecutorOptions{
		Planner: options.Planner, ReplicatedData: options.Replicated, Requests: requests,
		RecoveryPulseLimit: distributedtxn.MaxRecoveryPulses,
		PlanningLeaseSpan:  requestledger.MaxPlanningLeaseSpan,
	})
	if err != nil {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	return newReplicatedDurableRequestService(issuers, sql)
}
