package gateway

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// ReplicatedIssuerAuthority composes the immutable catalog grant with the
// mutable request-ledger lane frontier. The catalog record authenticates the
// installation and the RF3 high-water row is the linearizable admission and
// anti-resurrection authority. Neither component has a process-local fallback.
type ReplicatedIssuerAuthority struct {
	catalog  *ReplicatedCatalogAuthority
	topology *DurableRequestLedgerTopologyHolder
	ledger   DurableRequestLedger
	tenants  ReplicatedIssuerTenantResolver
}

func NewReplicatedIssuerAuthority(
	catalog *ReplicatedCatalogAuthority,
	topology *DurableRequestLedgerTopologyHolder,
	ledger DurableRequestLedger,
	tenants ReplicatedIssuerTenantResolver,
) (*ReplicatedIssuerAuthority, error) {
	if catalog == nil || topology == nil || topology.Current() == nil || ledger == nil || tenants == nil {
		return nil, ErrDurableRequest
	}
	return &ReplicatedIssuerAuthority{
		catalog: catalog, topology: topology, ledger: ledger, tenants: tenants,
	}, nil
}

// OpenIssuerLane is an idempotent two-group installation. The immutable
// catalog grant is published first; a lost response or crash can then safely
// resume the byte-identical request-ledger open on any gateway. Authority is
// returned only after a linearizable read proves the lane high-water witness.
func (authority *ReplicatedIssuerAuthority) OpenIssuerLane(
	ctx context.Context,
	authenticated serviceauthz.Authority,
	open ReplicatedIssuerOpen,
) (ReplicatedIssuerLaneGrant, error) {
	if authority == nil || ctx == nil || !authenticated.Valid() {
		return ReplicatedIssuerLaneGrant{}, ErrDurableRequest
	}
	grant, err := authority.catalog.OpenIssuerLaneGrant(ctx, authenticated, authority.tenants, open)
	if err != nil {
		return ReplicatedIssuerLaneGrant{}, err
	}
	key, err := replicatedIssuerGrantProbeKey(grant)
	if err != nil {
		return ReplicatedIssuerLaneGrant{}, err
	}
	highwater, err := requestledger.NewIssuerHighwater(key)
	if err != nil {
		return ReplicatedIssuerLaneGrant{}, errors.Join(err, ErrDurableRequestConflict)
	}
	home, err := authority.home(key)
	if err != nil {
		return ReplicatedIssuerLaneGrant{}, err
	}
	result, applyErr := authority.ledger.ApplyCAS(ctx, home, key, DurableRequestLifecycleCAS{
		Operation: requestledger.OperationOpenIssuerLane, Revision: 1, IssuerOpen: highwater,
	})
	minimum := uint64(1)
	if applyErr == nil {
		if result.Ledger.ResultCode != replicatedstate.ResultApplied {
			return ReplicatedIssuerLaneGrant{}, ErrDurableRequestConflict
		}
		minimum = result.Applied
	}
	_, readErr := authority.readLane(ctx, home, key, minimum)
	if readErr != nil {
		return ReplicatedIssuerLaneGrant{}, errors.Join(applyErr, readErr)
	}
	return grant, nil
}

// ValidateRequest linearly validates the current lane on every execute and
// terminal ACK. Cached immutable grants avoid repeated decode work, but never
// replace the RF3 high-water read. A collected sequence cannot be resurrected,
// and admission cannot skip beyond the exact next sequence.
func (authority *ReplicatedIssuerAuthority) ValidateRequest(
	ctx context.Context,
	authenticated serviceauthz.Authority,
	reference ReplicatedIssuerReference,
	request requestledger.RequestID,
	sequence uint64,
) (requestledger.RequestKey, error) {
	if authority == nil || ctx == nil || !authenticated.Valid() {
		return requestledger.RequestKey{}, ErrDurableRequest
	}
	key, err := authority.catalog.ValidateIssuerRequestKey(
		ctx, authenticated, authority.tenants, reference, request, sequence,
	)
	if err != nil {
		return requestledger.RequestKey{}, err
	}
	home, err := authority.home(key)
	if err != nil {
		return requestledger.RequestKey{}, err
	}
	status, err := authority.readLane(ctx, home, key, 1)
	if err != nil {
		return requestledger.RequestKey{}, err
	}
	if sequence <= status.Highwater.HighwaterSequence ||
		status.Highwater.AdmittedSequence == ^uint64(0) ||
		sequence > status.Highwater.AdmittedSequence+1 {
		return requestledger.RequestKey{}, ErrDurableRequestConflict
	}
	return key, nil
}

func replicatedIssuerGrantProbeKey(
	grant ReplicatedIssuerLaneGrant,
) (requestledger.RequestKey, error) {
	if !validReplicatedIssuerGrant(grant) {
		return requestledger.RequestKey{}, ErrDurableRequest
	}
	key := requestledger.RequestKey{
		Scope: grant.Scope, TenantDigest: grant.TenantDigest, Principal: grant.Principal,
		Request: requestledger.RequestID(grant.Installation), IssuerEpoch: grant.Epoch,
		IssuerSequence: 1, IssuerLane: grant.Lane,
	}
	if !key.Valid() {
		return requestledger.RequestKey{}, ErrDurableRequestConflict
	}
	return key, nil
}

func (authority *ReplicatedIssuerAuthority) home(
	key requestledger.RequestKey,
) (DurableRequestLedgerHome, error) {
	point, err := requestledger.Home(key)
	if err != nil {
		return DurableRequestLedgerHome{}, errors.Join(err, ErrDurableRequestConflict)
	}
	home, _, found := authority.topology.Lookup(point)
	if !found || home.Point != point || home.Identity == (replication.Digest{}) {
		return DurableRequestLedgerHome{}, ErrDurableRequestUnavailable
	}
	return home, nil
}

func (authority *ReplicatedIssuerAuthority) readLane(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key requestledger.RequestKey,
	minimum uint64,
) (requestledger.IssuerLaneStatus, error) {
	row, err := authority.ledger.ReadRow(ctx, home, DurableRequestLifecycleRead{
		Key: key, Kind: replicatedstate.RequestLedgerReadIssuerStatus,
		MinimumApplied: max(uint64(1), minimum),
	})
	if err != nil {
		return requestledger.IssuerLaneStatus{}, err
	}
	identity, identityErr := requestledger.IssuerIdentityFor(key)
	if identityErr != nil || !row.Found || row.Kind != replicatedstate.RequestLedgerReadIssuerStatus ||
		row.IssuerStatus.RangeIdentity != requestledger.Digest(home.Identity) ||
		row.IssuerStatus.Highwater.Identity != identity ||
		row.IssuerStatus.Highwater.Home != home.Point {
		return requestledger.IssuerLaneStatus{}, errors.Join(identityErr, ErrDurableRequestConflict)
	}
	return row.IssuerStatus, nil
}
