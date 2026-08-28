package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

type replicatedIssuerAuthorityLedger struct {
	mu        sync.Mutex
	highwater requestledger.IssuerHighwaterRecord
	identity  requestledger.Digest
	applied   uint64
	failApply bool
	reads     uint64
}

func (ledger *replicatedIssuerAuthorityLedger) ApplyCAS(
	_ context.Context, home DurableRequestLedgerHome, key requestledger.RequestKey,
	cas DurableRequestLifecycleCAS,
) (DurableRequestLifecycleCASResult, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.reads++
	if cas.Operation != requestledger.OperationOpenIssuerLane || cas.Revision != 1 ||
		cas.ExpectedRevision != 0 || cas.IssuerOpen.Identity.IssuerLane != key.IssuerLane ||
		cas.IssuerOpen.Home != home.Point {
		return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
	}
	if ledger.highwater.Revision == 0 {
		ledger.highwater = cas.IssuerOpen
		ledger.identity = requestledger.Digest(home.Identity)
		ledger.applied++
	} else if ledger.highwater != cas.IssuerOpen {
		return DurableRequestLifecycleCASResult{Ledger: replicatedstate.RequestLedgerCompletionResult{
			ResultCode: replicatedstate.ResultRequestLedgerConflict,
		}}, nil
	}
	result := DurableRequestLifecycleCASResult{
		Ledger:  replicatedstate.RequestLedgerCompletionResult{ResultCode: replicatedstate.ResultApplied},
		Applied: ledger.applied,
	}
	if ledger.failApply {
		ledger.failApply = false
		return DurableRequestLifecycleCASResult{}, errLifecycleRunnerFault
	}
	return result, nil
}

func (ledger *replicatedIssuerAuthorityLedger) ReadRow(
	_ context.Context, _ DurableRequestLedgerHome, read DurableRequestLifecycleRead,
) (DurableRequestLifecycleRow, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if read.Kind != replicatedstate.RequestLedgerReadIssuerStatus || read.MinimumApplied == 0 {
		return DurableRequestLifecycleRow{}, ErrDurableRequestConflict
	}
	identity, _ := requestledger.IssuerIdentityFor(read.Key)
	if ledger.highwater.Revision == 0 || identity != ledger.highwater.Identity {
		return DurableRequestLifecycleRow{Kind: read.Kind, Applied: ledger.applied}, nil
	}
	status, err := requestledger.NewIssuerLaneStatus(ledger.identity, ledger.highwater, nil, nil)
	return DurableRequestLifecycleRow{Found: err == nil, Kind: read.Kind,
		Applied: ledger.applied, IssuerStatus: status}, err
}

func replicatedIssuerAuthorityFixture(
	t *testing.T, catalog *ReplicatedCatalogAuthority, ledger DurableRequestLedger,
) *ReplicatedIssuerAuthority {
	t.Helper()
	holder := new(DurableRequestLedgerTopologyHolder)
	holder.current.Store(&DurableRequestLedgerTopology{Generation: 1, Ranges: []DurableRequestLedgerRange{{
		Identity: replication.Digest{0x91},
	}}})
	authority, err := NewReplicatedIssuerAuthority(catalog, holder, ledger, replicatedIssuerTenantFixture())
	if err != nil {
		t.Fatal(err)
	}
	return authority
}

func TestReplicatedIssuerAuthorityCrossGatewayLinearValidation(t *testing.T) {
	catalog, _, snapshot := newCatalogAuthorityFixture(t)
	peerCatalog := newCatalogAuthorityPeer(t, catalog, NewCatalogHolder(snapshot), 0xa1)
	ledger := new(replicatedIssuerAuthorityLedger)
	first := replicatedIssuerAuthorityFixture(t, catalog, ledger)
	peer := replicatedIssuerAuthorityFixture(t, peerCatalog, ledger)
	open := replicatedIssuerOpenFixture()
	grant, err := first.OpenIssuerLane(t.Context(), catalog.authority, open)
	if err != nil {
		t.Fatal(err)
	}
	reference := ReplicatedIssuerReference{Installation: grant.Installation, Epoch: grant.Epoch,
		LaneOrdinal: grant.LaneOrdinal, GrantDigest: grant.GrantDigest}
	key, err := peer.ValidateRequest(t.Context(), catalog.authority, reference,
		requestledger.RequestID{0x81}, 1)
	if err != nil || key.IssuerLane != grant.Lane || key.Principal != grant.Principal {
		t.Fatalf("key=%+v err=%v", key, err)
	}
	ledger.mu.Lock()
	reads := ledger.reads
	ledger.mu.Unlock()
	forged := catalog.authority
	forged.Node[0]++
	if _, err = peer.ValidateRequest(t.Context(), forged, reference,
		requestledger.RequestID{0x81}, 1); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("forged principal err=%v", err)
	}
	// Sequence ordering is deliberately not preflighted: the first lifecycle
	// CAS is its single linearization point.
	if _, err = peer.ValidateRequest(t.Context(), catalog.authority, reference,
		requestledger.RequestID{0x82}, 2); err != nil {
		t.Fatalf("local grant validation err=%v", err)
	}
	if _, err = peer.ValidateAcknowledge(t.Context(), catalog.authority, reference,
		requestledger.RequestID{0x81}, 1); err != nil {
		t.Fatalf("ack retry did not reach tombstone path err=%v", err)
	}
	ledger.mu.Lock()
	if ledger.reads != reads {
		t.Fatalf("warm validation performed ledger reads: before=%d after=%d", reads, ledger.reads)
	}
	ledger.mu.Unlock()
}

func TestReplicatedIssuerAuthorityOpenResponseLossRecovers(t *testing.T) {
	catalog, _, snapshot := newCatalogAuthorityFixture(t)
	peerCatalog := newCatalogAuthorityPeer(t, catalog, NewCatalogHolder(snapshot), 0xa2)
	ledger := &replicatedIssuerAuthorityLedger{failApply: true}
	first := replicatedIssuerAuthorityFixture(t, catalog, ledger)
	peer := replicatedIssuerAuthorityFixture(t, peerCatalog, ledger)
	open := replicatedIssuerOpenFixture()
	grant, err := first.OpenIssuerLane(t.Context(), catalog.authority, open)
	if err != nil || !validReplicatedIssuerGrant(grant) {
		t.Fatalf("resolved outcome-unknown grant=%+v err=%v", grant, err)
	}
	again, err := peer.OpenIssuerLane(t.Context(), catalog.authority, open)
	if err != nil || again != grant {
		t.Fatalf("replacement grant=%+v err=%v", again, err)
	}
}
