package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type durableAdapterIssuerStub struct{ key requestledger.RequestKey }

func (stub durableAdapterIssuerStub) OpenIssuerLane(
	context.Context, serviceauthz.Authority, gateway.ReplicatedIssuerOpen,
) (gateway.ReplicatedIssuerLaneGrant, error) {
	return gateway.ReplicatedIssuerLaneGrant{}, nil
}

func (stub durableAdapterIssuerStub) ValidateRequest(
	_ context.Context, _ serviceauthz.Authority, _ gateway.ReplicatedIssuerReference,
	request requestledger.RequestID, sequence uint64,
) (requestledger.RequestKey, error) {
	key := stub.key
	key.Request, key.IssuerSequence = request, sequence
	return key, nil
}

func (stub durableAdapterIssuerStub) ValidateAcknowledge(
	ctx context.Context, authority serviceauthz.Authority, reference gateway.ReplicatedIssuerReference,
	request requestledger.RequestID, sequence uint64,
) (requestledger.RequestKey, error) {
	return stub.ValidateRequest(ctx, authority, reference, request, sequence)
}

type durableAdapterSQLStub struct {
	wantTenant []byte
	key        gateway.DurableRequestLedgerKey
	ack        gateway.DurableRequestAckResult
}

func (stub *durableAdapterSQLStub) Execute(
	_ context.Context, key requestledger.RequestKey, tenant []byte, _ []gateway.Query,
) (gateway.DurableSQLRequestResult, error) {
	if !bytes.Equal(tenant, stub.wantTenant) || key != stub.key.RequestKey {
		return gateway.DurableSQLRequestResult{}, errInvalidDurableRequestAdapter
	}
	return gateway.DurableSQLRequestResult{
		Key: stub.key, Result: &gateway.Result{}, TerminalRevision: 7,
		ResultDigest: replication.Digest{8}, AckToken: gateway.DurableRequestAckToken{9},
	}, nil
}

func (stub *durableAdapterSQLStub) Acknowledge(
	_ context.Context, key gateway.DurableRequestLedgerKey, terminal uint64,
	result replication.Digest, token gateway.DurableRequestAckToken,
) (gateway.DurableRequestAckResult, error) {
	if key != stub.key || terminal != 7 || result != (replication.Digest{8}) ||
		token != (gateway.DurableRequestAckToken{9}) {
		return gateway.DurableRequestAckResult{}, errInvalidDurableRequestAdapter
	}
	return stub.ack, nil
}

func TestReplicatedDurableRequestAdapterCarriesExactStructuredIdentity(t *testing.T) {
	authority := serviceauthz.Authority{Generation: 3}
	authority.Node[0] = 0x41
	rawTenant, err := authenticatedIssuerTenantFor(authority)
	if err != nil {
		t.Fatal(err)
	}
	_, tenantDigest, err := (authenticatedIssuerTenantResolver{}).ResolveIssuerTenant(t.Context(), authority)
	if err != nil {
		t.Fatal(err)
	}
	base := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID(authority.Node),
		TenantDigest: tenantDigest, IssuerEpoch: 5, IssuerLane: requestledger.IssuerLane{6},
	}
	identity := durableExecBatchIdentity{
		RequestID: replication.ID128{1}, Reference: gateway.ReplicatedIssuerReference{
			Installation: replication.ID128{2}, Epoch: 5, LaneOrdinal: 3,
			GrantDigest: replication.Digest{4},
		}, IssuerSequence: 11,
	}
	key := base
	key.Request = requestledger.RequestID(identity.RequestID)
	key.IssuerSequence = identity.IssuerSequence
	ledgerKey, err := gateway.NewDurableRequestLedgerKey(key, replication.Digest{0x51})
	if err != nil {
		t.Fatal(err)
	}
	sql := &durableAdapterSQLStub{wantTenant: rawTenant[:], key: ledgerKey,
		ack: gateway.DurableRequestAckResult{Applied: 13, Rounds: 2}}
	service, err := newReplicatedDurableRequestServiceWithDependencies(
		durableAdapterIssuerStub{key: base}, sql,
	)
	if err != nil {
		t.Fatal(err)
	}
	executed, err := service.ExecBatch(t.Context(), authority, identity, []gateway.Query{{SQL: "delete from t where id = ?"}})
	if err != nil {
		t.Fatal(err)
	}
	if executed.Ack.Identity.RequestDigest != ledgerKey.Digest ||
		executed.Ack.Identity.Reference != identity.Reference ||
		executed.Ack.Identity.IssuerSequence != identity.IssuerSequence {
		t.Fatalf("ACK identity drifted: %+v", executed.Ack.Identity)
	}
	acknowledged, err := service.AckExecBatch(t.Context(), authority, executed.Ack)
	if err != nil {
		t.Fatal(err)
	}
	if acknowledged.Applied != 13 || acknowledged.CollectionRounds != 2 ||
		acknowledged.durableExecBatchAckWireRequest != executed.Ack {
		t.Fatalf("ACK settlement drifted: %+v", acknowledged)
	}
}
