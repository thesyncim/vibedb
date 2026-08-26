package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type replicatedIssuerTenantResolver struct{ tenant requestledger.Digest }

func (resolver replicatedIssuerTenantResolver) ResolveIssuerTenant(
	context.Context, serviceauthz.Authority,
) (requestledger.ScopeKind, requestledger.Digest, error) {
	return requestledger.ScopeAuthenticated, resolver.tenant, nil
}

func replicatedIssuerOpenFixture() ReplicatedIssuerOpen {
	return ReplicatedIssuerOpen{
		Installation: replication.ID128{0x71}, Epoch: 7, LaneOrdinal: 7,
	}
}

func replicatedIssuerTenantFixture() replicatedIssuerTenantResolver {
	return replicatedIssuerTenantResolver{tenant: requestledger.Digest{0x73}}
}

func TestReplicatedIssuerGrantCanonicalAndBounded(t *testing.T) {
	open := replicatedIssuerOpenFixture()
	grant, err := replicatedIssuerGrantFor(open, requestledger.ScopeAuthenticated,
		requestledger.PrincipalID{0x72}, replicatedIssuerTenantFixture().tenant)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := appendReplicatedIssuerGrant(nil, grant)
	if err != nil || len(raw) == 0 || len(raw) > maxReplicatedIssuerGrantBytes {
		t.Fatalf("bytes=%d err=%v", len(raw), err)
	}
	opened, err := openReplicatedIssuerGrant(raw, grant.Installation, grant.Epoch, grant.LaneOrdinal)
	if err != nil || opened != grant {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	again, err := appendReplicatedIssuerGrant(nil, opened)
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("noncanonical reencode err=%v", err)
	}
	if _, err = openReplicatedIssuerGrant(append(append([]byte(nil), raw...), ' '),
		grant.Installation, grant.Epoch, grant.LaneOrdinal); err == nil {
		t.Fatal("accepted trailing issuer grant bytes")
	}
	if _, err = openReplicatedIssuerGrant(raw, grant.Installation, grant.Epoch, grant.LaneOrdinal+1); err == nil {
		t.Fatal("accepted issuer grant under another lane key")
	}
}

func TestReplicatedIssuerGrantOpensIdempotentlyAcrossGateways(t *testing.T) {
	authority, client, snapshot := newCatalogAuthorityFixture(t)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(snapshot), 0x91)
	open := replicatedIssuerOpenFixture()
	tenants := replicatedIssuerTenantFixture()
	first, err := authority.OpenIssuerLaneGrant(t.Context(), authority.authority, tenants, open)
	if err != nil {
		t.Fatal(err)
	}
	second, err := peer.OpenIssuerLaneGrant(t.Context(), authority.authority, tenants, open)
	if err != nil || second != first {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	loaded, found, err := peer.ReadIssuerLaneGrant(
		t.Context(), first.Installation, first.Epoch, first.LaneOrdinal,
	)
	if err != nil || !found || loaded != first {
		t.Fatalf("loaded=%+v found=%v err=%v", loaded, found, err)
	}
	key, err := peer.ValidateIssuerRequestKey(
		t.Context(), authority.authority, tenants, ReplicatedIssuerReference{
			Installation: loaded.Installation, Epoch: loaded.Epoch,
			LaneOrdinal: loaded.LaneOrdinal, GrantDigest: loaded.GrantDigest,
		}, requestledger.RequestID{0x74}, 1,
	)
	if err != nil || key.IssuerEpoch != first.Epoch || key.IssuerLane != first.Lane ||
		key.Principal != first.Principal || key.TenantDigest != tenants.tenant {
		t.Fatalf("key=%+v err=%v", key, err)
	}
	forgedAuthority := authority.authority
	forgedAuthority.Node[0]++
	if _, err = peer.OpenIssuerLaneGrant(t.Context(), forgedAuthority, tenants, open); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("cross-principal installation reuse error=%v", err)
	}
	if len(client.rows) == 0 {
		t.Fatal("issuer grant was not persisted in catalog RF3")
	}
}

func TestReplicatedIssuerGrantResponseLossRecoversOnAnotherGateway(t *testing.T) {
	authority, client, snapshot := newCatalogAuthorityFixture(t)
	replacement := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(snapshot), 0x92)
	client.unknownNext = true
	open := replicatedIssuerOpenFixture()
	tenants := replicatedIssuerTenantFixture()
	if _, err := authority.OpenIssuerLaneGrant(t.Context(), authority.authority, tenants, open); !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("lost open response error=%v", err)
	}
	grant, err := replacement.OpenIssuerLaneGrant(t.Context(), authority.authority, tenants, open)
	if err != nil || !validReplicatedIssuerGrant(grant) {
		t.Fatalf("replacement grant=%+v err=%v", grant, err)
	}
}
