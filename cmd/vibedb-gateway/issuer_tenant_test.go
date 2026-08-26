package main

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestAuthenticatedIssuerTenantStableAcrossPolicyGeneration(t *testing.T) {
	resolver := authenticatedIssuerTenantResolver{}
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 0x71
	scope, first, err := resolver.ResolveIssuerTenant(context.Background(), authority)
	if err != nil || scope != requestledger.ScopeAuthenticated || first == (requestledger.Digest{}) {
		t.Fatalf("scope=%d tenant=%x err=%v", scope, first, err)
	}
	authority.Generation++
	_, second, err := resolver.ResolveIssuerTenant(context.Background(), authority)
	if err != nil || second != first {
		t.Fatalf("tenant changed across policy generation: first=%x second=%x err=%v", first, second, err)
	}
	authority.Node[0]++
	_, other, err := resolver.ResolveIssuerTenant(context.Background(), authority)
	if err != nil || other == first {
		t.Fatalf("distinct principal tenant=%x err=%v", other, err)
	}
}

func TestAuthenticatedIssuerTenantFailsClosed(t *testing.T) {
	resolver := authenticatedIssuerTenantResolver{}
	if _, _, err := resolver.ResolveIssuerTenant(nil, serviceauthz.Authority{}); !errors.Is(err, errInvalidIssuerTenantAuthority) {
		t.Fatalf("nil context error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	if _, _, err := resolver.ResolveIssuerTenant(ctx, authority); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error=%v", err)
	}
}
