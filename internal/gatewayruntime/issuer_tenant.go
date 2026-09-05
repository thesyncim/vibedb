package gatewayruntime

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var errInvalidIssuerTenantAuthority = errors.New("gateway: invalid authenticated issuer tenant authority")

var authenticatedIssuerTenantDomain = [...]byte{
	'v', 'i', 'b', 'e', 'd', 'b', '/', 'g', 'a', 't', 'e', 'w', 'a', 'y', '/',
	'i', 's', 's', 'u', 'e', 'r', '-', 't', 'e', 'n', 'a', 'n', 't', '/', '1', 0,
}

// authenticatedIssuerTenantResolver binds the unreleased public protocol to
// the authenticated client installation principal. Policy generations may
// rotate without moving the tenant; the stable certificate NodeID is the sole
// input and callers cannot supply or spoof a tenant string.
type authenticatedIssuerTenantResolver struct{}

type authenticatedIssuerTenant [len(authenticatedIssuerTenantDomain) + 16]byte

func authenticatedIssuerTenantFor(
	authority serviceauthz.Authority,
) (authenticatedIssuerTenant, error) {
	if !authority.Valid() {
		return authenticatedIssuerTenant{}, errInvalidIssuerTenantAuthority
	}
	var tenant authenticatedIssuerTenant
	at := copy(tenant[:], authenticatedIssuerTenantDomain[:])
	copy(tenant[at:], authority.Node[:])
	return tenant, nil
}

func (authenticatedIssuerTenantResolver) ResolveIssuerTenant(
	ctx context.Context,
	authority serviceauthz.Authority,
) (requestledger.ScopeKind, requestledger.Digest, error) {
	if ctx == nil || !authority.Valid() {
		return requestledger.ScopeInvalid, requestledger.Digest{}, errInvalidIssuerTenantAuthority
	}
	if err := ctx.Err(); err != nil {
		return requestledger.ScopeInvalid, requestledger.Digest{}, err
	}
	raw, err := authenticatedIssuerTenantFor(authority)
	if err != nil {
		return requestledger.ScopeInvalid, requestledger.Digest{}, err
	}
	tenant := requestledger.Digest(sha256.Sum256(raw[:]))
	if tenant == (requestledger.Digest{}) {
		return requestledger.ScopeInvalid, requestledger.Digest{}, errInvalidIssuerTenantAuthority
	}
	return requestledger.ScopeAuthenticated, tenant, nil
}
