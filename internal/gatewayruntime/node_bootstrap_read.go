package gatewayruntime

import (
	"fmt"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// newGatewayBootstrapReadService binds the read-only empty-node capability to
// the same replicated catalog authority used by the gateway controllers.  The
// caller supplies the already-authenticated gateway-control roster check; the
// nodecontrol service then rechecks the committed physical NodeRecord and its
// exact SPKI binding before reading an enrollment row.
func newGatewayBootstrapReadService(
	authority *gateway.ReplicatedCatalogAuthority,
	trustDomain rafttransport.TrustDomain,
	authorize nodecontrol.BootstrapReadAuthorizeFunc,
	readDeadline rafttransport.DeadlineFunc,
	writeDeadline rafttransport.DeadlineFunc,
	maxConcurrent int,
	authenticated ...nodecontrol.BootstrapReadAuthenticatedAuthorizeFunc,
) (*nodecontrol.BootstrapReadService, error) {
	if authority == nil {
		return nil, fmt.Errorf("%w: bootstrap read requires a replicated catalog authority", ErrInvalidConfig)
	}
	var authorizeAuthenticated nodecontrol.BootstrapReadAuthenticatedAuthorizeFunc
	if len(authenticated) > 1 {
		return nil, fmt.Errorf("%w: duplicate bootstrap peer authorizer", ErrInvalidConfig)
	} else if len(authenticated) == 1 {
		authorizeAuthenticated = authenticated[0]
	}
	service, err := nodecontrol.NewBootstrapReadService(nodecontrol.BootstrapReadServiceOptions{
		Authority: authority, TrustDomain: trustDomain, Authorize: authorize,
		AuthorizeAuthenticated: authorizeAuthenticated,
		ReadDeadline:           readDeadline, WriteDeadline: writeDeadline, MaxConcurrent: maxConcurrent,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: bootstrap read service: %v", ErrInvalidConfig, err)
	}
	return service, nil
}
