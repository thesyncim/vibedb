package gateway

import (
	"errors"

	"github.com/thesyncim/vibedb/distribution"
)

// The endpoint resolver: the transport-side translation from an opaque
// distribution.EndpointID — which the dependency-free router never interprets —
// to a network address, read from the pinned snapshot's membership map.

// ErrUnknownEndpoint is the sentinel a resolution failure matches under
// errors.Is: an endpoint absent from the snapshot's membership.
var ErrUnknownEndpoint = errors.New("gateway: endpoint has no address in the catalog")

// EndpointError reports an endpoint that resolves to no address. It wraps
// ErrUnknownEndpoint.
type EndpointError struct {
	Endpoint distribution.EndpointID
}

func (e *EndpointError) Error() string {
	return "gateway: endpoint " + string(e.Endpoint) + " has no address in the catalog"
}

func (e *EndpointError) Unwrap() error { return ErrUnknownEndpoint }

// Address resolves endpoint to its network address in this generation. A missing
// endpoint is a typed *EndpointError (matching ErrUnknownEndpoint) rather than an
// empty string, so a routed target that cannot be reached fails closed.
func (s *Snapshot) Address(endpoint distribution.EndpointID) (string, error) {
	if addr, ok := s.endpoints[endpoint]; ok {
		return addr, nil
	}
	return "", &EndpointError{Endpoint: endpoint}
}

// EndpointCount reports the number of endpoints in this generation's membership.
func (s *Snapshot) EndpointCount() int { return len(s.endpoints) }
