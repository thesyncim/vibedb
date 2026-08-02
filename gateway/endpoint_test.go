package gateway

import (
	"errors"
	"testing"
)

// TestSnapshotAddress proves the endpoint resolver returns an address for a
// known endpoint and a typed *EndpointError (matching ErrUnknownEndpoint) for a
// missing one.
func TestSnapshotAddress(t *testing.T) {
	s := testSnapshot(t, 1)

	addr, err := s.Address("ep-a")
	if err != nil || addr != "127.0.0.1:7001" {
		t.Fatalf("Address(ep-a) = %q,%v, want 127.0.0.1:7001,nil", addr, err)
	}

	_, err = s.Address("ep-missing")
	if !errors.Is(err, ErrUnknownEndpoint) {
		t.Fatalf("Address(ep-missing) err = %v, want ErrUnknownEndpoint", err)
	}
	var ee *EndpointError
	if !errors.As(err, &ee) || ee.Endpoint != "ep-missing" {
		t.Fatalf("err = %v, want *EndpointError for ep-missing", err)
	}

	if s.EndpointCount() != 2 {
		t.Fatalf("EndpointCount = %d, want 2", s.EndpointCount())
	}
}
