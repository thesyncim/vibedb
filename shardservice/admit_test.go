package shardservice

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

// TestAdmit exercises the full admission truth table: the configured ownership
// admits a matching request and refuses every single-field divergence with the
// documented typed error and wire kind.
func TestAdmit(t *testing.T) {
	owner := Ownership{
		Distribution:   "tenant_data",
		Shard:          "-80",
		Epoch:          7,
		RoutingVersion: 3,
	}
	// base is a request that matches owner exactly.
	base := func() *ShardRequest {
		return &ShardRequest{
			SQL:            "SELECT 1",
			Distribution:   "tenant_data",
			Shard:          "-80",
			RoutingVersion: 3,
			OwnershipEpoch: 7,
		}
	}

	tests := []struct {
		name     string
		mutate   func(*ShardRequest)
		wantKind ErrorKind // ErrorInvalid means "admitted"
		wantIs   error     // sentinel Admit's error must match, nil when admitted
	}{
		{
			name:     "admitted",
			mutate:   func(*ShardRequest) {},
			wantKind: ErrorInvalid,
			wantIs:   nil,
		},
		{
			name:     "wrong_distribution",
			mutate:   func(r *ShardRequest) { r.Distribution = "other" },
			wantKind: ErrorNotOwner,
			wantIs:   distribution.ErrNotShardOwner,
		},
		{
			name:     "wrong_shard",
			mutate:   func(r *ShardRequest) { r.Shard = "80-" },
			wantKind: ErrorNotOwner,
			wantIs:   distribution.ErrNotShardOwner,
		},
		{
			name:     "stale_routing_version",
			mutate:   func(r *ShardRequest) { r.RoutingVersion = 2 },
			wantKind: ErrorRoutingVersion,
			wantIs:   distribution.ErrRoutingVersion,
		},
		{
			name:     "stale_epoch",
			mutate:   func(r *ShardRequest) { r.OwnershipEpoch = 6 },
			wantKind: ErrorOwnershipEpoch,
			wantIs:   distribution.ErrOwnershipEpoch,
		},
		{
			name:     "future_epoch",
			mutate:   func(r *ShardRequest) { r.OwnershipEpoch = 8 },
			wantKind: ErrorOwnershipEpoch,
			wantIs:   distribution.ErrOwnershipEpoch,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := base()
			tc.mutate(req)
			err := owner.Admit(req)
			if tc.wantIs == nil {
				if err != nil {
					t.Fatalf("Admit = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Admit = nil, want %v", tc.wantIs)
			}
			if !errors.Is(err, tc.wantIs) {
				t.Fatalf("Admit err = %v, want errors.Is %v", err, tc.wantIs)
			}
			var ae *AdmissionError
			if !errors.As(err, &ae) {
				t.Fatalf("Admit err %v is not an *AdmissionError", err)
			}
			if ae.Kind != tc.wantKind {
				t.Fatalf("AdmissionError.Kind = %v, want %v", ae.Kind, tc.wantKind)
			}
			resp := ae.Response()
			if resp.Kind != ResponseError || resp.ErrorKind != tc.wantKind {
				t.Fatalf("Response = %+v, want error frame kind %v", resp, tc.wantKind)
			}
		})
	}
}

// TestAdmitPrecedence proves the documented outermost-first ordering: when more
// than one field diverges, identity is reported before generation, and
// generation before the fencing epoch.
func TestAdmitPrecedence(t *testing.T) {
	owner := Ownership{Distribution: "d", Shard: "s", Epoch: 5, RoutingVersion: 2}

	// Every field wrong at once: identity (not-owner) must win.
	all := &ShardRequest{Distribution: "x", Shard: "y", RoutingVersion: 99, OwnershipEpoch: 99}
	if err := owner.Admit(all); !errors.Is(err, distribution.ErrNotShardOwner) {
		t.Fatalf("all-wrong: got %v, want not-owner", err)
	}

	// Identity correct, both generation and epoch wrong: generation wins.
	gen := &ShardRequest{Distribution: "d", Shard: "s", RoutingVersion: 99, OwnershipEpoch: 99}
	if err := owner.Admit(gen); !errors.Is(err, distribution.ErrRoutingVersion) {
		t.Fatalf("gen+epoch wrong: got %v, want routing-version", err)
	}
}

// TestAdmitNilRequest proves a nil request is a malformed-request refusal rather
// than a panic.
func TestAdmitNilRequest(t *testing.T) {
	owner := Ownership{Distribution: "d", Shard: "s"}
	err := owner.Admit(nil)
	var ae *AdmissionError
	if !errors.As(err, &ae) || ae.Kind != ErrorMalformedRequest {
		t.Fatalf("Admit(nil) = %v, want malformed-request AdmissionError", err)
	}
}
