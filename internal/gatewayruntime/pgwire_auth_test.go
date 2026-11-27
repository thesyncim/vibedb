package gatewayruntime

import (
	"crypto/tls"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
)

func TestValidatePGEndpointOptionsEnforcesRemoteTLSAndSCRAM(t *testing.T) {
	verifier, err := pgwire.NewVerifier("correct-horse-battery-staple")
	if err != nil {
		t.Fatal(err)
	}
	validUser := PGUserCredential{
		User:         "app",
		Verifier:     verifier,
		Capabilities: serviceauthz.CapabilityDataRead | serviceauthz.CapabilityDataWrite,
	}
	dummyTLS := &tls.Config{MinVersion: tls.VersionTLS13}

	cases := []struct {
		name    string
		opts    PGEndpointOptions
		wantErr string
	}{
		{
			name: "loopback dev trust mode allowed",
			opts: PGEndpointOptions{Address: "127.0.0.1:5432"},
		},
		{
			name:    "non-loopback without TLS rejected",
			opts:    PGEndpointOptions{Address: "0.0.0.0:5432", Users: []PGUserCredential{validUser}},
			wantErr: "requires TLSConfig",
		},
		{
			name:    "non-loopback without SCRAM users rejected",
			opts:    PGEndpointOptions{Address: "0.0.0.0:5432", TLSConfig: dummyTLS},
			wantErr: "requires SCRAM Users",
		},
		{
			name: "non-loopback with TLS and SCRAM allowed",
			opts: PGEndpointOptions{Address: "0.0.0.0:5432", TLSConfig: dummyTLS, Users: []PGUserCredential{validUser}},
		},
		{
			name: "control capability rejected for SQL user",
			opts: PGEndpointOptions{
				Address:   "0.0.0.0:5432",
				TLSConfig: dummyTLS,
				Users: []PGUserCredential{{
					User:         "badadmin",
					Verifier:     verifier,
					Capabilities: serviceauthz.CapabilityDataRead | serviceauthz.CapabilityMembership,
				}},
			},
			wantErr: "outside allowed SQL set",
		},
		{
			name: "duplicate user rejected",
			opts: PGEndpointOptions{
				Address:   "0.0.0.0:5432",
				TLSConfig: dummyTLS,
				Users:     []PGUserCredential{validUser, validUser},
			},
			wantErr: "duplicate PostgreSQL user",
		},
		{
			name: "zero verifier rejected",
			opts: PGEndpointOptions{
				Address:   "0.0.0.0:5432",
				TLSConfig: dummyTLS,
				Users: []PGUserCredential{{
					User:         "emptyv",
					Capabilities: serviceauthz.CapabilityDataRead,
				}},
			},
			wantErr: "invalid SCRAM verifier",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePGEndpointOptions(tc.opts)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validatePGEndpointOptions() unexpected err: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validatePGEndpointOptions() = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}
