//go:build darwin || linux

package main

import (
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

func TestGatewayHotShardSeedClientProfilePreflight(t *testing.T) {
	client, _ := rf3CompositionClientNodes()
	group := rf3CommandGroup()
	credentials, roots, err := rf3testfixture.WriteCredentials(t.TempDir(), rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}, []rafttransport.NodeID{client})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := servicetls.LoadProfile(credentials[0].Certificate, credentials[0].Key, roots,
		"1.3.6.1.4.1.32473.1.1", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	options := gatewayHotShardSeedClientOptions(profile)
	pool, err := gateway.NewAuthenticatedReplicatedClient(options)
	if err != nil {
		t.Fatalf("actual seed-client fixture options rejected: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	for _, missing := range []string{"idle", "lifetime"} {
		invalid := options
		if missing == "idle" {
			invalid.MaxIdleAge = 0
		} else {
			invalid.MaxLifetime = 0
		}
		if _, err := gateway.NewAuthenticatedReplicatedClient(invalid); !errors.Is(err, gateway.ErrReplicatedTLSProfile) {
			t.Fatalf("missing %s bound accepted: %v", missing, err)
		}
	}
}
