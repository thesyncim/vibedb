//go:build darwin || linux

package rf3testfixture

import "testing"

func TestProcessClusterRetainsCompleteAddressCutUntilRelease(t *testing.T) {
	cluster, err := ReserveProcessCluster()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cluster.Close() })
	members := cluster.Members()
	seen := make(map[string]struct{}, ProcessClusterListeners)
	for index, member := range members {
		addresses := [...]string{member.Peer, member.Native, member.Snapshot, member.Control}
		for _, address := range addresses {
			if address == "" {
				t.Fatalf("member %d has an empty address", index+1)
			}
			if _, duplicate := seen[address]; duplicate {
				t.Fatalf("duplicate address %q", address)
			}
			seen[address] = struct{}{}
			if processClusterAddressAvailable(address) {
				t.Fatalf("reserved address %q was available before release", address)
			}
		}
	}
	if len(seen) != ProcessClusterListeners {
		t.Fatalf("address count = %d, want %d", len(seen), ProcessClusterListeners)
	}
	if err = cluster.ReleaseListeners(); err != nil {
		t.Fatal(err)
	}
	for address := range seen {
		if !processClusterAddressAvailable(address) {
			t.Fatalf("released address %q remains reserved", address)
		}
	}
	if err = cluster.ReleaseListeners(); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
	if _, live := cluster.Member(0); live {
		t.Fatal("released cluster still reports live reservation")
	}
}
