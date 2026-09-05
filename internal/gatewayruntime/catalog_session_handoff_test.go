package gatewayruntime

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
)

func TestCatalogSessionHandoffPersistsEveryCrashPhase(t *testing.T) {
	base := filepath.Join(t.TempDir(), "catalog-session")
	oldSnapshot := catalogSessionHandoffSnapshot(t, 1, "127.0.0.1:7101", 1)
	nextSnapshot := catalogSessionHandoffSnapshot(t, 2, "127.0.0.1:7199", 2)
	oldRoute := catalogRouteSeedRoute(t, oldSnapshot)
	nextRoute := catalogRouteSeedRoute(t, nextSnapshot)
	nextPath := catalogSessionJournalPath(base, nextSnapshot.Generation())
	handoff, err := catalogSessionHandoffFromRoutes(
		oldRoute, nextRoute, oldSnapshot, nextSnapshot, base, nextPath, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !catalogSessionHandoffPathsValid(handoff, base) {
		t.Fatal("prepared handoff failed path validation")
	}
	if err = validateCatalogSessionHandoffEvidence(
		handoff, oldRoute, nextRoute, oldSnapshot, nextSnapshot, 1,
	); err != nil {
		t.Fatalf("prepared evidence=%v", err)
	}

	path := catalogSessionHandoffPath(base)
	phases := []struct {
		phase       catalogSessionHandoffPhase
		currentPath string
		wantPath    string
	}{
		{catalogSessionHandoffPrepared, handoff.OldJournalPath, handoff.OldJournalPath},
		{catalogSessionHandoffOldSettled, handoff.OldJournalPath, handoff.OldJournalPath},
		// A crash here must reopen the exact next journal even when the route
		// seed promotion and Complete write have not happened yet.
		{catalogSessionHandoffNewReady, handoff.NextJournalPath, handoff.NextJournalPath},
		{catalogSessionHandoffComplete, handoff.NextJournalPath, handoff.NextJournalPath},
	}
	for index, testCase := range phases {
		if index != 0 {
			handoff, err = catalogSessionHandoffPhaseAdvance(
				handoff, testCase.phase, testCase.currentPath,
			)
			if err != nil {
				t.Fatalf("advance phase %d: %v", testCase.phase, err)
			}
		}
		if err = storeCatalogSessionHandoff(path, handoff); err != nil {
			t.Fatalf("store phase %d: %v", testCase.phase, err)
		}
		reopened, found, loadErr := loadCatalogSessionHandoff(path)
		if loadErr != nil || !found || reopened.Phase != testCase.phase {
			t.Fatalf("reopen phase=%d found=%t value=%+v err=%v",
				testCase.phase, found, reopened, loadErr)
		}
		resumePath, resumeErr := catalogSessionResumeJournalPath(reopened, base)
		if resumeErr != nil || resumePath != testCase.wantPath {
			t.Fatalf("resume phase=%d path=%q want=%q err=%v",
				testCase.phase, resumePath, testCase.wantPath, resumeErr)
		}
	}

	bad := handoff
	bad.CurrentJournalPath = bad.OldJournalPath
	if catalogSessionHandoffPathsValid(bad, base) {
		t.Fatal("NewReady/Complete recovery accepted predecessor journal")
	}
	if _, err = catalogSessionResumeJournalPath(bad, base); !errors.Is(err, gateway.ErrReplicatedCatalogConflict) {
		t.Fatalf("invalid phase path=%v", err)
	}
	bad = handoff
	bad.NextJournalPath = filepath.Join(base, "arbitrary-clean-file")
	if catalogSessionHandoffPathsValid(bad, base) {
		t.Fatal("handoff accepted an independently selected journal path")
	}
}

func TestCatalogSessionHandoffEvidenceRetainsExactBinding(t *testing.T) {
	base := filepath.Join(t.TempDir(), "catalog-session")
	oldSnapshot := catalogSessionHandoffSnapshot(t, 1, "127.0.0.1:7101", 1)
	nextSnapshot := catalogSessionHandoffSnapshot(t, 2, "127.0.0.1:7199", 2)
	oldRoute := catalogRouteSeedRoute(t, oldSnapshot)
	nextRoute := catalogRouteSeedRoute(t, nextSnapshot)
	handoff, err := catalogSessionHandoffFromRoutes(
		oldRoute, nextRoute, oldSnapshot, nextSnapshot, base,
		catalogSessionJournalPath(base, nextSnapshot.Generation()), 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if handoff.OldBinding == handoff.NextBinding {
		t.Fatal("binding-changing route produced one journal binding")
	}
	if err = validateCatalogSessionHandoffEvidence(
		handoff, oldRoute, nextRoute, oldSnapshot, nextSnapshot, 1,
	); err != nil {
		t.Fatal(err)
	}

	mutated := handoff
	mutated.NextBinding[0]++
	if err = validateCatalogSessionHandoffEvidence(
		mutated, oldRoute, nextRoute, oldSnapshot, nextSnapshot, 1,
	); !errors.Is(err, gateway.ErrReplicatedCatalogConflict) {
		t.Fatalf("mutated next binding=%v", err)
	}
	mutated = handoff
	mutated.NextSnapshotDigest[0]++
	if err = validateCatalogSessionHandoffEvidence(
		mutated, oldRoute, nextRoute, oldSnapshot, nextSnapshot, 1,
	); !errors.Is(err, gateway.ErrReplicatedCatalogConflict) {
		t.Fatalf("mutated next digest=%v", err)
	}
	mutatedRoute := nextRoute
	mutatedRoute.Command.RouteGeneration++
	if err = validateCatalogSessionHandoffEvidence(
		handoff, oldRoute, mutatedRoute, oldSnapshot, nextSnapshot, 1,
	); !errors.Is(err, gateway.ErrReplicatedCatalogConflict) {
		t.Fatalf("mutated route fence=%v", err)
	}
}

func TestCatalogSessionHandoffRejectsAddressOnlyBindingRollover(t *testing.T) {
	base := filepath.Join(t.TempDir(), "catalog-session")
	oldSnapshot := catalogRouteSeedSnapshot(t, 1, "127.0.0.1:7101")
	nextSnapshot := catalogRouteSeedSnapshot(t, 2, "127.0.0.1:7199")
	oldRoute := catalogRouteSeedRoute(t, oldSnapshot)
	nextRoute := catalogRouteSeedRoute(t, nextSnapshot)
	if _, err := catalogSessionHandoffFromRoutes(
		oldRoute, nextRoute, oldSnapshot, nextSnapshot, base,
		catalogSessionJournalPath(base, nextSnapshot.Generation()), 1,
	); !errors.Is(err, gateway.ErrReplicatedCatalog) {
		t.Fatalf("address-only route entered exact-binding handoff: %v", err)
	}
}

// catalogSessionHandoffSnapshot is a real catalog image with one monotonic
// placement-fence step. It is used to derive the binding and digest through
// the production route/snapshot code, rather than manufacturing handoff
// identity fields in a test.
func catalogSessionHandoffSnapshot(
	t testing.TB, generation uint64, nativeAddress string, routeGeneration uint64,
) *gateway.Snapshot {
	t.Helper()
	leaders := []distribution.EndpointID{"one", "two", "three"}
	manifest, err := distribution.NewManifest(
		gateway.ReplicatedCatalogDistribution, distribution.RoutingVersion(generation), []distribution.Shard{{
			ID: gateway.ReplicatedCatalogShard, AllocationGeneration: 1,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
			Leaders: leaders, Epoch: 1,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	endpoints := map[distribution.EndpointID]string{
		"one": "127.0.0.1:7001", "one-native": nativeAddress, "one-control": "127.0.0.1:7201",
		"two": "127.0.0.1:7002", "two-native": "127.0.0.1:7102", "two-control": "127.0.0.1:7202",
		"three": "127.0.0.1:7003", "three-native": "127.0.0.1:7103", "three-control": "127.0.0.1:7203",
	}
	replicas := make([]gateway.ReplicatedReplicaDescriptor, gateway.ServingReplicaCount)
	for index, name := range leaders {
		member := uint64(index + 1)
		replicas[index] = gateway.ReplicatedReplicaDescriptor{
			Member: member, Node: [16]byte{byte(member)}, StoreID: [16]byte{byte(member + 10)},
			NodeIncarnation: member + 20, Endpoint: name,
			NativeEndpoint:  distribution.EndpointID(string(name) + "-native"),
			ControlEndpoint: distribution.EndpointID(string(name) + "-control"),
		}
	}
	group := raftmember.GroupKey{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4},
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(
		distribution.ClusterConfig{
			Distributions: []distribution.DistributionSpec{{
				Name: gateway.ReplicatedCatalogDistribution, Arity: 1,
				MapperVersion: distribution.NativeMapperVersion,
			}},
			Manifests: []*distribution.Manifest{manifest},
		}, endpoints, generation, nil, nil, []gateway.ReplicatedShardDescriptor{{
			Distribution: gateway.ReplicatedCatalogDistribution,
			Shard:        gateway.ReplicatedCatalogShard, Group: group, AllocationGeneration: 1,
			Command: raftservice.CommandFence{
				ReplicaSetVersion: routeGeneration, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
				OwnershipEpoch: 1, SchemaGeneration: 1,
				RelationManifestDigest: [32]byte{5}, RoutingVersion: routeGeneration,
				RouteGeneration: routeGeneration,
			},
			RangeIdentity: [32]byte{6}, LineageDigest: [32]byte{7},
			ForwardingRuleDigest: [32]byte{8}, Replicas: replicas,
			RequestLedgerRanges: []gateway.DurableRequestLedgerRangeDescriptor{{Identity: [32]byte{9}}},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
