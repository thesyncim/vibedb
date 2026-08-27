package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	vibejson "github.com/thesyncim/vibejson"
)

func TestLoadReplicatedCatalogSeedsKeepsImmutableGenesisSeparate(t *testing.T) {
	root := t.TempDir()
	genesisPath := filepath.Join(root, "catalog-genesis.vibejson")
	routeSeedPath := filepath.Join(root, "catalog-route.vibejson")
	genesis := catalogRouteSeedSnapshot(t, 1, "127.0.0.1:7101")
	if err := gateway.SaveSnapshot(genesisPath, genesis); err != nil {
		t.Fatal(err)
	}
	immutable, seed, state, err := loadReplicatedCatalogSeeds(genesisPath, routeSeedPath)
	if err != nil || immutable.Generation() != 1 || seed.Generation() != 1 {
		t.Fatalf("initial immutable=%v seed=%v err=%v", immutable, seed, err)
	}
	if _, found := state.Active(); found {
		t.Fatal("missing mutable seed was reported as active")
	}

	active := catalogRouteSeedSnapshot(t, 2, "127.0.0.1:7199")
	if err = gateway.SaveSnapshot(routeSeedPath, active); err != nil {
		t.Fatal(err)
	}
	immutable, seed, state, err = loadReplicatedCatalogSeeds(genesisPath, routeSeedPath)
	if err != nil || immutable.Generation() != 1 || seed.Generation() != 2 {
		t.Fatalf("persisted immutable=%v seed=%v err=%v", immutable, seed, err)
	}
	loaded, found := state.Active()
	if !found || loaded.Generation() != 2 {
		t.Fatalf("active=%v found=%v", loaded, found)
	}
	genesisAgain, err := gateway.LoadSnapshot(genesisPath)
	if err != nil || genesisAgain.Generation() != 1 {
		t.Fatalf("immutable genesis changed=%v err=%v", genesisAgain, err)
	}
}

func TestLoadReplicatedCatalogSeedsRejectsPathAndInodeAliases(t *testing.T) {
	root := t.TempDir()
	genesisPath := filepath.Join(root, "catalog-genesis.vibejson")
	genesis := catalogRouteSeedSnapshot(t, 1, "127.0.0.1:7101")
	if err := gateway.SaveSnapshot(genesisPath, genesis); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := loadReplicatedCatalogSeeds(
		genesisPath, genesisPath,
	); !errors.Is(err, gateway.ErrReplicatedCatalogConflict) {
		t.Fatalf("identical immutable/mutable path=%v", err)
	}
	routeSeedPath := filepath.Join(root, "catalog-route.vibejson")
	if err := os.Link(genesisPath, routeSeedPath); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := loadReplicatedCatalogSeeds(
		genesisPath, routeSeedPath,
	); !errors.Is(err, gateway.ErrReplicatedCatalogConflict) {
		t.Fatalf("hard-linked immutable/mutable seed=%v", err)
	}
}

func TestReplicatedCatalogRouteBindingAndHandoffBudgetAreExactAndBounded(t *testing.T) {
	snapshot := catalogRouteSeedSnapshot(t, 1, "127.0.0.1:7101")
	route := catalogRouteSeedRoute(t, snapshot)
	changed := route
	changed.Command.RouteGeneration++
	if sameReplicatedCatalogRoute(route, changed) {
		t.Fatal("binding-changing command fence compared equal")
	}
	if !sameReplicatedCatalogRoute(route, route) {
		t.Fatal("byte-identical route compared different")
	}
	if got := replicatedCatalogRouteHandoffTimeout(8, 5*time.Second); got != 2*time.Minute {
		t.Fatalf("normal handoff budget=%v", got)
	}
	if got := replicatedCatalogRouteHandoffTimeout(1, time.Millisecond); got != 5*time.Second {
		t.Fatalf("minimum handoff budget=%v", got)
	}
	if got := replicatedCatalogRouteHandoffTimeout(int(^uint(0)>>1), time.Hour); got != 2*time.Minute {
		t.Fatalf("saturated handoff budget=%v", got)
	}
}

func TestRecoverReplicatedCatalogRouteSeedStartupPromotesSameRoute(t *testing.T) {
	genesisPath, _, active, route, state := catalogRouteSeedStartupFixture(
		t, "127.0.0.1:7101", "127.0.0.1:7101",
	)
	journalChecks, settlements := 0, 0
	next, nextRoute, recovered, err := recoverReplicatedCatalogRouteSeedStartup(
		context.Background(), filepath.Join(t.TempDir(), "journal"),
		active, route, state, replicatedCatalogRouteSeedStartupHooks{
			journalPresent: func(string) (bool, error) { journalChecks++; return true, nil },
			settleOldSession: func(context.Context, gateway.ReplicatedRoute) error {
				settlements++
				return nil
			},
		},
	)
	if err != nil || next.Generation() != 2 ||
		!sameReplicatedCatalogRoute(route, nextRoute) || journalChecks != 0 || settlements != 0 {
		t.Fatalf("same-route recovery next=%v checks=%d settlements=%d err=%v",
			next, journalChecks, settlements, err)
	}
	if _, found := recovered.Pending(); found {
		t.Fatal("same-route recovery retained pending candidate")
	}
	immutable, err := gateway.LoadSnapshot(genesisPath)
	if err != nil || immutable.Generation() != 1 {
		t.Fatalf("same-route recovery mutated immutable genesis=%v err=%v", immutable, err)
	}
}

func TestRecoverReplicatedCatalogRouteSeedStartupChangedWithoutJournal(t *testing.T) {
	_, _, active, route, state := catalogRouteSeedStartupFixture(
		t, "127.0.0.1:7101", "127.0.0.1:7199",
	)
	settlements := 0
	_, _, recovered, err := recoverReplicatedCatalogRouteSeedStartup(
		context.Background(), filepath.Join(t.TempDir(), "journal"),
		active, route, state, replicatedCatalogRouteSeedStartupHooks{
			journalPresent: func(string) (bool, error) { return false, nil },
			settleOldSession: func(context.Context, gateway.ReplicatedRoute) error {
				settlements++
				return nil
			},
		},
	)
	if !errors.Is(err, gateway.ErrReplicatedCatalogRouteRestartRequired) || settlements != 0 {
		t.Fatalf("journal-free changed recovery settlements=%d err=%v", settlements, err)
	}
	assertPromotedCatalogRouteSeedState(t, recovered, 2)
}

func TestRecoverReplicatedCatalogRouteSeedStartupSettlesExactOldBinding(t *testing.T) {
	_, _, active, route, state := catalogRouteSeedStartupFixture(
		t, "127.0.0.1:7101", "127.0.0.1:7199",
	)
	journalPath := filepath.Join(t.TempDir(), "journal")
	journalChecks, settlements := 0, 0
	_, _, recovered, err := recoverReplicatedCatalogRouteSeedStartup(
		context.Background(), journalPath, active, route, state,
		replicatedCatalogRouteSeedStartupHooks{
			journalPresent: func(got string) (bool, error) {
				journalChecks++
				if got != journalPath {
					t.Fatalf("journal path=%q want=%q", got, journalPath)
				}
				return true, nil
			},
			settleOldSession: func(_ context.Context, got gateway.ReplicatedRoute) error {
				settlements++
				if !sameReplicatedCatalogRoute(got, route) {
					t.Fatal("startup settlement did not reopen the exact old binding")
				}
				return nil
			},
		},
	)
	if !errors.Is(err, gateway.ErrReplicatedCatalogRouteRestartRequired) ||
		journalChecks != 1 || settlements != 1 {
		t.Fatalf("changed recovery checks=%d settlements=%d err=%v",
			journalChecks, settlements, err)
	}
	assertPromotedCatalogRouteSeedState(t, recovered, 2)
}

func TestRecoverReplicatedCatalogRouteSeedStartupDoesNotPromoteBeforeSettlement(t *testing.T) {
	_, routeSeedPath, active, route, state := catalogRouteSeedStartupFixture(
		t, "127.0.0.1:7101", "127.0.0.1:7199",
	)
	want := errors.New("retire outcome unknown")
	_, _, _, err := recoverReplicatedCatalogRouteSeedStartup(
		context.Background(), "journal", active, route, state,
		replicatedCatalogRouteSeedStartupHooks{
			journalPresent: func(string) (bool, error) { return true, nil },
			settleOldSession: func(context.Context, gateway.ReplicatedRoute) error {
				return want
			},
		},
	)
	if !errors.Is(err, want) {
		t.Fatalf("failed settlement=%v", err)
	}
	remaining, err := gateway.LoadReplicatedCatalogRouteSeed(routeSeedPath)
	if err != nil {
		t.Fatal(err)
	}
	activeSeed, activeFound := remaining.Active()
	pendingSeed, pendingFound := remaining.Pending()
	if !activeFound || !pendingFound || activeSeed.Generation() != 1 ||
		pendingSeed.Generation() != 2 {
		t.Fatalf("failed settlement advanced seed active=%v/%v pending=%v/%v",
			activeSeed, activeFound, pendingSeed, pendingFound)
	}
}

func TestRecoverReplicatedCatalogRouteSeedStartupCleansRenameOutcomeUnknown(t *testing.T) {
	_, routeSeedPath, _, _, _ := catalogRouteSeedStartupFixture(
		t, "127.0.0.1:7101", "127.0.0.1:7101",
	)
	next := catalogRouteSeedSnapshot(t, 2, "127.0.0.1:7101")
	// Model rename success followed by an unknown directory-fsync outcome: the
	// active file is already the candidate while the byte-identical pending
	// entry remains to be cleaned by the exact promotion retry.
	if err := gateway.SaveSnapshot(routeSeedPath, next); err != nil {
		t.Fatal(err)
	}
	state, err := gateway.LoadReplicatedCatalogRouteSeed(routeSeedPath)
	if err != nil {
		t.Fatal(err)
	}
	active, found := state.Active()
	if !found {
		t.Fatal("outcome-unknown fixture has no active seed")
	}
	route := catalogRouteSeedRoute(t, active)
	_, _, recovered, err := recoverReplicatedCatalogRouteSeedStartup(
		context.Background(), "unused", active, route, state,
		replicatedCatalogRouteSeedStartupHooks{},
	)
	if err != nil {
		t.Fatal(err)
	}
	assertPromotedCatalogRouteSeedState(t, recovered, 2)
}

type rawCatalogRouteSeedSnapshot []byte

func (raw rawCatalogRouteSeedSnapshot) MarshalJSON() ([]byte, error) {
	return raw, nil
}

type persistedCatalogRouteSeedCandidateFixture struct {
	Format             uint8                       `json:"format"`
	ExpectedGeneration uint64                      `json:"expected_generation"`
	HeadBytes          uint64                      `json:"head_bytes"`
	HeadDigest         [sha256.Size]byte           `json:"head_digest"`
	SnapshotBytes      uint64                      `json:"snapshot_bytes"`
	SnapshotDigest     [sha256.Size]byte           `json:"snapshot_digest"`
	Snapshot           rawCatalogRouteSeedSnapshot `json:"snapshot"`
}

func catalogRouteSeedStartupFixture(
	t testing.TB,
	activeAddress string,
	pendingAddress string,
) (string, string, *gateway.Snapshot, gateway.ReplicatedRoute,
	gateway.ReplicatedCatalogRouteSeedState) {
	t.Helper()
	root := t.TempDir()
	genesisPath := filepath.Join(root, "catalog-genesis.vibejson")
	routeSeedPath := filepath.Join(root, "catalog-route.vibejson")
	genesis := catalogRouteSeedSnapshot(t, 1, activeAddress)
	if err := gateway.SaveSnapshot(genesisPath, genesis); err != nil {
		t.Fatal(err)
	}
	if err := gateway.SaveSnapshot(routeSeedPath, genesis); err != nil {
		t.Fatal(err)
	}
	pending := catalogRouteSeedSnapshot(t, 2, pendingAddress)
	stageCatalogRouteSeedCandidateFixture(t, routeSeedPath, genesis.Generation(), pending)
	_, active, state, err := loadReplicatedCatalogSeeds(genesisPath, routeSeedPath)
	if err != nil {
		t.Fatal(err)
	}
	return genesisPath, routeSeedPath, active, catalogRouteSeedRoute(t, active), state
}

func stageCatalogRouteSeedCandidateFixture(
	t testing.TB,
	path string,
	expected uint64,
	snapshot *gateway.Snapshot,
) {
	t.Helper()
	canonical, err := gateway.AppendSnapshotDocument(nil, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	head := make([]byte, 0, len(canonical)+40)
	head = append(head, `{"id":"catalog/head","payload":`...)
	head = append(head, canonical...)
	head = append(head, '}')
	persisted := persistedCatalogRouteSeedCandidateFixture{
		Format: 1, ExpectedGeneration: expected,
		HeadBytes: uint64(len(head)), HeadDigest: sha256.Sum256(head),
		SnapshotBytes: uint64(len(canonical)), SnapshotDigest: sha256.Sum256(canonical),
		Snapshot: rawCatalogRouteSeedSnapshot(canonical),
	}
	raw, err := vibejson.Marshal(&persisted)
	if err == nil {
		raw, err = vibejson.AppendCanonicalize(nil, raw)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path+".pending", raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = gateway.LoadReplicatedCatalogRouteSeed(path); err != nil {
		t.Fatalf("invalid pending fixture: %v", err)
	}
}

func assertPromotedCatalogRouteSeedState(
	t testing.TB,
	state gateway.ReplicatedCatalogRouteSeedState,
	wantGeneration uint64,
) {
	t.Helper()
	active, found := state.Active()
	if !found || active.Generation() != wantGeneration {
		t.Fatalf("active=%v found=%v want generation=%d", active, found, wantGeneration)
	}
	if _, found = state.Pending(); found {
		t.Fatal("promoted state retained pending candidate")
	}
}

func catalogRouteSeedRoute(t testing.TB, snapshot *gateway.Snapshot) gateway.ReplicatedRoute {
	t.Helper()
	var scratch [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute(
		gateway.ReplicatedCatalogDistribution,
		gateway.ReplicatedCatalogShard,
		scratch[:0],
	)
	if !ok {
		t.Fatal("catalog self-route is missing")
	}
	return route
}

func catalogRouteSeedSnapshot(
	t testing.TB, generation uint64, firstNativeAddress string,
) *gateway.Snapshot {
	t.Helper()
	leaders := []distribution.EndpointID{"one", "two", "three"}
	manifest, err := distribution.NewManifest(
		gateway.ReplicatedCatalogDistribution, 1, []distribution.Shard{{
			ID: gateway.ReplicatedCatalogShard, AllocationGeneration: 1,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
			Leaders: leaders, Epoch: 1,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	endpoints := map[distribution.EndpointID]string{
		"one": "127.0.0.1:7001", "one-native": firstNativeAddress,
		"one-control": "127.0.0.1:7201",
		"two":         "127.0.0.1:7002", "two-native": "127.0.0.1:7102",
		"two-control": "127.0.0.1:7202",
		"three":       "127.0.0.1:7003", "three-native": "127.0.0.1:7103",
		"three-control": "127.0.0.1:7203",
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
		},
		endpoints, generation, nil, nil, []gateway.ReplicatedShardDescriptor{{
			Distribution: gateway.ReplicatedCatalogDistribution,
			Shard:        gateway.ReplicatedCatalogShard, Group: group, AllocationGeneration: 1,
			Command: raftservice.CommandFence{
				ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
				OwnershipEpoch: 1, SchemaGeneration: 1,
				RelationManifestDigest: [32]byte{5}, RoutingVersion: 1, RouteGeneration: 1,
			},
			RangeIdentity: [32]byte{6}, LineageDigest: [32]byte{7},
			ForwardingRuleDigest: [32]byte{8}, Replicas: replicas,
			RequestLedgerRanges: []gateway.DurableRequestLedgerRangeDescriptor{{
				Identity: [32]byte{9},
			}},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
