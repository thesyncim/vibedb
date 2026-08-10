package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/store/durable"
)

func testShardStoreBinding() ShardStoreBinding {
	return ShardStoreBinding{
		Distribution:         "tenant_data",
		Shard:                "-80",
		AllocationGeneration: 7,
	}
}

func TestShardStoreInitializeReopenAndWriteOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard.vdb")
	binding := testShardStoreBinding()

	database, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatalf("InitializeShardStore: %v", err)
	}
	identity, err := database.ShardStoreIdentity()
	if err != nil {
		t.Fatalf("ShardStoreIdentity: %v", err)
	}
	if identity.Binding() != binding || identity.LogID == ([16]byte{}) {
		t.Fatalf("initialized identity = %+v, want binding %+v and nonzero LogID", identity, binding)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close initialized store: %v", err)
	}

	// An exact retry is crash-idempotent and retains the already-published LogID.
	retried, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatalf("exact InitializeShardStore retry: %v", err)
	}
	retriedIdentity, err := retried.ShardStoreIdentity()
	if err != nil || retriedIdentity != identity {
		t.Fatalf("retried identity = (%+v, %v), want %+v", retriedIdentity, err, identity)
	}
	if err := retried.Close(); err != nil {
		t.Fatalf("close retried store: %v", err)
	}
	other := binding
	other.AllocationGeneration++
	if _, err := InitializeShardStore(path, other); !errors.Is(err, ErrShardStoreIdentityMismatch) {
		t.Fatalf("mismatched InitializeShardStore = %v, want ErrShardStoreIdentityMismatch", err)
	}

	reopened, err := OpenShardStore(path, binding)
	if err != nil {
		t.Fatalf("OpenShardStore: %v", err)
	}
	reopenedIdentity, err := reopened.ShardStoreIdentity()
	if err != nil {
		t.Fatalf("reopened ShardStoreIdentity: %v", err)
	}
	if reopenedIdentity != identity {
		t.Fatalf("reopened identity = %+v, want %+v", reopenedIdentity, identity)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
}

func TestShardStoreOpenRejectsMismatchBeforeRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard.vdb")
	binding := testShardStoreBinding()
	database, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := database.ShardStoreIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	// Removing the empty private table directory gives recovery an observable
	// mutation to perform. A mismatched open must fail before recreating it.
	dataDir := path + ".tables"
	if err := os.Remove(dataDir); err != nil {
		t.Fatalf("remove empty table directory: %v", err)
	}

	cases := []struct {
		name    string
		binding ShardStoreBinding
	}{
		{"distribution", ShardStoreBinding{Distribution: "other", Shard: binding.Shard, AllocationGeneration: binding.AllocationGeneration}},
		{"shard", ShardStoreBinding{Distribution: binding.Distribution, Shard: "80-", AllocationGeneration: binding.AllocationGeneration}},
		{"allocation", ShardStoreBinding{Distribution: binding.Distribution, Shard: binding.Shard, AllocationGeneration: binding.AllocationGeneration + 1}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := OpenShardStore(path, test.binding)
			if !errors.Is(err, ErrShardStoreIdentityMismatch) {
				t.Fatalf("OpenShardStore mismatch = %v, want ErrShardStoreIdentityMismatch", err)
			}
			var typed *ShardStoreError
			if !errors.As(err, &typed) {
				t.Fatalf("OpenShardStore mismatch type = %T, want *ShardStoreError", err)
			}
			if typed.Expected != test.binding || typed.Actual != identity {
				t.Fatalf("mismatch context = %+v, want expected %+v actual %+v", typed, test.binding, identity)
			}
			if _, statErr := os.Stat(dataDir); !os.IsNotExist(statErr) {
				t.Fatalf("mismatched open recovered table directory: %v", statErr)
			}
		})
	}

	correct, err := OpenShardStore(path, binding)
	if err != nil {
		t.Fatalf("correct OpenShardStore after mismatches: %v", err)
	}
	if _, err := os.Stat(dataDir); err != nil {
		t.Fatalf("correct open did not recover table directory: %v", err)
	}
	if err := correct.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShardStoreInitializationPublishesBindingBeforeStorageRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard.vdb")
	binding := testShardStoreBinding()
	fenceFailure := errors.New("injected initial catalog fence failure")
	syncCalls := 0
	_, err := openDatabaseWithShardStorePolicy(path, func(string) error {
		syncCalls++
		if syncCalls == 1 {
			return fenceFailure
		}
		return nil
	}, shardStoreOpenPolicy{mode: shardStoreOpenInitialize, expected: binding})
	if !errors.Is(err, fenceFailure) {
		t.Fatalf("InitializeShardStore fence failure = %v, want injected failure", err)
	}
	if _, err := os.Stat(path + ".tables"); !os.IsNotExist(err) {
		t.Fatalf("failed identity publication reached table namespace recovery: %v", err)
	}
	raw, exists, err := readCatalogFile(path)
	if err != nil || !exists {
		t.Fatalf("read published identity after ambiguous fence = (%t, %v)", exists, err)
	}
	var catalog catalogFile
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	if catalog.ShardStore == nil || catalog.ShardStore.Binding() != binding ||
		catalog.ShardStore.LogID == ([16]byte{}) {
		t.Fatalf("published identity = %+v, want binding %+v and nonzero LogID", catalog.ShardStore, binding)
	}
	published := *catalog.ShardStore

	// Retrying the exact initialization finishes recovery without minting a new
	// log identity. This is the only idempotent already-bound initialization.
	database, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatalf("exact retry after ambiguous publication: %v", err)
	}
	got, err := database.ShardStoreIdentity()
	if err != nil || got != published {
		t.Fatalf("identity after exact retry = (%+v, %v), want %+v", got, err, published)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShardStoreInitializationPrePublishFailureIsNotRetriedByCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard.vdb")
	binding := testShardStoreBinding()
	injected := errors.New("injected pre-publication failure")
	persistCalls := 0
	_, err := openDatabaseWithShardStorePolicy(
		path, nil,
		shardStoreOpenPolicy{
			mode: shardStoreOpenInitialize, expected: binding,
			persistIdentity: func(*database) (bool, error) {
				persistCalls++
				return false, injected
			},
		},
	)
	if !errors.Is(err, injected) {
		t.Fatalf("InitializeShardStore pre-publication failure = %v, want injected cause", err)
	}
	if persistCalls != 1 {
		t.Fatalf("identity persist calls = %d, want exactly one", persistCalls)
	}
	for _, candidate := range []string{path, path + ".tables"} {
		if _, statErr := os.Stat(candidate); !os.IsNotExist(statErr) {
			t.Fatalf("failed initialization published %s during cleanup: %v",
				filepath.Base(candidate), statErr)
		}
	}

	database, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatalf("fresh initialization after definite failure: %v", err)
	}
	defer database.Close()
	identity, err := database.ShardStoreIdentity()
	if err != nil || identity.Binding() != binding || identity.LogID == ([16]byte{}) {
		t.Fatalf("identity after retry = (%+v, %v), want bound nonzero identity", identity, err)
	}
}

func TestShardStoreOpenRejectsMissingUnboundAndGenericAccess(t *testing.T) {
	binding := testShardStoreBinding()

	t.Run("missing", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "missing.vdb")
		_, err := OpenShardStore(path, binding)
		if !errors.Is(err, ErrShardStoreUnbound) {
			t.Fatalf("OpenShardStore missing = %v, want ErrShardStoreUnbound", err)
		}
		for _, candidate := range []string{path, path + ".lock", path + ".tables"} {
			if _, statErr := os.Stat(candidate); !os.IsNotExist(statErr) {
				t.Fatalf("missing-store open created %s: %v", filepath.Base(candidate), statErr)
			}
		}
	})

	t.Run("unbound", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ordinary.vdb")
		ordinary, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		execShardStoreDDL(t, ordinary, `CREATE TABLE docs (id STRING PRIMARY KEY)`)
		execShardStoreDDL(t, ordinary, `DROP TABLE docs`)
		if _, err := ordinary.ShardStoreIdentity(); !errors.Is(err, ErrShardStoreUnbound) {
			t.Fatalf("ordinary ShardStoreIdentity = %v, want ErrShardStoreUnbound", err)
		}
		if err := ordinary.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := InitializeShardStore(path, binding); !errors.Is(err, ErrShardStoreIdentityMismatch) {
			t.Fatalf("InitializeShardStore ordinary catalog = %v, want ErrShardStoreIdentityMismatch", err)
		}
		if _, err := OpenShardStore(path, binding); !errors.Is(err, ErrShardStoreUnbound) {
			t.Fatalf("OpenShardStore ordinary catalog = %v, want ErrShardStoreUnbound", err)
		}
	})

	t.Run("empty unbound root", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "empty.vdb")
		ordinary, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := ordinary.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := InitializeShardStore(path, binding); !errors.Is(err, ErrShardStoreIdentityMismatch) {
			t.Fatalf("InitializeShardStore empty ordinary root = %v, want ErrShardStoreIdentityMismatch", err)
		}
	})

	t.Run("orphan table namespace", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "orphan.vdb")
		dataDir := path + ".tables"
		if err := os.Mkdir(dataDir, 0o700); err != nil {
			t.Fatal(err)
		}
		marker := filepath.Join(dataDir, "do-not-adopt")
		if err := os.WriteFile(marker, []byte("residual shard data"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := InitializeShardStore(path, binding); !errors.Is(err, ErrShardStoreIdentityMismatch) {
			t.Fatalf("InitializeShardStore orphan namespace = %v, want ErrShardStoreIdentityMismatch", err)
		}
		if raw, err := os.ReadFile(marker); err != nil || string(raw) != "residual shard data" {
			t.Fatalf("refused initialization changed residual data = (%q, %v)", raw, err)
		}
	})

	t.Run("generic APIs", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "shard.vdb")
		bound, err := InitializeShardStore(path, binding)
		if err != nil {
			t.Fatal(err)
		}
		if err := bound.Close(); err != nil {
			t.Fatal(err)
		}

		assertMismatch := func(name string, err error) {
			t.Helper()
			if !errors.Is(err, ErrShardStoreIdentityMismatch) {
				t.Fatalf("%s = %v, want ErrShardStoreIdentityMismatch", name, err)
			}
		}
		_, err = Open(path)
		assertMismatch("Open", err)
		_, err = (Driver{}).Open(path)
		assertMismatch("Driver.Open", err)
		_, err = OpenCluster(path, distribution.ClusterConfig{})
		assertMismatch("OpenCluster", err)
	})
}

func TestShardStoreIdentitySurvivesDDLPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard.vdb")
	binding := testShardStoreBinding()
	database, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := database.ShardStoreIdentity()
	if err != nil {
		t.Fatal(err)
	}
	fence := ShardStoreFence{OwnershipEpoch: 17, RoutingVersion: 19}
	claim, err := database.ClaimShardStoreServing(binding, fence)
	if err != nil {
		t.Fatal(err)
	}
	_ = claim.Close()

	statements := []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY, kind STRING)`,
		`CREATE INDEX by_kind ON docs(kind)`,
		`CREATE VIEW selected AS SELECT id FROM docs`,
		`DROP VIEW selected`,
	}
	for _, statement := range statements {
		execShardStoreDDL(t, database, statement)
		raw, exists, err := readCatalogFile(path)
		if err != nil || !exists {
			t.Fatalf("read catalog after %q = (%t, %v)", statement, exists, err)
		}
		var catalog catalogFile
		if err := json.Unmarshal(raw, &catalog); err != nil {
			t.Fatalf("decode catalog after %q: %v", statement, err)
		}
		if catalog.ShardStore == nil || *catalog.ShardStore != identity {
			t.Fatalf("identity after %q = %+v, want %+v", statement, catalog.ShardStore, identity)
		}
		if catalog.ShardStoreFence == nil || *catalog.ShardStoreFence != fence {
			t.Fatalf("serving fence after %q = %+v, want %+v", statement, catalog.ShardStoreFence, fence)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenShardStore(path, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.ShardStoreIdentity()
	if err != nil || got != identity {
		t.Fatalf("identity after DDL reopen = (%+v, %v), want %+v", got, err, identity)
	}
	retried, err := reopened.ClaimShardStoreServing(binding, fence)
	if err != nil {
		t.Fatalf("serving fence after DDL reopen: %v", err)
	}
	_ = retried.Close()
}

func TestShardStoreIdentityStrictCatalogDecode(t *testing.T) {
	base := `{"version":0,"tables":{},"shard_store":{` +
		`"distribution":"tenant_data","shard":"-80",` +
		`"allocation_generation":7,"log_id":"00112233445566778899aabbccddeeff"`
	tests := []string{
		base + `,"unknown":1}}`,
		base + `,"shard":"duplicate"}}`,
		`{"version":0,"tables":{},"shard_store":null}`,
		`{"version":0,"tables":{},"shard_store":{"distribution":"tenant_data","shard":"-80","allocation_generation":7}}`,
		`{"version":0,"tables":{},"shard_store":{"distribution":"tenant_data","shard":"-80","allocation_generation":7,"log_id":"00"}}`,
		`{"version":0,"tables":{},"shard_store":{"distribution":"tenant_data","shard":"-80","allocation_generation":7,"log_id":"00112233445566778899AABBCCDDEEFF"}}`,
	}
	for _, input := range tests {
		var catalog catalogFile
		if err := json.Unmarshal([]byte(input), &catalog); err == nil {
			t.Fatalf("corrupt shard identity decoded: %s", input)
		}
	}
}

func TestShardStoreServingClaimPersistsMonotonicFence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "serving.vdb")
	binding := testShardStoreBinding()
	database, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatal(err)
	}

	firstFence := ShardStoreFence{OwnershipEpoch: 11, RoutingVersion: 5}
	claim, err := database.ClaimShardStoreServing(binding, firstFence)
	if err != nil {
		t.Fatalf("first ClaimShardStoreServing: %v", err)
	}
	if claim.Identity().Binding() != binding || claim.Fence() != firstFence {
		t.Fatalf("claim = identity %+v fence %+v", claim.Identity(), claim.Fence())
	}
	if _, err := database.ClaimShardStoreServing(binding, firstFence); !errors.Is(err, ErrShardStoreServingClaimed) {
		t.Fatalf("second live claim = %v, want ErrShardStoreServingClaimed", err)
	}
	if _, err := database.ClaimShardStoreServing(binding, ShardStoreFence{
		OwnershipEpoch: firstFence.OwnershipEpoch - 1,
		RoutingVersion: firstFence.RoutingVersion,
	}); !errors.Is(err, distribution.ErrOwnershipEpoch) {
		t.Fatalf("live stale claim = %v, want typed ErrOwnershipEpoch", err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatalf("idempotent claim close: %v", err)
	}

	// Equal retry after close does not need to advance the catalog.
	equal, err := database.ClaimShardStoreServing(binding, firstFence)
	if err != nil {
		t.Fatalf("equal retry: %v", err)
	}
	_ = equal.Close()

	routingAdvance := ShardStoreFence{OwnershipEpoch: 11, RoutingVersion: 6}
	routingClaim, err := database.ClaimShardStoreServing(binding, routingAdvance)
	if err != nil {
		t.Fatalf("routing advance: %v", err)
	}
	_ = routingClaim.Close()
	epochAdvance := ShardStoreFence{OwnershipEpoch: 12, RoutingVersion: 6}
	epochClaim, err := database.ClaimShardStoreServing(binding, epochAdvance)
	if err != nil {
		t.Fatalf("epoch advance: %v", err)
	}
	_ = epochClaim.Close()

	_, err = database.ClaimShardStoreServing(binding, ShardStoreFence{
		OwnershipEpoch: 11, RoutingVersion: 6,
	})
	if !errors.Is(err, distribution.ErrOwnershipEpoch) {
		t.Fatalf("epoch regression = %v, want ErrOwnershipEpoch", err)
	}
	var fenceErr *ShardStoreFenceError
	if !errors.As(err, &fenceErr) || fenceErr.Durable != epochAdvance {
		t.Fatalf("epoch regression detail = %#v, want durable %+v", fenceErr, epochAdvance)
	}
	_, err = database.ClaimShardStoreServing(binding, ShardStoreFence{
		OwnershipEpoch: 12, RoutingVersion: 5,
	})
	if !errors.Is(err, distribution.ErrRoutingVersion) {
		t.Fatalf("routing regression = %v, want ErrRoutingVersion", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenShardStore(path, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.ClaimShardStoreServing(binding, firstFence); !errors.Is(err, distribution.ErrOwnershipEpoch) {
		t.Fatalf("durable regression after reopen = %v, want ErrOwnershipEpoch", err)
	}
	retried, err := reopened.ClaimShardStoreServing(binding, epochAdvance)
	if err != nil {
		t.Fatalf("durable equal retry after reopen: %v", err)
	}
	_ = retried.Close()
}

func TestShardStoreServingClaimValidatesCoordinatesAndBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "serving-validation.vdb")
	binding := testShardStoreBinding()
	database, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if _, err := database.ClaimShardStoreServing(binding, ShardStoreFence{
		RoutingVersion: 1,
	}); !errors.Is(err, distribution.ErrOwnershipEpoch) {
		t.Fatalf("zero epoch = %v, want ErrOwnershipEpoch", err)
	}
	if _, err := database.ClaimShardStoreServing(binding, ShardStoreFence{
		OwnershipEpoch: 1,
	}); !errors.Is(err, distribution.ErrRoutingVersion) {
		t.Fatalf("zero routing version = %v, want ErrRoutingVersion", err)
	}
	wrong := binding
	wrong.AllocationGeneration++
	if _, err := database.ClaimShardStoreServing(wrong, ShardStoreFence{
		OwnershipEpoch: 1, RoutingVersion: 1,
	}); !errors.Is(err, ErrShardStoreIdentityMismatch) {
		t.Fatalf("wrong binding = %v, want ErrShardStoreIdentityMismatch", err)
	}

	ordinary, err := Open(filepath.Join(t.TempDir(), "ordinary.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer ordinary.Close()
	if _, err := ordinary.ClaimShardStoreServing(binding, ShardStoreFence{
		OwnershipEpoch: 1, RoutingVersion: 1,
	}); !errors.Is(err, ErrShardStoreUnbound) {
		t.Fatalf("unbound claim = %v, want ErrShardStoreUnbound", err)
	}
}

func TestShardStoreServingClaimDefiniteFailureRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "serving-definite.vdb")
	binding := testShardStoreBinding()
	storeDB, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatal(err)
	}
	baseline := ShardStoreFence{OwnershipEpoch: 3, RoutingVersion: 4}
	claim, err := storeDB.ClaimShardStoreServing(binding, baseline)
	if err != nil {
		t.Fatal(err)
	}
	_ = claim.Close()

	cause := errors.New("injected definite pre-publication failure")
	advanced := ShardStoreFence{OwnershipEpoch: 5, RoutingVersion: 6}
	if _, err := storeDB.claimShardStoreServing(binding, advanced, func(*database) (bool, error) {
		return false, cause
	}); !errors.Is(err, cause) {
		t.Fatalf("definite failure = %v, want injected cause", err)
	}
	core := storeDB.connector.db
	core.mu.RLock()
	gotFence := *core.catalog.ShardStoreFence
	pending := core.catalogWritePending
	core.mu.RUnlock()
	if gotFence != baseline || pending {
		t.Fatalf("tentative state survived: fence=%+v pending=%t", gotFence, pending)
	}
	retriedBaseline, err := storeDB.ClaimShardStoreServing(binding, baseline)
	if err != nil {
		t.Fatalf("definite failure retained a live claim: %v", err)
	}
	_ = retriedBaseline.Close()
	if err := storeDB.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenShardStore(path, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	core = reopened.connector.db
	core.mu.RLock()
	gotFence = *core.catalog.ShardStoreFence
	core.mu.RUnlock()
	if gotFence != baseline {
		t.Fatalf("failed-close cleanup published tentative fence %+v, want %+v", gotFence, baseline)
	}
}

func TestShardStoreServingClaimAmbiguousPublicationFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "serving-ambiguous.vdb")
	binding := testShardStoreBinding()
	storeDB, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer storeDB.Close()
	baseline := ShardStoreFence{OwnershipEpoch: 7, RoutingVersion: 8}
	claim, err := storeDB.ClaimShardStoreServing(binding, baseline)
	if err != nil {
		t.Fatal(err)
	}
	_ = claim.Close()

	core := storeDB.connector.db
	fenceFailure := errors.New("injected catalog directory fence failure")
	core.mu.Lock()
	core.syncDir = func(string) error { return fenceFailure }
	core.mu.Unlock()
	advanced := ShardStoreFence{OwnershipEpoch: 9, RoutingVersion: 10}
	if claim, err := storeDB.ClaimShardStoreServing(binding, advanced); claim != nil ||
		!errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("ambiguous claim = (%v, %v), want nil ErrCommitOutcomeUnknown", claim, err)
	}
	core.mu.RLock()
	gotFence := *core.catalog.ShardStoreFence
	pendingFence := core.catalogFencePending
	core.mu.RUnlock()
	if gotFence != advanced || !pendingFence {
		t.Fatalf("ambiguous state = fence %+v pending=%t, want %+v,true", gotFence, pendingFence, advanced)
	}
	if _, err := storeDB.ClaimShardStoreServing(binding, baseline); !errors.Is(err, distribution.ErrOwnershipEpoch) {
		t.Fatalf("stale claim after ambiguity = %v, want ErrOwnershipEpoch", err)
	}

	core.mu.Lock()
	core.syncDir = nil
	core.mu.Unlock()
	retried, err := storeDB.ClaimShardStoreServing(binding, advanced)
	if err != nil {
		t.Fatalf("equal retry after settling ambiguity: %v", err)
	}
	_ = retried.Close()

	// A persistence boundary that cannot even prove whether rename happened is
	// also fail-closed: retain the proposed high-water and force a later settle.
	uncertain := ShardStoreFence{OwnershipEpoch: 11, RoutingVersion: 12}
	unknown := fmt.Errorf("%w: injected unresolved publication", durable.ErrCommitOutcomeUnknown)
	if claim, err := storeDB.claimShardStoreServing(binding, uncertain, func(*database) (bool, error) {
		return false, unknown
	}); claim != nil || !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("unresolved claim = (%v, %v), want nil ErrCommitOutcomeUnknown", claim, err)
	}
	core.mu.RLock()
	gotFence = *core.catalog.ShardStoreFence
	pendingWrite := core.catalogWritePending
	core.mu.RUnlock()
	if gotFence != uncertain || !pendingWrite {
		t.Fatalf("unresolved state = fence %+v pending=%t, want %+v,true", gotFence, pendingWrite, uncertain)
	}
	if _, err := storeDB.ClaimShardStoreServing(binding, advanced); !errors.Is(err, distribution.ErrOwnershipEpoch) {
		t.Fatalf("stale claim after unresolved publication = %v, want ErrOwnershipEpoch", err)
	}
	settled, err := storeDB.ClaimShardStoreServing(binding, uncertain)
	if err != nil {
		t.Fatalf("settle unresolved publication: %v", err)
	}
	_ = settled.Close()
}

func TestShardStoreFenceStrictCatalogDecodeAndBounds(t *testing.T) {
	standaloneFence := ShardStoreFence{OwnershipEpoch: 1, RoutingVersion: 1}
	if err := checkCatalogSize(catalogFile{
		Version: catalogVersion, Tables: map[string]*tableMeta{},
		ShardStoreFence: &standaloneFence,
	}); err == nil {
		t.Fatal("catalog encoder accepted a shard store fence without identity")
	}
	identity := `"shard_store":{"distribution":"tenant_data","shard":"-80",` +
		`"allocation_generation":7,"log_id":"00112233445566778899aabbccddeeff"}`
	validPrefix := `{"version":0,"tables":{},` + identity + `,"shard_store_fence":`
	invalid := []string{
		`{"version":0,"tables":{},"shard_store_fence":{"ownership_epoch":1,"routing_version":1}}`,
		validPrefix + `null}`,
		validPrefix + `{}}`,
		validPrefix + `{"ownership_epoch":1}}`,
		validPrefix + `{"routing_version":1}}`,
		validPrefix + `{"ownership_epoch":0,"routing_version":1}}`,
		validPrefix + `{"ownership_epoch":1,"routing_version":0}}`,
		validPrefix + `{"ownership_epoch":-1,"routing_version":1}}`,
		validPrefix + `{"ownership_epoch":1,"routing_version":18446744073709551616}}`,
		validPrefix + `{"ownership_epoch":1,"routing_version":1,"unknown":1}}`,
		validPrefix + `{"ownership_epoch":1,"ownership_epoch":2,"routing_version":1}}`,
	}
	for _, input := range invalid {
		var catalog catalogFile
		if err := json.Unmarshal([]byte(input), &catalog); err == nil {
			t.Fatalf("corrupt shard fence decoded: %s", input)
		}
	}

	max := validPrefix +
		`{"ownership_epoch":18446744073709551615,"routing_version":18446744073709551615}}`
	var catalog catalogFile
	if err := json.Unmarshal([]byte(max), &catalog); err != nil {
		t.Fatalf("maximum uint64 fence rejected: %v", err)
	}
	if catalog.ShardStoreFence == nil ||
		catalog.ShardStoreFence.OwnershipEpoch != distribution.OwnershipEpoch(^uint64(0)) ||
		catalog.ShardStoreFence.RoutingVersion != distribution.RoutingVersion(^uint64(0)) {
		t.Fatalf("maximum fence decoded as %+v", catalog.ShardStoreFence)
	}
}

func execShardStoreDDL(t *testing.T, database *Database, statement string) {
	t.Helper()
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession for %q: %v", statement, err)
	}
	prepared, err := session.Prepare(context.Background(), statement)
	if err == nil {
		_, err = prepared.Exec(context.Background(), nil)
	}
	closeErr := session.Close()
	if err != nil {
		t.Fatalf("execute %q: %v", statement, err)
	}
	if closeErr != nil {
		t.Fatalf("close session for %q: %v", statement, closeErr)
	}
}
