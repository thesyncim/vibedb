package driver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
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
