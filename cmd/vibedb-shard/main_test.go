package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// TestRunArgumentHandling covers the binary's dispatch and validation branches
// that do not start the blocking accept loop: usage errors return 2 and a store
// that cannot be opened returns 1.
func TestRunArgumentHandling(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want int
	}{
		{"no_subcommand", []string{"vibedb-shard"}, 2},
		{"unknown_subcommand", []string{"vibedb-shard", "wat"}, 2},
		{"missing_required_flags", []string{"vibedb-shard", "serve", "-listen", "127.0.0.1:0"}, 2},
		{"zero_epoch", []string{"vibedb-shard", "serve", "-store", "store.vdb", "-distribution", "d", "-shard", "s", "-allocation-generation", "1", "-routing-version", "1"}, 2},
		{"zero_routing_version", []string{"vibedb-shard", "serve", "-store", "store.vdb", "-distribution", "d", "-shard", "s", "-allocation-generation", "1", "-epoch", "1"}, 2},
		{"authentication_required", []string{"vibedb-shard", "serve", "-store", "store.vdb", "-distribution", "d", "-shard", "s", "-allocation-generation", "1", "-epoch", "1", "-routing-version", "1"}, 2},
		{"plaintext_tls_conflict", []string{"vibedb-shard", "serve", "-dev-plaintext-loopback", "-tls-certificate", "cert.pem", "-store", "store.vdb", "-distribution", "d", "-shard", "s", "-allocation-generation", "1", "-epoch", "1", "-routing-version", "1"}, 2},
		{"unparseable_flag", []string{"vibedb-shard", "serve", "-store"}, 2},
		{"missing_init_flags", []string{"vibedb-shard", "init", "-store", "store.vdb"}, 2},
		{"unparseable_init_flag", []string{"vibedb-shard", "init", "-store"}, 2},
		{
			name: "store_open_fails",
			// A directory cannot be opened as a store file, so Open fails before
			// the listener is created.
			args: []string{
				"vibedb-shard", "serve",
				"-dev-plaintext-loopback",
				"-store", t.TempDir(),
				"-distribution", "tenant_data", "-shard", "-80",
				"-allocation-generation", "1",
				"-epoch", "1", "-routing-version", "1",
			},
			want: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := run(tc.args); got != tc.want {
				t.Fatalf("run(%v) = %d, want %d", tc.args, got, tc.want)
			}
		})
	}
}

func TestRunInitCreatesIdentityAndRetriesExactly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard.vdb")
	args := []string{
		"vibedb-shard", "init",
		"-store", path,
		"-distribution", "tenant_data", "-shard", "-80",
		"-allocation-generation", "7",
	}
	if got := run(args); got != 0 {
		t.Fatalf("first init = %d, want 0", got)
	}
	binding := sqldriver.ShardStoreBinding{
		Distribution: "tenant_data", Shard: "-80",
		AllocationGeneration: distribution.ShardAllocationGeneration(7),
	}
	db, err := sqldriver.OpenShardStore(path, binding)
	if err != nil {
		t.Fatalf("OpenShardStore after init: %v", err)
	}
	identity, err := db.ShardStoreIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if identity.LogID == ([16]byte{}) {
		t.Fatal("init generated a zero LogID")
	}

	if got := run(args); got != 0 {
		t.Fatalf("exact init retry = %d, want 0", got)
	}
	db, err = sqldriver.OpenShardStore(path, binding)
	if err != nil {
		t.Fatal(err)
	}
	retried, err := db.ShardStoreIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if retried != identity {
		t.Fatalf("exact init retry changed identity: got %+v want %+v", retried, identity)
	}

	mismatch := append([]string(nil), args...)
	mismatch[len(mismatch)-1] = "8"
	if got := run(mismatch); got != 1 {
		t.Fatalf("mismatched init = %d, want 1", got)
	}
}

func TestRunServeRefusesMissingStoreWithoutInitializingIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-shard.vdb")
	args := []string{
		"vibedb-shard", "serve",
		"-dev-plaintext-loopback",
		"-store", path,
		"-distribution", "tenant_data", "-shard", "-80",
		"-allocation-generation", "1",
		"-epoch", "1", "-routing-version", "1",
	}
	if got := run(args); got != 1 {
		t.Fatalf("run(missing store) = %d, want 1", got)
	}
	for _, candidate := range []string{path, path + ".lock", path + ".tables"} {
		if _, err := os.Stat(candidate); !os.IsNotExist(err) {
			t.Fatalf("serve initialized missing path %s: %v", candidate, err)
		}
	}
}

// TestRunStoreOpenReportsPath is a light guard that the store path reaches Open:
// a nested, non-existent directory path also fails cleanly with code 1.
func TestRunStoreOpenReportsPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-dir", "shard.vdb")
	args := []string{
		"vibedb-shard", "serve",
		"-dev-plaintext-loopback",
		"-store", missing,
		"-distribution", "tenant_data", "-shard", "-80",
		"-allocation-generation", "1",
		"-epoch", "1", "-routing-version", "1",
	}
	if got := run(args); got != 1 {
		t.Fatalf("run(missing store dir) = %d, want 1", got)
	}
}
