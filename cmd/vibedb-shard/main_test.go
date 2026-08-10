package main

import (
	"path/filepath"
	"testing"
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
		{"unparseable_flag", []string{"vibedb-shard", "serve", "-store"}, 2},
		{
			name: "store_open_fails",
			// A directory cannot be opened as a store file, so Open fails before
			// the listener is created.
			args: []string{
				"vibedb-shard", "serve",
				"-store", t.TempDir(),
				"-distribution", "tenant_data", "-shard", "-80",
				"-allocation-generation", "1",
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

// TestRunStoreOpenReportsPath is a light guard that the store path reaches Open:
// a nested, non-existent directory path also fails cleanly with code 1.
func TestRunStoreOpenReportsPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-dir", "shard.vdb")
	args := []string{
		"vibedb-shard", "serve",
		"-store", missing,
		"-distribution", "tenant_data", "-shard", "-80",
		"-allocation-generation", "1",
	}
	if got := run(args); got != 1 {
		t.Fatalf("run(missing store dir) = %d, want 1", got)
	}
}
