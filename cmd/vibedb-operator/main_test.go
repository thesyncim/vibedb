package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPodOrdinalRequiresExactRF3StatefulSetOrdinal(t *testing.T) {
	for _, test := range []struct {
		host string
		want int
		ok   bool
	}{
		{"vibedb-shard-0", 0, true}, {"vibedb-shard-2", 2, true},
		{"vibedb-shard-3", 0, false}, {"vibedb-shard-01", 0, false},
		{"other-1", 0, false}, {"vibedb-shard", 0, false}, {"-1", 0, false},
	} {
		got, err := podOrdinal(test.host)
		if (err == nil) != test.ok || got != test.want {
			t.Fatalf("host=%q ordinal=%d err=%v", test.host, got, err)
		}
	}
}

func TestPrepareRejectsRelativeDirectoriesAndSymlinkResume(t *testing.T) {
	if err := prepare([]string{"-hostname=vibedb-shard-0", "-manifest-dir=relative"}); err == nil {
		t.Fatal("relative manifest directory accepted")
	}
	root := t.TempDir()
	data := filepath.Join(root, "member")
	if err := os.Mkdir(data, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("not a manifest"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(data, "serve-rf3.vibejson")); err != nil {
		t.Fatal(err)
	}
	if err := prepare([]string{"-hostname=vibedb-shard-0", "-manifest-dir=" + root, "-data-dir=" + data}); err == nil {
		t.Fatal("symlink serve manifest accepted as a completed preparation")
	}
}

func TestRenderAndPrepareRejectPositionalArguments(t *testing.T) {
	if err := render([]string{"junk"}); err == nil {
		t.Fatal("render positional argument accepted")
	}
	if err := prepare([]string{"junk"}); err == nil {
		t.Fatal("prepare positional argument accepted")
	}
}
