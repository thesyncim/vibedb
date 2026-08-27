package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/kubeoperator"
)

func TestPodOrdinalRequiresExactRF3StatefulSetOrdinal(t *testing.T) {
	for _, test := range []struct {
		host string
		want int
		ok   bool
	}{
		{"vibedb-catalog-0", 0, true}, {"vibedb-ledger-2", 2, true},
		{"vibedb-data-1", 1, true}, {"vibedb-shard-0", 0, false},
		{"vibedb-data-3", 0, false}, {"vibedb-data-01", 0, false},
		{"other-1", 0, false}, {"vibedb-data", 0, false}, {"-1", 0, false},
	} {
		got, err := podOrdinal(test.host)
		if (err == nil) != test.ok || got != test.want {
			t.Fatalf("host=%q ordinal=%d err=%v", test.host, got, err)
		}
	}
}

func TestValidateAcceptsOnlyCompleteRenderedContract(t *testing.T) {
	config := kubeoperator.Config{Namespace: "vibedb-test", Image: "vibedb:test",
		ShardNodeIDs: [9]string{
			"11000000000000000000000000000000",
			"12000000000000000000000000000000",
			"13000000000000000000000000000000",
			"21000000000000000000000000000000",
			"22000000000000000000000000000000",
			"23000000000000000000000000000000",
			"31000000000000000000000000000000",
			"32000000000000000000000000000000",
			"33000000000000000000000000000000",
		}, ManifestConfigMap: "rf3-manifests", TLSSecret: "rf3-tls",
		GatewayConfigMap: "gateway-config", GatewayTLSSecret: "gateway-tls",
		ShardStorage: "20Gi", GatewayStorage: "1Gi"}
	var rendered bytes.Buffer
	if err := kubeoperator.Render(&rendered, config); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	if err := os.WriteFile(path, rendered.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validate([]string{"-manifest=" + path}); err != nil {
		t.Fatalf("validate rendered manifest: %v", err)
	}
	if err := os.WriteFile(path, bytes.Replace(rendered.Bytes(), []byte("livenessProbe:"), []byte("lostProbe:"), 1), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validate([]string{"-manifest=" + path}); err == nil {
		t.Fatal("validate accepted a manifest missing a liveness contract")
	}
}

func TestPrepareRejectsRelativeDirectoriesAndSymlinkResume(t *testing.T) {
	if err := prepare([]string{"-hostname=vibedb-data-0", "-manifest-dir=relative"}); err == nil {
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
	if err := prepare([]string{"-hostname=vibedb-data-0", "-manifest-dir=" + root, "-data-dir=" + data}); err == nil {
		t.Fatal("symlink serve manifest accepted as a completed preparation")
	}
}

func TestRenderAndPrepareRejectPositionalArguments(t *testing.T) {
	if err := prepareGateway([]string{"junk"}); err == nil {
		t.Fatal("prepare-gateway positional argument accepted")
	}
	if err := prepareGateway([]string{"-catalog-source=relative", "-catalog-target=/tmp/catalog"}); err == nil {
		t.Fatal("prepare-gateway relative source accepted")
	}
	if err := render([]string{"junk"}); err == nil {
		t.Fatal("render positional argument accepted")
	}
	if err := prepare([]string{"junk"}); err == nil {
		t.Fatal("prepare positional argument accepted")
	}
	if err := render([]string{"-shard-node-ids=a,b,c,d,e,f,g,h,i",
		"-bootstrap-state-dir=" + t.TempDir()}); err == nil {
		t.Fatal("render accepted competing node identity authorities")
	}
}
