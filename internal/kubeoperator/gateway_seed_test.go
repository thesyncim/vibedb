package kubeoperator

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
)

func TestPrepareGatewaySeedPinsProjectedConfigMapWithoutWeakeningAliases(t *testing.T) {
	root := t.TempDir()
	version := filepath.Join(root, "projected-version")
	source, target := filepath.Join(root, "projected-seed"), filepath.Join(root, "catalog-genesis")
	seed := gatewaySeedFixture(t, 1, "127.0.0.1:7411")
	if err := gateway.SaveSnapshot(version, seed); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(version, source); err != nil {
		t.Fatal(err)
	}
	route := filepath.Join(root, "catalog-route")
	if gateway.ValidateReplicatedCatalogRouteSeedSeparation(source, route) == nil {
		t.Fatal("serving accepted ConfigMap projection symlink as immutable seed")
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := PrepareGatewaySeed(source, target); err != nil {
			t.Fatalf("pin/restart attempt %d: %v", attempt, err)
		}
	}
	if err := gateway.ValidateReplicatedCatalogRouteSeedSeparation(target, route); err != nil {
		t.Fatalf("pinned regular seed rejected by serving: %v", err)
	}
	info, err := os.Lstat(target)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("pinned seed mode: %v %v", info, err)
	}
	want, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	nextVersion := filepath.Join(root, "projected-next-version")
	if err := gateway.SaveSnapshot(nextVersion, gatewaySeedFixture(t, 1, "127.0.0.1:7412")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(nextVersion, source+".next"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(source+".next", source); err != nil {
		t.Fatal(err)
	}
	if PrepareGatewaySeed(source, target) == nil {
		t.Fatal("changed ConfigMap rewrote immutable PVC seed")
	}
	got, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("changed bootstrap altered pinned seed")
	}
}

func TestPrepareGatewaySeedRejectsAliasesAndNonGenesis(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := gateway.SaveSnapshot(source, gatewaySeedFixture(t, 1, "127.0.0.1:7411")); err != nil {
		t.Fatal(err)
	}
	if PrepareGatewaySeed(source, source) == nil || PrepareGatewaySeed("relative", filepath.Join(root, "target")) == nil {
		t.Fatal("invalid pin paths accepted")
	}
	for _, link := range []struct {
		name string
		make func(string, string) error
	}{{"symlink", os.Symlink}, {"hardlink", os.Link}} {
		target := filepath.Join(root, link.name)
		if err := link.make(source, target); err != nil {
			t.Fatal(err)
		}
		if PrepareGatewaySeed(source, target) == nil {
			t.Fatalf("%s target accepted", link.name)
		}
	}
	next := filepath.Join(root, "next")
	if err := gateway.SaveSnapshot(next, gatewaySeedFixture(t, 2, "127.0.0.1:7411")); err != nil {
		t.Fatal(err)
	}
	if PrepareGatewaySeed(next, filepath.Join(root, "invalid")) == nil {
		t.Fatal("mutable catalog used as immutable genesis")
	}
}

func gatewaySeedFixture(t *testing.T, generation uint64, endpoint string) *gateway.Snapshot {
	t.Helper()
	manifest, err := distribution.NewManifest("data", 1, []distribution.Shard{{ID: "all", AllocationGeneration: 1,
		Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: []distribution.EndpointID{"node"}, Epoch: 1}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := gateway.NewSnapshot(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: distribution.NativeMapperVersion}},
		Manifests:     []*distribution.Manifest{manifest},
	}, map[distribution.EndpointID]string{"node": endpoint}, generation)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
