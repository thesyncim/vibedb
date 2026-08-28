package driver

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
)

func schemaNamespaceFixture(t *testing.T) (string, replicatedSchemaStageMarker, ReplicatedShardStoreIdentity) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, replicatedSchemaTargetsDirectory), 0o700); err != nil {
		t.Fatal(err)
	}
	marker := replicatedSchemaStageMarker{storages: [][32]byte{{1}}, sourceStorages: [][32]byte{{2}}}
	target := ReplicatedShardStoreIdentity{Relations: []ReplicatedShardRelationIdentity{
		{Storage: hex.EncodeToString(marker.storages[0][:])},
	}}
	for _, part := range []struct {
		id                  [32]byte
		directory, contents string
	}{
		{marker.sourceStorages[0], dir, "source"},
		{marker.storages[0], filepath.Join(dir, replicatedSchemaTargetsDirectory), "target"},
	} {
		for _, suffix := range []string{".vjc", ".vjc.rjournal"} {
			if err := os.WriteFile(filepath.Join(part.directory, hex.EncodeToString(part.id[:])+suffix), []byte(part.contents+suffix), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	return dir, marker, target
}

func TestSchemaNamespaceResumesEveryPublicationCutWithoutDataLoss(t *testing.T) {
	for failAt := 1; failAt <= 16; failAt++ {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			dir, marker, target := schemaNamespaceFixture(t)
			injected := errors.New("namespace crash")
			calls := 0
			replicatedSchemaNamespaceFaultHook = func(string) error {
				calls++
				if calls == failAt {
					return injected
				}
				return nil
			}
			t.Cleanup(func() { replicatedSchemaNamespaceFaultHook = nil })
			if err := activateReplicatedSchemaNamespace(dir, marker, target); !errors.Is(err, injected) || !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
				t.Fatalf("cut %d calls=%d: %v", failAt, calls, err)
			}
			replicatedSchemaNamespaceFaultHook = nil
			// Every source and target byte survives in at least one of the two
			// exact names. A repeated open must settle even double-link cuts.
			for _, part := range []struct {
				id                 [32]byte
				from, to, contents string
			}{
				{marker.sourceStorages[0], "", replicatedSchemaSourcesDirectory, "source"},
				{marker.storages[0], replicatedSchemaTargetsDirectory, "", "target"},
			} {
				for _, suffix := range []string{".vjc", ".vjc.rjournal"} {
					name := hex.EncodeToString(part.id[:]) + suffix
					a, ea := os.ReadFile(filepath.Join(dir, part.from, name))
					b, eb := os.ReadFile(filepath.Join(dir, part.to, name))
					want := []byte(part.contents + suffix)
					if (ea != nil || !bytes.Equal(a, want)) && (eb != nil || !bytes.Equal(b, want)) {
						t.Fatalf("lost %s after crash: %v / %v", name, ea, eb)
					}
				}
			}
			for retry := 0; retry < 2; retry++ {
				if err := activateReplicatedSchemaNamespace(dir, marker, target); err != nil {
					t.Fatalf("resume %d: %v", retry, err)
				}
			}
			for _, suffix := range []string{".vjc", ".vjc.rjournal"} {
				source := hex.EncodeToString(marker.sourceStorages[0][:]) + suffix
				newName := hex.EncodeToString(marker.storages[0][:]) + suffix
				if _, err := os.Stat(filepath.Join(dir, source)); !os.IsNotExist(err) {
					t.Fatalf("unarchived source: %v", err)
				}
				if _, err := os.Stat(filepath.Join(dir, replicatedSchemaTargetsDirectory, newName)); !os.IsNotExist(err) {
					t.Fatalf("unpromoted target: %v", err)
				}
				old, err := os.ReadFile(filepath.Join(dir, replicatedSchemaSourcesDirectory, source))
				if err != nil || !bytes.Equal(old, []byte("source"+suffix)) {
					t.Fatalf("archive: %q %v", old, err)
				}
				fresh, err := os.ReadFile(filepath.Join(dir, newName))
				if err != nil || !bytes.Equal(fresh, []byte("target"+suffix)) {
					t.Fatalf("target: %q %v", fresh, err)
				}
			}
		})
	}
}

func TestSchemaNamespaceRefusesLiveOwnerAndForeignDestination(t *testing.T) {
	for _, variant := range []string{"live_source", "live_target", "foreign_destination", "symlink_directory", "substituted_target"} {
		t.Run(variant, func(t *testing.T) {
			dir, marker, target := schemaNamespaceFixture(t)
			source := hex.EncodeToString(marker.sourceStorages[0][:]) + ".vjc"
			fresh := hex.EncodeToString(marker.storages[0][:]) + ".vjc"
			switch variant {
			case "live_source", "live_target":
				path := filepath.Join(dir, source)
				if variant == "live_target" {
					path = filepath.Join(dir, replicatedSchemaTargetsDirectory, fresh)
				}
				file, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				if err := storeio.LockWriter(file); err != nil {
					t.Fatal(err)
				}
				defer storeio.UnlockWriter(file)
			case "foreign_destination":
				if err := os.WriteFile(filepath.Join(dir, fresh), []byte("foreign"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "symlink_directory":
				if err := os.Symlink(t.TempDir(), filepath.Join(dir, replicatedSchemaSourcesDirectory)); err != nil {
					t.Fatal(err)
				}
			case "substituted_target":
				marker.storages[0][1]++
			}
			if err := activateReplicatedSchemaNamespace(dir, marker, target); err == nil {
				t.Fatal("unsafe promotion accepted")
			}
			if raw, err := os.ReadFile(filepath.Join(dir, source)); err != nil || !bytes.Equal(raw, []byte("source.vjc")) {
				t.Fatalf("source moved before complete preflight: %q %v", raw, err)
			}
		})
	}
}
