package driver

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

func TestChildReservationRetrySettlesCatalogBeforeAcknowledging(t *testing.T) {
	for _, afterRename := range []bool{false, true} {
		name := "before-rename"
		if afterRename {
			name = "directory-fence"
		}
		t.Run(name, func(t *testing.T) {
			catalog, reserved := childReservationCatalogFixture(t)
			base := *catalog.ReplicatedShardStore
			catalog.ReplicatedChildApply = nil
			root := t.TempDir()
			parent := filepath.Join(root, "catalog-parent")
			injected := errors.New("injected directory fence")
			blocked, fences := true, 0
			core := &database{path: filepath.Join(parent, "catalog.vdb"), dataDir: root, catalog: catalog,
				syncDir: func(string) error {
					fences++
					if blocked && afterRename {
						return injected
					}
					return nil
				},
			}
			if afterRename {
				if err := os.Mkdir(parent, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			db := &Database{connector: &dbConnector{db: core}}
			if err := db.ReserveReplicatedChildApply(base, reserved); err == nil {
				t.Fatal("faulted reservation acknowledged")
			}
			if core.catalog.ReplicatedChildApply == nil {
				t.Fatal("lost pending reservation")
			}
			for range 2 {
				if err := db.ReserveReplicatedChildApply(base, reserved); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
					t.Fatalf("retry acknowledged unsettled catalog: %v", err)
				}
				if _, found, err := db.ReplicatedChildApplyReservation(base); found || !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
					t.Fatalf("read advertised unsettled reservation: found=%t err=%v", found, err)
				}
			}
			blocked = false
			if !afterRename {
				if err := os.Mkdir(parent, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.ReserveReplicatedChildApply(base, reserved); err != nil {
				t.Fatal(err)
			}
			if core.catalogWritePending || core.catalogFencePending {
				t.Fatal("acknowledged pending publication")
			}
			got, found, err := db.ReplicatedChildApplyReservation(base)
			if err != nil || !found || got != reserved {
				t.Fatalf("settled reservation: %+v %t %v", got, found, err)
			}
			raw, err := os.ReadFile(core.path)
			if err != nil {
				t.Fatal(err)
			}
			var reopened catalogFileVibe
			if err := decodeCatalogJSON(raw, &reopened); err != nil {
				t.Fatal(err)
			}
			if reopened.ReplicatedChildApply == nil || reopened.ReplicatedChildApply.identity() != reserved {
				t.Fatal("acknowledged reservation missing from persisted catalog")
			}
			before := fences
			if err := db.ReserveReplicatedChildApply(base, reserved); err != nil {
				t.Fatal(err)
			}
			if fences != before {
				t.Fatal("settled exact retry performed extra durability I/O")
			}
		})
	}
}
