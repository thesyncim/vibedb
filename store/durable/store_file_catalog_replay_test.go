package durable

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestFileStoreReplayNeverAdmitsCanonicalCatalogExtent(
	t *testing.T,
) {
	file, err := os.CreateTemp(t.TempDir(), "catalog-free-replay-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	collection, err := Create(file, testFileCatalogOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	originalInline := collection.inlineFree
	originalLoaded := collection.freeLoaded
	t.Cleanup(func() {
		collection.inlineFree = originalInline
		collection.freeLoaded = originalLoaded
		_ = collection.Close()
	})

	state := collection.state.Load()
	catalogExtent, ok := storeio.StateRootPageCatalogExtent(state.root)
	if !ok {
		t.Fatal("created Store has no canonical catalog run")
	}
	catalogExtent.RetiredGeneration = state.root.Generation
	malicious := storeio.NewInlineFreeDelta(
		storeio.PageRef{}, storeio.PageRef{},
	)
	if err := malicious.Append(
		[]storeio.FreeDelta{{
			Op: storeio.FreeOpSet, Extent: catalogExtent,
		}},
		state.root.PageSize, state.super.FileEnd,
	); err != nil {
		t.Fatal(err)
	}
	collection.inlineFree = malicious
	collection.freeLoaded = false
	before := len(collection.reusable)

	err = collection.refreshReusable(state)
	if !errors.Is(err, storeio.ErrFreeLogCorrupt) {
		t.Fatalf("catalog retirement replay = %v", err)
	}
	if len(collection.reusable) != before {
		t.Fatalf(
			"catalog replay changed reusable count from %d to %d",
			before, len(collection.reusable),
		)
	}
}
