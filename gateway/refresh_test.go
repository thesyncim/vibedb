package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestFileCatalogRefresherPublishesOnlyNewerValidSnapshots proves on-demand
// reload advances monotonically and a stale or malformed file leaves the last
// valid generation serving.
func TestFileCatalogRefresherPublishesOnlyNewerValidSnapshots(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	initial := testSnapshot(t, 1)
	if err := SaveSnapshot(path, initial); err != nil {
		t.Fatalf("SaveSnapshot initial: %v", err)
	}
	holder := NewCatalogHolder(initial)
	refresher := NewFileCatalogRefresher(path, holder)

	if _, err := refresher.Refresh(context.Background(), 1); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("unchanged refresh err = %v, want ErrStaleGeneration", err)
	}

	newer := testSnapshot(t, 2)
	if err := SaveSnapshot(path, newer); err != nil {
		t.Fatalf("SaveSnapshot newer: %v", err)
	}
	got, err := refresher.Refresh(context.Background(), 1)
	if err != nil {
		t.Fatalf("Refresh newer: %v", err)
	}
	if got.Generation() != 2 || holder.Current().Generation() != 2 {
		t.Fatalf("refreshed/current generations = %d/%d, want 2/2",
			got.Generation(), holder.Current().Generation())
	}

	if err := os.WriteFile(path, []byte(`{"version":`), 0o600); err != nil {
		t.Fatalf("write malformed catalog: %v", err)
	}
	if _, err := refresher.Refresh(context.Background(), 2); err == nil {
		t.Fatal("malformed refresh returned nil error")
	}
	if holder.Current().Generation() != 2 {
		t.Fatalf("malformed refresh replaced generation with %d, want 2",
			holder.Current().Generation())
	}
}

// TestFileCatalogRefresherWaitIsCancelable proves a refresh stampede cannot
// strand canceled requests behind the single file loader.
func TestFileCatalogRefresherWaitIsCancelable(t *testing.T) {
	holder := NewCatalogHolder(testSnapshot(t, 1))
	refresher := NewFileCatalogRefresher("unused", holder)
	<-refresher.gate
	defer func() { refresher.gate <- struct{}{} }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := refresher.Refresh(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Refresh err = %v, want context.Canceled", err)
	}
}

func TestFileCatalogRefresherRejectsInvalidConfiguration(t *testing.T) {
	for _, refresher := range []*FileCatalogRefresher{
		nil,
		NewFileCatalogRefresher("", NewCatalogHolder(nil)),
		NewFileCatalogRefresher("catalog.json", nil),
	} {
		if _, err := refresher.Refresh(context.Background(), 0); !errors.Is(err, ErrInvalidRefreshSource) {
			t.Fatalf("Refresh err = %v, want ErrInvalidRefreshSource", err)
		}
	}
}
