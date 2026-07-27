package vibesql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

func TestPathDSNOpensWithDurabilitySync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.vj")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Create(file, durable.Options{})
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	source, err := acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := source.release(); err != nil {
			t.Errorf("release: %v", err)
		}
	}()
	if got := source.collection.Stats().Durability; got != durable.DurabilitySync {
		t.Fatalf("path DSN durability = %d, want DurabilitySync", got)
	}
}
