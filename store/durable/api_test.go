package durable_test

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

func TestCanonicalDurableAPIAndWriterLease(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "durable-api-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var options durable.Options
	db, err := durable.Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durable.Open(file, options); !errors.Is(err, durable.ErrWriterLocked) {
		t.Fatalf("second Open = %v, want %v", err, durable.ErrWriterLocked)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
