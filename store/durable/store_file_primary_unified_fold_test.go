package durable

import (
	"bytes"
	"os"
	"testing"
)

// TestFilePrimaryUnifiedNativeFoldCrashBoundary exercises the checkpoint shape
// the native class-5 patcher accepts: several same-shape, same-size integer
// replacements in one durable leaf. Before Flush a copied store must recover
// the sealed base; after Flush a reopen must recover every patched canonical
// row. This pins the fresh-page + root publication boundary independently of
// the codec's byte-identity differential.
func TestFilePrimaryUnifiedNativeFoldCrashBoundary(t *testing.T) {
	built, keys, values := buildRedundantPrimaryCorpus(t, 2_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "unified-native-fold.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}

	state := collection.state.Load()
	firstRoute, err := collection.currentPrimaryResidentRoute(
		state, []byte(keys[0]),
	)
	if err != nil {
		t.Fatal(err)
	}
	var selected []int
	for i := range keys {
		route, routeErr := collection.currentPrimaryResidentRoute(
			state, []byte(keys[i]),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if route.Bucket == firstRoute.Bucket {
			selected = append(selected, i)
			if len(selected) == 4 {
				break
			}
		}
	}
	if len(selected) != 4 {
		t.Fatalf("fixture provided %d same-leaf rows, want 4", len(selected))
	}

	updated := make(map[int][]byte, len(selected))
	for _, index := range selected {
		value := append([]byte(nil), values[index]...)
		at := bytes.Index(value, []byte(`"group":`))
		if at < 0 {
			t.Fatalf("row %d has no group scalar", index)
		}
		at += len(`"group":`)
		if value[at] == '9' {
			value[at] = '8'
		} else {
			value[at]++
		}
		if len(value) != len(values[index]) ||
			bytes.Equal(value, values[index]) {
			t.Fatalf("row %d replacement is not fixed-size", index)
		}
		updated[index] = value
		if created, putErr := collection.Put(
			[]byte(keys[index]), value,
		); putErr != nil || created {
			t.Fatalf("Put row %d = %v,%v", index, created, putErr)
		}
	}

	before := clonePrimaryCrashFile(t, file, "before-native-fold.vibe")
	beforeCollection, err := Open(before, options)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range selected {
		assertPrimaryRaw(t, beforeCollection, keys[index], values[index], true)
	}
	if err := beforeCollection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}

	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedFile, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedFile.Close()
	reopened, err := Open(reopenedFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, index := range selected {
		assertPrimaryRaw(t, reopened, keys[index], updated[index], true)
	}
}
