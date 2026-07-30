package competitive

import (
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/badger/v4/options"
)

func TestParseStorageProfile(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  StorageProfile
	}{
		{value: "intrinsic", want: StorageProfileIntrinsic},
		{value: "production", want: StorageProfileProduction},
	} {
		got, err := ParseStorageProfile(tc.value)
		if err != nil {
			t.Fatalf("ParseStorageProfile(%q): %v", tc.value, err)
		}
		if got != tc.want || got.String() != tc.value {
			t.Fatalf("ParseStorageProfile(%q) = %v (%q), want %v", tc.value, got, got, tc.want)
		}
	}
	if _, err := ParseStorageProfile("default"); err == nil {
		t.Fatal(`ParseStorageProfile("default") succeeded`)
	}
}

func TestResolveStorageProfile(t *testing.T) {
	for _, engine := range []string{"badger", "pebble"} {
		intrinsic, err := ResolveStorageProfile(engine, StorageProfileIntrinsic)
		if err != nil {
			t.Fatal(err)
		}
		if intrinsic.Compression != "none" ||
			intrinsic.compression != storageCompressionNone ||
			!strings.Contains(intrinsic.Provenance, "NoCompression") &&
				!strings.Contains(intrinsic.Provenance, "options.None") {
			t.Fatalf("%s intrinsic resolution = %+v", engine, intrinsic)
		}

		production, err := ResolveStorageProfile(engine, StorageProfileProduction)
		if err != nil {
			t.Fatal(err)
		}
		if production.Compression != "snappy-sst-blocks" ||
			production.compression != storageCompressionSnappy ||
			!strings.Contains(production.Provenance, "Snappy") {
			t.Fatalf("%s production resolution = %+v", engine, production)
		}
	}

	for _, engine := range []string{"vibejson-durable", "bbolt", "sqlite"} {
		for _, profile := range []StorageProfile{
			StorageProfileIntrinsic,
			StorageProfileProduction,
		} {
			got, err := ResolveStorageProfile(engine, profile)
			if err != nil {
				t.Fatalf("%s/%s: %v", engine, profile, err)
			}
			if got.Compression != "unsupported/no-op" ||
				got.compression != storageCompressionUnsupported ||
				!strings.Contains(got.Provenance, "profile-no-op") {
				t.Fatalf("%s/%s resolution = %+v", engine, profile, got)
			}
		}
	}

	if _, err := ResolveStorageProfile("unknown", StorageProfileIntrinsic); err == nil {
		t.Fatal("unknown engine succeeded")
	}
	if _, err := ResolveStorageProfile("badger", StorageProfile(255)); err == nil {
		t.Fatal("unknown profile succeeded")
	}
}

func TestBadgerStorageProfileOptionsKeepCacheBudget(t *testing.T) {
	const cacheBytes = int64(17 << 20)

	intrinsic, err := ResolveStorageProfile("badger", StorageProfileIntrinsic)
	if err != nil {
		t.Fatal(err)
	}
	compression, block, index := badgerStorageOptions(intrinsic, cacheBytes)
	if compression != options.None || block != 0 || index != cacheBytes {
		t.Fatalf("intrinsic = (%v, %d, %d)", compression, block, index)
	}

	production, err := ResolveStorageProfile("badger", StorageProfileProduction)
	if err != nil {
		t.Fatal(err)
	}
	compression, block, index = badgerStorageOptions(production, cacheBytes)
	if compression != options.Snappy || block != cacheBytes || index != 0 {
		t.Fatalf("production = (%v, %d, %d)", compression, block, index)
	}
}

func TestPebbleStorageProfileCompression(t *testing.T) {
	intrinsic, err := ResolveStorageProfile("pebble", StorageProfileIntrinsic)
	if err != nil {
		t.Fatal(err)
	}
	if got := pebbleStorageCompression(intrinsic); got != pebble.NoCompression {
		t.Fatalf("intrinsic compression = %v", got)
	}

	production, err := ResolveStorageProfile("pebble", StorageProfileProduction)
	if err != nil {
		t.Fatal(err)
	}
	if got := pebbleStorageCompression(production); got != pebble.SnappyCompression {
		t.Fatalf("production compression = %v", got)
	}
}
