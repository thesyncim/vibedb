package competitive

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDurabilityModeParseRoundTrip(t *testing.T) {
	modes := []DurabilityMode{
		DurabilityDefault,
		DurabilityVolatile,
		DurabilityBufferedVisible,
		DurabilityAsyncStableInFlight,
		DurabilityOrdinarySync,
		DurabilityPowerSafe,
	}
	for _, mode := range modes {
		got, err := ParseDurabilityMode(mode.String())
		if err != nil {
			t.Fatalf("ParseDurabilityMode(%q): %v", mode, err)
		}
		if got != mode {
			t.Fatalf("ParseDurabilityMode(%q) = %v, want %v", mode, got, mode)
		}
	}
	if _, err := ParseDurabilityMode("sync"); err == nil {
		t.Fatal("ParseDurabilityMode accepted ambiguous legacy sync spelling")
	}
}

func TestDurabilityDefaultResolvesToConcreteMode(t *testing.T) {
	tests := []struct {
		engine string
		want   DurabilityMode
	}{
		{engine: "vibejson-heap", want: DurabilityVolatile},
		{engine: "vibejson-durable", want: DurabilityBufferedVisible},
		{engine: "bbolt", want: DurabilityBufferedVisible},
		{engine: "badger", want: DurabilityBufferedVisible},
		{engine: "pebble", want: DurabilityBufferedVisible},
		{engine: "sqlite", want: DurabilityBufferedVisible},
	}
	for _, test := range tests {
		got, err := ResolveDurabilityMode(test.engine, DurabilityDefault)
		if err != nil {
			t.Fatalf("%s default: %v", test.engine, err)
		}
		if got != test.want {
			t.Fatalf("%s default = %s, want %s", test.engine, got, test.want)
		}
		if got == DurabilityDefault {
			t.Fatalf("%s retained an ambiguous default", test.engine)
		}
	}
}

func TestUnsupportedDurabilityModesFailClosed(t *testing.T) {
	tests := []struct {
		engine string
		mode   DurabilityMode
	}{
		{engine: "vibejson-durable", mode: DurabilityOrdinarySync},
		{engine: "pebble", mode: DurabilityAsyncStableInFlight},
		{engine: "pebble", mode: DurabilityPowerSafe},
		{engine: "bbolt", mode: DurabilityPowerSafe},
		{engine: "badger", mode: DurabilityPowerSafe},
		{engine: "sqlite", mode: DurabilityAsyncStableInFlight},
		{engine: "vibejson-heap", mode: DurabilityBufferedVisible},
	}
	for _, test := range tests {
		if _, err := ResolveDurabilityMode(test.engine, test.mode); err == nil {
			t.Fatalf("%s accepted unsupported mode %s", test.engine, test.mode)
		}
	}
}

func TestBenchmarkDurabilityModesAreConcreteAndSupported(t *testing.T) {
	for _, factory := range Factories() {
		modes := BenchmarkDurabilityModes(factory.Name)
		if len(modes) == 0 {
			t.Fatalf("%s has no benchmark durability modes", factory.Name)
		}
		for _, mode := range modes {
			if mode == DurabilityDefault {
				t.Fatalf("%s benchmark modes contain ambiguous default", factory.Name)
			}
			resolved, err := ResolveDurabilityMode(factory.Name, mode)
			if err != nil {
				t.Fatalf("%s mode %s: %v", factory.Name, mode, err)
			}
			if resolved != mode {
				t.Fatalf("%s mode %s resolved as %s", factory.Name, mode, resolved)
			}
		}
	}
}

func TestSQLiteBufferedCheckpointRestoresOffAndTruncatesWAL(t *testing.T) {
	engine, err := newSQLite(Config{
		Dir:        t.TempDir(),
		Durability: DurabilityBufferedVisible,
		CacheBytes: DefaultCacheBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	sqlite := engine.(*sqliteEngine)
	fixture := Corpus(8)
	if err := sqlite.Load(fixture); err != nil {
		t.Fatal(err)
	}
	if err := sqlite.Put(fixture[0].Key, SameSizeUpdatedJSON(fixture, 0)); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := sqlite.db.QueryRow(`PRAGMA synchronous`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before != 0 {
		t.Fatalf("buffered synchronous before checkpoint = %d, want OFF(0)", before)
	}
	if err := sqlite.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	var after int
	if err := sqlite.db.QueryRow(`PRAGMA synchronous`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != 0 {
		t.Fatalf("buffered synchronous after checkpoint = %d, want OFF(0)", after)
	}
	info, err := os.Stat(filepath.Join(sqlite.cfg.Dir, "sqlite.db-wal"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err == nil && info.Size() != 0 {
		t.Fatalf("WAL size after TRUNCATE checkpoint = %d, want 0", info.Size())
	}
}
