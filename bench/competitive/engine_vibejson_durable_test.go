package competitive

import (
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

func TestVibeDurableCompactBulkIsExplicitAndCannotBePutReplay(t *testing.T) {
	engine, err := newVibeDurable(Config{})
	if err != nil {
		t.Fatal(err)
	}
	verbatim := engine.(*vibeDurable)
	if got := verbatim.options().DocumentFormat; got != durable.DocumentFormatVerbatim {
		t.Fatalf("default document format = %d, want verbatim", got)
	}
	if strings.Contains(verbatim.Tuning(), "DocumentFormatCompact") {
		t.Fatalf("default tuning falsely reports compact: %q", verbatim.Tuning())
	}

	engine, err = newVibeDurable(Config{Compact: true})
	if err != nil {
		t.Fatal(err)
	}
	compact := engine.(*vibeDurable)
	if got := compact.options().DocumentFormat; got != durable.DocumentFormatCompact {
		t.Fatalf("explicit document format = %d, want compact", got)
	}
	if !strings.Contains(compact.Tuning(), "DocumentFormatCompact") {
		t.Fatalf("compact tuning omits format: %q", compact.Tuning())
	}

	if _, err := newVibeDurable(Config{
		Compact: true,
		PutLoop: true,
	}); err == nil {
		t.Fatal("compact Put replay was accepted")
	}
}

func TestVibeDurableBufferedVisibleUsesFilesystemCheckpointLane(t *testing.T) {
	engine, err := newVibeDurable(Config{
		Durability: DurabilityBufferedVisible,
		CacheBytes: DefaultCacheBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	buffered := engine.(*vibeDurable)
	options := buffered.options()
	if options.Durability != durable.DurabilityBufferedVisible {
		t.Fatalf("store durability = %d, want buffered-visible", options.Durability)
	}
	if options.CheckpointStrength != durable.CheckpointFilesystem {
		t.Fatalf(
			"checkpoint strength = %d, want ordinary filesystem",
			options.CheckpointStrength,
		)
	}
	if options.Backend != durable.BackendPortable {
		t.Fatalf("backend = %d, want portable", options.Backend)
	}
	if options.MaxDocumentBytes != 1<<10 {
		t.Fatalf(
			"maximum document bytes = %d, want benchmark corpus bound",
			options.MaxDocumentBytes,
		)
	}

	powerSafeEngine, err := newVibeDurable(Config{
		Durability: DurabilityPowerSafe,
		CacheBytes: DefaultCacheBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	powerSafe := powerSafeEngine.(*vibeDurable).options()
	if powerSafe.Durability != durable.DurabilitySync {
		t.Fatalf("power-safe store durability = %d, want sync", powerSafe.Durability)
	}
	if powerSafe.CheckpointStrength != durable.CheckpointPowerSafe {
		t.Fatalf(
			"power-safe checkpoint strength = %d, want power-safe",
			powerSafe.CheckpointStrength,
		)
	}
}
