package main

import (
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"testing"
)

func TestEnrollmentConversionPreservesAbsentLedgerProfile(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		spec := nodecontrol.PreparationSpec{}
		if enabled {
			spec.Apply.RequestLedgerCapacityBytes = 1024
			spec.Apply.RequestLedgerCleanupReserveBytes = 128
			spec.Apply.RequestLedgerRangeIdentity = [32]byte{1}
		}
		manifest, err := prepareRF3ManifestFromSpec(spec, gateway.GroupEnrollmentIntent{}, t.TempDir(), rf3NodePreparationTemplate{})
		if err != nil {
			t.Fatal(err)
		}
		options, err := prepareRF3ApplyOptions(manifest.Apply)
		if err != nil {
			t.Fatalf("enabled=%v: %v", enabled, err)
		}
		if options.RequestLedgerRangeIdentity != spec.Apply.RequestLedgerRangeIdentity {
			t.Fatal("ledger identity changed")
		}
		if !enabled && (manifest.Apply.RequestLedgerRangeStart != "" || manifest.Apply.RequestLedgerRangeEnd != "" || manifest.Apply.RequestLedgerRangeIdentity != "") {
			t.Fatal("absent ledger became configured")
		}
	}
}
