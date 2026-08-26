package resultformat

import "testing"

func TestRegistryIsUniqueDenseAndFrozen(t *testing.T) {
	formats := [...]uint16{Mutation, Transaction, RouteGate, RequestLedger, ExecutionPin}
	var seen [1 << 8]bool
	for index, format := range formats {
		if format != uint16(index+1) || seen[format] {
			t.Fatalf("format[%d]=%d", index, format)
		}
		seen[format] = true
	}
}
