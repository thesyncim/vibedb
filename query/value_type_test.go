package query

import (
	"testing"
	"unsafe"
)

func TestValueTypesAreTheSixCurrentKinds(t *testing.T) {
	for kind, want := range []ValueType{
		TypeAny, TypeNull, TypeBool, TypeNumber, TypeString, TypeJSON,
	} {
		if want != ValueType(kind) {
			t.Fatalf("kind %d = %d", kind, want)
		}
	}
}

func TestExtensibleCellKeepsCompactLayout(t *testing.T) {
	if got := unsafe.Sizeof(Cell{}); got != 56 {
		t.Fatalf("Cell size = %d, want 56", got)
	}
}
