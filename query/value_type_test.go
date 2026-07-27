package query

import (
	"slices"
	"testing"
	"unsafe"
)

func TestCoreValueTypeRegistry(t *testing.T) {
	types := AppendCoreValueTypes(make([]ValueTypeDescriptor, 0, 6))
	if len(types) != 6 {
		t.Fatalf("type count = %d", len(types))
	}
	for _, descriptor := range types {
		if err := descriptor.Validate(); err != nil {
			t.Fatalf("%+v: %v", descriptor, err)
		}
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		types = AppendCoreValueTypes(types[:0])
	}); allocations != 0 {
		t.Fatalf("AppendCoreValueTypes allocations = %v", allocations)
	}
}

func TestExtensionCellIsLengthDelimitedOpaqueValue(t *testing.T) {
	payload := []byte{1, 2, 3, 4}
	cell, err := ExtensionCell(ValueTypeExtensionStart+7, 3, payload)
	if err != nil {
		t.Fatal(err)
	}
	if cell.Type() != ValueTypeExtensionStart+7 || cell.TypeVersion() != 3 ||
		!cell.IsExtension() || !slices.Equal(cell.Payload(), payload) {
		t.Fatalf("cell = type %#x version %d extension %v payload %v",
			cell.Type(), cell.TypeVersion(), cell.IsExtension(), cell.Payload())
	}
	if cell.JSON() != nil {
		t.Fatalf("extension JSON = %q", cell.JSON())
	}
	dst := []byte{9}
	if got := cell.AppendJSON(dst); !slices.Equal(got, dst) {
		t.Fatalf("extension AppendJSON = %v", got)
	}
	if got := cell.String(); got != "<value-type 0x8007 v3: 4 bytes>" {
		t.Fatalf("extension String = %q", got)
	}
	if _, err := ExtensionCell(TypeJSON, 1, payload); err == nil {
		t.Fatal("reserved extension type accepted")
	}
}

func TestValueTypeDescriptorExtensionValidation(t *testing.T) {
	good := ValueTypeDescriptor{
		Type: ValueTypeExtensionStart + 1, Version: 2,
		Flags: ValueTypeVariableWidth,
		Name:  "timestamp-tz",
	}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := []ValueTypeDescriptor{
		{Type: 0x100, Name: "unassigned"},
		{Type: ValueTypeExtensionStart, Version: 1},
		{Type: ValueTypeExtensionStart, Name: "bad-flags", Flags: 1 << 15},
		{Type: TypeNumber, Version: 1, Name: "number"},
	}
	for _, descriptor := range bad {
		if err := descriptor.Validate(); err == nil {
			t.Fatalf("accepted %+v", descriptor)
		}
	}
}

func TestExtensibleCellKeepsCompactLayout(t *testing.T) {
	if got := unsafe.Sizeof(Cell{}); got != 56 {
		t.Fatalf("Cell size = %d, want 56", got)
	}
}
