package query

import "fmt"

// ValueType is a versioned result-value identity. Values below
// ValueTypeExtensionStart are assigned by this package. Application and future
// negotiated types occupy the extension range and always carry a payload
// length, so an older batch reader can skip optional columns without knowing
// their representation.
//
// Numeric gaps are intentional. They leave room for new ordered scalar
// families without renumbering the JSON types or changing persisted prepared
// plans.
type ValueType uint16

const (
	// TypeAny is a schema-level dynamic JSON value. A cell never has TypeAny;
	// its concrete type is carried per value.
	TypeAny ValueType = 0x0000

	TypeNull   ValueType = 0x0010
	TypeBool   ValueType = 0x0020
	TypeNumber ValueType = 0x0030
	TypeString ValueType = 0x0040
	// TypeJSON is an array or object retained as exact JSON bytes.
	TypeJSON ValueType = 0x0050

	// ValueTypeExtensionStart is the first caller/feature-negotiated type id.
	// Extension payloads are opaque to the core query engine.
	ValueTypeExtensionStart ValueType = 0x8000
)

// CellKind is retained as the source-level name for JSON callers. It aliases
// ValueType rather than defining a second enum, so transport and result code
// cannot disagree about a cell's type id.
type CellKind = ValueType

const (
	KindNull   = TypeNull
	KindBool   = TypeBool
	KindNumber = TypeNumber
	KindString = TypeString
	KindJSON   = TypeJSON
)

// ValueTypeFlags describe storage properties, not semantic operator support.
// Unknown flag bits fail validation instead of being silently ignored.
type ValueTypeFlags uint16

const (
	ValueTypeVariableWidth ValueTypeFlags = 1 << iota
	ValueTypeNested
)

const valueTypeKnownFlags = ValueTypeVariableWidth | ValueTypeNested

// ValueTypeDescriptor is cold schema/negotiation metadata. Type and Version
// identify semantics; Flags describe framing; Name is display metadata and is
// never an execution key.
type ValueTypeDescriptor struct {
	Type    ValueType
	Version uint16
	Flags   ValueTypeFlags
	Name    string
}

// Validate rejects ambiguous or unsupported descriptors. Core JSON types are
// fixed at version zero. Extension types must carry a non-empty name so logs
// and negotiation failures remain diagnosable without interpreting payloads.
func (d ValueTypeDescriptor) Validate() error {
	if d.Flags&^valueTypeKnownFlags != 0 {
		return fmt.Errorf("query: value type %#x has unknown flags %#x", d.Type, d.Flags&^valueTypeKnownFlags)
	}
	switch d.Type {
	case TypeAny:
		if d.Version != 0 || d.Name != "" {
			return fmt.Errorf("query: dynamic value type has version or name")
		}
		return nil
	case TypeNull, TypeBool, TypeNumber, TypeString, TypeJSON:
		if d.Version != 0 {
			return fmt.Errorf("query: core value type %#x has version %d", d.Type, d.Version)
		}
		return nil
	default:
		if d.Type < ValueTypeExtensionStart || d.Name == "" {
			return fmt.Errorf("query: unassigned value type %#x", d.Type)
		}
		return nil
	}
}

// IsExtension reports whether t belongs to the negotiated extension range.
func (t ValueType) IsExtension() bool { return t >= ValueTypeExtensionStart }

// AppendCoreValueTypes appends the built-in type registry. It allocates only
// when dst lacks capacity.
func AppendCoreValueTypes(dst []ValueTypeDescriptor) []ValueTypeDescriptor {
	return append(dst,
		ValueTypeDescriptor{Type: TypeAny},
		ValueTypeDescriptor{Type: TypeNull, Name: "null"},
		ValueTypeDescriptor{Type: TypeBool, Name: "bool"},
		ValueTypeDescriptor{Type: TypeNumber, Flags: ValueTypeVariableWidth, Name: "number"},
		ValueTypeDescriptor{Type: TypeString, Flags: ValueTypeVariableWidth, Name: "string"},
		ValueTypeDescriptor{
			Type: TypeJSON, Flags: ValueTypeVariableWidth | ValueTypeNested,
			Name: "json",
		},
	)
}
