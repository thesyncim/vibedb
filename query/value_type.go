package query

// ValueType identifies one current result-value kind.
type ValueType uint16

const (
	// TypeAny is a schema-level dynamic JSON value. A cell never has TypeAny;
	// its concrete type is carried per value.
	TypeAny ValueType = iota
	TypeNull
	TypeBool
	TypeNumber
	TypeString
	// TypeJSON is an array or object retained as exact JSON bytes.
	TypeJSON
)
