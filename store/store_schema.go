package store

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// SchemaType is a set of JSON types accepted by one collection constraint.
// SchemaInteger is the subset of numbers written without a fraction or
// exponent. Combine constants with | to describe a union such as a nullable
// string.
type SchemaType uint16

const (
	// SchemaNull admits the JSON null literal.
	SchemaNull SchemaType = 1 << iota
	// SchemaBool admits the JSON true and false literals.
	SchemaBool
	// SchemaNumber admits any JSON number, integer-valued or not.
	SchemaNumber
	// SchemaInteger admits only numbers written without a fraction or exponent;
	// it is a strict subset of SchemaNumber, not an independent type.
	SchemaInteger
	// SchemaString admits any JSON string.
	SchemaString
	// SchemaArray admits any JSON array.
	SchemaArray
	// SchemaObject admits any JSON object.
	SchemaObject

	schemaKnownTypes = SchemaNull | SchemaBool | SchemaNumber |
		SchemaInteger | SchemaString | SchemaArray | SchemaObject

	// SchemaAny accepts every JSON value. Integer is already a subset of
	// number, so the canonical all-types mask does not need both bits.
	SchemaAny = SchemaNull | SchemaBool | SchemaNumber |
		SchemaString | SchemaArray | SchemaObject
)

var (
	// ErrSchemaDefinition reports an invalid root type, field type, path,
	// or duplicate path while compiling a schema.
	ErrSchemaDefinition = errors.New("vibedb: invalid collection schema")
	// ErrSchemaViolation reports a valid JSON document that does not
	// satisfy its collection's optional compiled schema.
	ErrSchemaViolation = errors.New("vibedb: collection schema violation")
)

// SchemaField constrains one RFC 6901 path. Required distinguishes an
// absent path from a present null: accept null explicitly with SchemaNull.
// Paths may address nested object members or array elements.
type SchemaField struct {
	Path     string
	Types    SchemaType
	Required bool
}

// SchemaDefinition is the declarative input to CompileSchema. Root
// zero means SchemaAny. Fields constrain named paths but deliberately allow
// unspecified JSON fields, keeping evolving document payloads compatible.
type SchemaDefinition struct {
	Root   SchemaType
	Fields []SchemaField
}

type compiledStoreSchemaField struct {
	path     string
	pointer  vibejson.CompiledPointer
	types    SchemaType
	required bool
}

// Schema is an immutable compiled schema safe for concurrent validation and
// reuse by any number of Collection, Builder, or durable Collection instances.
// Validation walks the document's existing structural index and allocates
// nothing on success.
type Schema struct {
	root   SchemaType
	fields []compiledStoreSchemaField
	Hash   uint64
}

// SchemaViolationError identifies one failed constraint. Path is empty for a
// root-type mismatch. Missing distinguishes an absent required path from a
// present value of the wrong type.
type SchemaViolationError struct {
	Path     string
	Expected SchemaType
	Actual   document.Kind
	Missing  bool
}

// Error renders the offending path together with the expected type set and the
// value actually found, so a rejected Put explains itself without the caller
// re-deriving the constraint. It wraps ErrSchemaViolation for errors.Is.
func (e *SchemaViolationError) Error() string {
	if e == nil {
		return ErrSchemaViolation.Error()
	}
	if e.Missing {
		return fmt.Sprintf(
			"%v: required path %q is absent",
			ErrSchemaViolation, e.Path,
		)
	}
	path := e.Path
	if path == "" {
		path = "<root>"
	}
	return fmt.Sprintf(
		"%v: path %q has type %s, expected %s",
		ErrSchemaViolation, path, e.Actual, e.Expected,
	)
}

// Unwrap lets errors.Is classify every constraint failure uniformly.
func (e *SchemaViolationError) Unwrap() error {
	return ErrSchemaViolation
}

// CompileSchema validates and canonically compiles definition. Field
// order does not affect the resulting schema identity.
func CompileSchema(
	definition SchemaDefinition,
) (*Schema, error) {
	root := definition.Root
	if root == 0 {
		root = SchemaAny
	}
	if !validSchemaTypes(root) {
		return nil, fmt.Errorf(
			"%w: invalid root types %#x",
			ErrSchemaDefinition, uint16(root),
		)
	}
	root = canonicalSchemaTypes(root)
	fields := make([]compiledStoreSchemaField, len(definition.Fields))
	for i, field := range definition.Fields {
		if field.Path == "" {
			return nil, fmt.Errorf(
				"%w: field %d uses the root path; use Root",
				ErrSchemaDefinition, i,
			)
		}
		if !utf8.ValidString(field.Path) {
			return nil, fmt.Errorf(
				"%w: field %d path is not valid UTF-8",
				ErrSchemaDefinition, i,
			)
		}
		if !validSchemaTypes(field.Types) {
			return nil, fmt.Errorf(
				"%w: field %q has invalid types %#x",
				ErrSchemaDefinition, field.Path,
				uint16(field.Types),
			)
		}
		field.Types = canonicalSchemaTypes(field.Types)
		path := strings.Clone(field.Path)
		pointer, err := vibejson.CompilePointer(path)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: field %q: %v",
				ErrSchemaDefinition, path, err,
			)
		}
		fields[i] = compiledStoreSchemaField{
			path: path, pointer: pointer,
			types: field.Types, required: field.Required,
		}
	}
	slices.SortFunc(fields, func(a, b compiledStoreSchemaField) int {
		return strings.Compare(a.path, b.path)
	})
	for i := 1; i < len(fields); i++ {
		if fields[i-1].path == fields[i].path {
			return nil, fmt.Errorf(
				"%w: duplicate path %q",
				ErrSchemaDefinition, fields[i].path,
			)
		}
	}
	schema := &Schema{root: root, fields: fields}
	schema.Hash = schema.identityHash()
	return schema, nil
}

func validSchemaTypes(types SchemaType) bool {
	return types != 0 && types&^schemaKnownTypes == 0
}

func canonicalSchemaTypes(types SchemaType) SchemaType {
	if types&SchemaNumber != 0 {
		types &^= SchemaInteger
	}
	return types
}

// Valid reports whether s is a fully compiled schema whose identity hash still
// matches its canonicalized contents. A nil, zero, or tampered schema is
// rejected, so callers can treat Valid as the precondition for trusting a
// schema recovered from disk or handed in through Options.
func (s *Schema) Valid() bool {
	if s == nil || !validSchemaTypes(s.root) ||
		s.root != canonicalSchemaTypes(s.root) ||
		s.Hash == 0 || s.Hash != s.identityHash() {
		return false
	}
	for i, field := range s.fields {
		if field.path == "" || !validSchemaTypes(field.types) ||
			field.types != canonicalSchemaTypes(field.types) ||
			i != 0 && s.fields[i-1].path >= field.path {
			return false
		}
	}
	return true
}

// Definition returns an independent canonical declarative copy of s.
func (s *Schema) Definition() SchemaDefinition {
	if s == nil {
		return SchemaDefinition{}
	}
	definition := SchemaDefinition{
		Root:   s.root,
		Fields: make([]SchemaField, len(s.fields)),
	}
	for i, field := range s.fields {
		definition.Fields[i] = SchemaField{
			Path: field.path, Types: field.types,
			Required: field.required,
		}
	}
	return definition
}

// ValidateIndex checks an already-built document index. Success is
// allocation-free; failures return a typed SchemaViolationError.
func (s *Schema) ValidateIndex(index vibejson.Index) error {
	if s == nil {
		return nil
	}
	root := index.Root()
	if !schemaAcceptsNode(s.root, root) {
		return &SchemaViolationError{
			Expected: s.root, Actual: root.Kind(),
		}
	}
	for _, field := range s.fields {
		node, found, err := index.PointerCompiled(field.pointer)
		if err != nil {
			return fmt.Errorf(
				"%w: compiled path %q: %v",
				ErrSchemaDefinition, field.path, err,
			)
		}
		if !found {
			if field.required {
				return &SchemaViolationError{
					Path: field.path, Expected: field.types,
					Missing: true,
				}
			}
			continue
		}
		if !schemaAcceptsNode(field.types, node) {
			return &SchemaViolationError{
				Path: field.path, Expected: field.types,
				Actual: node.Kind(),
			}
		}
	}
	return nil
}

func schemaAcceptsNode(types SchemaType, node vibejson.Node) bool {
	kind := node.Kind()
	if kind == document.Number {
		return types&SchemaNumber != 0 ||
			types&SchemaInteger != 0 && node.IsInteger()
	}
	return schemaAcceptsKind(types, kind)
}

func schemaAcceptsKind(types SchemaType, kind document.Kind) bool {
	var want SchemaType
	switch kind {
	case document.Null:
		want = SchemaNull
	case document.Bool:
		want = SchemaBool
	case document.Number:
		want = SchemaNumber
	case document.String:
		want = SchemaString
	case document.Array:
		want = SchemaArray
	case document.Object:
		want = SchemaObject
	default:
		return false
	}
	return types&want != 0
}

func (s *Schema) identityHash() uint64 {
	hash := uint64(14695981039346656037)
	appendByte := func(value byte) {
		hash = (hash ^ uint64(value)) * 1099511628211
	}
	appendUint32 := func(value uint32) {
		appendByte(byte(value))
		appendByte(byte(value >> 8))
		appendByte(byte(value >> 16))
		appendByte(byte(value >> 24))
	}
	appendByte(0x53)
	appendByte(byte(s.root))
	appendByte(byte(s.root >> 8))
	appendUint32(uint32(len(s.fields)))
	for _, field := range s.fields {
		appendUint32(uint32(len(field.path)))
		for i := range field.path {
			appendByte(field.path[i])
		}
		appendByte(byte(field.types))
		appendByte(byte(field.types >> 8))
		if field.required {
			appendByte(1)
		} else {
			appendByte(0)
		}
	}
	if hash == 0 {
		// Zero is reserved as the durable "no catalog" identity.
		return 1
	}
	return hash
}

// String returns a stable human-readable union.
func (t SchemaType) String() string {
	if t == SchemaAny {
		return "any"
	}
	names := [...]struct {
		bit  SchemaType
		name string
	}{
		{SchemaNull, "null"},
		{SchemaBool, "bool"},
		{SchemaNumber, "number"},
		{SchemaInteger, "integer"},
		{SchemaString, "string"},
		{SchemaArray, "array"},
		{SchemaObject, "object"},
	}
	var builder strings.Builder
	for _, item := range names {
		if t&item.bit == 0 {
			continue
		}
		if builder.Len() != 0 {
			builder.WriteByte('|')
		}
		builder.WriteString(item.name)
	}
	if builder.Len() == 0 {
		return "invalid"
	}
	return builder.String()
}
