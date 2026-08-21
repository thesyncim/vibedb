package storeio

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	PageCatalogMaxIndexColumns    = 4
	PageCatalogMaxLogicalIndexes  = 4096
	PageCatalogMaxPhysicalIndexes = 64
	// PageCatalogMaxSkipIndexes bounds the compact min/max summaries carried by
	// every primary stripe. A small fixed ceiling keeps write amplification and
	// per-leaf metadata predictable; wider predicates can still use exact
	// indexes or residual scans.
	PageCatalogMaxSkipIndexes  = 8
	PageCatalogMaxSchemaFields = 4096
	// PageCatalogMaxUniqueStrings is the exact sum of every independently
	// addressable maximum: 4096 aliases, 64*4 physical paths, 8 skip paths, and
	// 4096 schema fields.
	PageCatalogMaxUniqueStrings  = 8456
	PageCatalogMaxStringBytes    = 1<<16 - 1
	PageCatalogMaxCanonicalBytes = 32 << 20
	PageCatalogDigestSize        = 16

	PageCatalogCanonicalHeaderSize      = 64
	PageCatalogSegmentPageSize          = uint32(4096)
	PageCatalogSegmentPayloadHeaderSize = 64
	PageCatalogSegmentDataCapacity      = int(PageCatalogSegmentPageSize) -
		PageHeaderSize - PageTrailerSize - PageCatalogSegmentPayloadHeaderSize

	pageCatalogCanonicalVersion = DevelopmentFormatVersion
	pageCatalogSegmentVersion   = DevelopmentFormatVersion
)

const (
	pageCatalogSchemaNull uint16 = 1 << iota
	pageCatalogSchemaBool
	pageCatalogSchemaNumber
	pageCatalogSchemaInteger
	pageCatalogSchemaString
	pageCatalogSchemaArray
	pageCatalogSchemaObject
)

const (
	PageCatalogSchemaNull    = pageCatalogSchemaNull
	PageCatalogSchemaBool    = pageCatalogSchemaBool
	PageCatalogSchemaNumber  = pageCatalogSchemaNumber
	PageCatalogSchemaInteger = pageCatalogSchemaInteger
	PageCatalogSchemaString  = pageCatalogSchemaString
	PageCatalogSchemaArray   = pageCatalogSchemaArray
	PageCatalogSchemaObject  = pageCatalogSchemaObject
	PageCatalogSchemaAny     = pageCatalogSchemaNull | pageCatalogSchemaBool |
		pageCatalogSchemaNumber | pageCatalogSchemaString |
		pageCatalogSchemaArray | pageCatalogSchemaObject

	pageCatalogSchemaKnown = PageCatalogSchemaAny | PageCatalogSchemaInteger
)

const (
	pageCatalogCanonicalSchema    = uint16(1)
	pageCatalogCanonicalSkipPaths = uint16(1 << 1)
)

var (
	pageCatalogCanonicalMagic = [8]byte{'S', 'J', 'C', 'A', 'T', 'C', '0', '0'}

	// ErrPageCatalogDefinition reports caller input that cannot have one
	// unambiguous durable meaning.
	ErrPageCatalogDefinition = errors.New("vibedb: invalid Store page catalog definition")
	// ErrPageCatalogCorrupt reports a checksum-valid catalog page or complete
	// chain that is malformed, non-canonical, out of bounds, or grafted from a
	// different Store image.
	ErrPageCatalogCorrupt = errors.New("vibedb: corrupt Store page catalog")
)

// PageCatalogIndex is one logical alias and its ordered physical exact-index
// columns. Several names with identical Paths share one physical definition.
type PageCatalogIndex struct {
	Name  string
	Paths []string
}

// PageCatalogSchemaField is one canonical RFC 6901 schema constraint.
type PageCatalogSchemaField struct {
	Path     string
	Types    uint16
	Required bool
}

// PageCatalogSchema is an optional schema. A nil schema means no schema;
// a non-nil zero value means an explicit SchemaAny root.
type PageCatalogSchema struct {
	Root   uint16
	Fields []PageCatalogSchemaField
}

// PageCatalogDefinition is the complete configuration needed to reopen a
// Store without caller-supplied index or schema options.
type PageCatalogDefinition struct {
	Indexes   []PageCatalogIndex
	SkipPaths []string
	Schema    *PageCatalogSchema
}

type pageCatalogPhysicalDefinition struct {
	paths []string
}

// CanonicalPageCatalog owns immutable canonical bytes and the corresponding
// value model. The byte image, not Digest, is the durable authority.
type CanonicalPageCatalog struct {
	canonical []byte
	digest    [PageCatalogDigestSize]byte
	indexes   []PageCatalogIndex
	physical  []pageCatalogPhysicalDefinition
	skipPaths []string
	schema    *PageCatalogSchema
}

// CanonicalSize returns the exact number of authoritative catalog bytes.
func (c *CanonicalPageCatalog) CanonicalSize() int {
	if c == nil {
		return 0
	}
	return len(c.canonical)
}

// Digest returns the truncated SHA-256 fast-reject identity carried by every
// segment. Readers still reconstruct and validate the exact canonical bytes.
func (c *CanonicalPageCatalog) Digest() [PageCatalogDigestSize]byte {
	if c == nil {
		return [PageCatalogDigestSize]byte{}
	}
	return c.digest
}

// AppendCanonical appends the authoritative bytes without exposing mutable
// catalog-owned storage.
func (c *CanonicalPageCatalog) AppendCanonical(dst []byte) []byte {
	if c == nil {
		return dst
	}
	return append(dst, c.canonical...)
}

// Equal reports exact canonical-byte equality. Digest equality is only a
// cheap rejection before this comparison.
func (c *CanonicalPageCatalog) Equal(other *CanonicalPageCatalog) bool {
	if c == nil || other == nil {
		return c == other
	}
	return c.digest == other.digest && bytes.Equal(c.canonical, other.canonical)
}

// Definition returns an independent canonical declarative copy.
func (c *CanonicalPageCatalog) Definition() PageCatalogDefinition {
	if c == nil {
		return PageCatalogDefinition{}
	}
	out := PageCatalogDefinition{
		Indexes:   clonePageCatalogIndexes(c.indexes),
		SkipPaths: slices.Clone(c.skipPaths),
	}
	if c.schema != nil {
		out.Schema = &PageCatalogSchema{
			Root:   c.schema.Root,
			Fields: slices.Clone(c.schema.Fields),
		}
	}
	return out
}

// PhysicalIndexCount is the number of distinct physical exact-index layouts
// after alias deduplication.
func (c *CanonicalPageCatalog) PhysicalIndexCount() int {
	if c == nil {
		return 0
	}
	return len(c.physical)
}

// PhysicalIndexes returns independent ordered path vectors in their canonical
// durable-ID order. Runtime index IDs must be compiled from this order rather
// than caller alias order, or a zero-option reopen could reinterpret postings.
func (c *CanonicalPageCatalog) PhysicalIndexes() [][]string {
	if c == nil {
		return nil
	}
	out := make([][]string, len(c.physical))
	for i, definition := range c.physical {
		out[i] = slices.Clone(definition.paths)
	}
	return out
}

// SegmentCount returns the deterministic number of minimum-quantum pages
// required for this catalog.
func (c *CanonicalPageCatalog) SegmentCount() int {
	count, _ := c.SegmentCountFor(PageCatalogSegmentPageSize)
	return count
}

// SegmentCountFor returns the deterministic number of allocation-quantum
// pages required for this catalog. The bool is false for an invalid Store page
// size. An empty catalog needs zero segments at every valid page size.
func (c *CanonicalPageCatalog) SegmentCountFor(pageSize uint32) (int, bool) {
	capacity, ok := pageCatalogSegmentDataCapacity(pageSize)
	if !ok {
		return 0, false
	}
	if c == nil || len(c.canonical) == 0 {
		return 0, true
	}
	return (len(c.canonical) + capacity - 1) / capacity, true
}

// BuildCanonicalPageCatalog validates, owns, orders, deduplicates, front-codes,
// and encodes one definition. Caller order and nil versus empty slices do not
// affect the result.
func BuildCanonicalPageCatalog(
	definition PageCatalogDefinition,
) (*CanonicalPageCatalog, error) {
	normalized, physical, err := normalizePageCatalogDefinition(definition)
	if err != nil {
		return nil, err
	}
	if len(normalized.Indexes) == 0 && len(normalized.SkipPaths) == 0 &&
		normalized.Schema == nil {
		return &CanonicalPageCatalog{}, nil
	}
	stringsTable, stringIDs, err := pageCatalogStrings(normalized, physical)
	if err != nil {
		return nil, err
	}
	stringBytes := 0
	previous := ""
	for _, value := range stringsTable {
		prefix := pageCatalogCommonPrefix(previous, value)
		stringBytes += 4 + len(value) - prefix
		previous = value
	}
	physicalBytes := 0
	for _, definition := range physical {
		physicalBytes += 2 + 2*len(definition.paths)
	}
	total64 := uint64(PageCatalogCanonicalHeaderSize) +
		uint64(stringBytes) + uint64(physicalBytes) +
		uint64(len(normalized.Indexes))*4 +
		uint64(len(normalized.SkipPaths))*2
	if normalized.Schema != nil {
		total64 += uint64(len(normalized.Schema.Fields)) * 6
	}
	if total64 > PageCatalogMaxCanonicalBytes {
		return nil, fmt.Errorf(
			"%w: canonical image has %d bytes, maximum is %d",
			ErrPageCatalogDefinition, total64, PageCatalogMaxCanonicalBytes,
		)
	}
	canonical := make([]byte, int(total64))
	copy(canonical[0:8], pageCatalogCanonicalMagic[:])
	binary.LittleEndian.PutUint32(canonical[8:12], pageCatalogCanonicalVersion)
	binary.LittleEndian.PutUint16(canonical[12:14], PageCatalogCanonicalHeaderSize)
	flags := uint16(0)
	schemaFields := 0
	schemaRoot := uint16(0)
	if normalized.Schema != nil {
		flags |= pageCatalogCanonicalSchema
		schemaFields = len(normalized.Schema.Fields)
		schemaRoot = normalized.Schema.Root
	}
	if len(normalized.SkipPaths) != 0 {
		flags |= pageCatalogCanonicalSkipPaths
	}
	binary.LittleEndian.PutUint16(canonical[14:16], flags)
	binary.LittleEndian.PutUint32(canonical[16:20], uint32(len(canonical)))
	binary.LittleEndian.PutUint32(canonical[20:24], uint32(len(stringsTable)))
	binary.LittleEndian.PutUint32(canonical[24:28], uint32(len(physical)))
	binary.LittleEndian.PutUint32(canonical[28:32], uint32(len(normalized.Indexes)))
	binary.LittleEndian.PutUint32(canonical[32:36], uint32(schemaFields))
	binary.LittleEndian.PutUint32(canonical[36:40], uint32(stringBytes))
	binary.LittleEndian.PutUint32(canonical[40:44], uint32(physicalBytes))
	binary.LittleEndian.PutUint16(canonical[44:46], schemaRoot)
	binary.LittleEndian.PutUint32(canonical[46:50], uint32(len(normalized.SkipPaths)))

	cursor := PageCatalogCanonicalHeaderSize
	previous = ""
	for _, value := range stringsTable {
		prefix := pageCatalogCommonPrefix(previous, value)
		suffix := value[prefix:]
		binary.LittleEndian.PutUint16(canonical[cursor:cursor+2], uint16(prefix))
		binary.LittleEndian.PutUint16(canonical[cursor+2:cursor+4], uint16(len(suffix)))
		copy(canonical[cursor+4:], suffix)
		cursor += 4 + len(suffix)
		previous = value
	}
	for _, definition := range physical {
		canonical[cursor] = byte(len(definition.paths))
		cursor += 2
		for _, path := range definition.paths {
			binary.LittleEndian.PutUint16(canonical[cursor:cursor+2], stringIDs[path])
			cursor += 2
		}
	}
	for _, index := range normalized.Indexes {
		physicalID := pageCatalogPhysicalIndex(physical, index.Paths)
		binary.LittleEndian.PutUint16(canonical[cursor:cursor+2], stringIDs[index.Name])
		canonical[cursor+2] = byte(physicalID)
		cursor += 4
	}
	for _, path := range normalized.SkipPaths {
		binary.LittleEndian.PutUint16(canonical[cursor:cursor+2], stringIDs[path])
		cursor += 2
	}
	if normalized.Schema != nil {
		for _, field := range normalized.Schema.Fields {
			binary.LittleEndian.PutUint16(canonical[cursor:cursor+2], stringIDs[field.Path])
			binary.LittleEndian.PutUint16(canonical[cursor+2:cursor+4], field.Types)
			if field.Required {
				canonical[cursor+4] = 1
			}
			cursor += 6
		}
	}
	if cursor != len(canonical) {
		panic("page catalog size calculation disagrees with encoder")
	}
	return &CanonicalPageCatalog{
		canonical: canonical,
		digest:    pageCatalogDigest(canonical),
		indexes:   clonePageCatalogIndexes(normalized.Indexes),
		physical:  clonePageCatalogPhysical(physical),
		skipPaths: slices.Clone(normalized.SkipPaths),
		schema:    clonePageCatalogSchema(normalized.Schema),
	}, nil
}

// OpenCanonicalPageCatalog rejects any non-canonical byte spelling, even when
// it describes the same logical configuration.
func OpenCanonicalPageCatalog(src []byte) (*CanonicalPageCatalog, error) {
	if len(src) == 0 {
		return &CanonicalPageCatalog{}, nil
	}
	return openOwnedCanonicalPageCatalog(slices.Clone(src))
}

// openOwnedCanonicalPageCatalog validates and adopts canonical. Callers must
// not retain another mutable alias. The decoder checks every canonical
// ordering, identifier, reserved-byte, and section-tiling rule directly, so an
// already-owned image does not need a second encoded rebuild merely to compare
// it with itself.
func openOwnedCanonicalPageCatalog(
	canonical []byte,
) (*CanonicalPageCatalog, error) {
	if len(canonical) == 0 {
		return &CanonicalPageCatalog{}, nil
	}
	definition, physical, err := decodeCanonicalPageCatalog(canonical)
	if err != nil {
		return nil, err
	}
	return &CanonicalPageCatalog{
		canonical: canonical,
		digest:    pageCatalogDigest(canonical),
		indexes:   definition.Indexes,
		physical:  physical,
		skipPaths: definition.SkipPaths,
		schema:    definition.Schema,
	}, nil
}

func decodeCanonicalPageCatalog(
	src []byte,
) (PageCatalogDefinition, []pageCatalogPhysicalDefinition, error) {
	definition, err := decodeCanonicalPageCatalogDefinition(src)
	if err != nil {
		return PageCatalogDefinition{}, nil, err
	}
	physical := make(
		[]pageCatalogPhysicalDefinition, len(definition.Indexes),
	)
	for i, index := range definition.Indexes {
		physical[i] = pageCatalogPhysicalDefinition{paths: index.Paths}
	}
	slices.SortFunc(physical, comparePageCatalogPhysical)
	physical = slices.CompactFunc(
		physical,
		func(a, b pageCatalogPhysicalDefinition) bool {
			return comparePageCatalogPhysical(a, b) == 0
		},
	)
	return definition, physical, nil
}

func normalizePageCatalogDefinition(
	definition PageCatalogDefinition,
) (PageCatalogDefinition, []pageCatalogPhysicalDefinition, error) {
	if len(definition.Indexes) > PageCatalogMaxLogicalIndexes {
		return PageCatalogDefinition{}, nil, fmt.Errorf(
			"%w: catalog count exceeds format limit", ErrPageCatalogDefinition,
		)
	}
	indexes := make([]PageCatalogIndex, len(definition.Indexes))
	for i, index := range definition.Indexes {
		if err := pageCatalogString(index.Name, false); err != nil {
			return PageCatalogDefinition{}, nil, fmt.Errorf(
				"%w: index %d name: %v", ErrPageCatalogDefinition, i, err,
			)
		}
		if len(index.Paths) == 0 || len(index.Paths) > PageCatalogMaxIndexColumns {
			return PageCatalogDefinition{}, nil, fmt.Errorf(
				"%w: index %q path count", ErrPageCatalogDefinition, index.Name,
			)
		}
		indexes[i] = PageCatalogIndex{
			Name:  strings.Clone(index.Name),
			Paths: make([]string, len(index.Paths)),
		}
		for pathIndex, path := range index.Paths {
			if err := pageCatalogPointer(path, true); err != nil {
				return PageCatalogDefinition{}, nil, fmt.Errorf(
					"%w: index %q path %d: %v",
					ErrPageCatalogDefinition, index.Name, pathIndex, err,
				)
			}
			indexes[i].Paths[pathIndex] = strings.Clone(path)
		}
	}
	slices.SortFunc(indexes, func(a, b PageCatalogIndex) int {
		return strings.Compare(a.Name, b.Name)
	})
	for i := 1; i < len(indexes); i++ {
		if indexes[i-1].Name == indexes[i].Name {
			return PageCatalogDefinition{}, nil, fmt.Errorf(
				"%w: duplicate index alias %q",
				ErrPageCatalogDefinition, indexes[i].Name,
			)
		}
	}

	physical := make([]pageCatalogPhysicalDefinition, len(indexes))
	for i, index := range indexes {
		physical[i] = pageCatalogPhysicalDefinition{paths: slices.Clone(index.Paths)}
	}
	slices.SortFunc(physical, comparePageCatalogPhysical)
	physical = slices.CompactFunc(physical, func(a, b pageCatalogPhysicalDefinition) bool {
		return comparePageCatalogPhysical(a, b) == 0
	})
	skipPaths, err := normalizePageCatalogSkipPaths(definition.SkipPaths)
	if err != nil {
		return PageCatalogDefinition{}, nil, err
	}
	if len(physical) > PageCatalogMaxPhysicalIndexes {
		return PageCatalogDefinition{}, nil, fmt.Errorf(
			"%w: %d physical indexes, maximum is %d",
			ErrPageCatalogDefinition, len(physical), PageCatalogMaxPhysicalIndexes,
		)
	}

	schema, err := normalizePageCatalogSchema(definition.Schema)
	if err != nil {
		return PageCatalogDefinition{}, nil, err
	}
	return PageCatalogDefinition{
		Indexes: indexes, SkipPaths: skipPaths, Schema: schema,
	}, physical, nil
}

func normalizePageCatalogSkipPaths(paths []string) ([]string, error) {
	if len(paths) > PageCatalogMaxSkipIndexes {
		return nil, fmt.Errorf(
			"%w: %d skip paths, maximum is %d",
			ErrPageCatalogDefinition, len(paths), PageCatalogMaxSkipIndexes,
		)
	}
	out := make([]string, len(paths))
	for i, path := range paths {
		if err := pageCatalogPointer(path, true); err != nil {
			return nil, fmt.Errorf(
				"%w: skip path %d: %v", ErrPageCatalogDefinition, i, err,
			)
		}
		out[i] = strings.Clone(path)
	}
	slices.Sort(out)
	for i := 1; i < len(out); i++ {
		if out[i-1] == out[i] {
			return nil, fmt.Errorf(
				"%w: duplicate skip path %q", ErrPageCatalogDefinition, out[i],
			)
		}
	}
	return out, nil
}

func normalizePageCatalogSchema(schema *PageCatalogSchema) (*PageCatalogSchema, error) {
	if schema == nil {
		return nil, nil
	}
	if len(schema.Fields) > PageCatalogMaxSchemaFields {
		return nil, fmt.Errorf(
			"%w: schema has too many fields", ErrPageCatalogDefinition,
		)
	}
	root := schema.Root
	if root == 0 {
		root = PageCatalogSchemaAny
	}
	if !validPageCatalogSchemaTypes(root) {
		return nil, fmt.Errorf(
			"%w: schema root types %#x", ErrPageCatalogDefinition, root,
		)
	}
	root = canonicalPageCatalogSchemaTypes(root)
	fields := make([]PageCatalogSchemaField, len(schema.Fields))
	for i, field := range schema.Fields {
		if err := pageCatalogPointer(field.Path, false); err != nil {
			return nil, fmt.Errorf(
				"%w: schema field %d: %v", ErrPageCatalogDefinition, i, err,
			)
		}
		if !validPageCatalogSchemaTypes(field.Types) {
			return nil, fmt.Errorf(
				"%w: schema field %q types %#x",
				ErrPageCatalogDefinition, field.Path, field.Types,
			)
		}
		fields[i] = PageCatalogSchemaField{
			Path:     strings.Clone(field.Path),
			Types:    canonicalPageCatalogSchemaTypes(field.Types),
			Required: field.Required,
		}
	}
	slices.SortFunc(fields, func(a, b PageCatalogSchemaField) int {
		return strings.Compare(a.Path, b.Path)
	})
	for i := 1; i < len(fields); i++ {
		if fields[i-1].Path == fields[i].Path {
			return nil, fmt.Errorf(
				"%w: duplicate schema path %q",
				ErrPageCatalogDefinition, fields[i].Path,
			)
		}
	}
	return &PageCatalogSchema{Root: root, Fields: fields}, nil
}

func pageCatalogStrings(
	definition PageCatalogDefinition,
	physical []pageCatalogPhysicalDefinition,
) ([]string, map[string]uint16, error) {
	unique := make(map[string]struct{})
	for _, index := range definition.Indexes {
		unique[index.Name] = struct{}{}
	}
	for _, definition := range physical {
		for _, path := range definition.paths {
			unique[path] = struct{}{}
		}
	}
	for _, path := range definition.SkipPaths {
		unique[path] = struct{}{}
	}
	if definition.Schema != nil {
		for _, field := range definition.Schema.Fields {
			unique[field.Path] = struct{}{}
		}
	}
	if len(unique) > PageCatalogMaxUniqueStrings {
		return nil, nil, fmt.Errorf(
			"%w: %d unique strings, maximum is %d",
			ErrPageCatalogDefinition, len(unique), PageCatalogMaxUniqueStrings,
		)
	}
	values := make([]string, 0, len(unique))
	for value := range unique {
		values = append(values, value)
	}
	slices.Sort(values)
	ids := make(map[string]uint16, len(values))
	for i, value := range values {
		ids[value] = uint16(i)
	}
	return values, ids, nil
}

func decodeCanonicalPageCatalogDefinition(
	src []byte,
) (PageCatalogDefinition, error) {
	if len(src) < PageCatalogCanonicalHeaderSize ||
		!bytes.Equal(src[0:8], pageCatalogCanonicalMagic[:]) ||
		binary.LittleEndian.Uint32(src[8:12]) != pageCatalogCanonicalVersion ||
		binary.LittleEndian.Uint16(src[12:14]) != PageCatalogCanonicalHeaderSize ||
		!allZero(src[50:PageCatalogCanonicalHeaderSize]) {
		return PageCatalogDefinition{}, fmt.Errorf(
			"%w: canonical header", ErrPageCatalogCorrupt,
		)
	}
	flags := binary.LittleEndian.Uint16(src[14:16])
	total := binary.LittleEndian.Uint32(src[16:20])
	stringCount := binary.LittleEndian.Uint32(src[20:24])
	physicalCount := binary.LittleEndian.Uint32(src[24:28])
	aliasCount := binary.LittleEndian.Uint32(src[28:32])
	fieldCount := binary.LittleEndian.Uint32(src[32:36])
	stringBytes := binary.LittleEndian.Uint32(src[36:40])
	physicalBytes := binary.LittleEndian.Uint32(src[40:44])
	root := binary.LittleEndian.Uint16(src[44:46])
	skipCount := binary.LittleEndian.Uint32(src[46:50])
	if flags&^(pageCatalogCanonicalSchema|pageCatalogCanonicalSkipPaths) != 0 ||
		total != uint32(len(src)) || len(src) > PageCatalogMaxCanonicalBytes ||
		stringCount > PageCatalogMaxUniqueStrings ||
		aliasCount > PageCatalogMaxLogicalIndexes ||
		physicalCount > PageCatalogMaxPhysicalIndexes ||
		fieldCount > PageCatalogMaxSchemaFields ||
		skipCount > PageCatalogMaxSkipIndexes ||
		(flags&pageCatalogCanonicalSkipPaths == 0) != (skipCount == 0) ||
		flags&pageCatalogCanonicalSchema == 0 && (root != 0 || fieldCount != 0) ||
		flags&pageCatalogCanonicalSchema != 0 &&
			(!validPageCatalogSchemaTypes(root) ||
				canonicalPageCatalogSchemaTypes(root) != root) ||
		uint64(stringCount)*4 > uint64(stringBytes) {
		return PageCatalogDefinition{}, fmt.Errorf(
			"%w: canonical counts or flags", ErrPageCatalogCorrupt,
		)
	}
	sections64 := uint64(PageCatalogCanonicalHeaderSize) +
		uint64(stringBytes) + uint64(physicalBytes) +
		uint64(aliasCount)*4 + uint64(skipCount)*2 + uint64(fieldCount)*6
	if sections64 != uint64(len(src)) {
		return PageCatalogDefinition{}, fmt.Errorf(
			"%w: canonical section bounds", ErrPageCatalogCorrupt,
		)
	}

	cursor := PageCatalogCanonicalHeaderSize
	stringEnd := cursor + int(stringBytes)
	values := make([]string, int(stringCount))
	previous := ""
	for i := range values {
		if cursor > stringEnd-4 {
			return PageCatalogDefinition{}, fmt.Errorf(
				"%w: front-coded string header", ErrPageCatalogCorrupt,
			)
		}
		prefix := int(binary.LittleEndian.Uint16(src[cursor : cursor+2]))
		suffix := int(binary.LittleEndian.Uint16(src[cursor+2 : cursor+4]))
		cursor += 4
		if prefix > len(previous) || suffix > stringEnd-cursor ||
			prefix+suffix > PageCatalogMaxStringBytes {
			return PageCatalogDefinition{}, fmt.Errorf(
				"%w: front-coded string bounds", ErrPageCatalogCorrupt,
			)
		}
		valueBytes := make([]byte, prefix+suffix)
		copy(valueBytes, previous[:prefix])
		copy(valueBytes[prefix:], src[cursor:cursor+suffix])
		value := string(valueBytes)
		if !utf8.ValidString(value) ||
			i != 0 && value <= previous ||
			prefix != pageCatalogCommonPrefix(previous, value) {
			return PageCatalogDefinition{}, fmt.Errorf(
				"%w: front-coded string order", ErrPageCatalogCorrupt,
			)
		}
		values[i] = value
		previous = value
		cursor += suffix
	}
	if cursor != stringEnd {
		return PageCatalogDefinition{}, fmt.Errorf(
			"%w: front-coded string tail", ErrPageCatalogCorrupt,
		)
	}

	physicalEnd := cursor + int(physicalBytes)
	physical := make([]pageCatalogPhysicalDefinition, int(physicalCount))
	usedStrings := make([]bool, len(values))
	for i := range physical {
		if cursor > physicalEnd-2 {
			return PageCatalogDefinition{}, fmt.Errorf(
				"%w: physical index header", ErrPageCatalogCorrupt,
			)
		}
		count := int(src[cursor])
		if count < 1 || count > PageCatalogMaxIndexColumns ||
			src[cursor+1] != 0 ||
			count > (physicalEnd-cursor-2)/2 {
			return PageCatalogDefinition{}, fmt.Errorf(
				"%w: physical index bounds", ErrPageCatalogCorrupt,
			)
		}
		cursor += 2
		paths := make([]string, count)
		for pathIndex := range paths {
			id := binary.LittleEndian.Uint16(src[cursor : cursor+2])
			cursor += 2
			if int(id) >= len(values) ||
				pageCatalogPointer(values[id], true) != nil {
				return PageCatalogDefinition{}, fmt.Errorf(
					"%w: physical index string", ErrPageCatalogCorrupt,
				)
			}
			paths[pathIndex] = values[id]
			usedStrings[id] = true
		}
		physical[i] = pageCatalogPhysicalDefinition{paths: paths}
		if i != 0 && comparePageCatalogPhysical(physical[i-1], physical[i]) >= 0 {
			return PageCatalogDefinition{}, fmt.Errorf(
				"%w: physical index order", ErrPageCatalogCorrupt,
			)
		}
	}
	if cursor != physicalEnd {
		return PageCatalogDefinition{}, fmt.Errorf(
			"%w: physical index tail", ErrPageCatalogCorrupt,
		)
	}

	indexes := make([]PageCatalogIndex, int(aliasCount))
	usedPhysical := make([]bool, len(physical))
	previousName := ""
	for i := range indexes {
		nameID := binary.LittleEndian.Uint16(src[cursor : cursor+2])
		physicalID := src[cursor+2]
		recordFlags := src[cursor+3]
		cursor += 4
		if int(nameID) >= len(values) ||
			int(physicalID) >= len(physical) || recordFlags != 0 ||
			values[nameID] == "" || i != 0 && values[nameID] <= previousName {
			return PageCatalogDefinition{}, fmt.Errorf(
				"%w: logical index alias", ErrPageCatalogCorrupt,
			)
		}
		indexes[i] = PageCatalogIndex{
			Name:  values[nameID],
			Paths: slices.Clone(physical[physicalID].paths),
		}
		previousName = values[nameID]
		usedStrings[nameID] = true
		usedPhysical[physicalID] = true
	}
	for _, used := range usedPhysical {
		if !used {
			return PageCatalogDefinition{}, fmt.Errorf(
				"%w: unreferenced physical index", ErrPageCatalogCorrupt,
			)
		}
	}

	skipPaths := make([]string, int(skipCount))
	previous = ""
	for i := range skipPaths {
		id := binary.LittleEndian.Uint16(src[cursor : cursor+2])
		cursor += 2
		if int(id) >= len(values) ||
			pageCatalogPointer(values[id], true) != nil ||
			i != 0 && values[id] <= previous {
			return PageCatalogDefinition{}, fmt.Errorf(
				"%w: skip path", ErrPageCatalogCorrupt,
			)
		}
		skipPaths[i] = values[id]
		previous = values[id]
		usedStrings[id] = true
	}

	var schema *PageCatalogSchema
	if flags&pageCatalogCanonicalSchema != 0 {
		schema = &PageCatalogSchema{
			Root: root, Fields: make([]PageCatalogSchemaField, int(fieldCount)),
		}
		previous = ""
		for i := range schema.Fields {
			id := binary.LittleEndian.Uint16(src[cursor : cursor+2])
			types := binary.LittleEndian.Uint16(src[cursor+2 : cursor+4])
			required := src[cursor+4]
			reserved := src[cursor+5]
			cursor += 6
			if int(id) >= len(values) ||
				pageCatalogPointer(values[id], false) != nil ||
				!validPageCatalogSchemaTypes(types) ||
				canonicalPageCatalogSchemaTypes(types) != types ||
				required > 1 || reserved != 0 ||
				i != 0 && values[id] <= previous {
				return PageCatalogDefinition{}, fmt.Errorf(
					"%w: schema field", ErrPageCatalogCorrupt,
				)
			}
			schema.Fields[i] = PageCatalogSchemaField{
				Path: values[id], Types: types, Required: required != 0,
			}
			previous = values[id]
			usedStrings[id] = true
		}
	}
	if cursor != len(src) {
		return PageCatalogDefinition{}, fmt.Errorf(
			"%w: canonical tail", ErrPageCatalogCorrupt,
		)
	}
	for _, used := range usedStrings {
		if !used {
			return PageCatalogDefinition{}, fmt.Errorf(
				"%w: unreferenced string", ErrPageCatalogCorrupt,
			)
		}
	}
	return PageCatalogDefinition{
		Indexes: indexes, SkipPaths: skipPaths, Schema: schema,
	}, nil
}

func pageCatalogString(value string, allowEmpty bool) error {
	if !allowEmpty && value == "" {
		return errors.New("empty string")
	}
	if len(value) > PageCatalogMaxStringBytes {
		return fmt.Errorf("string has %d bytes, maximum is %d", len(value), PageCatalogMaxStringBytes)
	}
	if !utf8.ValidString(value) {
		return errors.New("string is not valid UTF-8")
	}
	return nil
}

func pageCatalogPointer(pointer string, allowRoot bool) error {
	if err := pageCatalogString(pointer, allowRoot); err != nil {
		return err
	}
	if pointer == "" {
		if allowRoot {
			return nil
		}
		return errors.New("schema field uses the root path")
	}
	if pointer[0] != '/' {
		return errors.New("pointer must be empty or start with slash")
	}
	for i := 1; i < len(pointer); i++ {
		if pointer[i] != '~' {
			continue
		}
		if i+1 >= len(pointer) {
			return errors.New("pointer has a dangling tilde escape")
		}
		if pointer[i+1] != '0' && pointer[i+1] != '1' {
			return errors.New("pointer has an unknown tilde escape")
		}
		i++
	}
	return nil
}

func validPageCatalogSchemaTypes(types uint16) bool {
	return types != 0 && types&^pageCatalogSchemaKnown == 0
}

func canonicalPageCatalogSchemaTypes(types uint16) uint16 {
	if types&PageCatalogSchemaNumber != 0 {
		types &^= PageCatalogSchemaInteger
	}
	return types
}

func pageCatalogCommonPrefix(a, b string) int {
	limit := min(len(a), len(b))
	index := 0
	for index < limit && a[index] == b[index] {
		index++
	}
	return index
}

func pageCatalogDigest(src []byte) [PageCatalogDigestSize]byte {
	full := sha256.Sum256(src)
	var digest [PageCatalogDigestSize]byte
	copy(digest[:], full[:PageCatalogDigestSize])
	return digest
}

func comparePageCatalogPhysical(a, b pageCatalogPhysicalDefinition) int {
	limit := min(len(a.paths), len(b.paths))
	for i := range limit {
		if order := strings.Compare(a.paths[i], b.paths[i]); order != 0 {
			return order
		}
	}
	return len(a.paths) - len(b.paths)
}

func pageCatalogPhysicalIndex(
	physical []pageCatalogPhysicalDefinition, paths []string,
) int {
	target := pageCatalogPhysicalDefinition{paths: paths}
	return slices.IndexFunc(physical, func(candidate pageCatalogPhysicalDefinition) bool {
		return comparePageCatalogPhysical(candidate, target) == 0
	})
}

func clonePageCatalogIndexes(indexes []PageCatalogIndex) []PageCatalogIndex {
	out := make([]PageCatalogIndex, len(indexes))
	for i, index := range indexes {
		out[i] = PageCatalogIndex{
			Name: index.Name, Paths: slices.Clone(index.Paths),
		}
	}
	return out
}

func clonePageCatalogPhysical(
	physical []pageCatalogPhysicalDefinition,
) []pageCatalogPhysicalDefinition {
	out := make([]pageCatalogPhysicalDefinition, len(physical))
	for i, definition := range physical {
		out[i] = pageCatalogPhysicalDefinition{paths: slices.Clone(definition.paths)}
	}
	return out
}

func clonePageCatalogSchema(schema *PageCatalogSchema) *PageCatalogSchema {
	if schema == nil {
		return nil
	}
	return &PageCatalogSchema{
		Root: schema.Root, Fields: slices.Clone(schema.Fields),
	}
}
