package distribution

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// ErrDocumentPoint reports a document that cannot produce the complete,
// scalar placement tuple required by a native distribution.
var ErrDocumentPoint = errors.New("distribution: document placement point")

// DocumentPointProgram is an immutable, compiled vibejson placement program.
// It keeps JSON-pointer parsing and mapper construction out of row scans.
type DocumentPointProgram struct {
	mapper   *NativeMapper
	pointers []vibejson.CompiledPointer
	digest   [sha256.Size]byte
}

var documentPointProgramDomain = []byte("vibedb/document-point-program\x00")

// DocumentPointWorkspace owns reusable structural-index and decoded-string
// storage. One workspace may be reused serially; it must not be shared by
// concurrent calls.
type DocumentPointWorkspace struct {
	entries []vibejson.IndexEntry
	text    []byte
	scalars [KeyspaceWidth]Scalar
}

// CompileDocumentPointProgram compiles one complete ordered placement tuple.
// columns are canonical JSON pointers and are retained only through their
// compiled representation.
func CompileDocumentPointProgram(
	columns []string,
	bucketBits uint8,
) (*DocumentPointProgram, error) {
	if len(columns) == 0 || len(columns) > KeyspaceWidth ||
		!ValidVirtualBucketBits(bucketBits) {
		return nil, fmt.Errorf("%w: invalid placement geometry", ErrDocumentPoint)
	}
	pointers := make([]vibejson.CompiledPointer, len(columns))
	for ordinal, column := range columns {
		pointer, err := vibejson.CompilePointer(column)
		if err != nil {
			return nil, fmt.Errorf("%w: column %d: %v", ErrDocumentPoint, ordinal, err)
		}
		pointers[ordinal] = pointer
	}
	h := sha256.New()
	_, _ = h.Write(documentPointProgramDomain)
	var fixed [8]byte
	binary.LittleEndian.PutUint32(fixed[0:4], uint32(NativeMapperVersion))
	fixed[4], fixed[5] = bucketBits, byte(len(columns))
	_, _ = h.Write(fixed[:])
	for _, column := range columns {
		binary.LittleEndian.PutUint64(fixed[:], uint64(len(column)))
		_, _ = h.Write(fixed[:])
		_, _ = h.Write([]byte(column))
	}
	program := &DocumentPointProgram{
		mapper: NewNativeMapperWithBucketBits(len(columns), bucketBits), pointers: pointers,
	}
	_ = h.Sum(program.digest[:0])
	return program, nil
}

// Digest returns the immutable mapper, bucket geometry, and ordered compiled
// pointer identity. Artifact builders bind it so the same topology cannot be
// populated with rows produced by a different placement program.
func (p *DocumentPointProgram) Digest() [sha256.Size]byte {
	if p == nil {
		return [sha256.Size]byte{}
	}
	return p.digest
}

// Arity returns the number of scalar components read from every document.
func (p *DocumentPointProgram) Arity() int {
	if p == nil {
		return 0
	}
	return len(p.pointers)
}

// Point parses document once with vibejson, extracts every compiled scalar,
// and returns the native virtual-bucket point. A warmed workspace performs no
// allocation for documents that fit its retained structural/text capacity.
func (p *DocumentPointProgram) Point(
	document []byte,
	workspace *DocumentPointWorkspace,
) (KeyspacePoint, error) {
	if p == nil || p.mapper == nil || workspace == nil || len(p.pointers) == 0 {
		return KeyspacePoint{}, fmt.Errorf("%w: nil program or workspace", ErrDocumentPoint)
	}
	needed, err := vibejson.RequiredIndexEntries(document)
	if err != nil {
		return KeyspacePoint{}, fmt.Errorf("%w: invalid JSON: %v", ErrDocumentPoint, err)
	}
	if cap(workspace.entries) < needed {
		workspace.entries = make([]vibejson.IndexEntry, needed)
	} else {
		workspace.entries = workspace.entries[:needed]
	}
	index, err := vibejson.BuildIndex(document, workspace.entries)
	if err != nil {
		return KeyspacePoint{}, fmt.Errorf("%w: invalid JSON: %v", ErrDocumentPoint, err)
	}
	root := index.Root()
	workspace.text = workspace.text[:0]
	scalars := workspace.scalars[:len(p.pointers)]
	for ordinal, pointer := range p.pointers {
		node, found, pointerErr := root.PointerCompiled(pointer)
		if pointerErr != nil {
			return KeyspacePoint{}, fmt.Errorf(
				"%w: column %d: %v", ErrDocumentPoint, ordinal, pointerErr,
			)
		}
		if !found {
			return KeyspacePoint{}, fmt.Errorf(
				"%w: column %d is missing", ErrDocumentPoint, ordinal,
			)
		}
		value := node.Raw()
		switch value.Kind() {
		case jsondoc.String:
			if text, ok := value.StringBytes(); ok {
				scalars[ordinal] = NewString(byteview.String(text))
				continue
			}
			if cap(workspace.text) < len(document) {
				workspace.text = make([]byte, 0, len(document))
			}
			start := len(workspace.text)
			var ok bool
			workspace.text, ok, err = value.AppendText(workspace.text)
			if err != nil || !ok {
				return KeyspacePoint{}, fmt.Errorf(
					"%w: column %d has an invalid string", ErrDocumentPoint, ordinal,
				)
			}
			scalars[ordinal] = NewString(byteview.String(workspace.text[start:]))
		case jsondoc.Number:
			number, ok := value.NumberBytes()
			if !ok {
				return KeyspacePoint{}, fmt.Errorf(
					"%w: column %d has an invalid number", ErrDocumentPoint, ordinal,
				)
			}
			scalar, numberErr := NewNumber(byteview.String(number))
			if numberErr != nil {
				return KeyspacePoint{}, fmt.Errorf(
					"%w: column %d: %v", ErrDocumentPoint, ordinal, numberErr,
				)
			}
			scalars[ordinal] = scalar
		default:
			return KeyspacePoint{}, fmt.Errorf(
				"%w: column %d is not a string or number", ErrDocumentPoint, ordinal,
			)
		}
	}
	return p.mapper.PointFor(scalars)
}
