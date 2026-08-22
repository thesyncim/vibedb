package storeio

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestStateRootOpaqueValuesRoundTripAndFailClosed(t *testing.T) {
	root, fileEnd := testStateRoot(11)
	root.Options |= StateOptionOpaqueValues
	payload := make([]byte, StateRootPayloadSize)
	encoded, err := encodeTestStateRootPayload(payload, root, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeTestStateRootPayload(encoded, root, fileEnd)
	if err != nil || decoded != root {
		t.Fatalf("opaque state root = (%+v,%v), want (%+v,nil)",
			decoded, err, root)
	}

	for name, mutate := range map[string]func(*StateRoot){
		"schema": func(root *StateRoot) {
			root.Options |= StateOptionSchema
		},
		"skip index": func(root *StateRoot) {
			root.Options |= StateOptionSkipIndexes
		},
		"exact index depth": func(root *StateRoot) {
			root.IndexMaxDepth = 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := root
			mutate(&invalid)
			if _, err := encodeTestStateRootPayload(
				make([]byte, StateRootPayloadSize), invalid, fileEnd,
			); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("opaque conflict = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}

	// Model a checksummed envelope that was resealed after flipping the semantic
	// option bits. The payload decoder must reject the incompatible mode instead
	// of reopening arbitrary bytes under JSON catalog semantics.
	corrupt := append([]byte(nil), encoded...)
	binary.LittleEndian.PutUint32(
		corrupt[4:8], root.Options|StateOptionSchema,
	)
	if _, err := decodeTestStateRootPayload(
		corrupt, root, fileEnd,
	); !errors.Is(err, ErrStateRootCorrupt) {
		t.Fatalf("resealed opaque/schema conflict = %v, want %v",
			err, ErrStateRootCorrupt)
	}
}
