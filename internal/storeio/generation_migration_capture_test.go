package storeio

import (
	"bytes"
	"testing"
)

func TestGenerationMigrationCaptureByteNativeAndOrdered(t *testing.T) {
	m := GenerationMigrationMutation{StoreID: [16]byte{1}, MigrationID: [16]byte{2}, Sequence: 7, Generation: 41, Key: []byte{0, 1, 0xff}, Value: []byte{0xff, 0, 2}}
	b, err := EncodeGenerationMigrationMutation(make([]byte, 4096), m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := OpenGenerationMigrationMutation(b)
	if err != nil || !bytes.Equal(got.Key, m.Key) || !bytes.Equal(got.Value, m.Value) {
		t.Fatalf("roundtrip = %+v,%v", got, err)
	}
	next := m
	next.Sequence++
	next.Generation++
	if err := ValidateGenerationMigrationMutationOrder(m, next); err != nil {
		t.Fatal(err)
	}
	next.Sequence++
	if err := ValidateGenerationMigrationMutationOrder(m, next); err == nil {
		t.Fatal("sequence gap accepted")
	}
	corrupt := bytes.Clone(b)
	corrupt[GenerationMigrationCaptureHeaderBytes] ^= 1
	if _, err := OpenGenerationMigrationMutation(corrupt); err == nil {
		t.Fatal("corruption accepted")
	}
}
