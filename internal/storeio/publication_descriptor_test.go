package storeio

import (
	"bytes"
	"testing"
)

func TestPublicationDescriptorCanonicalBinaryBatch(t *testing.T) {
	want := []PublicationMutation{{Key: []byte{0, 1}, Value: []byte{0xff}}, {Delete: true, Key: []byte{2, 0}}}
	b, err := EncodePublicationDescriptor(make([]byte, 4096), want)
	if err != nil {
		t.Fatal(err)
	}
	v, err := OpenPublicationDescriptor(b)
	if err != nil {
		t.Fatal(err)
	}
	for i := range want {
		got, ok, err := v.Next()
		if err != nil || !ok || got.Delete != want[i].Delete || !bytes.Equal(got.Key, want[i].Key) || !bytes.Equal(got.Value, want[i].Value) {
			t.Fatalf("mutation %d = %+v,%v,%v", i, got, ok, err)
		}
	}
	if _, ok, err := v.Next(); err != nil || ok {
		t.Fatalf("terminal = %v,%v", ok, err)
	}
	corrupt := bytes.Clone(b)
	corrupt[20] ^= 1
	if _, err := OpenPublicationDescriptor(corrupt); err == nil {
		t.Fatal("corruption accepted")
	}
}
