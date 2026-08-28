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

func TestPublicationDescriptorCanonicalNoop(t *testing.T) {
	b, err := EncodePublicationDescriptor(make([]byte, 4096), nil)
	if err != nil {
		t.Fatal(err)
	}
	v, err := OpenPublicationDescriptor(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := v.Next(); err != nil || ok {
		t.Fatalf("noop terminal = %v,%v", ok, err)
	}
}

func BenchmarkPublicationDescriptorEncode(b *testing.B) {
	mutations := make([]PublicationMutation, 64)
	var keys [64][16]byte
	var values [64][128]byte
	for i := range mutations {
		keys[i][0], values[i][0] = byte(i), byte(i+1)
		mutations[i] = PublicationMutation{Key: keys[i][:], Value: values[i][:]}
	}
	dst := make([]byte, 16<<10)
	encoded, err := EncodePublicationDescriptor(dst, mutations)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded) / len(mutations)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := EncodePublicationDescriptor(dst, mutations); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(encoded))/float64(len(mutations)), "bytes/mutation")
}

func BenchmarkPublicationDescriptorIterate(b *testing.B) {
	mutations := make([]PublicationMutation, 64)
	key, value := make([]byte, 16), make([]byte, 128)
	for i := range mutations {
		mutations[i] = PublicationMutation{Key: key, Value: value}
	}
	encoded, err := EncodePublicationDescriptor(make([]byte, 16<<10), mutations)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded) / len(mutations)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		view, err := OpenPublicationDescriptor(encoded)
		if err != nil {
			b.Fatal(err)
		}
		for {
			_, ok, err := view.Next()
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				break
			}
		}
	}
	b.ReportMetric(float64(len(encoded))/float64(len(mutations)), "bytes/mutation")
}
