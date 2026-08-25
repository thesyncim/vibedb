package raftmodel

import "testing"

func BenchmarkReadContextKey(b *testing.B) {
	context := [16]byte{1, 3, 5, 7, 9, 11, 13, 15}
	b.ReportAllocs()
	b.SetBytes(int64(len(context)))
	var key readContextKey
	for b.Loop() {
		key, _ = makeReadContextKey(context[:])
	}
	if key.len != uint16(len(context)) {
		b.Fatal(key.len)
	}
}
