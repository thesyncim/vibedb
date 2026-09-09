package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func TestPrimaryExactPackRoundTripRawAndLZ4(t *testing.T) {
	tests := []struct {
		name    string
		quantum int
		leaves  [][]byte
		codec   string
	}{
		{
			name:    "raw aligned tie",
			quantum: PrimaryExactPackMaxDecodedBytes,
			leaves:  [][]byte{{1, 3, 5, 7}, {2, 4, 6}},
			codec:   "raw",
		},
		{
			name:    "lz4 nondefault quantum",
			quantum: 8 << 10,
			leaves:  [][]byte{bytes.Repeat([]byte("canonical-leaf-a/"), 700), bytes.Repeat([]byte("canonical-leaf-b/"), 700)},
			codec:   "lz4",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoded := 0
			for _, leaf := range test.leaves {
				decoded += len(leaf)
			}
			var encoder PrimaryExactPackEncoder
			if err := encoder.Prepare(len(test.leaves), decoded); err != nil {
				t.Fatal(err)
			}
			for i, leaf := range test.leaves {
				if err := encoder.Append(uint32(41+i), leaf); err != nil {
					t.Fatal(err)
				}
			}
			wire := make([]byte, 0, PrimaryExactPackMaxDecodedBytes)
			wire, err := encoder.Encode(wire, test.quantum)
			if err != nil {
				t.Fatal(err)
			}
			var decoder PrimaryExactPackDecoder
			if err := decoder.Prepare(decoded + len(test.leaves)*PrimaryExactPackMemberBytes); err != nil {
				t.Fatal(err)
			}
			if err := decoder.Open(wire); err != nil {
				t.Fatal(err)
			}
			if decoder.Codec() != test.codec {
				t.Fatalf("codec=%q, want %q", decoder.Codec(), test.codec)
			}
			if decoder.MemberCount() != len(test.leaves) {
				t.Fatalf("count=%d", decoder.MemberCount())
			}
			for i, want := range test.leaves {
				got, err := decoder.Member(i, uint32(41+i))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("member %d differs", i)
				}
				if len(got) != cap(got) {
					t.Fatalf("member %d can grow into adjacent member", i)
				}
			}
			if _, err := decoder.Member(0, 999); !errors.Is(err, ErrPrimaryExactPackIndex) {
				t.Fatalf("wrong index: %v", err)
			}
		})
	}
}

func TestPrimaryExactPackBounds(t *testing.T) {
	maxSingle := PrimaryExactPackMaxPayloadBytes - PrimaryExactPackHeaderBytes - PrimaryExactPackMemberBytes
	for _, tc := range []struct {
		name  string
		sizes []int
	}{
		{"singleton", []int{maxSingle}},
		{"multiple", []int{32700, 32700}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			total := 0
			for _, size := range tc.sizes {
				total += size
			}
			var encoder PrimaryExactPackEncoder
			if err := encoder.Prepare(len(tc.sizes), total); err != nil {
				t.Fatal(err)
			}
			for i, size := range tc.sizes {
				if err := encoder.Append(uint32(i+1), bytes.Repeat([]byte{byte(i + 1)}, size)); err != nil {
					t.Fatal(err)
				}
			}
			wire := make([]byte, 0, PrimaryExactPackMaxDecodedBytes)
			wire, err := encoder.Encode(wire, PrimaryExactPackMaxDecodedBytes)
			if err != nil {
				t.Fatal(err)
			}
			if len(wire) != PrimaryExactPackHeaderBytes+len(tc.sizes)*PrimaryExactPackMemberBytes+total {
				t.Fatalf("len=%d", len(wire))
			}
		})
	}
	var encoder PrimaryExactPackEncoder
	if err := encoder.Prepare(1, maxSingle); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Append(1, make([]byte, maxSingle+1)); !errors.Is(err, ErrPrimaryExactPackBounds) {
		t.Fatalf("oversize append: %v", err)
	}
	if _, err := encoder.Encode(make([]byte, 0, 1), 4096); !errors.Is(err, ErrPrimaryExactPackBounds) {
		t.Fatalf("empty encode: %v", err)
	}
}

func TestPrimaryExactPackRejectsCorruption(t *testing.T) {
	leaves := [][]byte{bytes.Repeat([]byte("same-prefix/"), 300), bytes.Repeat([]byte("same-prefix/"), 300)}
	var encoder PrimaryExactPackEncoder
	if err := encoder.Prepare(2, len(leaves[0])+len(leaves[1])); err != nil {
		t.Fatal(err)
	}
	for i := range leaves {
		if err := encoder.Append(uint32(i+7), leaves[i]); err != nil {
			t.Fatal(err)
		}
	}
	wire, err := encoder.Encode(make([]byte, 0, 8000), 4096)
	if err != nil {
		t.Fatal(err)
	}
	if wire[4] != primaryExactPackLZ4 {
		t.Fatalf("fixture codec=%d", wire[4])
	}
	var decoder PrimaryExactPackDecoder
	if err := decoder.Prepare(8000); err != nil {
		t.Fatal(err)
	}

	mutate := func(name string, fn func([]byte)) {
		t.Helper()
		bad := append([]byte(nil), wire...)
		fn(bad)
		if err := decoder.Open(bad); !errors.Is(err, ErrPrimaryExactPackCorrupt) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	mutate("magic", func(b []byte) { b[0] ^= 1 })
	mutate("reserved header", func(b []byte) { b[28] = 1 })
	mutate("oversized decoded", func(b []byte) { binary.LittleEndian.PutUint32(b[8:], PrimaryExactPackMaxDecodedBytes) })
	mutate("body", func(b []byte) { b[len(b)-1] ^= 0xff })
	if err := decoder.Open(wire[:len(wire)-1]); !errors.Is(err, ErrPrimaryExactPackCorrupt) {
		t.Fatalf("truncated: %v", err)
	}

	var small PrimaryExactPackDecoder
	if err := small.Prepare(PrimaryExactPackMemberBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := small.Open(wire); !errors.Is(err, ErrPrimaryExactPackCorrupt) {
		t.Fatalf("small workspace: %v", err)
	}
}

func TestPrimaryExactPackRejectsMalformedRawDirectoryAndSmallOutput(t *testing.T) {
	var encoder PrimaryExactPackEncoder
	if err := encoder.Prepare(2, 32); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Append(1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Append(2, []byte("second")); err != nil {
		t.Fatal(err)
	}
	prefix := []byte("keep")
	if got, err := encoder.Encode(prefix[:len(prefix):len(prefix)], 4096); !errors.Is(err, ErrPrimaryExactPackBounds) || !bytes.Equal(got, prefix) {
		t.Fatalf("small output: %q, %v", got, err)
	}
	wire, err := encoder.Encode(make([]byte, 0, 256), PrimaryExactPackMaxDecodedBytes)
	if err != nil {
		t.Fatal(err)
	}
	if wire[4] != primaryExactPackRaw {
		t.Fatalf("codec=%d", wire[4])
	}
	var decoder PrimaryExactPackDecoder
	if err := decoder.Prepare(64); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"gap", func(body []byte) { binary.LittleEndian.PutUint32(body[4:], 1) }},
		{"empty", func(body []byte) { binary.LittleEndian.PutUint32(body[8:], 0) }},
		{"reserved", func(body []byte) { binary.LittleEndian.PutUint32(body[12:], 1) }},
	} {
		bad := append([]byte(nil), wire...)
		body := bad[PrimaryExactPackHeaderBytes:]
		tc.mutate(body)
		binary.LittleEndian.PutUint32(bad[24:], crc32.Checksum(body, primaryExactPackCRCTable))
		if err := decoder.Open(bad); !errors.Is(err, ErrPrimaryExactPackCorrupt) {
			t.Fatalf("%s: %v", tc.name, err)
		}
	}
}

func TestPrimaryExactPackPreparedWorkspaceVariesShapeWithoutAlloc(t *testing.T) {
	large := bytes.Repeat([]byte("large-canonical-leaf/"), 900)
	small := bytes.Repeat([]byte("small-leaf/"), 8)
	var encoder PrimaryExactPackEncoder
	if err := encoder.Prepare(128, len(large)); err != nil {
		t.Fatal(err)
	}
	wire := make([]byte, 0, PrimaryExactPackMaxPayloadBytes)
	var decoder PrimaryExactPackDecoder
	if err := decoder.Prepare(PrimaryExactPackMaxPayloadBytes - PrimaryExactPackHeaderBytes); err != nil {
		t.Fatal(err)
	}
	iteration := 0
	run := func() {
		encoder.Reset()
		if iteration&1 == 0 {
			if err := encoder.Append(7, large); err != nil {
				panic(err)
			}
		} else {
			for i := 0; i < 64; i++ {
				if err := encoder.Append(uint32(i), small); err != nil {
					panic(err)
				}
			}
		}
		iteration++
		var err error
		wire, err = encoder.Encode(wire[:0], 4096)
		if err != nil {
			panic(err)
		}
		if err = decoder.Open(wire); err != nil {
			panic(err)
		}
	}
	run()
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("allocs/run=%v", got)
	}
}

func TestPrimaryExactPackRejectedValidationZeroAlloc(t *testing.T) {
	var decoder PrimaryExactPackDecoder
	if err := decoder.Prepare(1024); err != nil {
		t.Fatal(err)
	}
	bad := make([]byte, PrimaryExactPackHeaderBytes)
	copy(bad, primaryExactPackMagic)
	bad[5] = primaryExactPackVersion
	if err := decoder.Open(bad); !errors.Is(err, ErrPrimaryExactPackCorrupt) {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		if err := decoder.Open(bad); !errors.Is(err, ErrPrimaryExactPackCorrupt) {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("allocs/run=%v", got)
	}
}

func TestPrimaryExactPackPreparedZeroAlloc(t *testing.T) {
	leafA := bytes.Repeat([]byte("shared/canonical/exact/leaf/a/"), 300)
	leafB := bytes.Repeat([]byte("shared/canonical/exact/leaf/b/"), 300)
	decoded := len(leafA) + len(leafB)
	var encoder PrimaryExactPackEncoder
	if err := encoder.Prepare(2, decoded); err != nil {
		t.Fatal(err)
	}
	wire := make([]byte, 0, PrimaryExactPackMaxDecodedBytes)
	var decoder PrimaryExactPackDecoder
	if err := decoder.Prepare(decoded + 2*PrimaryExactPackMemberBytes); err != nil {
		t.Fatal(err)
	}
	run := func() {
		encoder.Reset()
		if err := encoder.Append(91, leafA); err != nil {
			panic(err)
		}
		if err := encoder.Append(92, leafB); err != nil {
			panic(err)
		}
		var err error
		wire, err = encoder.Encode(wire[:0], 4096)
		if err != nil {
			panic(err)
		}
		if err = decoder.Open(wire); err != nil {
			panic(err)
		}
		got, err := decoder.Member(1, 92)
		if err != nil || len(got) != len(leafB) {
			panic("member")
		}
	}
	run()
	if got := testing.AllocsPerRun(1000, run); got != 0 {
		t.Fatalf("allocs/run=%v", got)
	}
}
