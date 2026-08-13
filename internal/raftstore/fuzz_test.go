package raftstore

import "testing"

func FuzzRecordDecoderFailsClosed(f *testing.F) {
	options, err := normalizeOptions(testFormatOptions())
	if err != nil {
		f.Fatal(err)
	}
	_, header, err := marshalStaticHeader(testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		f.Fatal(err)
	}
	payload, _, err := marshalBootstrap(testBootstrap(), 1)
	if err != nil {
		f.Fatal(err)
	}
	seed, _, _, err := marshalRecord(recordKindBootstrap, 0, 1, 0, 0, header.headerDigest, payload, header, options)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(uint32(len(seed)), uint16(0), byte(0), uint16(1), byte(0))
	f.Fuzz(func(t *testing.T, length uint32, firstPosition uint16, firstXOR byte, secondPosition uint16, secondXOR byte) {
		size := int(length % uint32(2*recordDamageGranule+1))
		data := make([]byte, size)
		copy(data, seed)
		if size != 0 {
			data[int(firstPosition)%size] ^= firstXOR
			data[int(secondPosition)%size] ^= secondXOR
		}
		_, _ = unmarshalRecord(data, header, options)
	})
}

func FuzzCurrentSlotDecoderFailsClosed(f *testing.F) {
	header, _, _, seed, _, _, _, _ := testFormatStateForFuzz(f)
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = unmarshalCurrentSlot(data, 0, header)
	})
}

func testFormatStateForFuzz(t testing.TB) (headerState, normalizedOptions, currentState, []byte, currentState, []byte, currentState, []byte) {
	t.Helper()
	options, err := normalizeOptions(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	_, header, err := marshalStaticHeader(testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, _ := marshalBootstrap(testBootstrap(), 1)
	record, digest, _, err := marshalRecord(recordKindBootstrap, 0, 1, 0, 0, header.headerDigest, payload, header, options)
	if err != nil {
		t.Fatal(err)
	}
	first := initialCurrent(header, HeaderBytes+int64(len(record)), 1, digest)
	firstBytes, _, err := marshalCurrentSlot(first, 0, header)
	if err != nil {
		t.Fatal(err)
	}
	return header, options, first, firstBytes, currentState{}, nil, currentState{}, nil
}
