package requestledger

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestPayloadBuildCommandEpochCanonicalBinding(t *testing.T) {
	head, _, _ := testHead(t, false)
	root := testDigest("payload-root")
	build, err := NewPayloadBuild(head, root, 1, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendPayloadBuild(nil, build)
	if err != nil || len(raw) != 392 || binary.LittleEndian.Uint64(raw[288:296]) != 7 {
		t.Fatalf("fixed epoch geometry len=%d err=%v", len(raw), err)
	}
	got, err := OpenPayloadBuild(raw)
	if err != nil || got != build {
		t.Fatal("epoch roundtrip", err)
	}
	other, err := NewPayloadBuild(head, root, 1, 1, 8)
	if err != nil || other.BuildDigest == build.BuildDigest {
		t.Fatal("epoch absent from digest")
	}
	if _, err = NewPayloadBuild(head, root, 1, 1, 0); err == nil {
		t.Fatal("missing epoch accepted")
	}
	changed := build
	changed.CommandEpoch++
	if _, err = AppendPayloadBuild(nil, changed); err == nil {
		t.Fatal("changed epoch reused digest")
	}
	for _, at := range []int{288, 296} {
		hostile := bytes.Clone(raw)
		hostile[at] ^= 1
		// Reseal the outer checksum: canonical validation must still reject
		// either a changed epoch/digest pair or nonzero reserved bytes.
		hostile = appendChecksum(hostile[:len(hostile)-checksumBytes], 0)
		if _, err = OpenPayloadBuild(hostile); err == nil {
			t.Fatalf("tamper at%d accepted", at)
		}
	}
	storage := make([]byte, 0, PayloadBuildRecordBytes)
	if n := testing.AllocsPerRun(1000, func() {
		encoded, err := AppendPayloadBuild(storage[:0], build)
		if err != nil {
			panic(err)
		}
		if _, err = OpenPayloadBuild(encoded); err != nil {
			panic(err)
		}
	}); n != 0 {
		t.Fatalf("epoch codec allocations=%g", n)
	}
}
