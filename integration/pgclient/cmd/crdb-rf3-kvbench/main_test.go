package main

import (
	"bytes"
	"testing"
)

func TestEvidenceKeyAndValueMatchVibeDBWorkload(t *testing.T) {
	if got, want := evidenceKey(7), []byte{0x40, 'r', 'f', '3', '-', 'e', 'v', 'i', 'd', 'e', 'n', 'c', 'e', '-', '7', 0x00, 0x00}; !bytes.Equal(got, want) {
		t.Fatalf("key=%x want=%x", got, want)
	}
	if got, want := evidenceValue(7, 11), []byte(`{"client":7,"id":"rf3-evidence-7","sequence":11}`); !bytes.Equal(got, want) {
		t.Fatalf("value=%s want=%s", got, want)
	}
}

func TestConfigRejectsUnboundedRuns(t *testing.T) {
	err := run(nil, config{})
	if err == nil {
		t.Fatal("invalid configuration succeeded")
	}
}
