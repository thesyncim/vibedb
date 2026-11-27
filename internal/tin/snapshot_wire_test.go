package tin

import (
	"fmt"
	"testing"
)

func TestSnapshotWireRoundTripAndIncrementalAfterRestore(t *testing.T) {
	orig := NewIndex()
	for i := 0; i < 300; i++ {
		text := fmt.Sprintf("common shared token doc%d", i)
		if i%15 == 0 {
			text += " luxury vintage café watch"
		}
		orig.Add(DocID(i), text)
	}
	orig.Seal()

	raw, err := orig.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	restored, err := UnmarshalSnapshot(raw)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot: %v", err)
	}
	if restored.Docs() != orig.Docs() {
		t.Fatalf("restored.Docs() = %d, want %d", restored.Docs(), orig.Docs())
	}
	for _, q := range []string{"common", "luxury AND watch", `"luxury vintage"`, "caf*", "doc45"} {
		qo, err := orig.ParseTINQL(q)
		if err != nil {
			t.Fatal(err)
		}
		qr, err := restored.ParseTINQL(q)
		if err != nil {
			t.Fatal(err)
		}
		got := restored.Score(qr, 25, nil)
		want := orig.Score(qo, 25, nil)
		if len(got) != len(want) {
			t.Fatalf("query %q: restored %d hits, orig %d hits", q, len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("query %q hit %d: got=%+v want=%+v", q, i, got[i], want[i])
			}
		}
	}

	// Mutating the restored index (Remove + Add) must work identically to mutating orig.
	orig.Remove(DocID(15))
	orig.Add(DocID(300), "luxury sapphire watch")
	orig.Seal()
	restored.Remove(DocID(15))
	restored.Add(DocID(300), "luxury sapphire watch")
	restored.Seal()

	for _, q := range []string{"luxury", "sapphire", "doc15"} {
		qo, _ := orig.ParseTINQL(q)
		qr, _ := restored.ParseTINQL(q)
		got := restored.Score(qr, 25, nil)
		want := orig.Score(qo, 25, nil)
		if len(got) != len(want) {
			t.Fatalf("after mutate %q: got %d, want %d", q, len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("after mutate %q hit %d: got=%+v want=%+v", q, i, got[i], want[i])
			}
		}
	}
}

func TestSnapshotWireRejectsCorruption(t *testing.T) {
	orig := NewIndex()
	orig.Add(DocID(1), "hello world")
	orig.Seal()
	raw, err := orig.MarshalSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	// Truncated.
	if _, err := UnmarshalSnapshot(raw[:len(raw)-5]); err == nil {
		t.Fatal("expected error on truncated snapshot")
	}
	// Bit-flipped payload.
	corrupt := append([]byte(nil), raw...)
	corrupt[10] ^= 0xff
	if _, err := UnmarshalSnapshot(corrupt); err == nil {
		t.Fatal("expected error on corrupted payload")
	}
	// Bit-flipped checksum.
	badSum := append([]byte(nil), raw...)
	badSum[len(badSum)-1] ^= 0x01
	if _, err := UnmarshalSnapshot(badSum); err == nil {
		t.Fatal("expected error on corrupted checksum")
	}
}
