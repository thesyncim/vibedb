package storemem

import "testing"

func TestBlock(t *testing.T) {
	b, err := Allocate(4096)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Bytes()) != 4096 || b.Len() != 4096 {
		t.Fatalf("block length = (%d, %d), want 4096", len(b.Bytes()), b.Len())
	}
	if outsideHeap != b.OutsideHeap() {
		t.Fatalf("OutsideHeap = %v, want %v", b.OutsideHeap(), outsideHeap)
	}
	for i, value := range b.Bytes() {
		if value != 0 {
			t.Fatalf("byte %d = %d, want zero", i, value)
		}
	}
	b.Bytes()[17] = 91
	if b.Bytes()[17] != 91 {
		t.Fatal("block did not retain write")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if b.Bytes() != nil || b.Len() != 0 {
		t.Fatal("closed block still exposes bytes")
	}
}
