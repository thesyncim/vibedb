package storeio

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestCommitterPublicationObserverPrecedesVisibility(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "observer")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	pageSize := os.Getpagesize()
	if err = f.Truncate(int64(testMutableStoreDataStart(uint32(pageSize)))); err != nil {
		t.Fatal(err)
	}
	veto := errors.New("capture unavailable")
	allow := false
	var seen []byte
	c, err := NewCommitter(f, DeviceOptions{Backend: BackendPortable, BufferCount: 4, BufferSize: pageSize}, CommitterOptions{QueueSlots: 2, MaxPagesPerBatch: 1, PublicationObserver: func(_ uint64, descriptor []byte) error {
		seen = append(seen[:0], descriptor...)
		if !allow {
			return veto
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Begin(0)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := []byte{0, 1, 0xff}
	if err = b.SetPublicationDescriptor(descriptor); err != nil {
		t.Fatal(err)
	}
	root := testInlineSuperblock(1)
	root.PageSize = uint32(pageSize)
	root.State.PageSize = uint32(pageSize)
	root.State.MaxPageSize = uint32(pageSize)
	root.FileEnd = testMutableStoreDataStart(uint32(pageSize))
	if err = b.SetInlineSuperblock(root); err != nil {
		t.Fatal(err)
	}
	if err = b.Publish(1); !errors.Is(err, veto) {
		t.Fatalf("veto = %v", err)
	}
	if c.PublishedGeneration() != 0 || !bytes.Equal(seen, descriptor) {
		t.Fatalf("visible=%d seen=%x", c.PublishedGeneration(), seen)
	}
	allow = true
	if err = b.Publish(1); err != nil {
		t.Fatal(err)
	}
	if err = c.SetRequiredPublicationObserver(func(uint64, []byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	missing, err := c.Begin(0)
	if err != nil {
		t.Fatal(err)
	}
	root = testInlineSuperblock(2)
	root.PageSize = uint32(pageSize)
	root.State.PageSize = uint32(pageSize)
	root.State.MaxPageSize = uint32(pageSize)
	root.FileEnd = testMutableStoreDataStart(uint32(pageSize))
	if err = missing.SetInlineSuperblock(root); err != nil {
		t.Fatal(err)
	}
	if err = missing.Publish(2); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("missing descriptor = %v", err)
	}
	if err = missing.Abort(); err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}
