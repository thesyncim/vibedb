// Package storemem owns pointer-free anonymous memory used by the mapped
// Store's cold metadata. It is deliberately separate from storeio: lookup
// metadata has no file-descriptor, durability, or completion-queue semantics.
package storemem

// Block is a fixed-size byte region. Bytes returns its complete mutable view;
// callers must not retain that view after Close. A Block is not safe for
// concurrent Close and use.
type Block struct {
	data []byte
}

// Allocate returns a zeroed block of size bytes. Common Unix platforms place
// the bytes in anonymous memory outside the Go heap; other platforms use an
// ordinary pointer-free byte slice with the same behavior.
func Allocate(size int) (*Block, error) {
	data, err := allocate(size)
	if err != nil {
		return nil, err
	}
	return &Block{data: data}, nil
}

// Bytes returns the complete block. The view becomes invalid after Close.
func (b *Block) Bytes() []byte {
	if b == nil {
		return nil
	}
	return b.data
}

// Len returns the block's byte size.
func (b *Block) Len() int {
	if b == nil {
		return 0
	}
	return len(b.data)
}

// OutsideHeap reports whether the backing bytes are owned by the operating
// system rather than Go HeapAlloc on this platform.
func (b *Block) OutsideHeap() bool {
	return b != nil && outsideHeap
}

// Close releases the block. It is idempotent.
func (b *Block) Close() error {
	if b == nil || b.data == nil {
		return nil
	}
	data := b.data
	b.data = nil
	return release(data)
}
