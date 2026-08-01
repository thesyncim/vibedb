package distribution

import "sync/atomic"

// ManifestHolder publishes immutable Manifest generations for lock-free
// concurrent reads. A reader always observes one whole generation — the old or
// the new — never a mixed or partially published state, because each Manifest
// is immutable and only its pointer is swapped.
type ManifestHolder struct {
	ptr atomic.Pointer[Manifest]
}

// NewManifestHolder returns a holder seeded with initial, which may be nil.
func NewManifestHolder(initial *Manifest) *ManifestHolder {
	h := &ManifestHolder{}
	if initial != nil {
		h.ptr.Store(initial)
	}
	return h
}

// Publish atomically installs m as the current generation.
func (h *ManifestHolder) Publish(m *Manifest) {
	h.ptr.Store(m)
}

// Current returns the most recently published Manifest, or nil if none has
// been published. The read path takes no lock.
func (h *ManifestHolder) Current() *Manifest {
	return h.ptr.Load()
}
