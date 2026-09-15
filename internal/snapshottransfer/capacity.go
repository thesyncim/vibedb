package snapshottransfer

import "context"

// ArtifactCapacity is a detached, bounded view used by cold capacity
// admission. ArtifactBytes is the authenticated final snapshot size; Offset
// reports the durably received prefix. It contains no file handle or path.
type ArtifactCapacity struct {
	Descriptor Descriptor
	Offset     uint64
	DiskBytes  uint64
	Complete   bool
}

// Capacity returns the exact repository accounting for one descriptor. The
// lookup is metadata-only and does not read the artifact payload. A cold
// capacity provider can therefore reserve the final snapshot before learner
// installation without scanning rows or copying bytes.
func (r *Repository) Capacity(ctx context.Context, descriptor Descriptor) (ArtifactCapacity, error) {
	if r == nil || ctx == nil || !descriptor.Valid() {
		return ArtifactCapacity{}, ErrDescriptor
	}
	if err := context.Cause(ctx); err != nil {
		return ArtifactCapacity{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return ArtifactCapacity{}, ErrRepository
	}
	record := r.records[descriptor.ArtifactHash]
	if record == nil || record.descriptor != descriptor {
		return ArtifactCapacity{}, ErrStaleFence
	}
	offset := record.offset
	if record.complete {
		offset = record.descriptor.ArtifactBytes
	}
	return ArtifactCapacity{Descriptor: record.descriptor, Offset: offset,
		DiskBytes: r.diskBytes, Complete: record.complete}, nil
}
