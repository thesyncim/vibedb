package durable

import (
	"fmt"
	"os"
)

// fileStoreCapacityOps are narrow package-test seams for strict allocation and
// its durability fence. Production always uses the platform implementation and
// File.Sync; tests can force ENOSPC or a post-allocation sync failure without
// weakening the public contract.
var fileStoreCapacityOps = struct {
	allocate func(*os.File, int64, int64) error
	sync     func(*os.File) error
}{
	allocate: strictlyAllocateFile,
	sync:     strictlySyncAllocatedFile,
}

// PhysicalCapacityBytes returns the immutable main-file ceiling. Zero denotes
// elastic allocation.
func (c *Collection) PhysicalCapacityBytes() uint64 {
	if c == nil {
		return 0
	}
	return c.options.PhysicalCapacityBytes
}

// PhysicalHighWaterBytes returns the prefix whose complete physical allocation
// has been strictly proved and synced. It is zero for elastic collections.
func (c *Collection) PhysicalHighWaterBytes() uint64 {
	if c == nil {
		return 0
	}
	c.writer.RLock()
	defer c.writer.RUnlock()
	return c.physicalHighWater
}

// EnsurePhysicalAllocation strictly allocates and syncs the complete prefix
// through highWater before publishing it as the collection's current physical
// certificate. The operation is monotone and writer-serialized. It never
// changes logical Store state; a failed allocation or sync leaves the previous
// certificate intact even when the filesystem already grew EOF. Callers must
// not change file allocation through another descriptor while the collection
// is open.
func (c *Collection) EnsurePhysicalAllocation(highWater uint64) error {
	if c == nil {
		return fmt.Errorf("%w: nil collection", ErrPhysicalCapacity)
	}
	c.writer.Lock()
	defer c.writer.Unlock()
	if c.closed {
		return ErrClosed
	}
	return c.ensurePhysicalAllocationLocked(highWater)
}

func (c *Collection) ensurePhysicalAllocationLocked(highWater uint64) error {
	if failure := c.PersistenceError(); failure != nil {
		return failure
	}
	ceiling := c.options.PhysicalCapacityBytes
	if ceiling == 0 {
		return fmt.Errorf(
			"%w: collection uses elastic allocation", ErrPhysicalCapacity,
		)
	}
	pageSize := uint64(c.options.PageSize)
	if highWater == 0 || highWater%pageSize != 0 || highWater > ceiling {
		return fmt.Errorf(
			"%w: requested=%d ceiling=%d", ErrPhysicalCapacity,
			highWater, ceiling,
		)
	}
	if highWater <= c.physicalHighWater {
		return nil
	}
	// The producer lock excludes new publications, but the committer may still
	// own a previously published Device commit (or a manual-lane prewrite) on a
	// separate descriptor for this inode. Settle that cut before changing and
	// syncing allocation metadata. This keeps the allocation fence from racing
	// main-file writes or accidentally becoming their durability acknowledgement.
	if err := c.committer.Flush(); err != nil {
		return err
	}
	info, err := c.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < 0 {
		return fmt.Errorf("%w: negative main-file size", ErrPhysicalCapacity)
	}
	current := uint64(info.Size())
	settled, err := settleApparentPhysicalHighWater(
		current, c.physicalHighWater, ceiling, pageSize,
	)
	if err != nil {
		return err
	}
	target := highWater
	if settled > target {
		// A preceding attempt may have allocated/grown EOF and then failed its
		// sync. That larger prefix has no authority yet, so round it up, settle,
		// and certify the complete apparent prefix instead of shrinking it. A
		// partial EOF extension from a failed allocator is therefore recoverable.
		target = settled
	}
	if err := fileStoreCapacityOps.allocate(
		c.file, int64(current), int64(target),
	); err != nil {
		return fmt.Errorf("%w: strictly allocate main file: %w", ErrPhysicalCapacity, err)
	}
	if err := fileStoreCapacityOps.sync(c.file); err != nil {
		return fmt.Errorf("%w: sync strictly allocated main file: %w", ErrPhysicalCapacity, err)
	}
	if failure := c.PersistenceError(); failure != nil {
		return failure
	}
	// Advance only after allocation and its durability fence have both
	// succeeded. In particular, a sync failure may leave a larger stat size but
	// grants no in-memory authority; retry repeats the proof and fence.
	c.physicalHighWater = target
	return nil
}

// settleApparentPhysicalHighWater turns an allocator's partial EOF extension
// into an aligned target that can be re-proved. The apparent size may lead the
// last certificate after an allocation or sync failure, but it may never trail
// the trusted lower bound or exceed the immutable ceiling.
func settleApparentPhysicalHighWater(
	apparent, lowerBound, ceiling, pageSize uint64,
) (uint64, error) {
	if pageSize == 0 || apparent < lowerBound || apparent > ceiling {
		return 0, fmt.Errorf(
			"%w: main-file size=%d lower=%d ceiling=%d",
			ErrPhysicalCapacity, apparent, lowerBound, ceiling,
		)
	}
	remainder := apparent % pageSize
	if remainder == 0 {
		return apparent, nil
	}
	delta := pageSize - remainder
	if apparent > ceiling-delta {
		return 0, fmt.Errorf(
			"%w: rounded main-file size exceeds sealed ceiling: size=%d ceiling=%d",
			ErrPhysicalCapacity, apparent, ceiling,
		)
	}
	return apparent + delta, nil
}

// restorePhysicalAllocation proves the complete apparent file prefix during
// Open. It repairs sparse holes where the platform can do so, fences the repair,
// and returns the only high-water the opener may trust.
func restorePhysicalAllocation(
	file *os.File, logicalFileEnd, ceiling, pageSize uint64,
) (uint64, error) {
	if ceiling == 0 {
		return 0, nil
	}
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() < 0 {
		return 0, fmt.Errorf("%w: negative main-file size", ErrPhysicalCapacity)
	}
	apparent := uint64(info.Size())
	target, err := settleApparentPhysicalHighWater(
		apparent, logicalFileEnd, ceiling, pageSize,
	)
	if err != nil {
		return 0, err
	}
	if err := fileStoreCapacityOps.allocate(
		file, int64(apparent), int64(target),
	); err != nil {
		return 0, fmt.Errorf("%w: restore main-file allocation: %w", ErrPhysicalCapacity, err)
	}
	if err := fileStoreCapacityOps.sync(file); err != nil {
		return 0, fmt.Errorf("%w: sync restored main-file allocation: %w", ErrPhysicalCapacity, err)
	}
	return target, nil
}
