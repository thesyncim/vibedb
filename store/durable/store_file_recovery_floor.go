package durable

import "github.com/thesyncim/vibedb/internal/storeio"

// installExactRootRecoveryFloor installs the floor retained by the complete
// exact-root bank set. A zero floor is never accepted: treating it as an
// unbounded allocator permission would make a missing bank look safe.
func (c *Collection) installExactRootRecoveryFloor(floor uint64) error {
	if c == nil || floor == 0 {
		return storeio.ErrInvalidWrite
	}
	for {
		current := c.exactRootRecoveryFloor.Load()
		if current == floor {
			return nil
		}
		if current != 0 && floor < current {
			return storeio.ErrRootVectorSequence
		}
		if c.exactRootRecoveryFloor.CompareAndSwap(current, floor) {
			return nil
		}
	}
}

// clearExactRootRecoveryFloor returns a collection to its ordinary local
// committer/readers fence. This is only used when an exact-root owner is
// explicitly detached; ordinary opens never clear a selected floor.
func (c *Collection) clearExactRootRecoveryFloor() {
	if c != nil {
		c.exactRootRecoveryFloor.Store(0)
	}
}

// effectiveRecoveryFloor centralizes the strict generation fence used by all
// allocator and physical-release paths. An extent retired at or above this
// floor may still be named by a reader, a direct-read epoch, the ordinary
// fallback root, or any selectable exact-root bank.
func (c *Collection) effectiveRecoveryFloor(current uint64) uint64 {
	if c == nil || current == 0 {
		return 0
	}
	floor := current
	if c.committer != nil {
		if fallback := c.committer.FallbackGeneration(); fallback != 0 && fallback < floor {
			floor = fallback
		}
	}
	if exact := c.exactRootRecoveryFloor.Load(); exact != 0 && exact < floor {
		floor = exact
	}
	if c.leases != nil {
		if reader := c.leases.Minimum(current); reader < floor {
			floor = reader
		}
	}
	if c.readEpochs != nil {
		if reader := c.readEpochs.Minimum(current); reader < floor {
			floor = reader
		}
	}
	return floor
}

// exactRootRecoveryFloorForTest exposes the installed floor to package tests
// without making it a detached public authority.
func (c *Collection) exactRootRecoveryFloorForTest() uint64 {
	if c == nil {
		return 0
	}
	return c.exactRootRecoveryFloor.Load()
}
