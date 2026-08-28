package durable

import "github.com/thesyncim/vibedb/internal/storeio"

// ImageIdentity is an opaque, constant-size identity for one fully durable
// reader-visible collection image. It binds the durable store UUID, exact
// logical generation, checkpoint watermark, row cardinality, and physical
// rooted graph. Callers may retain and compare it but cannot construct one.
// Reachable pages remain protected independently by their checksums and
// StoreID/generation identities.
type ImageIdentity struct {
	storeID    [16]byte
	generation uint64
	checkpoint uint64
	documents  uint64
	fileEnd    uint64
	root       storeio.StateRoot
}

// DurableImageIdentity returns the exact current image only when its complete
// reader-visible generation is crash safe. It performs no file I/O or scan.
func (c *Collection) DurableImageIdentity() (ImageIdentity, bool) {
	if c == nil || c.committer == nil || !c.synchronous() {
		return ImageIdentity{}, false
	}
	c.visibilityMu.Lock()
	defer c.visibilityMu.Unlock()
	view := c.visibleLogicalViewNoError()
	if view.state == nil || view.generation == 0 {
		return ImageIdentity{}, false
	}
	checkpoint := max(c.committer.DurableGeneration(), c.journalDeltaGeneration.Load())
	return ImageIdentity{
		storeID: c.storeID, generation: view.generation,
		checkpoint: checkpoint, documents: view.documentCount,
		fileEnd: view.state.fileEnd, root: view.state.root,
	}, true
}

// MatchesDurableImage reports whether identity still names this collection's
// exact fully durable image. It is the O(1) audit-to-ownership handoff fence.
func (c *Collection) MatchesDurableImage(identity ImageIdentity) bool {
	current, ok := c.DurableImageIdentity()
	return ok && current == identity
}
