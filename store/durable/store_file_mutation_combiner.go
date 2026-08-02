package durable

import (
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	vibejson "github.com/thesyncim/vibejson"
)

// primaryMutationCombineWindow is intentionally short. It is long enough for
// callers that arrived together at a synchronous writer to join one batch, but
// far below the device-fence latency this path is meant to amortize. A lone
// synchronous mutation still pays its normal journal barrier once.
const primaryMutationCombineWindow = 50 * time.Microsecond

const primaryMutationCombinerMaxGroup = 64

type primaryMutationKind uint8

const (
	primaryMutationPut primaryMutationKind = iota + 1
	primaryMutationDelete
)

type primaryMutationRequest struct {
	kind  primaryMutationKind
	key   []byte
	value []byte

	created bool
	deleted bool
	err     error
	done    chan struct{}
}

// primaryMutationCombiner is a bounded arrival queue. It is only used for the
// journal-backed synchronous lane, where a group can replace N journal appends
// and N barriers with one batch append and one barrier. The queue is not a
// reader-visible structure: one worker drains it into Collection.Update, whose
// existing one-root publication keeps the group's state atomic.
type primaryMutationCombiner struct {
	mu       sync.Mutex
	cond     *sync.Cond
	queue    []*primaryMutationRequest
	capacity int
	groupMax int
	byteMax  int
	running  bool
	closed   bool
}

func newPrimaryMutationCombiner(
	queueSlots, groupMax, byteMax int,
) *primaryMutationCombiner {
	capacity := fileVisibilitySlots(queueSlots)
	groupMax = min(groupMax, capacity, primaryMutationCombinerMaxGroup)
	if groupMax < 1 {
		groupMax = 1
	}
	combiner := &primaryMutationCombiner{
		capacity: capacity,
		groupMax: groupMax,
		byteMax:  byteMax,
	}
	combiner.cond = sync.NewCond(&combiner.mu)
	return combiner
}

func (c *Collection) primaryMutationCombinerEligible() bool {
	if c == nil || c.mutationCombiner == nil || !c.syncJournalLane() ||
		c.onlineIndexBuild.Load() {
		return false
	}
	// Keep exact-indexed requests on their qualified direct publication path for
	// now: the concurrent combiner's admission/latency envelope has not yet been
	// certified for posting pressure. Read the epoch under the same gate
	// CreateIndex uses; a race simply falls back to the ordinary path.
	c.snapshotGate.RLock()
	indexed := c.primaryEpoch != nil
	c.snapshotGate.RUnlock()
	return !indexed
}

// shouldQueuePrimaryMutation keeps the uncontended single-writer path at its
// original cost. A caller joins the combiner only when another operation is
// currently holding the writer; a later caller may still race past the probe,
// which is valid because the queued operation has not completed and the two
// calls overlap.
func (c *Collection) shouldQueuePrimaryMutation(key, value []byte) bool {
	if !c.primaryMutationCombinerEligible() ||
		len(key) == 0 ||
		len(key) > c.options.MaxKeyBytes ||
		len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return false
	}
	if value != nil && (len(value) == 0 ||
		len(value) > c.options.InlineValueBytes) {
		return false
	}
	if c.writer.TryLock() {
		c.writer.Unlock()
		return false
	}
	return true
}

func (c *Collection) submitPrimaryMutation(
	kind primaryMutationKind,
	key, value []byte,
) (created, deleted bool, err error) {
	request := &primaryMutationRequest{
		kind: kind,
		key:  append([]byte(nil), key...),
		done: make(chan struct{}),
	}
	if len(value) != 0 {
		request.value = append([]byte(nil), value...)
	}

	combiner := c.mutationCombiner
	combiner.mu.Lock()
	for len(combiner.queue) >= combiner.capacity && !combiner.closed {
		combiner.cond.Wait()
	}
	if combiner.closed {
		combiner.mu.Unlock()
		return false, false, ErrClosed
	}
	combiner.queue = append(combiner.queue, request)
	if !combiner.running {
		combiner.running = true
		c.mutationWait.Add(1)
		go c.runPrimaryMutationCombiner()
	}
	combiner.mu.Unlock()

	<-request.done
	return request.created, request.deleted, request.err
}

func (c *Collection) runPrimaryMutationCombiner() {
	defer c.mutationWait.Done()
	for {
		// Let concurrent callers already inside the API reach the bounded queue.
		// This is the only intentional wait in this lane; the normal single-writer
		// path remains unchanged for non-synchronous collections.
		time.Sleep(primaryMutationCombineWindow)
		group := c.takePrimaryMutationGroup()
		if len(group) == 0 {
			return
		}
		c.applyPrimaryMutationGroup(group)
		for _, request := range group {
			close(request.done)
		}
	}
}

func (c *Collection) takePrimaryMutationGroup() []*primaryMutationRequest {
	combiner := c.mutationCombiner
	combiner.mu.Lock()
	defer combiner.mu.Unlock()
	if len(combiner.queue) == 0 {
		combiner.running = false
		combiner.cond.Broadcast()
		return nil
	}
	group := make([]*primaryMutationRequest, 0,
		min(len(combiner.queue), combiner.groupMax),
	)
	bytesUsed := 0
	for len(group) < combiner.groupMax && len(combiner.queue) != 0 {
		request := combiner.queue[0]
		requestBytes := len(request.key) + len(request.value)
		if len(group) != 0 &&
			bytesUsed > combiner.byteMax-requestBytes {
			break
		}
		combiner.queue = combiner.queue[1:]
		group = append(group, request)
		bytesUsed += requestBytes
	}
	combiner.cond.Broadcast()
	return group
}

func (c *Collection) applyPrimaryMutationGroup(
	group []*primaryMutationRequest,
) {
	if len(group) == 1 {
		c.applyPrimaryMutationDirect(group[0])
		return
	}
	if !c.primaryMutationCombinerEligible() {
		for _, request := range group {
			c.applyPrimaryMutationDirect(request)
		}
		return
	}

	// A prior one-document fast-path mutation may have left the bounded resident
	// overlay pending. Fold it first so Update starts from a canonical leaf image;
	// the fold is device-silent on this journal-backed lane because those records
	// were already synced before their original publication.
	if c.primaryUnifiedOverlay.hasPending() {
		c.writer.Lock()
		foldErr := c.materializePrimaryOverlayPressureLocked()
		c.writer.Unlock()
		if foldErr != nil {
			for _, request := range group {
				request.err = foldErr
			}
			return
		}
	}

	batchErr := c.Update(func(batch *WriteBatch) error {
		var seen [64]primaryMutationPresence
		seenCount := 0
		for _, request := range group {
			if request.err = c.validatePrimaryMutationRequest(request); request.err != nil {
				continue
			}
			present := false
			seenAt := -1
			for at := 0; at < seenCount; at++ {
				if bytes.Equal(seen[at].key, request.key) {
					seenAt = at
					present = seen[at].present
					break
				}
			}
			if seenAt < 0 {
				var presentErr error
				present, presentErr = c.primaryKeyPresentLocked(request.key)
				if presentErr != nil {
					request.err = presentErr
					continue
				}
				seen[seenCount] = primaryMutationPresence{
					key:     append(seen[seenCount].key[:0], request.key...),
					present: present,
				}
				seenCount++
			}
			switch request.kind {
			case primaryMutationPut:
				request.created = !present
				if seenAt < 0 {
					seen[seenCount-1].present = true
				} else {
					seen[seenAt].present = true
				}
				err := batch.Put(request.key, request.value)
				if err != nil {
					request.err = err
				}
			case primaryMutationDelete:
				request.deleted = present
				if seenAt < 0 {
					seen[seenCount-1].present = false
				} else {
					seen[seenAt].present = false
				}
				err := batch.Delete(request.key)
				if err != nil {
					request.err = err
				}
			default:
				request.err = errors.New("vibedb: invalid primary mutation kind")
			}
		}
		return nil
	})
	if batchErr == nil {
		return
	}
	// A live online-index race or a defensive lane change means no callback ran;
	// retry those requests through the exact single-document path. Other errors
	// may follow publication and therefore must be returned, never replayed.
	if errors.Is(batchErr, ErrPrimaryBatchUnsupportedLane) {
		for _, request := range group {
			if request.err == nil {
				c.applyPrimaryMutationDirect(request)
			}
		}
		return
	}
	for _, request := range group {
		if request.err == nil {
			request.created = false
			request.deleted = false
			request.err = batchErr
		}
	}
}

type primaryMutationPresence struct {
	key     []byte
	present bool
}

func (c *Collection) validatePrimaryMutationRequest(
	request *primaryMutationRequest,
) error {
	if len(request.key) == 0 ||
		len(request.key) > c.options.MaxKeyBytes ||
		len(request.key) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return ErrKeyTooLarge
	}
	if request.kind == primaryMutationDelete {
		return nil
	}
	if len(request.value) == 0 ||
		len(request.value) > c.options.MaxDocumentBytes ||
		len(request.value) > c.options.InlineValueBytes {
		return ErrDocumentTooLarge
	}
	if err := vibejson.Validate(request.value); err != nil {
		return err
	}
	return c.validatePrimarySchema(request.value)
}

// primaryKeyPresentLocked reads logical presence from the writer's newest
// state. It is deliberately narrower than AppendRaw: the combiner only needs
// Put/Delete result semantics, and this avoids taking a reader lease while the
// Update callback already owns c.writer.
func (c *Collection) primaryKeyPresentLocked(key []byte) (bool, error) {
	state := c.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return false, ErrClosed
	}
	route, err := c.currentPrimaryResidentRoute(state, key)
	if err != nil {
		return false, err
	}
	lease, err := c.primaryRouter.Load().AcquireLeaf(c.cache, route)
	if err != nil {
		return false, err
	}
	defer lease.Release()
	page := lease.Page()
	if storeio.PrimaryLeafClass(page) == storeio.CommonPrimaryLeafCompact {
		leaf, ok := storeio.AdmittedCompactPrimaryStripe(
			page, c.storeID, route.Bucket,
		)
		if !ok {
			return false, storeio.ErrCommonPrimaryLeafCorrupt
		}
		_, found := leaf.FindKey(key)
		if c.primaryUnifiedOverlay != nil {
			_, disposition, _ := c.primaryUnifiedOverlay.lookup(
				route.Bucket, route.Hash, key, state.root.Generation,
			)
			switch disposition {
			case primaryUnifiedOverlayValue:
				return true, nil
			case primaryUnifiedOverlayDeleted:
				return false, nil
			}
		}
		return found, nil
	}
	leaf, err := storeio.AdmittedPrimaryLeafForMutationWithScratch(
		page, c.storeID, route.Bucket,
		storeio.CommonPrimaryLeafBounds{
			FileEnd:           state.fileEnd,
			NextLogicalID:     state.root.NextLogicalID,
			AllocationQuantum: state.root.PageSize,
		}, c.primaryLeafMutationScratch,
	)
	if err != nil {
		return false, err
	}
	_, _, _, found := leaf.LookupRawHashed(route.Hash, key)
	return found, nil
}

func (c *Collection) applyPrimaryMutationDirect(
	request *primaryMutationRequest,
) {
	switch request.kind {
	case primaryMutationPut:
		request.created, request.err = c.putPrimaryWithSplit(
			request.key, request.value,
		)
	case primaryMutationDelete:
		request.deleted, request.err = c.deletePrimaryWithEmptyReclaim(request.key)
	default:
		request.err = errors.New("vibedb: invalid primary mutation kind")
	}
}

func (c *primaryMutationCombiner) stop() []*primaryMutationRequest {
	c.mu.Lock()
	c.closed = true
	pending := c.queue
	c.queue = nil
	c.cond.Broadcast()
	c.mu.Unlock()
	return pending
}
