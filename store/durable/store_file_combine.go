package durable

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// fileMutationCombiner is a bounded caller-assisted queue in front of the
// serialized page materializer. It exists only on the write side: a completed
// group publishes one ordinary canonical state root and leaves every read path
// byte-for-byte unchanged.
type fileMutationCombiner struct {
	mu       sync.Mutex
	changed  *sync.Cond
	slots    []fileMutationRequest
	free     []int
	queue    []int
	group    []int
	exists   []bool
	bytes    int
	maxBytes int
	leader   bool
	active   atomic.Bool
}

type fileMutationRequest struct {
	key     string
	value   []byte
	remove  bool
	changed bool
	bytes   int
	done    chan fileMutationResult
}

type fileMutationResult struct {
	changed bool
	err     error
}

func newFileMutationCombiner(slots, maxBytes int) *fileMutationCombiner {
	c := &fileMutationCombiner{
		slots:    make([]fileMutationRequest, slots),
		free:     make([]int, slots),
		queue:    make([]int, 0, slots),
		group:    make([]int, slots),
		exists:   make([]bool, slots),
		maxBytes: maxBytes,
	}
	c.changed = sync.NewCond(&c.mu)
	for i := range c.slots {
		c.slots[i].done = make(chan fileMutationResult, 1)
		c.free[i] = len(c.slots) - 1 - i
	}
	return c
}

func (c *Collection) combineFileMutation(
	key string, value []byte, remove bool,
) (bool, error) {
	combiner := c.combiner
	if combiner == nil {
		return false, ErrClosed
	}
	requestBytes := len(key) + len(value)
	combiner.mu.Lock()
	waited := false
	for len(combiner.free) == 0 ||
		requestBytes > combiner.maxBytes-combiner.bytes {
		if !waited {
			c.automaticMutationWaits.Add(1)
			waited = true
		}
		combiner.changed.Wait()
	}
	last := len(combiner.free) - 1
	index := combiner.free[last]
	combiner.free = combiner.free[:last]
	request := &combiner.slots[index]
	request.key = key
	request.value = value
	request.remove = remove
	request.bytes = requestBytes
	combiner.bytes += requestBytes
	combiner.queue = append(combiner.queue, index)
	updateAtomicMax32(&c.automaticMutationQueueHigh, uint32(len(combiner.queue)))
	updateAtomicMax64(&c.automaticMutationBytesHigh, uint64(combiner.bytes))
	leader := !combiner.leader
	if leader {
		combiner.leader = true
		combiner.active.Store(true)
	}
	combiner.mu.Unlock()

	if leader {
		c.runFileMutationCombiner()
	}
	result := <-request.done

	combiner.mu.Lock()
	request.key = ""
	request.value = nil
	request.remove = false
	request.changed = false
	combiner.bytes -= request.bytes
	request.bytes = 0
	combiner.free = append(combiner.free, index)
	combiner.changed.Broadcast()
	combiner.mu.Unlock()
	return result.changed, result.err
}

func (c *Collection) runFileMutationCombiner() {
	combiner := c.combiner
	for {
		// Wait for the current writer before taking the queue snapshot. This is
		// the combining opportunity: callers arriving behind that writer join
		// the same canonical materialization instead of being drained one at a
		// time while the leader itself is still blocked.
		c.writer.Lock()
		combiner.mu.Lock()
		if len(combiner.queue) == 0 {
			combiner.leader = false
			combiner.active.Store(false)
			combiner.mu.Unlock()
			c.writer.Unlock()
			return
		}
		count := len(combiner.queue)
		copy(combiner.group[:count], combiner.queue)
		group := combiner.group[:count]
		combiner.queue = combiner.queue[:0]
		combiner.mu.Unlock()

		generation, err := c.applyCombinedFileMutations(group)
		wait := generation != 0 && c.synchronous()
		if wait {
			c.durabilityWait.Add(1)
		}
		c.writer.Unlock()
		if wait {
			err = errors.Join(err, c.waitPublished(generation))
			c.durabilityWait.Done()
		}
		if err != nil {
			for _, index := range group {
				combiner.slots[index].done <- fileMutationResult{err: err}
			}
		} else {
			for _, index := range group {
				request := &combiner.slots[index]
				request.done <- fileMutationResult{changed: request.changed}
			}
		}
		c.automaticMutationGroups.Add(1)
		c.automaticMutationRequests.Add(uint64(count))
		updateAtomicMax32(&c.automaticMutationGroupHigh, uint32(count))
	}
}

// applyCombinedFileMutations materializes the final state once, then replays
// each overlapping request against the initial existence result to preserve
// arrival-order Put/Delete return values.
func (c *Collection) applyCombinedFileMutations(group []int) (uint64, error) {
	combiner := c.combiner
	if c.closed {
		return 0, ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return 0, failure
	}
	batch := c.fileWriteBatch()
	defer c.releaseFileWriteBatch(batch)
	// Only the last operation for each key belongs in the physical batch.
	// Traversing backwards avoids copying values that a later overlapping
	// request supersedes while the forward replay below still computes every
	// caller's result in arrival order.
	for i := len(group) - 1; i >= 0; i-- {
		index := group[i]
		request := &combiner.slots[index]
		if _, exists := batch.position[request.key]; exists {
			continue
		}
		if err := batch.record(request.key, request.value, request.remove); err != nil {
			return 0, err
		}
	}
	state := c.state.Load()
	if state == nil {
		return 0, ErrClosed
	}
	if err := c.ensureDirtyCapacity(); err != nil {
		return 0, err
	}
	published, err := c.applyFileBatch(state, batch)
	if err != nil {
		return 0, err
	}
	exists := combiner.exists[:len(batch.entries)]
	clear(exists)
	// resolveFileBatch keeps one exact document-backed result for every
	// effective mutation. Missing deletes are deliberately absent, so the
	// cleared default is their correct initial state. The chunk materializer
	// may reorder mutations by stable location; map their copied complete keys
	// back to the deduplicated batch instead of relying on lookup-rank scratch
	// from the retired full-key directory.
	for i := range c.batchMutations {
		mutation := &c.batchMutations[i]
		at, ok := batch.position[string(mutation.key)]
		if !ok {
			return 0, storeio.ErrKeyDirectoryCorrupt
		}
		exists[at] = !mutation.created
	}
	for _, index := range group {
		request := &combiner.slots[index]
		at := batch.position[request.key]
		if request.remove {
			request.changed = exists[at]
			exists[at] = false
		} else {
			request.changed = !exists[at]
			exists[at] = true
		}
	}
	if !published {
		return 0, nil
	}
	return state.root.Generation + 1, nil
}

func (c *Collection) fileWriteBatch() *WriteBatch {
	if c.batch == nil {
		c.batch = &WriteBatch{
			collection: c,
			position:   make(map[string]int, c.options.MaxBatchDocuments),
		}
	}
	batch := c.batch
	batch.reset()
	batch.active = true
	return batch
}

func (c *Collection) releaseFileWriteBatch(batch *WriteBatch) {
	batch.active = false
	batch.reset()
}

func updateAtomicMax32(value *atomic.Uint32, candidate uint32) {
	for old := value.Load(); candidate > old &&
		!value.CompareAndSwap(old, candidate); old = value.Load() {
	}
}

func updateAtomicMax64(value *atomic.Uint64, candidate uint64) {
	for old := value.Load(); candidate > old &&
		!value.CompareAndSwap(old, candidate); old = value.Load() {
	}
}
