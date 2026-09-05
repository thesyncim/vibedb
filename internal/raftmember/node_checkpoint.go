package raftmember

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

var (
	ErrNodeCheckpointCoordinator = errors.New("raftmember: node checkpoint coordinator unavailable")
	ErrNodeCheckpointPanic       = errors.New("raftmember: node checkpoint capture panicked")
)

// NodeCheckpointOptions enables asynchronous application checkpoints for one
// node-log Runtime. Logical ticks only schedule work; one shared coordinator
// performs bounded snapshot capture for every group attached to the device.
type NodeCheckpointOptions struct {
	IntervalTicks uint64
	OnError       func(error)
}

// NodeCheckpointCoordinator owns exactly one bounded state-checkpoint worker
// for a node durability sequencer. It deliberately does not own the sequencer
// or any Runtime. Close drains accepted capture tasks before stopping.
type NodeCheckpointCoordinator struct {
	sequencer *raftstore.NodeSubmissionSequencer
	queue     chan *nodeCheckpointTask
	done      chan struct{}
	workspace []byte
	mu        sync.Mutex
	closing   bool
	release   sync.Once
}

type nodeCheckpointTask struct {
	apply    *sqldriver.ReplicatedApply
	result   chan nodeCheckpointBuildResult
	queuedAt time.Time
}

type nodeCheckpointBuildResult struct {
	preparation *sqldriver.WALBasePreparation
	snapshot    *pb.Snapshot
	err         error
}

// NewNodeCheckpointCoordinator creates the single cold maintenance worker for
// one node. pendingGroups bounds memory and admission; the workspace is reused
// serially across all captures and never retained by a result.
func NewNodeCheckpointCoordinator(sequencer *raftstore.NodeSubmissionSequencer, pendingGroups int) (*NodeCheckpointCoordinator, error) {
	if sequencer == nil || pendingGroups < 1 || pendingGroups > 1<<20 {
		return nil, ErrNodeCheckpointCoordinator
	}
	if err := sequencer.ClaimMaintenanceLane(); err != nil {
		return nil, errors.Join(ErrNodeCheckpointCoordinator, err)
	}
	c := &NodeCheckpointCoordinator{
		sequencer: sequencer,
		queue:     make(chan *nodeCheckpointTask, pendingGroups),
		done:      make(chan struct{}),
		workspace: make([]byte, 0, replicatedstate.DefaultSnapshotArtifactChunkBytes),
	}
	go c.run()
	return c, nil
}

func (c *NodeCheckpointCoordinator) run() {
	defer close(c.done)
	for task := range c.queue {
		dequeuedAt := time.Now()
		if !task.queuedAt.IsZero() {
			c.sequencer.ObserveCheckpointQueueWait(dequeuedAt.Sub(task.queuedAt))
		}
		serviceStarted := time.Now()
		result := c.capture(task.apply)
		c.sequencer.ObserveCheckpointService(time.Since(serviceStarted))
		task.result <- result
	}
}

func (c *NodeCheckpointCoordinator) capture(apply *sqldriver.ReplicatedApply) (result nodeCheckpointBuildResult) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nodeCheckpointBuildResult{err: fmt.Errorf("%w: %v", ErrNodeCheckpointPanic, recovered)}
		}
	}()
	preparation, err := apply.CaptureWALBase(sqldriver.WALBaseCaptureOptions{Workspace: c.workspace})
	if err != nil {
		return nodeCheckpointBuildResult{err: err}
	}
	snapshot, err := preparation.SnapshotBase()
	if err != nil {
		return nodeCheckpointBuildResult{err: err}
	}
	return nodeCheckpointBuildResult{preparation: preparation, snapshot: snapshot}
}

func (c *NodeCheckpointCoordinator) submit(task *nodeCheckpointTask) error {
	if c == nil || task == nil || task.apply == nil || task.result == nil {
		return ErrNodeCheckpointCoordinator
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return ErrNodeCheckpointCoordinator
	}
	task.queuedAt = time.Now()
	select {
	case c.queue <- task:
		c.sequencer.ObserveCheckpointQueueSubmission()
		return nil
	default:
		c.sequencer.ObserveCheckpointQueueRejected()
		return raftstore.ErrSubmissionBackpressure
	}
}

func (c *NodeCheckpointCoordinator) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if !c.closing {
		c.closing = true
		close(c.queue)
	}
	c.mu.Unlock()
	<-c.done
	c.release.Do(c.sequencer.ReleaseMaintenanceLane)
	return nil
}

type nodeCheckpointDriver struct {
	coordinator *NodeCheckpointCoordinator
	interval    uint64
	ticks       uint64
	onError     func(error)
	task        nodeCheckpointTask
	result      nodeCheckpointBuildResult
	building    bool
	pending     bool
	submitted   bool
}

// ConfigureNodeCheckpointing attaches one Runtime to the node-wide checkpoint
// worker. It must be configured by the exclusive owner before Host adoption.
func (runtime *Runtime) ConfigureNodeCheckpointing(coordinator *NodeCheckpointCoordinator, options NodeCheckpointOptions) error {
	if err := runtime.checkUsable(); err != nil {
		return err
	}
	if runtime.nodePersistence == nil || coordinator == nil ||
		coordinator.sequencer != runtime.nodePersistence.sequencer ||
		options.IntervalTicks == 0 || runtime.nodeCheckpoint != nil {
		return ErrRuntimeOwnership
	}
	runtime.nodeCheckpoint = &nodeCheckpointDriver{
		coordinator: coordinator,
		interval:    options.IntervalTicks,
		onError:     options.OnError,
		task:        nodeCheckpointTask{result: make(chan nodeCheckpointBuildResult, 1)},
	}
	return nil
}

func (runtime *Runtime) driveNodeCheckpoint(advanceClock bool) error {
	driver := runtime.nodeCheckpoint
	if driver == nil {
		return nil
	}
	if driver.submitted {
		_, done, err := runtime.nodePersistence.pollCheckpoint()
		if !done {
			return nil
		}
		driver.submitted = false
		driver.pending = false
		driver.result = nodeCheckpointBuildResult{}
		if err != nil {
			if driver.onError != nil {
				driver.onError(err)
			}
			return err
		}
	}
	if driver.building {
		select {
		case driver.result = <-driver.task.result:
			driver.building = false
			driver.pending = true
		default:
			return nil
		}
	}
	if driver.pending {
		result := &driver.result
		if result.err != nil {
			runtime.reportNodeCheckpointError(result.err)
			driver.pending = false
			driver.result = nodeCheckpointBuildResult{}
			return nil
		}
		if result.preparation == nil || result.snapshot == nil {
			runtime.reportNodeCheckpointError(sqldriver.ErrWALBasePreparation)
			driver.pending = false
			driver.result = nodeCheckpointBuildResult{}
			return nil
		}
		if err := runtime.apply.ValidateWALBasePreparation(result.preparation); err != nil {
			runtime.reportNodeCheckpointError(err)
			driver.pending = false
			driver.result = nodeCheckpointBuildResult{}
			return nil
		}
		base, err := runtime.nodePersistence.stable.Snapshot()
		if err != nil {
			return err
		}
		if base.GetMetadata() == nil || result.snapshot.GetMetadata() == nil {
			return sqldriver.ErrWALBasePreparation
		}
		if result.snapshot.GetMetadata().GetIndex() <= base.GetMetadata().GetIndex() {
			driver.pending = false
			driver.result = nodeCheckpointBuildResult{}
			return nil
		}
		if _, err = runtime.nodePersistence.submitCheckpoint(result.snapshot); err != nil {
			if errors.Is(err, raftstore.ErrSubmissionBackpressure) {
				return nil
			}
			return err
		}
		driver.submitted = true
		return nil
	}
	if !advanceClock {
		return nil
	}
	driver.ticks++
	if driver.ticks < driver.interval {
		return nil
	}
	driver.ticks = 0
	if !runtime.walGenerationQuiescent() {
		return nil
	}
	base, err := runtime.nodePersistence.stable.Snapshot()
	if err != nil {
		return err
	}
	if base.GetMetadata() == nil || runtime.apply.Applied() <= base.GetMetadata().GetIndex() {
		return nil
	}
	driver.task.apply = runtime.apply
	if err = driver.coordinator.submit(&driver.task); err != nil {
		if errors.Is(err, raftstore.ErrSubmissionBackpressure) {
			driver.ticks = driver.interval
			return nil
		}
		runtime.reportNodeCheckpointError(err)
		return nil
	}
	driver.building = true
	return nil
}

func (runtime *Runtime) reportNodeCheckpointError(err error) {
	if err != nil && runtime.nodeCheckpoint != nil && runtime.nodeCheckpoint.onError != nil {
		runtime.nodeCheckpoint.onError(err)
	}
}

func (driver *nodeCheckpointDriver) stopAndWait(persistence *NodeRuntimePersistence) error {
	if driver == nil {
		return nil
	}
	var err error
	if driver.building {
		driver.result = <-driver.task.result
		driver.building = false
	}
	if driver.submitted {
		_, err = persistence.waitCheckpoint()
		driver.submitted = false
	}
	driver.pending = false
	driver.result = nodeCheckpointBuildResult{}
	driver.task.apply = nil
	return err
}
