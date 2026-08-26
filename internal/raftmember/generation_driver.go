package raftmember

import (
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// WALGenerationDriverOptions enables automatic WAL generation replacement on
// the Runtime's logical-tick lane. Capture, authority revalidation, selection,
// and activation remain serialized there; only the immutable candidate build
// runs on one bounded worker. OnError is observational only.
type WALGenerationDriverOptions struct {
	IntervalTicks uint64
	Key           raftstore.Key
	OnError       func(error)
}

type walGenerationDriver struct {
	interval          uint64
	ticks             uint64
	key               raftstore.Key
	workspace         []byte
	onError           func(error)
	activationPending bool
	building          bool
	stop              chan struct{}
	result            chan walGenerationBuildResult
	worker            sync.WaitGroup
	stopOnce          sync.Once
}

type walGenerationBuildResult struct {
	preparation *sqldriver.WALBasePreparation
	builder     *raftstore.GenerationBuilder
	checkpoint  uint64
	err         error
}

var walGenerationBuild = func(builder *raftstore.GenerationBuilder) error {
	_, err := builder.Build()
	return err
}

// ConfigureWALGeneration enables the production generation driver. It must be
// called by the exclusive Runtime owner before the Runtime is handed to Host.
func (runtime *Runtime) ConfigureWALGeneration(options WALGenerationDriverOptions) error {
	if err := runtime.checkUsable(); err != nil {
		return err
	}
	if options.IntervalTicks == 0 || options.Key.ID == "" ||
		options.Key.Material == ([32]byte{}) || runtime.walGeneration != nil {
		return ErrRuntimeOwnership
	}
	key := options.Key
	key.Wrapped = append([]byte(nil), options.Key.Wrapped...)
	runtime.walGeneration = &walGenerationDriver{
		interval:  options.IntervalTicks,
		key:       key,
		workspace: make([]byte, 0, replicatedstate.DefaultSnapshotArtifactChunkBytes),
		onError:   options.OnError,
		stop:      make(chan struct{}),
		result:    make(chan walGenerationBuildResult, 1),
	}
	return nil
}

func (runtime *Runtime) tickWALGeneration() {
	driver := runtime.walGeneration
	if driver == nil {
		return
	}
	if driver.activationPending {
		if err := runtime.commitWALGeneration(driver); err != nil && driver.onError != nil {
			driver.onError(err)
		}
		return
	}
	if driver.building {
		select {
		case result := <-driver.result:
			driver.building = false
			driver.ticks = 0
			if err := runtime.publishBuiltWALGeneration(driver, result); err != nil && driver.onError != nil {
				driver.onError(err)
			}
		default:
		}
		return
	}
	driver.ticks++
	if driver.ticks < driver.interval {
		return
	}
	driver.ticks = 0
	if err := runtime.prepareWALGenerationBuild(driver); err != nil && driver.onError != nil {
		driver.onError(err)
	}
}

func (runtime *Runtime) prepareWALGenerationBuild(driver *walGenerationDriver) error {
	checkpoint, err := runtime.WALRetentionInput()
	if err != nil {
		return err
	}
	base, err := runtime.wal.Snapshot()
	if err != nil {
		return err
	}
	// A generation which cannot advance the retained base has no deletion
	// benefit. This also prevents idle runtimes from manufacturing generations.
	if base.GetMetadata() == nil || checkpoint <= base.GetMetadata().GetIndex() {
		return nil
	}
	preparation, err := runtime.apply.CaptureWALBase(sqldriver.WALBaseCaptureOptions{
		Workspace: driver.workspace,
	})
	if err != nil {
		return err
	}
	input, err := preparation.GenerationInput()
	if err != nil {
		return err
	}
	if input.Snapshot.GetMetadata() == nil ||
		input.Snapshot.GetMetadata().GetIndex() <= base.GetMetadata().GetIndex() {
		return nil
	}
	builder, err := PrepareWALGeneration(runtime.wal, runtime.apply, preparation, driver.key)
	if err != nil {
		return err
	}
	driver.building = true
	driver.worker.Add(1)
	go func() {
		defer driver.worker.Done()
		result := walGenerationBuildResult{
			preparation: preparation, builder: builder, checkpoint: checkpoint,
			err: walGenerationBuild(builder),
		}
		if result.err != nil {
			result.err = errors.Join(result.err, builder.Close())
			result.builder = nil
		}
		select {
		case driver.result <- result:
		case <-driver.stop:
			if result.builder != nil {
				_ = result.builder.Close()
			}
		}
	}()
	return nil
}

func (runtime *Runtime) publishBuiltWALGeneration(
	driver *walGenerationDriver,
	result walGenerationBuildResult,
) error {
	if result.err != nil {
		return result.err
	}
	if result.builder == nil || result.preparation == nil {
		return ErrWALUnavailable
	}
	closeBuilder := func() error { return result.builder.Close() }
	checkpoint, err := runtime.WALRetentionInput()
	if err != nil || checkpoint != result.checkpoint {
		return errors.Join(err, closeBuilder())
	}
	if err := runtime.apply.ValidateWALBasePreparation(result.preparation); err != nil {
		return errors.Join(err, closeBuilder())
	}
	publishErr := PublishWALGeneration(
		runtime.wal, runtime.apply, result.preparation, result.builder,
	)
	closeErr := closeBuilder()
	if publishErr != nil {
		if _, pendingErr := runtime.wal.PendingGenerationActivation(); pendingErr == nil {
			driver.activationPending = true
		}
		return errors.Join(publishErr, closeErr)
	}
	driver.activationPending = true
	return errors.Join(runtime.commitWALGeneration(driver), closeErr)
}

func (runtime *Runtime) commitWALGeneration(driver *walGenerationDriver) error {
	if err := runtime.wal.CommitGenerationSelection(runtime.apply); err != nil {
		return err
	}
	driver.activationPending = false
	return nil
}

func (driver *walGenerationDriver) stopAndWait() {
	if driver == nil {
		return
	}
	driver.stopOnce.Do(func() { close(driver.stop) })
	driver.worker.Wait()
	select {
	case result := <-driver.result:
		if result.builder != nil {
			_ = result.builder.Close()
		}
	default:
	}
}

// OpenBoundSQLWithApplyRecoveringGeneration is the production restart path.
// It settles an authenticated selecting generation before returning the same
// ordinary bound handles used by a clean startup.
func OpenBoundSQLWithApplyRecoveringGeneration(
	path string,
	wal *raftstore.Store,
	authority sqldriver.ReplicatedAuthorityProfile,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	expectedApply sqldriver.ReplicatedApplyIdentity,
) (*sqldriver.Database, *sqldriver.ReplicatedApply, error) {
	if wal == nil {
		return nil, nil, ErrWALUnavailable
	}
	if _, err := wal.PendingGenerationActivation(); err != nil {
		if errors.Is(err, raftstore.ErrGenerationActivationPending) {
			return OpenBoundSQLWithApply(path, wal, authority, expectedSQL, expectedApply)
		}
		return nil, nil, err
	}
	database, apply, err := OpenBoundSQLWithApplyForGenerationActivation(
		path, wal, authority, expectedSQL, expectedApply,
	)
	if err != nil {
		return nil, nil, err
	}
	if err := wal.CommitGenerationSelection(apply); err != nil {
		return nil, nil, errors.Join(err, apply.Close(), database.Close())
	}
	return database, apply, nil
}
