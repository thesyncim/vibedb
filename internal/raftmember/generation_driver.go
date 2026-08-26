package raftmember

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// WALGenerationDriverOptions enables automatic WAL generation replacement on
// the Runtime's serialized logical-tick lane. IntervalTicks is deliberately a
// logical cadence: it introduces no second goroutine which could race Ready,
// apply, or shutdown ownership. OnError is observational only; generation
// maintenance never sacrifices Raft liveness after a maintenance failure.
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
	}
	return nil
}

func (runtime *Runtime) tickWALGeneration() {
	driver := runtime.walGeneration
	if driver == nil {
		return
	}
	driver.ticks++
	if !driver.activationPending && driver.ticks < driver.interval {
		return
	}
	driver.ticks = 0
	if err := runtime.driveWALGeneration(driver); err != nil && driver.onError != nil {
		driver.onError(err)
	}
}

func (runtime *Runtime) driveWALGeneration(driver *walGenerationDriver) error {
	if driver.activationPending {
		if err := runtime.wal.CommitGenerationSelection(runtime.apply); err != nil {
			return err
		}
		driver.activationPending = false
		return nil
	}
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
	defer func() {
		if closeErr := builder.Close(); closeErr != nil && driver.onError != nil {
			driver.onError(closeErr)
		}
	}()
	if _, err := builder.Build(); err != nil {
		return err
	}
	if err := PublishWALGeneration(runtime.wal, runtime.apply, preparation, builder); err != nil {
		return err
	}
	driver.activationPending = true
	if err := runtime.wal.CommitGenerationSelection(runtime.apply); err != nil {
		return err
	}
	driver.activationPending = false
	return nil
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
