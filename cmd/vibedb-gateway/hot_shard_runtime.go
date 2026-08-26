package main

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

type gatewayHotShardRuntime struct {
	mu              sync.Mutex
	holder          *gateway.CatalogHolder
	authority       gatewayHotShardPressureAuthority
	config          hotshard.StaticCapacityConfig
	controller      *hotshard.Controller
	operations      gatewayHotShardOperationAuthorities
	operationsBound bool
	admitted        uint64
	collector       atomic.Pointer[hotshard.Collector]
	generation      atomic.Uint64
}

type gatewayHotShardPressureAuthority interface {
	hotshard.ReplicatedDirectory
	hotshard.PressurePublisher
}

// gatewayHotShardOperationAuthorities are deliberately colder than request
// routing. A nil member is absence of exact topology authority, never a cue to
// synthesize an allocation, member, WAL, or store identity.
type gatewayHotShardOperationAuthorities struct {
	splits  hotshard.SplitPlanFactory
	journal splitcontroller.ReplicatedOperationJournal
	moves   hotshard.MovePlanFactory
	moveRun hotshard.MoveSubmitter
}

type refusingGatewayHotShardSink struct{}

func (refusingGatewayHotShardSink) SubmitHotShardAdmission(context.Context, hotshard.Admission) error {
	return hotshard.ErrInvalidPressureCut
}

func loadGatewayHotShardCapacity(path string) (hotshard.StaticCapacityConfig, error) {
	if path == "" {
		return hotshard.StaticCapacityConfig{}, hotshard.ErrInvalidPressureCut
	}
	file, err := os.Open(path)
	if err != nil {
		return hotshard.StaticCapacityConfig{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() <= 0 || info.Size() > hotshard.MaxStaticCapacityBytes {
		return hotshard.StaticCapacityConfig{}, errors.Join(err, hotshard.ErrInvalidPressureCut)
	}
	raw := make([]byte, int(info.Size()))
	if _, err = io.ReadFull(file, raw); err != nil {
		return hotshard.StaticCapacityConfig{}, err
	}
	return hotshard.OpenStaticCapacityConfig(raw)
}

func newGatewayHotShardRuntime(
	ctx context.Context,
	holder *gateway.CatalogHolder,
	authority *gateway.ReplicatedCatalogAuthority,
	config hotshard.StaticCapacityConfig,
) (*gatewayHotShardRuntime, error) {
	if ctx == nil || holder == nil || authority == nil || holder.Current() == nil {
		return nil, hotshard.ErrInvalidPressureCut
	}
	runtime := &gatewayHotShardRuntime{holder: holder, authority: authority, config: config}
	if err := runtime.rebuild(ctx); err != nil {
		return nil, err
	}
	return runtime, nil
}

// InstallOperationAuthorities binds the already-shipped operation journals.
// It is a startup-only seam: replacing authority while a pressure cut is being
// retried could change an outcome-unknown admission into different bytes.
func (runtime *gatewayHotShardRuntime) InstallOperationAuthorities(
	authorities gatewayHotShardOperationAuthorities,
) bool {
	splits := authorities.splits != nil || authorities.journal != nil
	moves := authorities.moves != nil || authorities.moveRun != nil
	if runtime == nil || !splits && !moves ||
		splits && (authorities.splits == nil || authorities.journal == nil) ||
		moves && (authorities.moves == nil || authorities.moveRun == nil) {
		return false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.operationsBound {
		return false
	}
	runtime.operations = authorities
	runtime.operationsBound = true
	return true
}

func (runtime *gatewayHotShardRuntime) ObservePressure(observation gateway.PressureObservation) {
	if runtime == nil {
		return
	}
	if collector := runtime.collector.Load(); collector != nil {
		collector.ObservePressure(observation)
	}
}

// PublishOnce detaches recorder lanes and performs catalog-Raft I/O outside
// request execution. Wall time decides only when to try; the replicated
// revision remains the sole evidence/cooldown authority.
func (runtime *gatewayHotShardRuntime) PublishOnce(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return hotshard.ErrInvalidPressureCut
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	snapshot := runtime.holder.Current()
	if snapshot == nil {
		return gateway.ErrNoCatalog
	}
	if runtime.generation.Load() != snapshot.Generation() {
		if err := runtime.rebuild(ctx); err != nil {
			return err
		}
	}
	if runtime.admitted == snapshot.Generation() {
		return nil
	}
	collector := runtime.collector.Load()
	if collector == nil {
		return hotshard.ErrInvalidPressureCut
	}
	// Settle the current replicated cut before publishing a successor. This is
	// what makes an outcome-unknown operation submission retry byte-identically
	// instead of skipping ahead to a later pressure revision.
	pass, err := runtime.runPressurePass(ctx, snapshot)
	if err != nil &&
		!errors.Is(err, gateway.ErrReplicatedPressureMissing) {
		return err
	}
	if err == nil && !pass.Admission.Empty() {
		runtime.admitted = snapshot.Generation()
		return nil
	}
	nodes := runtime.config.NodeCapacities(snapshot.Generation())
	if _, err = collector.Publish(ctx, runtime.authority, nodes); err != nil {
		return err
	}
	pass, err = runtime.runPressurePass(ctx, snapshot)
	if err == nil && !pass.Admission.Empty() {
		runtime.admitted = snapshot.Generation()
	}
	return err
}

func (runtime *gatewayHotShardRuntime) runPressurePass(
	ctx context.Context, snapshot *gateway.Snapshot,
) (hotshard.Pass, error) {
	if runtime.controller == nil {
		return hotshard.Pass{}, hotshard.ErrInvalidPressureCut
	}
	record, err := runtime.authority.ReadPressureRecord(ctx)
	if err != nil {
		return hotshard.Pass{}, err
	}
	if record.CatalogGeneration < snapshot.Generation() {
		return hotshard.Pass{}, nil
	}
	if record.CatalogGeneration > snapshot.Generation() {
		return hotshard.Pass{}, hotshard.ErrInvalidPressureCut
	}
	var sink hotshard.Sink = refusingGatewayHotShardSink{}
	operations := runtime.operations
	if runtime.operationsBound {
		sink = hotshard.OperationSink{Catalog: snapshot, Splits: operations.splits,
			Journal: operations.journal, Moves: operations.moves, MoveRun: operations.moveRun}
	}
	return hotshard.RunReplicatedPass(
		ctx, snapshot, runtime.authority, runtime.controller, sink,
	)
}

func (runtime *gatewayHotShardRuntime) rebuild(ctx context.Context) error {
	snapshot := runtime.holder.Current()
	if snapshot == nil {
		return gateway.ErrNoCatalog
	}
	for _, node := range runtime.config.Nodes {
		if _, err := snapshot.Address(node.Endpoint); err != nil {
			return errors.Join(err, hotshard.ErrInvalidPressureCut)
		}
	}
	first := uint64(1)
	record, err := runtime.authority.ReadPressureRecord(ctx)
	if err == nil {
		if record.CatalogGeneration > snapshot.Generation() || record.AuthorityRevision == ^uint64(0) {
			return hotshard.ErrInvalidPressureCut
		}
		first = record.AuthorityRevision + 1
	} else if !errors.Is(err, gateway.ErrReplicatedPressureMissing) {
		return err
	}
	collector, err := hotshard.NewCollector(snapshot, first, int(runtime.config.RecorderLanes),
		hotshard.StaticCapacityProvider{Capacity: runtime.config.WindowCapacity,
			MigrationBytes: runtime.config.ShardMigrationBytes}, autosplit.DefaultPolicy())
	if err != nil {
		return err
	}
	controller, err := hotshard.New(hotshard.DefaultPolicy())
	if err != nil {
		return err
	}
	runtime.collector.Store(collector)
	runtime.controller = controller
	runtime.admitted = 0
	runtime.generation.Store(snapshot.Generation())
	return nil
}

func runGatewayHotShardPublisher(
	ctx context.Context, runtime *gatewayHotShardRuntime, interval time.Duration,
	logf func(string, ...any),
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := runtime.PublishOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
					logf("gateway: publish hot-shard pressure: %v", err)
				}
			}
		}
	}()
	return done
}

var _ gateway.PressureObserver = (*gatewayHotShardRuntime)(nil)
var _ hotshard.CapacityProvider = hotshard.StaticCapacityProvider{}
