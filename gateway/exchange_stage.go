package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/exchange"
	"github.com/thesyncim/vibedb/shardservice"
)

// exchangeStage owns one mailbox partition on each routed target. The gateway
// is only the lifecycle coordinator: producers can use the sink directly and
// mailbox backpressure remains enforced by the destination worker.
type exchangeStage struct {
	client      *Client
	targets     []shardCall
	key         exchange.Key
	spec        exchange.Spec
	deadline    time.Duration
	concurrency int
}

func (s *exchangeStage) repartitionRequest(
	producer uint16,
	keyColumns []uint16,
	blockRows, blockBytes uint32,
	maxMemory uint64,
) shardservice.RepartitionRequest {
	request := shardservice.RepartitionRequest{
		Operation: s.key.Operation, Stage: s.key.Stage, Attempt: s.key.Attempt,
		Producer: producer, KeyColumns: append([]uint16(nil), keyColumns...),
		Targets:   make([]shardservice.RepartitionTarget, len(s.targets)),
		BlockRows: blockRows, BlockBytes: blockBytes, MaxMemory: maxMemory,
	}
	for partition := range s.targets {
		call := s.targets[partition]
		request.Targets[partition] = shardservice.RepartitionTarget{
			Address: []byte(call.address), Distribution: call.req.Distribution,
			Shard: call.target.Shard, AllocationGeneration: call.target.AllocationGeneration,
			RoutingVersion: call.req.RoutingVersion, OwnershipEpoch: call.target.OwnershipEpoch,
		}
	}
	return request
}

func newExchangeStage(
	client *Client,
	targets []shardCall,
	key exchange.Key,
	spec exchange.Spec,
	deadline time.Duration,
	concurrency int,
) (*exchangeStage, error) {
	if client == nil || len(targets) == 0 || len(targets) > exchange.MaxPartitions ||
		key.Operation.IsZero() || concurrency <= 0 || deadline < 0 {
		return nil, ErrExchangeProducer
	}
	for i := range targets {
		call := targets[i]
		if call.req == nil || call.address == "" || call.target.Shard == "" ||
			call.target.AllocationGeneration == 0 || call.target.OwnershipEpoch == 0 ||
			call.req.Distribution == "" || call.req.RoutingVersion == 0 {
			return nil, ErrExchangeProducer
		}
	}
	spec.Key = key
	spec.Key.Partition = 0
	spec.DeadlineUnixNano = 0
	if !spec.Valid() {
		return nil, errors.Join(ErrExchangeProducer, exchange.ErrInvalidSpec)
	}
	ownedTargets := make([]shardCall, len(targets))
	for i := range targets {
		ownedTargets[i] = targets[i]
		request := *targets[i].req
		// Only immutable routing coordinates are retained. SQL, parameters, and
		// other execution envelopes cannot accidentally leak into control calls.
		ownedTargets[i].req = &shardservice.ShardRequest{
			Distribution: request.Distribution, RoutingVersion: request.RoutingVersion,
		}
	}
	return &exchangeStage{
		client: client, targets: ownedTargets,
		key: key, spec: spec, deadline: deadline,
		concurrency: min(concurrency, len(targets)),
	}, nil
}

func (s *exchangeStage) keyFor(partition uint32) exchange.Key {
	key := s.key
	key.Partition = partition
	return key
}

func (s *exchangeStage) request(partition uint32, operation shardservice.ExchangeOperation) *shardservice.ShardRequest {
	call := s.targets[partition]
	mode := shardservice.ExecutionReadWrite
	if operation == shardservice.ExchangePull {
		mode = shardservice.ExecutionReadOnly
	}
	return &shardservice.ShardRequest{
		Distribution: call.req.Distribution, Shard: call.target.Shard,
		AllocationGeneration: call.target.AllocationGeneration,
		RoutingVersion:       call.req.RoutingVersion, OwnershipEpoch: call.target.OwnershipEpoch,
		ExecutionMode: mode, Deadline: s.deadline,
		Exchange: shardservice.ExchangeRequest{Operation: operation, Key: s.keyFor(partition)},
	}
}

func (s *exchangeStage) Open(ctx context.Context) error {
	err := s.parallel(ctx, func(ctx context.Context, partition uint32) error {
		req := s.request(partition, shardservice.ExchangeOpen)
		req.Exchange.Producers = s.spec.Producers
		req.Exchange.QueuedBatches = s.spec.QueuedBatches
		req.Exchange.ProducerBatches = s.spec.ProducerBatches
		req.Exchange.BufferedRows = s.spec.BufferedRows
		req.Exchange.BufferedBytes = s.spec.BufferedBytes
		req.Exchange.TotalRows = s.spec.TotalRows
		req.Exchange.TotalBytes = s.spec.TotalBytes
		if err := s.do(ctx, partition, req, shardservice.ExchangeOpen); err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		return nil
	}
	// Cleanup is bounded independently of the canceled operation context. The
	// original failure remains primary; cancellation errors are joined only for
	// observability.
	cleanupBudget := s.deadline
	if cleanupBudget <= 0 || cleanupBudget > 5*time.Second {
		cleanupBudget = 5 * time.Second
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupBudget)
	defer cancel()
	// Cancel every attempted partition: an Open may have reached the worker even
	// when its response was lost, and Cancel is intentionally idempotent.
	cleanupErr := s.parallel(cleanupCtx, s.cancelPartition)
	return errors.Join(err, cleanupErr)
}

func (s *exchangeStage) Push(ctx context.Context, partition uint32, batch exchange.Batch) error {
	if s == nil || int(partition) >= len(s.targets) {
		return exchange.ErrPartitions
	}
	req := s.request(partition, shardservice.ExchangePush)
	req.Exchange.Batch = batch
	return s.do(ctx, partition, req, shardservice.ExchangePush)
}

func (s *exchangeStage) Pull(
	ctx context.Context,
	partition uint32,
	hasAck bool,
	ackProducer uint16,
	ackSequence uint32,
) (exchange.Batch, bool, error) {
	if s == nil || int(partition) >= len(s.targets) {
		return exchange.Batch{}, false, exchange.ErrPartitions
	}
	req := s.request(partition, shardservice.ExchangePull)
	req.Exchange.HasAck = hasAck
	req.Exchange.AckProducer = ackProducer
	req.Exchange.AckSequence = ackSequence
	resp, err := s.client.Do(ctx, s.targets[partition].address, req)
	if err != nil {
		return exchange.Batch{}, false, err
	}
	if resp.Kind != shardservice.ResponseCompletion ||
		resp.Exchange.Operation != shardservice.ExchangePull {
		return exchange.Batch{}, false, ErrUnexpectedError
	}
	return resp.Exchange.Batch, resp.Exchange.EOF, nil
}

func (s *exchangeStage) Cancel(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.parallel(ctx, s.cancelPartition)
}

func (s *exchangeStage) cancelPartition(ctx context.Context, partition uint32) error {
	req := s.request(partition, shardservice.ExchangeCancel)
	return s.do(ctx, partition, req, shardservice.ExchangeCancel)
}

func (s *exchangeStage) do(
	ctx context.Context,
	partition uint32,
	req *shardservice.ShardRequest,
	want shardservice.ExchangeOperation,
) error {
	resp, err := s.client.Do(ctx, s.targets[partition].address, req)
	if err != nil {
		return err
	}
	if resp.Kind != shardservice.ResponseCompletion || resp.Exchange.Operation != want {
		return fmt.Errorf("%w: exchange %d returned %s/%d",
			ErrUnexpectedError, partition, resp.Kind, resp.Exchange.Operation)
	}
	return nil
}

func (s *exchangeStage) parallel(
	ctx context.Context,
	operation func(context.Context, uint32) error,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan uint32)
	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	fail := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}
	for range s.concurrency {
		wg.Go(func() {
			for partition := range jobs {
				if err := operation(workCtx, partition); err != nil {
					fail(err)
					return
				}
			}
		})
	}
	go func() {
		defer close(jobs)
		for partition := range s.targets {
			select {
			case jobs <- uint32(partition):
			case <-workCtx.Done():
				return
			}
		}
	}()
	wg.Wait()
	errMu.Lock()
	err := firstErr
	errMu.Unlock()
	if err != nil {
		return err
	}
	return ctx.Err()
}
