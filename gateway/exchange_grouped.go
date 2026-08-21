package gateway

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedagg"
	"github.com/thesyncim/vibedb/internal/exchange"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

type groupedExchangeLimits struct {
	blockRows, blockBytes uint32
	producerMemory        uint64
	reducerMemory         uint64
	input, output         exchange.Spec
}

const exchangeJSONOID int32 = 114

func makeGroupedExchangeLimits(profile Profile, partitions int, columns int) (groupedExchangeLimits, error) {
	if partitions < 2 || partitions > int(exchange.MaxProducers) || columns <= 0 ||
		columns > shardservice.MaxExchangeReducerColumns || profile.MaxAggregateRows == 0 ||
		profile.MaxAggregateBytes == 0 || profile.MaxAggregateRows > exchange.MaxMailboxRows ||
		profile.MaxAggregateBytes > exchange.MaxMailboxBytes {
		return groupedExchangeLimits{}, ErrExchangeProducer
	}
	rows := min(uint64(distributedBatchRows), profile.MaxAggregateRows)
	rows = min(rows, exchange.MaxBlockCells/uint64(columns))
	perPartitionBytes := profile.MaxAggregateBytes / uint64(partitions)
	if rows == 0 || perPartitionBytes < 64 {
		return groupedExchangeLimits{}, ErrExchangeProducer
	}
	blockBytes := uint32(min(uint64(distributedBatchBytes), perPartitionBytes))
	if !exchange.ValidBlockLimits(uint32(columns), uint32(rows), blockBytes) {
		return groupedExchangeLimits{}, ErrExchangeProducer
	}
	producerMemory := uint64(partitions) * uint64(blockBytes)
	if producerMemory > profile.MaxAggregateBytes {
		return groupedExchangeLimits{}, ErrExchangeProducer
	}
	bufferedBytes := min(perPartitionBytes, uint64(blockBytes)*4)
	bufferedBytes = max(uint64(blockBytes), bufferedBytes)
	bufferedRows := min(profile.MaxAggregateRows, max(rows, profile.MaxAggregateRows/uint64(partitions)))
	queued := min(uint64(exchange.MaxQueuedBatches), max(uint64(partitions), bufferedBytes/uint64(blockBytes)+1))
	base := exchange.Spec{
		QueuedBatches: uint16(queued), ProducerBatches: 1,
		BufferedRows: bufferedRows, BufferedBytes: bufferedBytes,
		TotalRows: profile.MaxAggregateRows, TotalBytes: profile.MaxAggregateBytes,
	}
	input := base
	input.Producers = uint16(partitions)
	output := base
	output.Producers = 1
	return groupedExchangeLimits{
		blockRows: uint32(rows), blockBytes: blockBytes,
		producerMemory: producerMemory, reducerMemory: perPartitionBytes,
		input: input, output: output,
	}, nil
}

func distributedAggregateProgram(
	kinds []sqlast.AggKind,
	groupKeys []int,
) ([]distributedagg.Kind, []uint16, error) {
	program := make([]distributedagg.Kind, len(kinds))
	for i, kind := range kinds {
		var err error
		program[i], err = distributedAggregateKind(kind)
		if err != nil {
			return nil, nil, err
		}
	}
	keys := make([]uint16, len(groupKeys))
	for i, column := range groupKeys {
		if column < 0 || column > int(^uint16(0)) {
			return nil, nil, ErrMergeColumn
		}
		keys[i] = uint16(column)
	}
	return program, keys, nil
}

func distributedAggregateKind(kind sqlast.AggKind) (distributedagg.Kind, error) {
	switch kind {
	case sqlast.AggNone:
		return distributedagg.None, nil
	case sqlast.AggCount:
		return distributedagg.Count, nil
	case sqlast.AggSum:
		return distributedagg.Sum, nil
	case sqlast.AggMin:
		return distributedagg.Min, nil
	case sqlast.AggMax:
		return distributedagg.Max, nil
	default:
		return distributedagg.None, fmt.Errorf("%w: aggregate %s has no exchange reducer", ErrMergeAggregate, kind)
	}
}

// fanoutRepartitionGrouped executes the physical OpRepartition shape. Source
// shards stream partial groups directly to destination workers; reducers drain
// those inputs concurrently and stream disjoint finalized partitions back.
func (e *Executor) fanoutRepartitionGrouped(
	ctx context.Context,
	pl *plan,
	profile Profile,
) (*Result, error) {
	if len(pl.calls) < 2 || len(pl.aggregates) == 0 || len(pl.groupKeys) == 0 {
		return nil, ErrExchangeProducer
	}
	program, keys, err := distributedAggregateProgram(pl.aggregates, pl.groupKeys)
	if err != nil {
		return nil, err
	}
	limits, err := makeGroupedExchangeLimits(profile, len(pl.calls), len(program))
	if err != nil {
		return nil, err
	}
	var operation exchange.ID
	if _, err := cryptorand.Read(operation[:]); err != nil {
		return nil, err
	}
	inputKey := exchange.Key{Operation: operation, Stage: 1, Attempt: 1}
	outputKey := exchange.Key{Operation: operation, Stage: 2, Attempt: 1}
	input, err := newExchangeStage(
		e.client, pl.calls, inputKey, limits.input, profile.GlobalDeadline, profile.MaxConcurrency,
	)
	if err != nil {
		return nil, err
	}
	output, err := newExchangeStage(
		e.client, pl.calls, outputKey, limits.output, profile.GlobalDeadline, profile.MaxConcurrency,
	)
	if err != nil {
		return nil, err
	}
	if err := input.Open(ctx); err != nil {
		return nil, err
	}
	if err := output.Open(ctx); err != nil {
		return nil, errors.Join(err, cancelExchangeStages(input, output))
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	repartition := input.repartitionRequest(
		0, keys, limits.blockRows, limits.blockBytes, limits.producerMemory,
	)
	rowsByPartition := make([][][]shardservice.Cell, len(pl.calls))
	var totals exchangeResultTotals
	done := make(chan error, 3)
	go func() {
		done <- runExchangePartitions(workCtx, len(pl.calls), len(pl.calls),
			func(ctx context.Context, partition int) error {
				return consumeReducedPartition(
					ctx, output, uint32(partition), len(program), profile,
					&totals, &rowsByPartition[partition],
				)
			})
	}()
	go func() {
		done <- runExchangePartitions(workCtx, len(pl.calls), len(pl.calls),
			func(ctx context.Context, partition int) error {
				return input.Reduce(
					ctx, uint32(partition), output, program, keys,
					limits.reducerMemory, limits.blockRows, limits.blockBytes,
				)
			})
	}()
	go func() {
		done <- runExchangePartitions(workCtx, len(pl.calls), profile.MaxConcurrency,
			func(ctx context.Context, producer int) error {
				req := *pl.calls[producer].req
				req.PartialAggregate = true
				req.Repartition = repartition
				req.Repartition.Producer = uint16(producer)
				sourceCtx, stop := context.WithTimeout(ctx, profile.PerShardDeadline)
				defer stop()
				resp, err := e.client.Do(sourceCtx, pl.calls[producer].address, &req)
				if err != nil {
					return err
				}
				if resp.Kind != shardservice.ResponseCompletion {
					return ErrUnexpectedError
				}
				return nil
			})
	}()

	var firstErr error
	for range 3 {
		if stageErr := <-done; stageErr != nil && firstErr == nil {
			firstErr = stageErr
			cancel()
		}
	}
	cleanupErr := cancelExchangeStages(input, output)
	if firstErr != nil {
		return nil, errors.Join(firstErr, cleanupErr)
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, cleanupErr)
	}
	if cleanupErr != nil {
		return nil, cleanupErr
	}

	rows := make([][]shardservice.Cell, 0, totals.rows)
	for partition := range rowsByPartition {
		rows = append(rows, rowsByPartition[partition]...)
	}
	rows, err = finalizeGroupedRowsWindow(
		rows, pl.order, pl.offset, pl.limit, pl.hasLimit, profile.MaxAggregateBytes,
	)
	if err != nil {
		return nil, err
	}
	columns := make([]shardservice.Column, len(pl.aggHeaders))
	for i := range columns {
		columns[i] = shardservice.Column{Name: pl.aggHeaders[i], TypeOID: exchangeJSONOID}
	}
	e.metrics.observeResult(totals.rows, totals.bytes)
	return &Result{Kind: shardservice.ResponseRows, Columns: columns, Rows: rows}, nil
}

type exchangeResultTotals struct {
	mu    sync.Mutex
	rows  uint64
	bytes uint64
}

func (t *exchangeResultTotals) add(profile Profile, rows, bytes uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ^uint64(0)-t.rows < rows || ^uint64(0)-t.bytes < bytes {
		return ErrResultLimit
	}
	t.rows += rows
	t.bytes += bytes
	return checkAggregate(profile, t.rows, t.bytes)
}

func consumeReducedPartition(
	ctx context.Context,
	stage *exchangeStage,
	partition uint32,
	columns int,
	profile Profile,
	totals *exchangeResultTotals,
	rows *[][]shardservice.Cell,
) error {
	var hasAck bool
	var ackProducer uint16
	var ackSequence uint32
	for {
		batch, eof, err := stage.Pull(ctx, partition, hasAck, ackProducer, ackSequence)
		if err != nil {
			return err
		}
		if eof {
			return nil
		}
		hasAck, ackProducer, ackSequence = true, batch.Producer, batch.Sequence
		if batch.Rows == 0 {
			continue
		}
		block, err := exchange.OpenBlock(batch.Data)
		if err != nil || block.Columns() != uint32(columns) || block.Rows() != batch.Rows {
			return errors.Join(ErrMergeSchema, err)
		}
		cellCount := int(batch.Rows) * columns
		cells := make([]shardservice.Cell, cellCount)
		partitionRows := make([][]shardservice.Cell, 0, batch.Rows)
		var valueBytes uint64
		for row := 0; row < int(batch.Rows); row++ {
			decoded := cells[row*columns : (row+1)*columns : (row+1)*columns]
			if !block.NextInto(decoded) {
				return ErrMergeSchema
			}
			for column := range decoded {
				if ^uint64(0)-valueBytes < uint64(len(decoded[column].Bytes)) {
					return ErrResultLimit
				}
				valueBytes += uint64(len(decoded[column].Bytes))
			}
			partitionRows = append(partitionRows, decoded)
		}
		if err := totals.add(profile, uint64(batch.Rows), valueBytes); err != nil {
			return err
		}
		*rows = append(*rows, partitionRows...)
	}
}

func runExchangePartitions(
	ctx context.Context,
	partitions int,
	concurrency int,
	operation func(context.Context, int) error,
) error {
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	workers := min(max(1, concurrency), partitions)
	var wait sync.WaitGroup
	var once sync.Once
	var firstErr error
	for range workers {
		wait.Go(func() {
			for partition := range jobs {
				if err := operation(workCtx, partition); err != nil {
					once.Do(func() { firstErr = err; cancel() })
					return
				}
			}
		})
	}
	for partition := 0; partition < partitions; partition++ {
		select {
		case jobs <- partition:
		case <-workCtx.Done():
			partition = partitions
		}
	}
	close(jobs)
	wait.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func cancelExchangeStages(stages ...*exchangeStage) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errs := make([]error, 0, len(stages))
	for _, stage := range stages {
		if stage != nil {
			errs = append(errs, stage.Cancel(ctx))
		}
	}
	return errors.Join(errs...)
}
