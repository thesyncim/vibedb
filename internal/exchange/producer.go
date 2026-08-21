package exchange

import (
	"context"
	"errors"
	"fmt"
)

var ErrProducerState = errors.New("exchange: partitioned producer failed")

// Partitioner maps one canonical row to a fixed stage partition.
type Partitioner interface {
	Partition([]Cell) (uint32, error)
}

// BatchSink synchronously consumes one borrowed batch. Returning nil transfers
// the bytes across the transport boundary and permits arena reuse.
type BatchSink interface {
	Push(context.Context, uint32, Batch) error
}

// Producer partitions rows into bounded reusable blocks. Per-partition
// sequences advance only after a synchronous push succeeds; Finish terminates
// every partition, including partitions that received no rows.
type Producer struct {
	partitioner Partitioner
	blocks      *PartitionedBlocks
	sequences   []uint32
	producer    uint16
	sink        BatchSink
	finished    bool
}

func NewProducer(
	partitioner Partitioner,
	partitions, columns, maxRows, maxBytes uint32,
	maxMemory uint64,
	producer uint16,
	sink BatchSink,
) (*Producer, error) {
	if partitioner == nil || sink == nil || producer >= MaxProducers {
		return nil, ErrProducerState
	}
	blocks, err := NewPartitionedBlocks(partitions, columns, maxRows, maxBytes, maxMemory)
	if err != nil {
		return nil, errors.Join(ErrProducerState, err)
	}
	return &Producer{
		partitioner: partitioner, blocks: blocks,
		sequences: make([]uint32, partitions), producer: producer, sink: sink,
	}, nil
}

func (p *Producer) Add(ctx context.Context, row []Cell) error {
	if p == nil || p.finished {
		return ErrProducerState
	}
	partition, err := p.partitioner.Partition(row)
	if err != nil {
		return errors.Join(ErrProducerState, err)
	}
	flush, err := p.blocks.Append(partition, row)
	if err != nil {
		return errors.Join(ErrProducerState, err)
	}
	if !flush {
		return nil
	}
	if err := p.push(ctx, partition, false); err != nil {
		return err
	}
	if err := p.blocks.ResetPartition(partition); err != nil {
		return errors.Join(ErrProducerState, err)
	}
	flush, err = p.blocks.Append(partition, row)
	if err != nil || flush {
		return errors.Join(ErrProducerState, err)
	}
	return nil
}

func (p *Producer) Finish(ctx context.Context) error {
	if p == nil || p.finished {
		return ErrProducerState
	}
	for partition := uint32(0); partition < p.blocks.Partitions(); partition++ {
		if err := p.push(ctx, partition, true); err != nil {
			return err
		}
	}
	p.finished = true
	return nil
}

func (p *Producer) push(ctx context.Context, partition uint32, final bool) error {
	data, rows, err := p.blocks.Block(partition)
	if err != nil {
		return errors.Join(ErrProducerState, err)
	}
	if rows == 0 {
		data = nil
	}
	sequence := p.sequences[partition]
	if !final && sequence == ^uint32(0) {
		return fmt.Errorf("%w: partition %d exhausted its sequence space", ErrProducerState, partition)
	}
	batch := Batch{Producer: p.producer, Sequence: sequence, Rows: rows, Data: data, Final: final}
	if err := p.sink.Push(ctx, partition, batch); err != nil {
		return errors.Join(ErrProducerState, err)
	}
	if !final {
		p.sequences[partition]++
	}
	return nil
}
