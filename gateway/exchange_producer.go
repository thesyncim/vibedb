package gateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/exchange"
	"github.com/thesyncim/vibedb/shardservice"
)

var ErrExchangeProducer = errors.New("gateway: exchange producer failed")

// exchangeBatchSink synchronously consumes one borrowed batch. Returning nil
// means its bytes have crossed the transport ownership boundary and the source
// arena may be reused.
type exchangeBatchSink interface {
	PushExchange(context.Context, uint32, exchange.Batch) error
}

// exchangeBatchProducer exact-hash partitions rows into bounded reusable
// blocks. Per-partition sequences advance only after a synchronous push
// succeeds; Finish terminates every partition so consumers never infer EOF from
// connection closure or from another partition's progress.
type exchangeBatchProducer struct {
	partitioner exchangeRowPartitioner
	blocks      *exchange.PartitionedBlocks
	sequences   []uint32
	producer    uint16
	sink        exchangeBatchSink
	finished    bool
}

func newExchangeBatchProducer(
	keyColumns []int,
	partitions, columns, maxRows, maxBytes uint32,
	maxMemory uint64,
	producer uint16,
	sink exchangeBatchSink,
) (*exchangeBatchProducer, error) {
	if sink == nil || producer >= exchange.MaxProducers {
		return nil, ErrExchangeProducer
	}
	partitioner, err := newExchangeRowPartitioner(keyColumns, partitions)
	if err != nil {
		return nil, errors.Join(ErrExchangeProducer, err)
	}
	blocks, err := exchange.NewPartitionedBlocks(
		partitions, columns, maxRows, maxBytes, maxMemory,
	)
	if err != nil {
		return nil, errors.Join(ErrExchangeProducer, err)
	}
	return &exchangeBatchProducer{
		partitioner: partitioner, blocks: blocks,
		sequences: make([]uint32, partitions), producer: producer, sink: sink,
	}, nil
}

func (p *exchangeBatchProducer) Add(ctx context.Context, row []shardservice.Cell) error {
	if p == nil || p.finished {
		return ErrExchangeProducer
	}
	partition, err := p.partitioner.partition(row)
	if err != nil {
		return errors.Join(ErrExchangeProducer, err)
	}
	flush, err := p.blocks.Append(partition, row)
	if err != nil {
		return errors.Join(ErrExchangeProducer, err)
	}
	if !flush {
		return nil
	}
	if err := p.push(ctx, partition, false); err != nil {
		return err
	}
	if err := p.blocks.ResetPartition(partition); err != nil {
		return errors.Join(ErrExchangeProducer, err)
	}
	flush, err = p.blocks.Append(partition, row)
	if err != nil || flush {
		return errors.Join(ErrExchangeProducer, err)
	}
	return nil
}

func (p *exchangeBatchProducer) Finish(ctx context.Context) error {
	if p == nil || p.finished {
		return ErrExchangeProducer
	}
	for partition := uint32(0); partition < p.blocks.Partitions(); partition++ {
		if err := p.push(ctx, partition, true); err != nil {
			return err
		}
	}
	p.finished = true
	return nil
}

func (p *exchangeBatchProducer) push(ctx context.Context, partition uint32, final bool) error {
	data, rows, err := p.blocks.Block(partition)
	if err != nil {
		return errors.Join(ErrExchangeProducer, err)
	}
	if rows == 0 {
		data = nil
	}
	sequence := p.sequences[partition]
	if !final && sequence == ^uint32(0) {
		return fmt.Errorf("%w: partition %d exhausted its sequence space", ErrExchangeProducer, partition)
	}
	batch := exchange.Batch{
		Producer: p.producer, Sequence: sequence, Rows: rows, Data: data, Final: final,
	}
	if err := p.sink.PushExchange(ctx, partition, batch); err != nil {
		return errors.Join(ErrExchangeProducer, err)
	}
	if !final {
		p.sequences[partition]++
	}
	return nil
}
