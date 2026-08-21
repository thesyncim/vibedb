package gateway

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/exchange"
	"github.com/thesyncim/vibedb/shardservice"
)

var ErrExchangeProducer = errors.New("gateway: exchange producer failed")

// exchangeBatchProducer adapts the shared producer state machine to gateway
// errors while retaining the exact JSON partitioner selected here.
type exchangeBatchProducer struct {
	inner *exchange.Producer
}

func newExchangeBatchProducer(
	keyColumns []int,
	partitions, columns, maxRows, maxBytes uint32,
	maxMemory uint64,
	producer uint16,
	sink exchange.BatchSink,
) (*exchangeBatchProducer, error) {
	partitioner, err := newExchangeRowPartitioner(keyColumns, partitions)
	if err != nil {
		return nil, errors.Join(ErrExchangeProducer, err)
	}
	inner, err := exchange.NewProducer(
		&partitioner, partitions, columns, maxRows, maxBytes, maxMemory, producer, sink,
	)
	if err != nil {
		return nil, errors.Join(ErrExchangeProducer, err)
	}
	return &exchangeBatchProducer{inner: inner}, nil
}

func (p *exchangeBatchProducer) Add(ctx context.Context, row []shardservice.Cell) error {
	if p == nil || p.inner == nil {
		return ErrExchangeProducer
	}
	if err := p.inner.Add(ctx, row); err == nil {
		return nil
	} else {
		return errors.Join(ErrExchangeProducer, err)
	}
}

func (p *exchangeBatchProducer) Finish(ctx context.Context) error {
	if p == nil || p.inner == nil {
		return ErrExchangeProducer
	}
	if err := p.inner.Finish(ctx); err == nil {
		return nil
	} else {
		return errors.Join(ErrExchangeProducer, err)
	}
}
