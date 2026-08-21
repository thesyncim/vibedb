package gateway

import (
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/exchange"
	queryengine "github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
)

var ErrExchangePartitionKey = errors.New("gateway: exchange partition key is invalid")

// exchangeRowPartitioner maps canonical result cells onto a fixed stage degree.
// It reuses the query engine's exact GROUP BY identity, so numeric and escaped
// string spelling variants that compare equal cannot split across workers.
type exchangeRowPartitioner struct {
	columns    []int
	partitions uint32
	key        []byte
	encoder    queryengine.JSONGroupKeyEncoder
}

func newExchangeRowPartitioner(columns []int, partitions uint32) (exchangeRowPartitioner, error) {
	if len(columns) == 0 || partitions == 0 || partitions > exchange.MaxPartitions {
		return exchangeRowPartitioner{}, ErrExchangePartitionKey
	}
	for i, column := range columns {
		if column < 0 || slices.Contains(columns[:i], column) {
			return exchangeRowPartitioner{}, ErrExchangePartitionKey
		}
	}
	return exchangeRowPartitioner{
		columns: slices.Clone(columns), partitions: partitions,
	}, nil
}

func (p *exchangeRowPartitioner) partition(row []shardservice.Cell) (uint32, error) {
	if p == nil || len(p.columns) == 0 {
		return 0, ErrExchangePartitionKey
	}
	p.key = p.key[:0]
	for _, column := range p.columns {
		if column >= len(row) {
			return 0, ErrExchangePartitionKey
		}
		value := row[column].Bytes
		if row[column].Null {
			value = groupedNullJSON[:]
		}
		var ok bool
		p.key, ok = p.encoder.Append(p.key, value)
		if !ok {
			return 0, ErrExchangePartitionKey
		}
	}
	partition, err := exchange.PartitionForKey(p.key, p.partitions)
	if err != nil {
		return 0, errors.Join(ErrExchangePartitionKey, err)
	}
	return partition, nil
}
