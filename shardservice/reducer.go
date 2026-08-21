package shardservice

import (
	"context"
	"errors"
	"io"

	"github.com/thesyncim/vibedb/internal/distributedagg"
	"github.com/thesyncim/vibedb/internal/exchange"
)

// executeExchangeReduce drains one hash partition, combines equal grouped
// partials locally, and transfers the final rows into a second local mailbox.
// The output producer's terminal marker is the retry proof: a duplicate reduce
// request returns success without touching the already-drained input.
func (c *shardConn) executeExchangeReduce(
	ctx context.Context,
	input *exchange.Mailbox,
	command ExchangeRequest,
) *ShardResponse {
	output, ok := c.server.exchanges.Lookup(command.Output)
	if !ok {
		return reducerError(exchange.ErrNotFound)
	}
	progress, err := output.ProducerProgress(0)
	if err != nil {
		return reducerError(err)
	}
	if progress.Final {
		return exchangeCompletion(ExchangeReduce)
	}
	// A prior reducer that emitted only a prefix cannot reconstruct acknowledged
	// input. The attempt must be canceled and restarted with a fresh attempt ID.
	if progress.Accepted {
		return reducerError(exchange.ErrSpecConflict)
	}
	if !input.ClaimConsumer() {
		return reducerError(exchange.ErrProducerBusy)
	}
	defer input.ReleaseConsumer()

	merger, err := distributedagg.NewMerger(
		command.Kinds, command.GroupKeys, command.MaxStateBytes,
	)
	if err != nil {
		return reducerError(err)
	}
	row := make([]exchange.Cell, len(command.Kinds))
	for {
		batch, pullErr := input.Pull(ctx)
		if errors.Is(pullErr, io.EOF) {
			break
		}
		if pullErr != nil {
			return reducerError(pullErr)
		}
		if batch.Rows != 0 {
			block, openErr := exchange.OpenBlock(batch.Data)
			if openErr != nil || block.Columns() != uint32(len(command.Kinds)) ||
				block.Rows() != batch.Rows {
				return reducerError(errors.Join(exchange.ErrBlockData, openErr))
			}
			for block.NextInto(row) {
				if addErr := merger.Add(row); addErr != nil {
					return reducerError(addErr)
				}
			}
		}
		if ackErr := input.Ack(batch.Producer, batch.Sequence); ackErr != nil {
			return reducerError(ackErr)
		}
	}
	rows, err := merger.Finish()
	if err != nil {
		return reducerError(err)
	}
	if err := pushReducedRows(ctx, output, rows, command.BlockRows, command.BlockBytes); err != nil {
		return reducerError(err)
	}
	return exchangeCompletion(ExchangeReduce)
}

func reducerError(err error) *ShardResponse {
	if errors.Is(err, distributedagg.ErrLimit) {
		return NewErrorResponse(ErrorResourceLimit, err.Error())
	}
	return exchangeError(err)
}

func pushReducedRows(
	ctx context.Context,
	output *exchange.Mailbox,
	rows [][]exchange.Cell,
	maxRows, maxBytes uint32,
) error {
	if len(rows) == 0 {
		return output.Push(ctx, exchange.Batch{Producer: 0, Final: true})
	}
	var builder exchange.BlockBuilder
	if err := builder.Reset(nil, uint32(len(rows[0])), maxRows, maxBytes); err != nil {
		return err
	}
	sequence := uint32(0)
	for i := range rows {
		err := builder.AppendRow(rows[i])
		if !errors.Is(err, exchange.ErrBlockLimit) {
			if err != nil {
				return err
			}
			continue
		}
		data, count := builder.Bytes()
		if count == 0 {
			return err
		}
		if err := output.Push(ctx, exchange.Batch{
			Producer: 0, Sequence: sequence, Rows: count, Data: data,
		}); err != nil {
			return err
		}
		sequence++
		// The successful Push transferred data ownership to output. Starting
		// with nil preserves that ownership and allocates only the next block.
		if err := builder.Reset(nil, uint32(len(rows[0])), maxRows, maxBytes); err != nil {
			return err
		}
		if err := builder.AppendRow(rows[i]); err != nil {
			return err
		}
	}
	data, count := builder.Bytes()
	return output.Push(ctx, exchange.Batch{
		Producer: 0, Sequence: sequence, Rows: count, Data: data, Final: true,
	})
}
