package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/exchange"
	"github.com/thesyncim/vibedb/shardservice"
)

type recordedExchangeBatch struct {
	partition uint32
	batch     exchange.Batch
}

type recordingExchangeSink struct {
	batches []recordedExchangeBatch
	failAt  int
}

func (s *recordingExchangeSink) PushExchange(
	_ context.Context,
	partition uint32,
	batch exchange.Batch,
) error {
	if s.failAt != 0 && len(s.batches)+1 == s.failAt {
		return errors.New("injected sink failure")
	}
	batch.Data = bytes.Clone(batch.Data)
	s.batches = append(s.batches, recordedExchangeBatch{partition: partition, batch: batch})
	return nil
}

func TestExchangeBatchProducerPartitionsSequencesAndTerminates(t *testing.T) {
	sink := new(recordingExchangeSink)
	producer, err := newExchangeBatchProducer(
		[]int{0}, 7, 2, 2, 64, 7*64, 3, sink,
	)
	if err != nil {
		t.Fatal(err)
	}
	rows := [][]shardservice.Cell{
		{{Bytes: []byte(`1`)}, {Bytes: []byte(`"a"`)}},
		{{Bytes: []byte(`1.0`)}, {Bytes: []byte(`"b"`)}},
		{{Bytes: []byte(`2`)}, {Bytes: []byte(`"c"`)}},
		{{Bytes: []byte(`3`)}, {Bytes: []byte(`"d"`)}},
		{{Bytes: []byte(`4`)}, {Bytes: []byte(`"e"`)}},
		{{Bytes: []byte(`5`)}, {Bytes: []byte(`"f"`)}},
	}
	wantPartitions := make([]uint32, len(rows))
	check, err := newExchangeRowPartitioner([]int{0}, 7)
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		wantPartitions[i], err = check.partition(rows[i])
		if err != nil {
			t.Fatal(err)
		}
		if err := producer.Add(context.Background(), rows[i]); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	if wantPartitions[0] != wantPartitions[1] {
		t.Fatalf("equal exact key spellings split across %d/%d", wantPartitions[0], wantPartitions[1])
	}
	if err := producer.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := producer.Finish(context.Background()); !errors.Is(err, ErrExchangeProducer) {
		t.Fatalf("second Finish = %v", err)
	}

	nextSequence := make([]uint32, 7)
	finals := make([]bool, 7)
	gotRows := make([]int, 7)
	for _, recorded := range sink.batches {
		batch := recorded.batch
		if batch.Producer != 3 || batch.Sequence != nextSequence[recorded.partition] {
			t.Fatalf("partition %d batch identity = producer %d sequence %d, want 3/%d",
				recorded.partition, batch.Producer, batch.Sequence, nextSequence[recorded.partition])
		}
		if batch.Final {
			if finals[recorded.partition] {
				t.Fatalf("partition %d has duplicate final", recorded.partition)
			}
			finals[recorded.partition] = true
		} else {
			nextSequence[recorded.partition]++
		}
		if batch.Rows == 0 {
			if !batch.Final || len(batch.Data) != 0 {
				t.Fatalf("partition %d has noncanonical empty batch %+v", recorded.partition, batch)
			}
			continue
		}
		block, err := exchange.OpenBlock(batch.Data)
		if err != nil || block.Rows() != batch.Rows || block.Columns() != 2 {
			t.Fatalf("partition %d block = %dx%d, %v; batch rows %d",
				recorded.partition, block.Rows(), block.Columns(), err, batch.Rows)
		}
		decoded := make([]exchange.Cell, 2)
		for block.NextInto(decoded) {
			partition, err := check.partition(decoded)
			if err != nil || partition != recorded.partition {
				t.Fatalf("decoded row partition = %d,%v want %d", partition, err, recorded.partition)
			}
			gotRows[recorded.partition]++
		}
	}
	for partition := range finals {
		if !finals[partition] {
			t.Fatalf("partition %d was not terminated", partition)
		}
	}
	for _, partition := range wantPartitions {
		gotRows[partition]--
	}
	for partition, remaining := range gotRows {
		if remaining != 0 {
			t.Fatalf("partition %d row delta = %d", partition, remaining)
		}
	}
}

func TestExchangeBatchProducerStopsOnSynchronousPushFailure(t *testing.T) {
	sink := &recordingExchangeSink{failAt: 1}
	producer, err := newExchangeBatchProducer(
		[]int{0}, 2, 1, 1, 64, 2*64, 0, sink,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Add(context.Background(), []shardservice.Cell{{Bytes: []byte(`1`)}}); err != nil {
		t.Fatal(err)
	}
	if err := producer.Finish(context.Background()); !errors.Is(err, ErrExchangeProducer) {
		t.Fatalf("Finish failure = %v, want ErrExchangeProducer", err)
	}
	if len(sink.batches) != 0 {
		t.Fatalf("sink accepted %d batches after first failure", len(sink.batches))
	}
}
