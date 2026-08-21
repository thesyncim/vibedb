package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/exchange"
	"github.com/thesyncim/vibedb/shardservice"
)

func testExchangeStageKey(seed byte) exchange.Key {
	var id exchange.ID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return exchange.Key{Operation: id, Stage: 2, Attempt: 1}
}

func TestExchangeStageEndToEndPartitionedBlocks(t *testing.T) {
	server := newShardServer(t)
	client := pipeClient(server)
	t.Cleanup(func() { _ = client.Close() })
	owner := testOwnership()
	base := ownedReq("")
	targets := make([]shardCall, 7)
	for partition := range targets {
		targets[partition] = shardCall{
			address: "worker-a", req: base,
			target: distribution.Target{
				Shard: owner.Shard, AllocationGeneration: owner.AllocationGeneration,
				OwnershipEpoch: owner.Epoch, Role: distribution.RoleLeader,
			},
		}
	}
	key := testExchangeStageKey(101)
	stage, err := newExchangeStage(client, targets, key, exchange.Spec{
		Producers: 1, QueuedBatches: 16, ProducerBatches: 16,
		BufferedRows: 128, BufferedBytes: 64 << 10,
		TotalRows: 1024, TotalBytes: 1 << 20,
	}, 5*time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stage.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := stage.Open(ctx); err != nil {
		t.Fatalf("Open retry: %v", err)
	}
	producer, err := newExchangeBatchProducer(
		[]int{0}, uint32(len(targets)), 2, 2, 64, uint64(len(targets))*64,
		0, stage,
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
	partitioner, err := newExchangeRowPartitioner([]int{0}, uint32(len(targets)))
	if err != nil {
		t.Fatal(err)
	}
	want := make([]int, len(targets))
	for i := range rows {
		partition, err := partitioner.partition(rows[i])
		if err != nil {
			t.Fatal(err)
		}
		want[partition]++
		if err := producer.Add(ctx, rows[i]); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	if err := producer.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	got := make([]int, len(targets))
	for partition := range targets {
		var (
			hasAck      bool
			ackProducer uint16
			ackSequence uint32
		)
		for {
			batch, eof, err := stage.Pull(
				ctx, uint32(partition), hasAck, ackProducer, ackSequence,
			)
			if err != nil {
				t.Fatalf("Pull partition %d: %v", partition, err)
			}
			if eof {
				break
			}
			hasAck, ackProducer, ackSequence = true, batch.Producer, batch.Sequence
			if batch.Rows == 0 {
				continue
			}
			block, err := exchange.OpenBlock(batch.Data)
			if err != nil || block.Rows() != batch.Rows {
				t.Fatalf("partition %d block rows = %d/%d, %v", partition, block.Rows(), batch.Rows, err)
			}
			decoded := make([]exchange.Cell, block.Columns())
			for block.NextInto(decoded) {
				actual, err := partitioner.partition(decoded)
				if err != nil || actual != uint32(partition) {
					t.Fatalf("partition %d decoded to %d, %v", partition, actual, err)
				}
				got[partition]++
			}
		}
	}
	for partition := range want {
		if got[partition] != want[partition] {
			t.Fatalf("partition %d rows = %d, want %d", partition, got[partition], want[partition])
		}
	}
	if err := stage.Cancel(ctx); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := stage.Cancel(ctx); err != nil {
		t.Fatalf("Cancel retry: %v", err)
	}
}
