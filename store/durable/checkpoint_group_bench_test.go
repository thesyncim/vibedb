package durable

import (
	"fmt"
	"testing"
)

func BenchmarkCheckpointGroupApply(b *testing.B) {
	for _, batch := range []uint64{1, 8, 32, 128} {
		b.Run(fmt.Sprintf("B=%d", batch), func(b *testing.B) {
			_, members, _, group := newCheckpointGroupTestStore(b, batch)
			values := [][]byte{[]byte(`{"n":0}`), []byte(`{"n":1}`)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := group.Update(
					uint64(i+1), members, defaultTxnLimits(),
					func(transaction *DatabaseBatch) error {
						for _, member := range members {
							write, err := transaction.Collection(member.Name)
							if err != nil {
								return err
							}
							if err := write.Put([]byte("hot"), values[i&1]); err != nil {
								return err
							}
						}
						return nil
					},
				)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := group.Checkpoint(); err != nil {
				b.Fatal(err)
			}
			stats := group.Stats()
			if b.N != 0 {
				b.ReportMetric(float64(stats.BarrierSyncs)/float64(b.N), "sync/op")
				b.ReportMetric(float64(stats.Checkpoints), "barriers")
			}
		})
	}
}

func BenchmarkCheckpointGroupRetentionWitness(b *testing.B) {
	_, members, _, group := newCheckpointGroupTestStore(b, 128)
	if err := group.Update(
		1, members, defaultTxnLimits(),
		func(transaction *DatabaseBatch) error {
			for _, member := range members {
				write, err := transaction.Collection(member.Name)
				if err != nil {
					return err
				}
				if err := write.Put([]byte("hot"), []byte(`{"n":1}`)); err != nil {
					return err
				}
			}
			return nil
		},
	); err != nil {
		b.Fatal(err)
	}
	witness, err := group.SealRetentionFloor(1)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("idempotent-seal", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			got, err := group.SealRetentionFloor(1)
			if err != nil || got != witness {
				b.Fatalf("SealRetentionFloor = %+v, %v", got, err)
			}
		}
	})
	b.Run("exact-validation", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := group.ValidateRetentionWitness(witness); err != nil {
				b.Fatal(err)
			}
		}
	})
}
