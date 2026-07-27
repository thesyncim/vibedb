package storeio

import "testing"

var (
	bufferedCheckpointLabBenchmarkMutation BufferedCheckpointLabMutation
	bufferedCheckpointLabBenchmarkPlan     BufferedCheckpointLabCheckpoint
	bufferedCheckpointLabBenchmarkValue    uint64
)

func BenchmarkBufferedCheckpointLab(b *testing.B) {
	b.Run("BufferedVisible/OwnedCoalesced", func(b *testing.B) {
		lab := newBufferedCheckpointLabTest(b, BufferedCheckpointLabOptions{
			Keys: 1, FrameCapacity: 8, MaxDirtyFrames: 2,
		})
		if _, err := lab.Mutate(0, 1); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			mutation, err := lab.Mutate(0, uint64(index+2))
			if err != nil {
				b.Fatal(err)
			}
			bufferedCheckpointLabBenchmarkMutation = mutation
		}
	})

	b.Run("BufferedVisible/SnapshotFirstTouch", func(b *testing.B) {
		lab := newBufferedCheckpointLabTest(b, BufferedCheckpointLabOptions{
			Keys: 1, FrameCapacity: 8, MaxDirtyFrames: 1,
		})
		if _, err := lab.Mutate(0, 1); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			snapshot, err := lab.Snapshot()
			if err != nil {
				b.Fatal(err)
			}
			mutation, err := lab.Mutate(0, uint64(index+2))
			if err != nil {
				b.Fatal(err)
			}
			if mutation.Mode != BufferedCheckpointLabCopied ||
				!snapshot.Close() {
				b.Fatal("snapshot did not force exactly one copy")
			}
			bufferedCheckpointLabBenchmarkMutation = mutation
		}
	})

	b.Run("Snapshot/OpenClose", func(b *testing.B) {
		lab := newBufferedCheckpointLabTest(b, BufferedCheckpointLabOptions{
			Keys: 1, FrameCapacity: 8, MaxDirtyFrames: 2,
		})
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			snapshot, err := lab.Snapshot()
			if err != nil || !snapshot.Close() {
				b.Fatal("snapshot open/close")
			}
		}
	})

	b.Run("Read/SelectedCanonicalRoot", func(b *testing.B) {
		lab := newBufferedCheckpointLabTest(b, BufferedCheckpointLabOptions{
			Keys: 1, FrameCapacity: 8, MaxDirtyFrames: 2,
		})
		if _, err := lab.Mutate(0, 7); err != nil {
			b.Fatal(err)
		}
		root := lab.VisibleRoot()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			value, err := lab.ReadRoot(root, 0)
			if err != nil {
				b.Fatal(err)
			}
			bufferedCheckpointLabBenchmarkValue = value
		}
	})

	b.Run("Checkpoint/64WritesOnePage", func(b *testing.B) {
		const logicalMutations = 64
		lab := newBufferedCheckpointLabTest(b, BufferedCheckpointLabOptions{
			Keys: 1, FrameCapacity: 8, MaxDirtyFrames: 2,
		})
		b.ReportMetric(logicalMutations, "logical-mutations/op")
		b.ReportMetric(1, "page-writes/op")
		b.ReportAllocs()
		b.ResetTimer()
		var nextValue uint64
		for range b.N {
			for range logicalMutations {
				nextValue++
				if _, err := lab.Mutate(0, nextValue); err != nil {
					b.Fatal(err)
				}
			}
			plan, err := lab.SealCheckpoint()
			if err != nil {
				b.Fatal(err)
			}
			if plan.PageWrites != 1 ||
				plan.CoalescedMutations != logicalMutations-1 {
				b.Fatalf("checkpoint = %+v", plan)
			}
			if err := lab.CompleteCheckpoint(plan.Sequence); err != nil {
				b.Fatal(err)
			}
			bufferedCheckpointLabBenchmarkPlan = plan
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/
				float64(int64(b.N)*logicalMutations),
			"ns/logical-mutation",
		)
	})
}
