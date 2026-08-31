package replicatedstate

import (
	"fmt"
	"testing"
)

var snapshotAuthorizationBenchmarkFence SnapshotFence

// BenchmarkSnapshotAuthorizationFenceVsFullSnapshot measures the serving
// authorization metadata cut against the former Probe path. The fixture is
// saturated before timing so the full snapshot must validate every retained
// hidden session row on every iteration.
func BenchmarkSnapshotAuthorizationFenceVsFullSnapshot(b *testing.B) {
	for _, window := range []uint16{8, MaxSessionRetryWindow} {
		b.Run(fmt.Sprintf("RetryWindow-%d", window), func(b *testing.B) {
			fixture := newSessionReleaseFixture(b, 1, window)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				b.Fatal(err)
			}
			applySessionOpen(b, fixture.machine, 2, commandValue(fixture.binding, 1))
			nextIndex := uint64(3)
			for sequence := uint64(2); sequence <= uint64(window); sequence++ {
				command := lifecycleDeleteCommand(fixture.binding, 2, sequence)
				applyLifecycleCommand(
					b, fixture.machine, nextIndex, encodeCommand(b, command),
				)
				nextIndex++
			}
			rows := fixture.system.Collection.Len()

			b.Run("FullSnapshot", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cut, err := fixture.machine.Snapshot()
					if err != nil {
						b.Fatal(err)
					}
					snapshotAuthorizationBenchmarkFence = cut.Fence()
					if err := cut.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(rows), "retained_rows")
			})
			b.Run("AuthorizationFence", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					fence, err := fixture.machine.SnapshotAuthorizationFence()
					if err != nil {
						b.Fatal(err)
					}
					snapshotAuthorizationBenchmarkFence = fence
				}
				b.ReportMetric(float64(rows), "retained_rows")
			})
		})
	}
}
