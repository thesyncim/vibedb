package replicatedstate

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

var sessionLifecyclePlanSink commandPlan

// BenchmarkSessionLifecycleApplyMutation measures the complete ordered apply
// path, including the synchronous single-collection durability fence. The ring
// is saturated before timing so window 256 cannot look cheap by measuring only
// its growth phase. Command encoding is intentionally outside the timer.
func BenchmarkSessionLifecycleApplyMutation(b *testing.B) {
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

			commands := make([][]byte, b.N)
			firstSequence := uint64(window) + 1
			for i := range commands {
				sequence := firstSequence + uint64(i)
				command := lifecycleDeleteCommand(fixture.binding, 2, sequence)
				command.AckThrough = sequence - uint64(window)
				commands[i] = encodeCommand(b, command)
			}
			if len(commands) != 0 {
				b.SetBytes(int64(len(commands[0])))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				publication, err := fixture.machine.ApplyNormal(
					normalMeta(nextIndex+uint64(i)), commands[i],
				)
				if err != nil || publication.Applied != nextIndex+uint64(i) {
					b.Fatalf("apply mutation %d = %+v, %v", i, publication, err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(fixture.system.Collection.Len()), "retained_rows")
		})
	}
}

// BenchmarkSessionLifecycleApplyOpen measures an authenticated serving
// primitive's storage-side work: one missing-prefix seek, compact header+slot
// encoding, and one synchronous ordered apply. Each identity is unique so no
// iteration is a duplicate fast path.
func BenchmarkSessionLifecycleApplyOpen(b *testing.B) {
	for _, window := range []uint16{1, 8, MaxSessionRetryWindow} {
		b.Run(fmt.Sprintf("RetryWindow-%d", window), func(b *testing.B) {
			if uint64(b.N) > MaxRetainedSessions {
				b.Skip("benchmark iteration count exceeds retained-session profile")
			}
			fixture := newSessionReleaseFixture(b, max(1, uint64(b.N)), window)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				b.Fatal(err)
			}
			commands := make([][]byte, b.N)
			for i := range commands {
				prototype := commandValue(fixture.binding, 1)
				prototype.ClientID = sessionLifecycleBenchmarkID(uint64(i))
				commands[i] = encodeCommand(b, sessionOpenFor(prototype))
			}
			if len(commands) != 0 {
				b.SetBytes(int64(len(commands[0])))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				index := uint64(i) + 2
				publication, err := fixture.machine.ApplyNormal(
					normalMeta(index), commands[i],
				)
				if err != nil || publication.Applied != index {
					b.Fatalf("apply open %d = %+v, %v", i, publication, err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(fixture.system.Collection.Len()), "retained_rows")
		})
	}
}

// BenchmarkSessionLifecyclePlanOpen isolates the CPU/allocation geometry that
// the serving layer pays before the durable apply. It is repeatable against one
// immutable missing-session cut and therefore compares window profiles without
// accumulating identities or filesystem noise.
func BenchmarkSessionLifecyclePlanOpen(b *testing.B) {
	for _, window := range []uint16{1, 8, MaxSessionRetryWindow} {
		b.Run(fmt.Sprintf("RetryWindow-%d", window), func(b *testing.B) {
			fixture := newSessionReleaseFixture(b, 1, window)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				b.Fatal(err)
			}
			encoded := encodeCommand(
				b, sessionOpenFor(commandValue(fixture.binding, 1)),
			)
			command, err := replication.OpenCommand(encoded)
			if err != nil {
				b.Fatal(err)
			}
			cut, system, user, err := fixture.machine.captureApplyCut()
			if err != nil {
				b.Fatal(err)
			}
			defer cut.Close()
			b.SetBytes(int64(len(encoded)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sessionLifecyclePlanSink, err = fixture.machine.planCommand(
					command, 2, fixture.machine.state,
					pointSnapshot{value: system}, pointSnapshot{value: user},
					nil,
				)
				if err != nil || !sessionLifecyclePlanSink.newSession ||
					sessionLifecyclePlanSink.advanceEpoch != 2 {
					b.Fatalf("plan open = %+v, %v", sessionLifecyclePlanSink, err)
				}
			}
		})
	}
}

// BenchmarkSessionLifecyclePlanRelease isolates the deliberately cold O(R)
// validation pass. Every timed iteration scans the same saturated immutable
// ring and proves all canonical slots before returning a deletion plan; setup
// and fsync latency are excluded so the 1/8/256 slope is directly visible.
func BenchmarkSessionLifecyclePlanRelease(b *testing.B) {
	for _, window := range []uint16{1, 8, MaxSessionRetryWindow} {
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
			retirement := commandValue(fixture.binding, uint64(window))
			retirement = sessionRetirement(retirement)
			applyLifecycleCommand(
				b, fixture.machine, nextIndex, encodeCommand(b, retirement),
			)
			nextIndex++
			encoded := encodeCommand(b, sessionRelease(retirement))
			command, err := replication.OpenCommand(encoded)
			if err != nil {
				b.Fatal(err)
			}
			cut, system, user, err := fixture.machine.captureApplyCut()
			if err != nil {
				b.Fatal(err)
			}
			defer cut.Close()
			b.SetBytes(int64(len(encoded)))
			b.ReportAllocs()
			b.ReportMetric(float64(window), "slots_scanned/op")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sessionLifecyclePlanSink, err = fixture.machine.planCommand(
					command, nextIndex, fixture.machine.state,
					pointSnapshot{value: system}, pointSnapshot{value: user},
					nil,
				)
				if err != nil || !sessionLifecyclePlanSink.deleteSession ||
					sessionLifecyclePlanSink.deleteSlots != window {
					b.Fatalf("plan release = %+v, %v", sessionLifecyclePlanSink, err)
				}
			}
		})
	}
}

func sessionLifecycleBenchmarkID(sequence uint64) replication.ID128 {
	var id replication.ID128
	binary.LittleEndian.PutUint64(id[:8], sequence+1)
	binary.LittleEndian.PutUint64(id[8:], 0x73657373696f6e73)
	return id
}
