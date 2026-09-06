package raftservice

import (
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"testing"
)

type benchmarkReadIndexHost struct {
	*countingReadIndexHost
	context [16]byte
}

func (h *benchmarkReadIndexHost) ReadIndex(_ raftmember.GroupKey, value []byte) error {
	copy(h.context[:], value)
	return nil
}

// Measures owner publication, admission and barrier settlement only. It does
// not include network quorum latency, state-machine reads or SQL execution.
func BenchmarkOwnerReadBarrier(b *testing.B) {
	for _, batch := range []int{1, 8} {
		name := "solo"
		if batch == 8 {
			name = "batch8"
		}
		b.Run(name, func(b *testing.B) {
			f := newSharedBarrierFixture(b)
			h := &benchmarkReadIndexHost{countingReadIndexHost: f.host}
			f.owner.host = h
			requests := make([]ownerRequest, batch)
			outcome := []raftmodel.ReadOutcome{{Barrier: raftmodel.ReadBarrier{Context: h.context[:], Index: 9}}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for i := range requests {
					d := &readDelivery{reply: make(chan ownerReply, 1)}
					r := ownerRequest{kind: requestReadLinear, group: f.group, reply: d.reply, read: readRequest{fence: f.fence, minimumApplied: 1, delivery: d}}
					if err := f.owner.publish(r); err != nil {
						b.Fatal(err)
					}
					requests[i] = <-f.owner.ingress
				}
				for _, r := range requests {
					if err := f.owner.handle(r); err != nil {
						b.Fatal(err)
					}
					f.owner.release(r.bytes)
				}
				f.owner.finishReadOutcomes(outcome)
				for _, r := range requests {
					reply := <-r.read.delivery.reply
					if reply.err != nil {
						b.Fatal(reply.err)
					}
					reply.read.generation.release()
				}
			}
		})
	}
}
