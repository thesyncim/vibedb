package gateway

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func TestDurableRequestStreamBudgetIsHardAndContextBound(t *testing.T) {
	worstOwned := requestledger.MaxPlanPageBytes + durableRequestMaxParticipantFrameBytes +
		replication.MaxCommandBytes + requestledger.MaxContinuationRecordBytes + (8 << 20)
	if durableRequestStreamSlotBytes < worstOwned {
		t.Fatalf("slot=%d cannot hold conservative owned bytes=%d",
			durableRequestStreamSlotBytes, worstOwned)
	}
	if cap(durableRequestStreamSlots) == 0 ||
		cap(durableRequestStreamSlots)*durableRequestStreamSlotBytes >
			durableRequestStreamBudgetBytes {
		t.Fatalf("invalid process budget slots=%d slot=%d budget=%d",
			cap(durableRequestStreamSlots), durableRequestStreamSlotBytes,
			durableRequestStreamBudgetBytes)
	}
	releases := make([]func(), 0, cap(durableRequestStreamSlots))
	for range cap(durableRequestStreamSlots) {
		release, err := acquireDurableRequestStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	defer func() {
		for _, release := range releases {
			if release != nil {
				release()
			}
		}
	}()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if release, err := acquireDurableRequestStream(cancelled); release != nil ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf("exhausted acquire release=%v err=%v", release != nil, err)
	}
	releases[0]()
	releases[0] = nil
	release, err := acquireDurableRequestStream(t.Context())
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	release()
}

var durableRequestLookupSink DurableRequestLedgerHome

func TestDurableRequestTopologyLookupAllocationsIgnoreRangeCount(t *testing.T) {
	base := durableFaultParticipants(t)[0].Route
	for _, count := range []int{1, 1_000, 100_000} {
		ranges := make([]DurableRequestLedgerRange, count)
		for index := range ranges {
			if index != 0 {
				binary.BigEndian.PutUint32(
					ranges[index].Start[:4],
					uint32(uint64(index)*(uint64(1)<<32)/uint64(count)),
				)
			}
			if index+1 != count {
				binary.BigEndian.PutUint32(
					ranges[index].End[:4],
					uint32(uint64(index+1)*(uint64(1)<<32)/uint64(count)),
				)
			}
			binary.BigEndian.PutUint64(ranges[index].Identity[:8], uint64(index)+1)
			ranges[index].Route = base
		}
		holder, err := NewDurableRequestLedgerTopologyHolder(
			DurableRequestLedgerTopology{Generation: 1, Ranges: ranges},
		)
		if err != nil {
			t.Fatalf("ranges=%d publish: %v", count, err)
		}
		point := requestledger.LedgerHome(replication.Digest{0x91})
		allocations := testing.AllocsPerRun(1_000, func() {
			home, _, ok := holder.Lookup(point)
			if !ok {
				panic("lookup failed")
			}
			durableRequestLookupSink = home
		})
		if allocations != 0 {
			t.Fatalf("ranges=%d allocations=%v, want 0", count, allocations)
		}
	}
}
