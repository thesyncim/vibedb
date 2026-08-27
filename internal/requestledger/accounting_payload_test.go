package requestledger

import (
	"bytes"
	"math"
	"testing"
)

func TestPayloadReservationMatchesExactAdmissionBudget(t *testing.T) {
	for _, limits := range [][2]uint64{{0, 0}, {1, 1}, {MaxDynamicWavePayloadBytes, MaxDynamicWavePayloadChunks}} {
		head, _, _ := testHead(t, false)
		head.MaxActivePayloadBytes, head.MaxActivePayloadChunks = limits[0], limits[1]
		before, err := AppendHead(nil, head)
		if err != nil {
			t.Fatal(err)
		}
		got, err := PayloadReservation(head)
		if err != nil {
			t.Fatal(err)
		}
		want := limits[0] + limits[1]*uint64(PayloadStorageKeyBytes+payloadChunkHeaderBytes+checksumBytes)
		if limits[0] != 0 {
			want += FixedStorageKeyBytes + PayloadBuildRecordBytes
		}
		if got != want {
			t.Fatalf("limits=%v payload=%d want=%d", limits, got, want)
		}
		_, withPayload, err := Reservation(head)
		if err != nil {
			t.Fatal(err)
		}
		// This initial head has no cleanup witness: zero limits are valid here.
		base := head
		base.MaxActivePayloadBytes, base.MaxActivePayloadChunks = 0, 0
		_, withoutPayload, err := Reservation(base)
		if err != nil || withPayload-withoutPayload != got {
			t.Fatalf("admission formula drift: %v", err)
		}
		after, err := AppendHead(nil, head)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("accounting changed the authenticated head")
		}
		if allocations := testing.AllocsPerRun(100, func() {
			if _, err := PayloadReservation(head); err != nil {
				panic(err)
			}
		}); allocations != 0 {
			t.Fatalf("payload accounting allocations=%g", allocations)
		}
	}
}

func TestPayloadReservationRejectsInvalidHeadAndOverflow(t *testing.T) {
	head, _, _ := testHead(t, false)
	for _, mutate := range []func(*HeadRecord){
		func(h *HeadRecord) { h.MaxActivePayloadBytes = math.MaxUint64 },
		func(h *HeadRecord) { h.MaxActivePayloadChunks = math.MaxUint64 },
		func(h *HeadRecord) { h.MaxActivePayloadChunks = 0 },
		func(h *HeadRecord) { h.CleanupTotalDataBytes = h.MaxActivePayloadBytes + 1 },
		func(h *HeadRecord) { h.KeyDigest[0] ^= 1 },
	} {
		bad := head
		mutate(&bad)
		if _, err := PayloadReservation(bad); err == nil {
			t.Fatal("invalid authenticated head accepted")
		}
	}
	if _, err := payloadReservation(HeadRecord{MaxActivePayloadBytes: math.MaxUint64, MaxActivePayloadChunks: 1}); err == nil {
		t.Fatal("sum overflow accepted")
	}
	if _, err := payloadReservation(HeadRecord{MaxActivePayloadChunks: math.MaxUint64}); err == nil {
		t.Fatal("chunk multiplication overflow accepted")
	}
}
