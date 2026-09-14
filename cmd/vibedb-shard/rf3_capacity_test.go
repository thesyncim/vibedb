package main

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/replicacontrol"
)

func TestRF3CapacityGroupUsesLiveBytesInsteadOfNodeReservation(t *testing.T) {
	fixture := newRF3NodeRecoveryFixture(t)
	group, ok := fixture.store.GroupByID(fixture.boots[0].Descriptor.GroupID)
	if !ok {
		t.Fatal("missing prepared group")
	}
	live, err := group.LiveMetrics()
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := group.CapacityReservationBytes()
	if err != nil {
		t.Fatal(err)
	}
	got, kind, err := rf3CapacityRecoveryLogBytes(group)
	if err != nil {
		t.Fatal(err)
	}
	if got != live.LiveBytes || kind != replicacontrol.CapacityDemandMeasured {
		t.Fatalf("capacity sample=%d/%v, want live=%d/measured", got, kind, live.LiveBytes)
	}
	if got >= reservation {
		t.Fatalf("capacity sample=%d unexpectedly includes reservation=%d", got, reservation)
	}
}
