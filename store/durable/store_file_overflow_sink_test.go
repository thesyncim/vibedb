package durable

import (
	"bytes"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestPrimaryOverflowEmitsIntoUnrootedGenerationSink(t *testing.T) {
	normalized, err := (Options{}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	storeID := [16]byte{3}
	collection := &Collection{storeID: storeID, options: normalized}
	file, err := os.CreateTemp(t.TempDir(), "overflow-unrooted-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reservation := storeio.UnrootedGenerationReservation{
		Offset: 64 << 10, Length: 16 << 20,
		FirstLogicalID: storeio.PrimaryFirstDynamicLogicalID,
		LogicalIDCount: 1 << 20,
	}
	writer, err := storeio.NewUnrootedGenerationWriter(
		file, reservation, storeID, 23, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := storeio.NewUnrootedPrimaryGraphSink(
		writer, storeID, 23, reservation.FirstLogicalID,
		reservation.Offset+reservation.Length, make([]byte, 512<<10),
	)
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte("overflow-vibejson-"), 9000)
	head, err := collection.stagePrimaryOverflowChainToSink(sink, value, 23)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	var rebuilt []byte
	for ref := head; ref != (storeio.PageRef{}); {
		image := make([]byte, ref.Length)
		if _, err := file.ReadAt(image, int64(ref.Offset)); err != nil {
			t.Fatal(err)
		}
		view, err := storeio.OpenOverflowPage(
			image, reservation.Offset+reservation.Length,
			sink.BuildNextLogicalID(), uint32(normalized.PageSize),
		)
		if err != nil {
			t.Fatal(err)
		}
		rebuilt = append(rebuilt, view.Data()...)
		ref = view.Header().Next
	}
	if !bytes.Equal(rebuilt, value) {
		t.Fatalf("rebuilt overflow bytes=%d, want %d", len(rebuilt), len(value))
	}
}
