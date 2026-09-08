package raftstore

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
)

func TestNodeMaintenanceSignalsRegistrationAndCheckpointsDescriptorCatalog(t *testing.T) {
	_, store, _ := createDescriptorCatalogTestStore(t, 8)
	defer store.Close()
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case <-q.NodeMaintenanceWake():
		default:
			goto drained
		}
	}
drained:
	var registration Submission
	if err = registration.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err = registration.PrepareRegisterGroup(testGroupDescriptor(200)); err != nil {
		t.Fatal(err)
	}
	if _, err = q.TrySubmit(&registration); err != nil {
		t.Fatal(err)
	}
	if _, err = registration.Wait(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-q.NodeMaintenanceWake():
	default:
		t.Fatal("successful registration did not signal maintenance")
	}
	if err = q.MaintainNodeLog(); err != nil && !errors.Is(err, seglog.ErrBounds) {
		t.Fatal(err)
	}
	metadata, ok := store.engine.Metadata(nodeDescriptorGroup)
	if !ok || metadata.Checkpoint.Index != uint64(len(store.descriptors)) {
		t.Fatalf("descriptor checkpoint=%+v descriptors=%d", metadata, len(store.descriptors))
	}
}
