package hotshard

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
)

type pressureDirectory struct {
	record gateway.ReplicatedPressureRecord
}

func (directory pressureDirectory) ReadPressureRecord(context.Context) (gateway.ReplicatedPressureRecord, error) {
	return directory.record, nil
}

func TestRunReplicatedPassFencesPayloadAndIdempotentlySkipsAppliedRevision(t *testing.T) {
	catalog, source, nodes := hotCatalog(t)
	view := hotView(source, nodes, 1)
	raw, err := AppendView(nil, view)
	if err != nil {
		t.Fatal(err)
	}
	directory := pressureDirectory{gateway.ReplicatedPressureRecord{CatalogGeneration: 10,
		AuthorityRevision: 1, PayloadDigest: sha256.Sum256(raw), Payload: raw}}
	controller, _ := New(hotPolicy())
	sink := &admissionSink{}
	pass, err := RunReplicatedPass(context.Background(), catalog, directory, controller, sink)
	if err != nil || pass.AuthorityRevision != 1 || pass.AlreadyApplied {
		t.Fatalf("pass=%+v err=%v", pass, err)
	}
	again, err := RunReplicatedPass(context.Background(), catalog, directory, controller, sink)
	if err != nil || !again.AlreadyApplied {
		t.Fatalf("again=%+v err=%v", again, err)
	}
	directory.record.CatalogGeneration++
	if _, err := RunReplicatedPass(context.Background(), catalog, directory, controller, sink); err == nil {
		t.Fatal("stale catalog pressure record accepted")
	}
}
