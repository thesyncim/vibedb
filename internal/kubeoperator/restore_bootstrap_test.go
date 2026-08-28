package kubeoperator

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/clusterrestore"
)

func TestRestoreBootstrapOperationIsImmutableAndExact(t *testing.T) {
	operation := clusterrestore.Operation{Digest: [32]byte{1}}
	schema := [32]byte{2}
	bootstrap := restoreRF3Bootstrap(operation, 7, schema)
	got, ordinal, gotSchema, restored, err := RestoreBootstrapOperation(bootstrap)
	if err != nil || !restored || got != operation.Digest || ordinal != 7 || gotSchema != schema {
		t.Fatalf("restore=%t ordinal=%d operation=%x schema=%x err=%v", restored, ordinal, got, gotSchema, err)
	}
	bootstrap.Data = bootstrap.Data[:len(bootstrap.Data)-1]
	if _, _, _, _, err := RestoreBootstrapOperation(bootstrap); err == nil {
		t.Fatal("truncated immutable restore bootstrap accepted")
	}
	bootstrap = restoreRF3Bootstrap(operation, 7, [32]byte{})
	if _, _, _, _, err := RestoreBootstrapOperation(bootstrap); err == nil {
		t.Fatal("unbound schema bootstrap accepted")
	}
	bootstrap.Data = []byte("ordinary unrelated bootstrap")
	if _, _, _, restored, err := RestoreBootstrapOperation(bootstrap); err != nil || restored {
		t.Fatalf("ordinary bootstrap restored=%t err=%v", restored, err)
	}
}
