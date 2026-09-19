package gateway

import (
	"context"
	"errors"
	"testing"
)

func TestReplicatedServiceDirectoryRejectsCorruptCanonicalRow(t *testing.T) {
	authority, client, _ := newCatalogAuthorityFixture(t)
	client.rows[string(replicatedServiceDirectoryKey)] = append([]byte(`{"id":"service/directory","payload":{"revision":1,"grants":[]}}`), ' ')
	if _, _, err := authority.ReadServiceDirectoryContinuationGrantCut(context.Background()); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("empty grant row=%v", err)
	}
}
