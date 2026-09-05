package gatewayruntime

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestServeRequestCapabilitySeparatesReadWriteSchemaAndMixedBatch(t *testing.T) {
	checks := []struct {
		request serveRequest
		want    serviceauthz.Capability
	}{
		{serveRequest{Op: "query", SQL: "SELECT 1"}, serviceauthz.CapabilityDataRead},
		{serveRequest{Op: "exec", SQL: "INSERT INTO docs VALUES (?)"}, serviceauthz.CapabilityDataWrite},
		{serveRequest{Op: "exec", SQL: "  CREATE TABLE docs (id TEXT)"}, serviceauthz.CapabilitySchema},
		{serveRequest{Op: "query", SQL: "DELETE FROM docs"}, serviceauthz.CapabilityDataWrite},
		{serveRequest{Op: "exec", SQL: "SELECT * FROM docs"}, serviceauthz.CapabilityDataRead},
		{serveRequest{Op: "exec_batch", Statements: []serveStatement{
			{SQL: "CREATE TABLE archive (id TEXT)"}, {SQL: `UPDATE docs SET "$doc" = ? WHERE id = ?`},
		}}, serviceauthz.CapabilitySchema | serviceauthz.CapabilityDataWrite},
	}
	for _, check := range checks {
		if got := serveRequestCapability(&check.request); got != check.want {
			t.Fatalf("capability=%x want %x", got, check.want)
		}
	}
}
