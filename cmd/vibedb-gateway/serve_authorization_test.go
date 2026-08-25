package main

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
		{serveRequest{Op: "exec_batch", Statements: []serveStatement{
			{SQL: "ALTER TABLE docs ADD COLUMN n INT"}, {SQL: "UPDATE docs SET n = 1"},
		}}, serviceauthz.CapabilitySchema | serviceauthz.CapabilityDataWrite},
	}
	for _, check := range checks {
		if got := serveRequestCapability(&check.request); got != check.want {
			t.Fatalf("capability=%x want %x", got, check.want)
		}
	}
}
