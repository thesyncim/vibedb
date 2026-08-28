package raftservice_test

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/rf3bench"
)

func TestRF3EvidenceMetadataUsesCompileTimeProvenance(t *testing.T) {
	revision, modified := rf3bench.BuildProvenance()
	metadata := evidenceMetadata(128)
	values := make(map[string]string, len(metadata))
	for _, pair := range metadata {
		key := string(pair.Key)
		if _, duplicate := values[key]; duplicate {
			t.Fatalf("duplicate %q", key)
		}
		values[key] = string(pair.Value)
	}
	if values["vcs_revision"] != revision || values["vcs_modified"] != modified {
		t.Fatalf("benchmark metadata differs from linked source: %+v", values)
	}
}
