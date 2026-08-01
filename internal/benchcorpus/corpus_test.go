package benchcorpus

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

func corpusDigest(documents []Document) string {
	digest := sha256.New()
	for i := range documents {
		_, _ = digest.Write([]byte(documents[i].Key))
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write(documents[i].JSON)
		_, _ = digest.Write([]byte{0})
	}
	return fmt.Sprintf("%x", digest.Sum(nil))
}

// TestCorpusStableFixture pins the exact cross-engine input. Changing either
// digest is an intentional benchmark-corpus migration that must regenerate all
// published speed and space results, never a silent local-test adjustment.
func TestCorpusStableFixture(t *testing.T) {
	const documents = 1024
	low := Corpus(documents, false)
	high := Corpus(documents, true)
	for i := range low {
		if low[i].Key != high[i].Key {
			t.Fatalf("document %d key mismatch: %q != %q", i, low[i].Key, high[i].Key)
		}
		if len(low[i].JSON) != len(high[i].JSON) {
			t.Fatalf("document %d length mismatch: %d != %d", i, len(low[i].JSON), len(high[i].JSON))
		}
	}
	for name, fixture := range map[string]struct {
		documents []Document
		want      string
	}{
		"low":  {documents: low, want: "73d484a3fc981eb4d8a0bc94f13aaa122c5ec00ddf58d9e99aed2a16b19de7d1"},
		"high": {documents: high, want: "f311e96358ba21ca257a86f1db40d8f653dead1dc4a26de728344a1a5fbfbb3b"},
	} {
		if got := corpusDigest(fixture.documents); got != fixture.want {
			t.Errorf("%s corpus digest = %s, want %s", name, got, fixture.want)
		}
	}
}
