package distributedtxn

import (
	"bytes"
	"testing"
)

func FuzzManifestSegmentCanonicalUniqueness(f *testing.F) {
	_, pages := buildManifest(f, 256)
	f.Add(pages[0])
	f.Add(pages[0][:len(pages[0])-1])
	f.Add([]byte("not-a-manifest-segment"))

	f.Fuzz(func(t *testing.T, raw []byte) {
		participants := make([]ParticipantRef, MaxManifestPageParticipants)
		identities := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)
		page, err := OpenManifestSegment(raw, participants, identities)
		if err != nil {
			return
		}
		if page.Segment.Index != 0 || page.Segment.FirstParticipant != 0 {
			return
		}
		arena := make([]byte, ManifestSegmentBytes)
		var canonical []byte
		builder, err := NewManifestBuilder(arena, func(segment ManifestSegment) error {
			canonical = bytes.Clone(segment.Raw)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := range page.Participants {
			if err := builder.Append(page.Participants[i]); err != nil {
				t.Fatalf("accepted segment did not rebuild: %v", err)
			}
		}
		if _, err := builder.Seal(); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(canonical, raw) {
			t.Fatal("accepted noncanonical segment has a distinct canonical encoding")
		}
	})
}
