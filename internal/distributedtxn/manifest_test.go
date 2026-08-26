package distributedtxn

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func manifestParticipant(index uint64) ParticipantRef {
	var shard [16]byte
	const hex = "0123456789abcdef"
	for i := range shard {
		shift := uint((len(shard) - 1 - i) * 4)
		shard[i] = hex[(index>>shift)&15]
	}
	var digest Digest
	binary.LittleEndian.PutUint64(digest[:8], index+1)
	for i := 8; i < len(digest); i++ {
		digest[i] = byte(i)
	}
	return ParticipantRef{
		Distribution: []byte("docs"), Shard: shard[:], RoutingVersion: 7,
		AllocationGeneration: 11, OwnershipEpoch: 13,
		MutationDigest: digest, State: ParticipantStaged,
	}
}

func buildManifest(t testing.TB, count uint64) (ManifestDescriptor, [][]byte) {
	t.Helper()
	pageArena := make([]byte, ManifestSegmentBytes)
	var pages [][]byte
	builder, err := NewManifestBuilder(pageArena, func(segment ManifestSegment) error {
		pages = append(pages, bytes.Clone(segment.Raw))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < count; i++ {
		if err := builder.Append(manifestParticipant(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	descriptor, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	return descriptor, pages
}

func TestManifestRoundTripCanonicalAndMoreThan64Participants(t *testing.T) {
	descriptor, pages := buildManifest(t, 4097)
	if descriptor.ParticipantCount != 4097 || descriptor.SegmentCount <= 1 {
		t.Fatalf("descriptor = %+v", descriptor)
	}
	reader, err := NewManifestReader(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	participantArena := make([]ParticipantRef, MaxManifestPageParticipants)
	identityArena := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)
	var seen uint64
	for pageIndex, raw := range pages {
		page, err := reader.OpenNext(raw, participantArena, identityArena)
		if err != nil {
			t.Fatalf("page %d len=%d count=%d: %v", pageIndex, len(raw), binary.LittleEndian.Uint32(raw[12:16]), err)
		}
		for i := range page.Participants {
			want := manifestParticipant(seen)
			got := page.Participants[i]
			if compareParticipantIdentity(got, want) != 0 || !equalParticipantRef(got, want) {
				t.Fatalf("participant %d differs", seen)
			}
			seen++
		}
	}
	if err := reader.Seal(); err != nil {
		t.Fatal(err)
	}
	if seen != descriptor.ParticipantCount {
		t.Fatalf("decoded %d participants", seen)
	}

	// Decode and re-encode through one page of caller scratch. Canonical form
	// must be byte-identical, including greedy segment boundaries.
	reencodedArena := make([]byte, ManifestSegmentBytes)
	var reencoded [][]byte
	rebuilder, err := NewManifestBuilder(reencodedArena, func(segment ManifestSegment) error {
		reencoded = append(reencoded, bytes.Clone(segment.Raw))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range pages {
		page, err := OpenManifestSegment(raw, participantArena, identityArena)
		if err != nil {
			t.Fatal(err)
		}
		for i := range page.Participants {
			if err := rebuilder.Append(page.Participants[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
	reencodedDescriptor, err := rebuilder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if reencodedDescriptor != descriptor || len(reencoded) != len(pages) {
		t.Fatalf("reencoded descriptor = %+v, want %+v", reencodedDescriptor, descriptor)
	}
	for i := range pages {
		if !bytes.Equal(reencoded[i], pages[i]) {
			t.Fatalf("page %d is not canonical", i)
		}
	}
}

func TestManifestDeduplicatesExactAdjacentIdentity(t *testing.T) {
	arena := make([]byte, ManifestSegmentBytes)
	var raw []byte
	builder, err := NewManifestBuilder(arena, func(segment ManifestSegment) error {
		raw = bytes.Clone(segment.Raw)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	first := manifestParticipant(1)
	if err := builder.Append(first); err != nil {
		t.Fatal(err)
	}
	if err := builder.Append(first); err != nil {
		t.Fatalf("exact duplicate: %v", err)
	}
	conflict := first
	conflict.OwnershipEpoch++
	if err := builder.Append(conflict); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("conflicting duplicate = %v", err)
	}
	if raw != nil {
		t.Fatal("failed builder emitted a page")
	}
}

func TestManifestRejectsReorderedInputAndResourceByteOverflow(t *testing.T) {
	arena := make([]byte, ManifestSegmentBytes)
	builder, err := NewManifestBuilder(arena, func(ManifestSegment) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append(manifestParticipant(2)); err != nil {
		t.Fatal(err)
	}
	if err := builder.Append(manifestParticipant(1)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("reordered input = %v", err)
	}

	builder, err = NewManifestBuilder(arena, func(ManifestSegment) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	builder.totalBytes = MaxManifestBytes
	if err := builder.Append(manifestParticipant(1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("byte overflow = %v", err)
	}
}

func TestManifestDescriptorRejectsImpossiblePageGeometry(t *testing.T) {
	descriptor := ManifestDescriptor{
		ParticipantCount: 1, EncodedBytes: 1, SegmentCount: 1, Root: Digest{1},
	}
	if _, err := NewManifestReader(descriptor); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("NewManifestReader impossible descriptor = %v", err)
	}
	if _, err := AppendManifestCoordinator(nil, ManifestCoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 1, Manifest: descriptor,
	}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("AppendManifestCoordinator impossible descriptor = %v", err)
	}

	descriptor.EncodedBytes = ManifestSegmentBytes
	descriptor.ParticipantCount = uint64(MaxManifestPageParticipants + 1)
	if descriptor.valid() {
		t.Fatal("one-page descriptor admitted more participants than a page can encode")
	}
}

func TestManifestRejectsReorderedSparseTruncatedAndOversizedPages(t *testing.T) {
	descriptor, pages := buildManifest(t, 4097)
	participants := make([]ParticipantRef, MaxManifestPageParticipants)
	identities := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)

	reader, _ := NewManifestReader(descriptor)
	if _, err := reader.OpenNext(pages[1], participants, identities); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("reordered first page = %v", err)
	}

	sparse := bytes.Clone(pages[0])
	binary.LittleEndian.PutUint32(sparse[8:12], 3)
	binary.LittleEndian.PutUint32(sparse[len(sparse)-4:], crc32Checksum(sparse[:len(sparse)-4]))
	reader, _ = NewManifestReader(descriptor)
	if _, err := reader.OpenNext(sparse, participants, identities); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("sparse page = %v", err)
	}

	truncated := pages[0][:len(pages[0])-1]
	if _, err := OpenManifestSegment(truncated, participants, identities); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("truncated page = %v", err)
	}

	oversized := make([]byte, ManifestSegmentBytes+1)
	copy(oversized, pages[0])
	if _, err := OpenManifestSegment(oversized, participants, identities); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("oversized page = %v", err)
	}
}

func TestManifestOneHundredThousandParticipantsUsesPagedScratch(t *testing.T) {
	descriptor, pages := buildManifest(t, 100_000)
	if descriptor.ParticipantCount != 100_000 || descriptor.EncodedBytes > MaxManifestBytes {
		t.Fatalf("descriptor = %+v", descriptor)
	}
	reader, err := NewManifestReader(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	participants := make([]ParticipantRef, MaxManifestPageParticipants)
	identities := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)
	for _, page := range pages {
		if _, err := reader.OpenNext(page, participants, identities); err != nil {
			t.Fatal(err)
		}
	}
	if err := reader.Seal(); err != nil {
		t.Fatal(err)
	}
}

func TestManifestCoordinatorRoundTripAndDescriptorBinding(t *testing.T) {
	descriptor, _ := buildManifest(t, 1000)
	record := ManifestCoordinatorRecord{
		ID: testID(), State: CoordinatorStaging, Revision: 1,
		CatalogGeneration: 9, RecoveryDeadline: 1234, Manifest: descriptor,
	}
	raw, err := AppendManifestCoordinator(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	got, err := OpenManifestCoordinator(raw)
	if err != nil || got != record {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
	raw[80] ^= 1
	if _, err := OpenManifestCoordinator(raw); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("descriptor corruption = %v", err)
	}
}

func TestManifestSealIsIdempotentAndFinal(t *testing.T) {
	arena := make([]byte, ManifestSegmentBytes)
	builder, err := NewManifestBuilder(arena, func(ManifestSegment) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append(manifestParticipant(1)); err != nil {
		t.Fatal(err)
	}
	first, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	second, err := builder.Seal()
	if err != nil || second != first {
		t.Fatalf("second seal = %+v, %v", second, err)
	}
	if err := builder.Append(manifestParticipant(2)); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("append after seal = %v", err)
	}
}

func BenchmarkManifestCodecPage(b *testing.B) {
	participants := make([]ParticipantRef, 700)
	for i := range participants {
		participants[i] = manifestParticipant(uint64(i))
	}
	pageArena := make([]byte, ManifestSegmentBytes)
	participantArena := make([]ParticipantRef, MaxManifestPageParticipants)
	identityArena := make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)
	var builder ManifestBuilder
	emit := func(segment ManifestSegment) error {
		_, openErr := OpenManifestSegment(segment.Raw, participantArena, identityArena)
		return openErr
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := builder.Reset(pageArena, emit); err != nil {
			b.Fatal(err)
		}
		for i := range participants {
			if err := builder.Append(participants[i]); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := builder.Seal(); err != nil {
			b.Fatal(err)
		}
	}
}

func crc32Checksum(value []byte) uint32 {
	return crc32.Checksum(value, castagnoli)
}
