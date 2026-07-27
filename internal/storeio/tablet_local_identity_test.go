package storeio

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"testing"
)

func tabletLocalIdentityAssignments(
	count int,
) []TabletLocalIdentityAssignment {
	assignments := make([]TabletLocalIdentityAssignment, count)
	for id := range assignments {
		assignments[id] = TabletLocalIdentityAssignment{
			LocalID: uint16(id),
			Location: TabletLocalIdentityLocation{
				AnchorPageID: uint8(id >> 8),
				RowSlot:      uint8(id),
			},
		}
	}
	return assignments
}

func tabletLocalIdentityTestView(
	t testing.TB, count int,
) TabletLocalIdentityView {
	t.Helper()
	image, descriptor, err := EncodeTabletLocalIdentity(
		make([]byte, TabletLocalIdentityImageBytes),
		42, 7, 1, tabletLocalIdentityAssignments(count),
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenTabletLocalIdentity(image, descriptor, nil)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func TestTabletLocalIdentityBucketRoundTripAndCapacity(t *testing.T) {
	tests := []struct {
		tablet uint32
		local  uint16
	}{
		{0, 0},
		{42, 17},
		{TabletLocalIdentityTabletCount - 1,
			TabletLocalIdentityLocalCount - 1},
	}
	for _, test := range tests {
		bucket, ok := MakeTabletLocalIdentityBucket(
			test.tablet, uint32(test.local),
		)
		if !ok {
			t.Fatalf("Make(%d,%d) failed", test.tablet, test.local)
		}
		tablet, local, ok := SplitTabletLocalIdentityBucket(bucket)
		if !ok || tablet != test.tablet || local != test.local {
			t.Fatalf(
				"Split(%d) = %d,%d,%v", bucket, tablet, local, ok,
			)
		}
	}
	maxBucket, ok := MakeTabletLocalIdentityBucket(
		TabletLocalIdentityTabletCount-1,
		TabletLocalIdentityLocalCount-1,
	)
	if !ok || maxBucket != TabletLocalIdentityBucketCount-1 {
		t.Fatalf("max bucket = %d,%v", maxBucket, ok)
	}
	if _, ok := MakeTabletLocalIdentityBucket(
		TabletLocalIdentityTabletCount, 0,
	); ok {
		t.Fatal("overflow tablet accepted")
	}
	if _, ok := MakeTabletLocalIdentityBucket(
		0, TabletLocalIdentityLocalCount,
	); ok {
		t.Fatal("overflow local ID accepted")
	}
	if _, _, ok := SplitTabletLocalIdentityBucket(
		TabletLocalIdentityBucketCount,
	); ok {
		t.Fatal("overflow bucket accepted")
	}

	const documents = uint64(100_000_000_000)
	for _, rows := range []uint64{187, 230} {
		capacity, ok := TabletLocalIdentityDocumentCapacity(rows)
		if !ok || capacity < documents {
			t.Fatalf("capacity(%d) = %d,%v", rows, capacity, ok)
		}
		leaves := (documents + rows - 1) / rows
		tablets := (leaves + TabletLocalIdentityLocalCount - 1) /
			TabletLocalIdentityLocalCount
		if tablets > TabletLocalIdentityTabletCount {
			t.Fatalf(
				"100B at %d rows needs %d tablets", rows, tablets,
			)
		}
	}
	if _, ok := TabletLocalIdentityDocumentCapacity(0); ok {
		t.Fatal("zero rows accepted")
	}
	if _, ok := TabletLocalIdentityDocumentCapacity(math.MaxUint64); ok {
		t.Fatal("overflow capacity accepted")
	}
	if got, ok := TabletLocalIdentityEnvelopeBytes(0); !ok ||
		got != TabletLocalIdentityImageBytes+
			TabletLocalIdentityDescriptorBytes {
		t.Fatalf("empty envelope = %d,%v", got, ok)
	}
	if got, ok := TabletLocalIdentityEnvelopeBytes(
		TabletLocalIdentityLocalCount,
	); !ok || got != TabletLocalIdentityMaxEnvelopeBytes ||
		TabletLocalIdentityMaxRetirementBytes != 40960 {
		t.Fatalf(
			"worst envelope = %d,%v retirement=%d",
			got, ok, TabletLocalIdentityMaxRetirementBytes,
		)
	}
	if _, ok := TabletLocalIdentityEnvelopeBytes(
		TabletLocalIdentityLocalCount + 1,
	); ok {
		t.Fatal("overflow retirement envelope accepted")
	}
}

func TestTabletLocalIdentityResolveAndWalk(t *testing.T) {
	view := tabletLocalIdentityTestView(t, 3072)
	for _, id := range []uint16{0, 1, 255, 256, 2047, 3071} {
		location, ok := view.Resolve(id)
		if !ok || location != (TabletLocalIdentityLocation{
			AnchorPageID: uint8(id >> 8),
			RowSlot:      uint8(id),
		}) {
			t.Fatalf("Resolve(%d) = %+v,%v", id, location, ok)
		}
	}
	if _, ok := view.Resolve(3072); ok {
		t.Fatal("empty ID resolved")
	}
	cursor := view.Cursor()
	count := 0
	for {
		id, location, ok := cursor.Next()
		if !ok {
			break
		}
		if int(id) != count ||
			location.AnchorPageID != uint8(id>>8) ||
			location.RowSlot != uint8(id) {
			t.Fatalf("walk %d = %d,%+v", count, id, location)
		}
		count++
	}
	if count != 3072 {
		t.Fatalf("walked %d live IDs", count)
	}
}

func TestTabletLocalIdentitySnapshotSafeRetireAndReuse(t *testing.T) {
	base := tabletLocalIdentityTestView(t, 32)
	image8, descriptor8, retirements8, err := RewriteTabletLocalIdentity(
		make([]byte, TabletLocalIdentityImageBytes),
		make([]TabletLocalIdentityRetirement, 0, 4),
		base, 8, 7,
		[]TabletLocalIdentityEdit{{
			LocalID: 9, Operation: TabletLocalIdentityRetire,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(retirements8) != 1 ||
		retirements8[0] != (TabletLocalIdentityRetirement{
			LocalID: 9, RetiredGeneration: 7,
		}) {
		t.Fatalf("retirements = %+v", retirements8)
	}
	view8, err := OpenTabletLocalIdentity(
		image8, descriptor8, retirements8,
	)
	if err != nil {
		t.Fatal(err)
	}
	state, _, retiredGeneration, ok := view8.Entry(9)
	if !ok || state != TabletLocalIdentityRetired ||
		retiredGeneration != 7 {
		t.Fatalf("retired entry = %d,%d,%v", state, retiredGeneration, ok)
	}
	if _, ok := view8.Resolve(9); ok {
		t.Fatal("retired ID resolved")
	}

	_, _, _, err = RewriteTabletLocalIdentity(
		make([]byte, TabletLocalIdentityImageBytes),
		make([]TabletLocalIdentityRetirement, 0, 4),
		view8, 9, 7,
		[]TabletLocalIdentityEdit{{
			LocalID: 9, Operation: TabletLocalIdentityAssign,
			Location: TabletLocalIdentityLocation{
				AnchorPageID: 3, RowSlot: 9,
			},
		}},
	)
	if !errors.Is(err, ErrTabletLocalIdentityReuse) {
		t.Fatalf("premature reuse = %v", err)
	}

	image10, descriptor10, retirements10, err :=
		RewriteTabletLocalIdentity(
			make([]byte, TabletLocalIdentityImageBytes),
			make([]TabletLocalIdentityRetirement, 0, 4),
			view8, 10, 8,
			[]TabletLocalIdentityEdit{{
				LocalID: 9, Operation: TabletLocalIdentityAssign,
				Location: TabletLocalIdentityLocation{
					AnchorPageID: 3, RowSlot: 9,
				},
			}},
		)
	if err != nil {
		t.Fatal(err)
	}
	if len(retirements10) != 0 {
		t.Fatalf("eligible retirement retained: %+v", retirements10)
	}
	view10, err := OpenTabletLocalIdentity(
		image10, descriptor10, retirements10,
	)
	if err != nil {
		t.Fatal(err)
	}
	location, ok := view10.Resolve(9)
	if !ok || location != (TabletLocalIdentityLocation{
		AnchorPageID: 3, RowSlot: 9,
	}) {
		t.Fatalf("reused Resolve = %+v,%v", location, ok)
	}

	// The old snapshot retains the old COW image and therefore still sees the
	// pre-retirement identity even after the new root recycles its number.
	oldLocation, ok := base.Resolve(9)
	if !ok || oldLocation == location {
		t.Fatalf("old snapshot mapping = %+v,%v", oldLocation, ok)
	}
}

func TestTabletLocalIdentityRetirementGenerationIsNotTruncated(t *testing.T) {
	const oldGeneration = uint64(1)<<48 + 12345
	image, descriptor, err := EncodeTabletLocalIdentity(
		make([]byte, TabletLocalIdentityImageBytes),
		7, oldGeneration, 1, tabletLocalIdentityAssignments(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	base, err := OpenTabletLocalIdentity(image, descriptor, nil)
	if err != nil {
		t.Fatal(err)
	}
	retiredImage, retiredDescriptor, retirements, err :=
		RewriteTabletLocalIdentity(
			make([]byte, TabletLocalIdentityImageBytes),
			make([]TabletLocalIdentityRetirement, 0, 1),
			base, oldGeneration+1, oldGeneration,
			[]TabletLocalIdentityEdit{{
				LocalID: 1, Operation: TabletLocalIdentityRetire,
			}},
		)
	if err != nil {
		t.Fatal(err)
	}
	if len(retirements) != 1 ||
		retirements[0].RetiredGeneration != oldGeneration {
		t.Fatalf("exact retirement = %+v", retirements)
	}
	retiredView, err := OpenTabletLocalIdentity(
		retiredImage, retiredDescriptor, retirements,
	)
	if err != nil {
		t.Fatal(err)
	}
	reusedImage, reusedDescriptor, reusedRetirements, err :=
		RewriteTabletLocalIdentity(
			make([]byte, TabletLocalIdentityImageBytes),
			make([]TabletLocalIdentityRetirement, 0, 1),
			retiredView, oldGeneration+2, oldGeneration+1,
			[]TabletLocalIdentityEdit{{
				LocalID: 1, Operation: TabletLocalIdentityAssign,
				Location: TabletLocalIdentityLocation{
					AnchorPageID: 15, RowSlot: 255,
				},
			}},
		)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenTabletLocalIdentity(
		reusedImage, reusedDescriptor, reusedRetirements,
	); err != nil {
		t.Fatal(err)
	}
}

func TestTabletLocalIdentityBatchMoveRetireAndImmediateReclaim(t *testing.T) {
	base := tabletLocalIdentityTestView(t, 128)
	image, descriptor, retirements, err := RewriteTabletLocalIdentity(
		make([]byte, TabletLocalIdentityImageBytes),
		make([]TabletLocalIdentityRetirement, 0, 4),
		base, 8, 7,
		[]TabletLocalIdentityEdit{
			{
				LocalID: 3, Operation: TabletLocalIdentityMove,
				Location: TabletLocalIdentityLocation{
					AnchorPageID: 2, RowSlot: 200,
				},
			},
			{LocalID: 7, Operation: TabletLocalIdentityRetire},
			{
				LocalID: 200, Operation: TabletLocalIdentityAssign,
				Location: TabletLocalIdentityLocation{
					AnchorPageID: 3, RowSlot: 201,
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(retirements) != 1 ||
		retirements[0].RetiredGeneration != 7 {
		t.Fatalf("retirements = %+v", retirements)
	}
	view, err := OpenTabletLocalIdentity(image, descriptor, retirements)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := view.Resolve(3); !ok ||
		got != (TabletLocalIdentityLocation{
			AnchorPageID: 2, RowSlot: 200,
		}) {
		t.Fatalf("moved = %+v,%v", got, ok)
	}
	if _, ok := view.Resolve(7); ok {
		t.Fatal("retired identity resolved")
	}
	if got, ok := view.Resolve(200); !ok ||
		got != (TabletLocalIdentityLocation{
			AnchorPageID: 3, RowSlot: 201,
		}) {
		t.Fatalf("assigned = %+v,%v", got, ok)
	}

	// With no reader at generation 8 or below, retiring under root 8 does not
	// need a queue entry: the old identity is unreachable immediately.
	image9, descriptor9, retirements9, err :=
		RewriteTabletLocalIdentity(
			make([]byte, TabletLocalIdentityImageBytes),
			make([]TabletLocalIdentityRetirement, 0, 4),
			view, 9, 9,
			[]TabletLocalIdentityEdit{{
				LocalID: 11, Operation: TabletLocalIdentityRetire,
			}},
		)
	if err != nil {
		t.Fatal(err)
	}
	if len(retirements9) != 0 {
		t.Fatalf("immediately safe retirement = %+v", retirements9)
	}
	view9, err := OpenTabletLocalIdentity(
		image9, descriptor9, retirements9,
	)
	if err != nil {
		t.Fatal(err)
	}
	state, _, _, ok := view9.Entry(11)
	if !ok || state != TabletLocalIdentityEmpty {
		t.Fatalf("reclaimed state = %d,%v", state, ok)
	}
}

func TestTabletLocalIdentityRejectsCorruptionAndNonCanonicalInput(t *testing.T) {
	view := tabletLocalIdentityTestView(t, 1024)
	corrupt := append([]byte(nil), view.PersistentBytes()...)
	corrupt[777] ^= 0x40
	if _, err := OpenTabletLocalIdentity(
		corrupt, view.Descriptor(), nil,
	); !errors.Is(err, ErrTabletLocalIdentityCorrupt) {
		t.Fatalf("corruption = %v", err)
	}

	descriptor := view.Descriptor()
	descriptor.TabletID++
	if _, err := OpenTabletLocalIdentity(
		view.PersistentBytes(), descriptor, nil,
	); !errors.Is(err, ErrTabletLocalIdentityCorrupt) {
		t.Fatalf("descriptor graft = %v", err)
	}

	assignments := tabletLocalIdentityAssignments(2)
	assignments[1].Location = assignments[0].Location
	if _, _, err := EncodeTabletLocalIdentity(
		make([]byte, TabletLocalIdentityImageBytes),
		1, 1, 1, assignments,
	); err == nil {
		t.Fatal("duplicate live location accepted")
	}

	base := tabletLocalIdentityTestView(t, 2)
	if _, _, _, err := RewriteTabletLocalIdentity(
		make([]byte, TabletLocalIdentityImageBytes),
		make([]TabletLocalIdentityRetirement, 0, 1),
		base, 8, 1,
		[]TabletLocalIdentityEdit{{
			LocalID: 1, Operation: TabletLocalIdentityMove,
			Location: TabletLocalIdentityLocation{},
		}},
	); err == nil {
		t.Fatal("post-edit duplicate location accepted")
	}
}

func TestTabletLocalIdentityRetirementQueueIsChecksummed(t *testing.T) {
	base := tabletLocalIdentityTestView(t, 16)
	image, descriptor, retirements, err := RewriteTabletLocalIdentity(
		make([]byte, TabletLocalIdentityImageBytes),
		make([]TabletLocalIdentityRetirement, 0, 2),
		base, 8, 7,
		[]TabletLocalIdentityEdit{{
			LocalID: 4, Operation: TabletLocalIdentityRetire,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	tampered := append([]TabletLocalIdentityRetirement(nil), retirements...)
	tampered[0].RetiredGeneration--
	if _, err := OpenTabletLocalIdentity(
		image, descriptor, tampered,
	); !errors.Is(err, ErrTabletLocalIdentityCorrupt) {
		t.Fatalf("retirement corruption = %v", err)
	}
	if _, err := OpenTabletLocalIdentity(
		image, descriptor, nil,
	); !errors.Is(err, ErrTabletLocalIdentityCorrupt) {
		t.Fatalf("missing retirement queue = %v", err)
	}
}

func TestTabletLocalIdentityRewriteScratchBound(t *testing.T) {
	base := tabletLocalIdentityTestView(t, 4)
	_, _, _, err := RewriteTabletLocalIdentity(
		make([]byte, TabletLocalIdentityImageBytes),
		nil, base, 8, 7,
		[]TabletLocalIdentityEdit{{
			LocalID: 2, Operation: TabletLocalIdentityRetire,
		}},
	)
	if !errors.Is(err, ErrTabletLocalIdentityScratch) {
		t.Fatalf("zero retirement scratch = %v", err)
	}
}

func TestTabletLocalIdentityResolveAndWalkAllocateZero(t *testing.T) {
	view := tabletLocalIdentityTestView(t, 3686)
	if got := testing.AllocsPerRun(100, func() {
		if _, err := OpenTabletLocalIdentity(
			view.PersistentBytes(), view.Descriptor(), nil,
		); err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("Open allocations = %f", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_, _ = view.Resolve(2048)
	}); got != 0 {
		t.Fatalf("Resolve allocations = %f", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		cursor := view.Cursor()
		for {
			_, _, ok := cursor.Next()
			if !ok {
				break
			}
		}
	}); got != 0 {
		t.Fatalf("walk allocations = %f", got)
	}
}

func TestTabletLocalIdentityScalarCRCMatchesCRC32C(t *testing.T) {
	prefix := []byte("root-bound-image")
	var suffix [14]byte
	binary.LittleEndian.PutUint16(suffix[0:2], 0x1234)
	binary.LittleEndian.PutUint32(suffix[2:6], 0x89abcdef)
	binary.LittleEndian.PutUint64(
		suffix[6:14], 0xfedcba9876543210,
	)
	got := PageChecksum(prefix)
	got = tabletLocalIdentityCRC16(got, 0x1234)
	got = tabletLocalIdentityCRC32(got, 0x89abcdef)
	got = tabletLocalIdentityCRC64(got, 0xfedcba9876543210)
	want := crc32.Update(
		PageChecksum(prefix), pageChecksumTable, suffix[:],
	)
	if got != want {
		t.Fatalf("scalar CRC = %08x, want %08x", got, want)
	}
}

func FuzzOpenTabletLocalIdentity(f *testing.F) {
	view := tabletLocalIdentityTestView(f, 512)
	f.Add(0, byte(1))
	f.Add(8191, byte(0x80))
	f.Fuzz(func(t *testing.T, at int, xor byte) {
		image := append([]byte(nil), view.PersistentBytes()...)
		if len(image) != 0 {
			at %= len(image)
			if at < 0 {
				at += len(image)
			}
			image[at] ^= xor
		}
		opened, err := OpenTabletLocalIdentity(
			image, view.Descriptor(), nil,
		)
		if xor == 0 {
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := opened.Resolve(17); !ok {
				t.Fatal("valid image lost live identity")
			}
		} else if err == nil {
			t.Fatal("mutated image admitted")
		}
	})
}
