package rangesplit

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

type tailOperationCopy struct {
	kind  replication.MutationKind
	key   []byte
	value []byte
}

func TestInitialTailCursorRequiresCompleteExactArtifactSet(t *testing.T) {
	partitioner, cursor, set := testTailCursor(t)
	cut := cursor.SourceCut()
	if cursor.PlanDigest() != partitioner.Digest() ||
		cursor.PlacementDigest() != partitioner.program.Digest() ||
		cursor.SourceCoordinates() != (TailSourceCoordinates{
			OwnershipEpoch:  uint64(partitioner.source.OwnershipEpoch),
			RoutingVersion:  uint64(partitioner.source.RoutingVersion),
			RouteGeneration: set.Partition.RouteGeneration,
		}) || cursor.Sealed() ||
		cut.Applied != set.Partition.SourceApplied ||
		cut.Term != set.Partition.SourceTerm ||
		cut.RouteGeneration != set.Partition.RouteGeneration ||
		cut.LogicalDigest != set.Partition.SourceDigest ||
		cut.BaseDigest != set.Partition.SourceBase ||
		cut.EntryDigest != set.Partition.SourceEntry {
		t.Fatalf("cursor cut = %+v set = %+v", cut, set.Partition)
	}

	wrongSource := set
	wrongSource.Children[1].Source.Applied++
	if _, err := partitioner.InitialTailCursor(wrongSource); !errors.Is(err, ErrTailCursor) {
		t.Fatalf("wrong source error = %v", err)
	}
	missing := set
	missing.Children[1] = ChildArtifactManifest{}
	if _, err := partitioner.InitialTailCursor(missing); !errors.Is(err, ErrTailCursor) {
		t.Fatalf("missing artifact error = %v", err)
	}
	retainedCopy := set
	retainedCopy.Children[0].Present = true
	if _, err := partitioner.InitialTailCursor(retainedCopy); !errors.Is(err, ErrTailCursor) {
		t.Fatalf("retained artifact error = %v", err)
	}
}

func TestTranslateTailEntryRoutesInsertDeleteUpdateAndMove(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	left := documentsForChild(t, partitioner, 0, 2)
	right := documentsForChild(t, partitioner, 1, 2)
	entry := nextTailEntry(cursor, []TailTransition{
		{Key: []byte("a"), After: left[0]},
		{Key: []byte("b"), After: right[0]},
		{Key: []byte("c"), Before: left[0], After: right[1]},
		{Key: []byte("d"), Before: right[0], After: left[1]},
		{Key: []byte("e"), Before: left[0], After: left[1]},
		{Key: []byte("f"), Before: right[1]},
	}, 9)
	var got [2][]tailOperationCopy
	seen := [2]int{}
	sinks := []TailSink{
		func(batch TailBatch) error {
			seen[0]++
			got[0] = appendTailBatch(got[0], batch)
			return nil
		},
		func(batch TailBatch) error {
			seen[1]++
			got[1] = appendTailBatch(got[1], batch)
			return nil
		},
	}
	var workspace TailWorkspace
	next, stats, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	if seen != [2]int{1, 1} || stats.Operations != [3]uint64{4, 4, 0} ||
		stats.TranslationDigest == ([32]byte{}) ||
		stats.ChildDigests[0] == ([32]byte{}) || stats.ChildDigests[1] == ([32]byte{}) ||
		stats.ChildDigests[0] == stats.ChildDigests[1] {
		t.Fatalf("seen=%v stats=%+v", seen, stats)
	}
	wantLeft := []tailOperationCopy{
		{kind: replication.MutationPut, key: []byte("a"), value: left[0]},
		{kind: replication.MutationDelete, key: []byte("c")},
		{kind: replication.MutationPut, key: []byte("d"), value: left[1]},
		{kind: replication.MutationPut, key: []byte("e"), value: left[1]},
	}
	wantRight := []tailOperationCopy{
		{kind: replication.MutationPut, key: []byte("b"), value: right[0]},
		{kind: replication.MutationPut, key: []byte("c"), value: right[1]},
		{kind: replication.MutationDelete, key: []byte("d")},
		{kind: replication.MutationDelete, key: []byte("f")},
	}
	assertTailOperations(t, got[0], wantLeft)
	assertTailOperations(t, got[1], wantRight)
	cut := next.SourceCut()
	if cut.Applied != entry.Applied || cut.Term != entry.Term ||
		cut.RouteGeneration != entry.AfterRouteGeneration ||
		cut.EntryDigest != entry.EntryDigest ||
		cut.LogicalDigest != entry.AfterLogicalDigest ||
		cut.BaseDigest != cursor.SourceCut().BaseDigest {
		t.Fatalf("next cut = %+v entry=%+v", cut, entry)
	}
}

func TestTranslateTailEntryAdvancesEveryChildThroughEmptyEntry(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	entry := nextTailEntry(cursor, nil, 10)
	entry.AfterLogicalDigest = entry.BeforeLogicalDigest
	seen := [2]int{}
	sinks := []TailSink{
		func(batch TailBatch) error {
			seen[0]++
			iterator := batch.Iterator()
			if batch.Child != 0 || batch.Operations != 0 || iterator.Next() {
				t.Fatalf("left empty batch = %+v", batch)
			}
			return nil
		},
		func(batch TailBatch) error {
			seen[1]++
			iterator := batch.Iterator()
			if batch.Child != 1 || batch.Operations != 0 || iterator.Next() {
				t.Fatalf("right empty batch = %+v", batch)
			}
			return nil
		},
	}
	var workspace TailWorkspace
	next, stats, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &workspace)
	if err != nil || seen != [2]int{1, 1} || stats.Operations != [3]uint64{} ||
		next.SourceCut().Applied != entry.Applied ||
		stats.ChildDigests[0] == stats.ChildDigests[1] {
		t.Fatalf("next=%+v stats=%+v seen=%v error=%v", next.SourceCut(), stats, seen, err)
	}
}

func TestTranslateTailEntrySealsOnExactOwnershipFence(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	entry := nextTailEntry(cursor, nil, 17)
	entry.AfterLogicalDigest = entry.BeforeLogicalDigest
	entry.AfterOwnershipEpoch++
	entry.AfterRoutingVersion++
	entry.AfterRouteGeneration++
	seen := 0
	sinks := []TailSink{
		func(batch TailBatch) error {
			seen++
			if batch.beforeCoordinates() == batch.afterCoordinates() || batch.Operations != 0 {
				t.Fatalf("left seal batch=%+v", batch)
			}
			return nil
		},
		func(batch TailBatch) error {
			seen++
			if batch.beforeCoordinates() == batch.afterCoordinates() || batch.Operations != 0 {
				t.Fatalf("right seal batch=%+v", batch)
			}
			return nil
		},
	}
	var workspace TailWorkspace
	sealed, _, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &workspace)
	if err != nil || seen != 2 || !sealed.Sealed() ||
		sealed.SourceCoordinates() != entry.afterCoordinates() {
		t.Fatalf("sealed=%+v seen=%d err=%v", sealed, seen, err)
	}
	postSeal := nextTailEntry(sealed, nil, 18)
	if next, _, err := partitioner.TranslateTailEntry(sealed, postSeal, sinks, &workspace); !errors.Is(err, ErrTailEntry) || next != sealed {
		t.Fatalf("post-seal next=%+v err=%v", next, err)
	}
}

func TestTranslateTailEntryAdvancesThreeChildrenWithMiddleRetained(t *testing.T) {
	partitioner, cursor := testTernaryTailCursor(t)
	entry := nextTailEntry(cursor, nil, 15)
	entry.AfterLogicalDigest = entry.BeforeLogicalDigest
	seen := [autosplit.MaxSplitChildren]int{}
	sinks := make([]TailSink, autosplit.MaxSplitChildren)
	for child := range sinks {
		child := child
		sinks[child] = func(batch TailBatch) error {
			seen[child]++
			iterator := batch.Iterator()
			if batch.Child != uint8(child) || batch.Operations != 0 ||
				iterator.Next() {
				t.Fatalf("child %d empty batch = %+v", child, batch)
			}
			return nil
		}
	}
	var workspace TailWorkspace
	next, stats, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &workspace)
	if err != nil || seen != [autosplit.MaxSplitChildren]int{1, 1, 1} ||
		stats.Operations != [autosplit.MaxSplitChildren]uint64{} ||
		next.SourceCut().Applied != entry.Applied {
		t.Fatalf("next=%+v stats=%+v seen=%v error=%v", next.SourceCut(), stats, seen, err)
	}
	for child := range autosplit.MaxSplitChildren {
		if cursor.childBaseDigests[child] == ([32]byte{}) ||
			stats.ChildDigests[child] == ([32]byte{}) {
			t.Fatalf("child %d missing identity: cursor=%+v stats=%+v", child, cursor, stats)
		}
		for earlier := 0; earlier < child; earlier++ {
			if cursor.childBaseDigests[child] == cursor.childBaseDigests[earlier] ||
				stats.ChildDigests[child] == stats.ChildDigests[earlier] {
				t.Fatalf("child %d identity aliases child %d", child, earlier)
			}
		}
	}
}

func TestTranslateTailEntryFailsClosedAndLeavesCursorRetryable(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	left := documentForChild(t, partitioner, 0)
	right := documentForChild(t, partitioner, 1)
	valid := nextTailEntry(cursor, []TailTransition{{
		Key: []byte("a"), Before: left, After: right,
	}}, 11)
	cases := []struct {
		name string
		edit func(*TailEntry)
	}{
		{"applied-gap", func(entry *TailEntry) { entry.Applied++ }},
		{"term-regression", func(entry *TailEntry) { entry.Term = cursor.SourceCut().Term - 1 }},
		{"route-change", func(entry *TailEntry) { entry.AfterRouteGeneration++ }},
		{"previous-entry", func(entry *TailEntry) { entry.PreviousEntryDigest[0] ^= 1 }},
		{"logical-prefix", func(entry *TailEntry) { entry.BeforeLogicalDigest[0] ^= 1 }},
		{"nil-transition", func(entry *TailEntry) {
			entry.Transitions = []TailTransition{{Key: []byte("a")}}
		}},
		{"empty-document", func(entry *TailEntry) {
			entry.Transitions = []TailTransition{{Key: []byte("a"), After: []byte{}}}
		}},
		{"invalid-document", func(entry *TailEntry) {
			entry.Transitions = []TailTransition{{Key: []byte("a"), After: []byte(`{"tenant":`)}}
		}},
		{"unordered", func(entry *TailEntry) {
			entry.Transitions = []TailTransition{
				{Key: []byte("b"), After: left}, {Key: []byte("a"), After: right},
			}
		}},
	}
	sinks := []TailSink{func(TailBatch) error { return nil }, func(TailBatch) error { return nil }}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			entry := valid
			test.edit(&entry)
			var workspace TailWorkspace
			next, stats, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &workspace)
			if !errors.Is(err, ErrTailEntry) || next != cursor || stats != (TailStats{}) {
				t.Fatalf("next=%+v stats=%+v error=%v", next, stats, err)
			}
		})
	}

	want := errors.New("destination outcome unknown")
	calls := 0
	failing := []TailSink{
		func(TailBatch) error { calls++; return nil },
		func(TailBatch) error { calls++; return want },
	}
	var workspace TailWorkspace
	next, stats, err := partitioner.TranslateTailEntry(cursor, valid, failing, &workspace)
	if !errors.Is(err, want) || next != cursor || stats != (TailStats{}) || calls != 2 {
		t.Fatalf("next=%+v stats=%+v calls=%d error=%v", next, stats, calls, err)
	}
}

func TestTailDigestsBindExactBeforeImageEvenWhenChildOperationsMatch(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	left := documentsForChild(t, partitioner, 0, 2)
	right := documentForChild(t, partitioner, 1)
	first := nextTailEntry(cursor, []TailTransition{{
		Key: []byte("move"), Before: left[0], After: right,
	}}, 12)
	second := first
	second.Transitions = []TailTransition{{
		Key: []byte("move"), Before: left[1], After: right,
	}}
	sinks := []TailSink{func(TailBatch) error { return nil }, func(TailBatch) error { return nil }}
	var firstWorkspace, secondWorkspace TailWorkspace
	_, a, err := partitioner.TranslateTailEntry(cursor, first, sinks, &firstWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	_, b, err := partitioner.TranslateTailEntry(cursor, second, sinks, &secondWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	if a.Operations != b.Operations || a.Bytes != b.Bytes ||
		a.TranslationDigest == b.TranslationDigest ||
		a.ChildDigests[0] == b.ChildDigests[0] || a.ChildDigests[1] == b.ChildDigests[1] {
		t.Fatalf("digests did not bind before image: a=%+v b=%+v", a, b)
	}
}

func TestTranslateTailEntryAllocatesZeroWhenWarm(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	entry := nextTailEntry(cursor, []TailTransition{{
		Key: []byte("move"), Before: documentForChild(t, partitioner, 0),
		After: documentForChild(t, partitioner, 1),
	}}, 13)
	sinks := []TailSink{
		func(batch TailBatch) error { return consumeTailBatch(batch) },
		func(batch TailBatch) error { return consumeTailBatch(batch) },
	}
	workspace := TailWorkspace{routes: make([]tailRoute, 0, len(entry.Transitions))}
	if _, _, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &workspace); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1_000, func() {
		if _, _, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &workspace); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warm tail translation allocations = %v, want 0", allocs)
	}
}

func BenchmarkTranslateTailEntryMove(b *testing.B) {
	partitioner, cursor, _ := testTailCursor(b)
	before := documentForChild(b, partitioner, 0)
	after := documentForChild(b, partitioner, 1)
	entry := nextTailEntry(cursor, []TailTransition{{
		Key: []byte("move"), Before: before, After: after,
	}}, 14)
	sinks := []TailSink{
		func(batch TailBatch) error { return consumeTailBatch(batch) },
		func(batch TailBatch) error { return consumeTailBatch(batch) },
	}
	workspace := TailWorkspace{routes: make([]tailRoute, 0, 1)}
	if _, _, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &workspace); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(before) + len(after)))
	b.ResetTimer()
	for range b.N {
		if _, _, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &workspace); err != nil {
			b.Fatal(err)
		}
	}
}

func testTailCursor(t testing.TB) (*Partitioner, TailCursor, ChildArtifactSet) {
	t.Helper()
	partitioner := testChildArtifactPartitioner(t)
	state := testSourceState(testSplitPlan(t, "node-b"))
	rows := []childArtifactTestRow{{
		key: []byte("base"), value: documentForChild(t, partitioner, 1),
	}}
	_, set, _ := writeChildArtifactRows(t, partitioner, state, rows)
	cursor, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	return partitioner, cursor, set
}

func testTernaryTailCursor(t testing.TB) (*Partitioner, TailCursor) {
	t.Helper()
	current, err := distribution.NewManifest("orders", 11, []distribution.Shard{{
		ID: "source", AllocationGeneration: 7,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"node-a"}, Epoch: 5,
	}})
	if err != nil {
		t.Fatal(err)
	}
	source := autosplit.SourceIdentity{
		Distribution: "orders", Shard: "source", AllocationGeneration: 7,
		Range:          distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		BucketBits:     distribution.MinVirtualBucketBits,
		RoutingVersion: 11, OwnershipEpoch: 5,
	}
	plan, err := autosplit.PlanSplit(current, autosplit.SplitRequest{
		Recommendation: autosplit.Recommendation{
			Source: source, Kind: autosplit.RecommendationIsolateBucket,
			Boundaries: [2]distribution.KeyspacePoint{{0x40}, {0x41}}, BoundaryCount: 2,
			CandidateBin: 16, HotBucketStart: distribution.KeyspacePoint{0x40}, BenefitPPM: 1,
		},
		RetainChild: 1, NextRoutingVersion: 12, AllocationHighWater: 7,
		Destinations: []autosplit.Destination{
			{Shard: "left", AllocationGeneration: 8, Leaders: []distribution.EndpointID{"node-b"}, OwnershipEpoch: 1},
			{Shard: "right", AllocationGeneration: 9, Leaders: []distribution.EndpointID{"node-c"}, OwnershipEpoch: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitioner, err := NewPartitioner(
		plan, "docs", []string{"/tenant", "/sequence"},
		distribution.MinVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	var outputs [autosplit.MaxSplitChildren]bytes.Buffer
	options := ChildArtifactOptions{TargetChunkBytes: MinChildArtifactChunkBytes}
	for _, child := range []int{0, 2} {
		options.Writers[child] = &outputs[child]
		options.PayloadBuffers[child] = make([]byte, 0, MaxChildArtifactChunkBytes)
	}
	var workspace ChildArtifactWorkspace
	set, err := partitioner.writeChildArtifacts(
		testSourceState(plan), rangeChildArtifactRows(nil, nil), options, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	return partitioner, cursor
}

func nextTailEntry(cursor TailCursor, transitions []TailTransition, marker byte) TailEntry {
	cut := cursor.SourceCut()
	return TailEntry{
		Applied: cut.Applied + 1, Term: cut.Term + 1,
		BeforeOwnershipEpoch:  cursor.ownershipEpoch,
		AfterOwnershipEpoch:   cursor.ownershipEpoch,
		BeforeRoutingVersion:  cursor.routingVersion,
		AfterRoutingVersion:   cursor.routingVersion,
		BeforeRouteGeneration: cursor.routeGeneration,
		AfterRouteGeneration:  cursor.routeGeneration,
		PreviousEntryDigest:   cut.EntryDigest,
		EntryDigest:           [32]byte{marker},
		BeforeLogicalDigest:   cut.LogicalDigest,
		AfterLogicalDigest:    [32]byte{marker, 1},
		Transitions:           transitions,
	}
}

func appendTailBatch(dst []tailOperationCopy, batch TailBatch) []tailOperationCopy {
	iterator := batch.Iterator()
	count := uint64(0)
	bytesCount := uint64(0)
	for iterator.Next() {
		operation := iterator.Operation()
		dst = append(dst, tailOperationCopy{
			kind: operation.Kind, key: bytes.Clone(operation.Key), value: bytes.Clone(operation.Value),
		})
		count++
		bytesCount += uint64(len(operation.Key) + len(operation.Value))
	}
	if count != batch.Operations || bytesCount != batch.Bytes ||
		batch.Digest == ([32]byte{}) || batch.TranslationDigest == ([32]byte{}) ||
		batch.SourceBaseDigest == ([32]byte{}) || batch.ChildBaseDigest == ([32]byte{}) {
		panic("tail batch counters or digests")
	}
	return dst
}

func consumeTailBatch(batch TailBatch) error {
	iterator := batch.Iterator()
	count := uint64(0)
	for iterator.Next() {
		_ = iterator.Operation()
		count++
	}
	if count != batch.Operations {
		return ErrTailEntry
	}
	return nil
}

func assertTailOperations(t testing.TB, got, want []tailOperationCopy) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("operations = %d, want %d: %+v", len(got), len(want), got)
	}
	for ordinal := range want {
		if got[ordinal].kind != want[ordinal].kind ||
			!bytes.Equal(got[ordinal].key, want[ordinal].key) ||
			!bytes.Equal(got[ordinal].value, want[ordinal].value) {
			t.Fatalf("operation[%d] = %+v, want %+v", ordinal, got[ordinal], want[ordinal])
		}
	}
}
