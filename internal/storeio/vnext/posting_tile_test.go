package vnext

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"math/rand"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

type postingTileCase struct {
	name    string
	live    [storeio.TermPostingTileChunks]uint64
	posting [storeio.TermPostingTileChunks]uint64
	codec   storeio.TermPostingCodec
}

func fullPostingTile() [storeio.TermPostingTileChunks]uint64 {
	var tile [storeio.TermPostingTileChunks]uint64
	for chunk := range tile {
		tile[chunk] = ^uint64(0)
	}
	return tile
}

func postingTileCases() []postingTileCase {
	full := fullPostingTile()
	empty := postingTileCase{
		name: "empty", live: full, codec: storeio.TermPostingEmpty,
	}
	allLive := postingTileCase{
		name: "all-live", live: full, posting: full,
		codec: storeio.TermPostingAllLive,
	}
	dense := postingTileCase{
		name: "dense-alternating", live: full,
		codec: storeio.TermPostingDense,
	}
	for chunk := range dense.posting {
		dense.posting[chunk] = 0xaaaaaaaaaaaaaaaa
	}
	runs := postingTileCase{
		name: "two-runs", live: full, codec: storeio.TermPostingRuns,
	}
	setPostingRows(&runs.posting, 100, 201)
	setPostingRows(&runs.posting, 1000, 1301)
	sparseMasks := postingTileCase{
		name: "one-wide-mask", live: full,
		codec: storeio.TermPostingSparseMasks,
	}
	sparseMasks.posting[3] = 0x5555555555555555
	sparseRows := postingTileCase{
		name: "one-row-per-chunk", live: full,
		codec: storeio.TermPostingSparseRows,
	}
	for chunk := range sparseRows.posting {
		sparseRows.posting[chunk] = 1
	}
	inline := postingTileCase{
		name: "inline-singleton", live: full,
		codec: storeio.TermPostingSparseMasks,
	}
	inline.posting[0] = 1
	return []postingTileCase{
		empty, allLive, dense, runs, sparseMasks, sparseRows, inline,
	}
}

func TestTermPostingTileEveryCodecRoundTrip(t *testing.T) {
	for _, test := range postingTileCases() {
		t.Run(test.name, func(t *testing.T) {
			record, component, view := buildAndOpenPostingTile(
				t, 91, &test.posting, &test.live,
			)
			if record.Codec != test.codec || view.Codec() != test.codec {
				t.Fatalf(
					"codec = (%v,%v), want %v",
					record.Codec, view.Codec(), test.codec,
				)
			}
			if int(record.EncodedBytes) <= storeio.TermPostingInlineBytes {
				if record.Placement != storeio.TermPostingInline ||
					component != 0 {
					t.Fatalf(
						"small placement = (%v,%d), want inline",
						record.Placement, component,
					)
				}
			} else if record.Placement != storeio.TermPostingManifest ||
				component != int(record.EncodedBytes) {
				t.Fatalf(
					"large placement = (%v,%d), want manifest/%d",
					record.Placement, component, record.EncodedBytes,
				)
			}
			assertPostingTileView(t, view, &test.posting)
		})
	}
}

func TestTermPostingAllLiveRequiresACompletelyOccupiedTile(t *testing.T) {
	live := fullPostingTile()
	live[len(live)-1] &^= uint64(1) << 63
	posting := live
	var component [storeio.TermPostingMaxPayloadBytes]byte
	record, componentBytes, err := storeio.BuildTermPosting(
		component[:], 9, &posting, &live,
	)
	if err != nil {
		t.Fatal(err)
	}
	if record.Codec == storeio.TermPostingAllLive {
		t.Fatal("partial mutable live mask selected zero-byte all-live codec")
	}
	if _, err := storeio.OpenTermPosting(
		storeio.TermPosting{
			TileID: 9, Rows: storeio.TermPostingTileRows - 1,
			Codec: storeio.TermPostingAllLive,
		},
		nil, &live,
	); !errors.Is(err, storeio.ErrTermPostingCorrupt) {
		t.Fatalf("partial all-live admission error = %v", err)
	}
	var payload []byte
	if componentBytes != 0 {
		payload = component[:componentBytes]
	}
	view, err := storeio.OpenTermPosting(record, payload, &live)
	if err != nil {
		t.Fatal(err)
	}
	if view.Rows() != storeio.TermPostingTileRows-1 {
		t.Fatalf("partial explicit rows = %d", view.Rows())
	}
}

// The first twelve stable rows exhaust every small sparse/run topology,
// including empty, singleton, alternating, adjacent, and split-run shapes.
func TestTermPostingTileExhaustiveTwelveRows(t *testing.T) {
	live := fullPostingTile()
	var component [storeio.TermPostingMaxPayloadBytes]byte
	for pattern := 0; pattern < 1<<12; pattern++ {
		var posting [storeio.TermPostingTileChunks]uint64
		posting[0] = uint64(pattern)
		record, n, err := storeio.BuildTermPosting(
			component[:], uint32(pattern), &posting, &live,
		)
		if err != nil {
			t.Fatalf("pattern %03x encode: %v", pattern, err)
		}
		view, err := storeio.OpenTermPosting(
			record, component[:n:n], &live,
		)
		if err != nil {
			t.Fatalf(
				"pattern %03x open codec %v bytes %d: %v",
				pattern, record.Codec, record.EncodedBytes, err,
			)
		}
		assertPostingTileView(t, view, &posting)
	}
}

func TestTermPostingTileSeededRandomRoundTrip(t *testing.T) {
	var component [storeio.TermPostingMaxPayloadBytes]byte
	for seed := int64(0); seed < 512; seed++ {
		random := rand.New(rand.NewSource(seed))
		var live, posting [storeio.TermPostingTileChunks]uint64
		for chunk := range live {
			live[chunk] = random.Uint64()
			posting[chunk] = random.Uint64() & live[chunk]
			switch seed % 7 {
			case 0:
				posting[chunk] &= 0x0101010101010101
			case 1:
				posting[chunk] &= 0x000000000000ffff
			case 2:
				posting[chunk] = live[chunk]
			}
		}
		record, n, err := storeio.BuildTermPosting(
			component[:], uint32(seed), &posting, &live,
		)
		if err != nil {
			t.Fatalf("seed %d encode: %v", seed, err)
		}
		view, err := storeio.OpenTermPosting(record, component[:n:n], &live)
		if err != nil {
			t.Fatalf(
				"seed %d open codec %v bytes %d: %v",
				seed, record.Codec, record.EncodedBytes, err,
			)
		}
		assertPostingTileView(t, view, &posting)
	}
}

func TestTermPostingTileContentSharingAndCOWMutation(t *testing.T) {
	dense := postingTileCases()[2]
	var firstBytes, secondBytes [storeio.TermPostingMaxPayloadBytes]byte
	first, firstN, err := storeio.BuildTermPosting(
		firstBytes[:], 10, &dense.posting, &dense.live,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, secondN, err := storeio.BuildTermPosting(
		secondBytes[:], 999, &dense.posting, &dense.live,
	)
	if err != nil {
		t.Fatal(err)
	}
	if firstN == 0 || firstN != secondN ||
		first.ComponentID != second.ComponentID ||
		!slices.Equal(firstBytes[:firstN], secondBytes[:secondN]) {
		t.Fatalf(
			"tile-independent content = (%d,%d,%x,%x), want identical",
			firstN, secondN, first.ComponentID, second.ComponentID,
		)
	}

	oldView, err := storeio.OpenTermPosting(
		first, firstBytes[:firstN:firstN], &dense.live,
	)
	if err != nil {
		t.Fatal(err)
	}
	var edited [storeio.TermPostingTileChunks]uint64
	if !oldView.MasksInto(&edited) || edited != dense.posting {
		t.Fatal("MasksInto did not reproduce the immutable source")
	}
	edited[0] &^= uint64(1) << 1
	var nextBytes [storeio.TermPostingMaxPayloadBytes]byte
	next, nextN, err := storeio.BuildTermPosting(
		nextBytes[:], first.TileID, &edited, &dense.live,
	)
	if err != nil {
		t.Fatal(err)
	}
	if next.ComponentID == first.ComponentID ||
		nextN == firstN && slices.Equal(nextBytes[:nextN], firstBytes[:firstN]) {
		t.Fatal("stable-slot delete reused changed component content")
	}
	// The prior snapshot's view still names the old immutable bytes.
	assertPostingTileView(t, oldView, &dense.posting)
	nextView, err := storeio.OpenTermPosting(
		next, nextBytes[:nextN:nextN], &dense.live,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertPostingTileView(t, nextView, &edited)
}

func TestTermPostingTileRejectsInvalidWrites(t *testing.T) {
	full := fullPostingTile()
	posting := full
	posting[0] = 1
	live := full
	live[0] = 0
	var component [storeio.TermPostingMaxPayloadBytes]byte
	if _, _, err := storeio.BuildTermPosting(
		component[:], 1, &posting, &live,
	); err == nil {
		t.Fatal("BuildTermPosting accepted a dead stable slot")
	}
	if _, _, err := storeio.BuildTermPosting(
		component[:], 1, nil, &live,
	); err == nil {
		t.Fatal("BuildTermPosting accepted a nil posting")
	}
	dense := postingTileCases()[2]
	if _, _, err := storeio.BuildTermPosting(
		component[:storeio.TermPostingDenseBytes-1],
		1, &dense.posting, &dense.live,
	); err == nil {
		t.Fatal("BuildTermPosting accepted a short component buffer")
	}
}

func TestTermPostingTileRejectsCorruptionAndNonCanonicalPayloads(t *testing.T) {
	cases := postingTileCases()
	dense := cases[2]
	record, n, _ := buildAndOpenPostingTile(
		t, 7, &dense.posting, &dense.live,
	)
	var component [storeio.TermPostingMaxPayloadBytes]byte
	_, got, err := storeio.BuildTermPosting(
		component[:], 7, &dense.posting, &dense.live,
	)
	if err != nil || got != n {
		t.Fatalf("dense rebuild = (%d,%v), want %d", got, err, n)
	}
	for offset := 0; offset < n; offset++ {
		corrupt := slices.Clone(component[:n])
		corrupt[offset] ^= 1
		if _, err := storeio.OpenTermPosting(
			record, corrupt, &dense.live,
		); !errors.Is(err, storeio.ErrTermPostingCorrupt) {
			t.Fatalf("component byte %d corruption = %v", offset, err)
		}
	}

	inline := cases[len(cases)-1]
	inlineRecord, _, _ := buildAndOpenPostingTile(
		t, 8, &inline.posting, &inline.live,
	)
	badTail := inlineRecord
	badTail.Inline[storeio.TermPostingInlineBytes-1] = 1
	if _, err := storeio.OpenTermPosting(
		badTail, nil, &inline.live,
	); !errors.Is(err, storeio.ErrTermPostingCorrupt) {
		t.Fatalf("dirty inline tail = %v", err)
	}
	badRows := inlineRecord
	badRows.Rows++
	if _, err := storeio.OpenTermPosting(
		badRows, nil, &inline.live,
	); !errors.Is(err, storeio.ErrTermPostingCorrupt) {
		t.Fatalf("wrong row count = %v", err)
	}
	if _, err := storeio.OpenTermPosting(
		inlineRecord, []byte{0}, &inline.live,
	); !errors.Is(err, storeio.ErrTermPostingCorrupt) {
		t.Fatalf("inline posting accepted an external component = %v", err)
	}
	badPlacement := inlineRecord
	badPlacement.Placement = storeio.TermPostingManifest
	if _, err := storeio.OpenTermPosting(
		badPlacement, inlineRecord.Inline[:inlineRecord.EncodedBytes],
		&inline.live,
	); !errors.Is(err, storeio.ErrTermPostingCorrupt) {
		t.Fatalf("non-canonical placement = %v", err)
	}

	for _, malformed := range []struct {
		name    string
		codec   storeio.TermPostingCodec
		rows    uint16
		payload []byte
	}{
		{
			name:  "overlong sparse-row varint",
			codec: storeio.TermPostingSparseRows, rows: 1,
			payload: []byte{0x81, 0x00},
		},
		{
			name:  "duplicate sparse row",
			codec: storeio.TermPostingSparseRows, rows: 2,
			payload: []byte{1, 0},
		},
		{
			name:  "multi-mask spelling singleton",
			codec: storeio.TermPostingSparseMasks, rows: 1,
			payload: append([]byte{1}, littleEndianMask(1)...),
		},
		{
			name:  "adjacent non-maximal runs",
			codec: storeio.TermPostingRuns, rows: 2,
			payload: []byte{0, 0, 0, 0},
		},
		{
			name:  "dense spelling sparse singleton",
			codec: storeio.TermPostingDense, rows: 1,
			payload: denseSingletonPayload(),
		},
	} {
		t.Run(malformed.name, func(t *testing.T) {
			candidate := inlineTermPosting(
				3, malformed.codec, malformed.rows, malformed.payload,
			)
			var external []byte
			if len(malformed.payload) > storeio.TermPostingInlineBytes {
				candidate.Placement = storeio.TermPostingManifest
				clear(candidate.Inline[:])
				id, identityErr := storeio.TermPostingComponentIdentity(
					candidate.Codec, candidate.Rows, malformed.payload,
				)
				if identityErr != nil {
					t.Fatal(identityErr)
				}
				candidate.ComponentID = id
				external = malformed.payload
			}
			if _, err := storeio.OpenTermPosting(
				candidate, external, &dense.live,
			); !errors.Is(err, storeio.ErrTermPostingCorrupt) {
				t.Fatalf("OpenTermPosting = %v", err)
			}
		})
	}
}

func TestTermPostingTileZeroAllocationBuildOpenAndIteration(t *testing.T) {
	for _, test := range postingTileCases() {
		t.Run(test.name, func(t *testing.T) {
			var component [storeio.TermPostingMaxPayloadBytes]byte
			record, n, err := storeio.BuildTermPosting(
				component[:], 41, &test.posting, &test.live,
			)
			if err != nil {
				t.Fatal(err)
			}
			view, err := storeio.OpenTermPosting(
				record, component[:n:n], &test.live,
			)
			if err != nil {
				t.Fatal(err)
			}
			var expanded [storeio.TermPostingTileChunks]uint64
			if allocs := testing.AllocsPerRun(1000, func() {
				record, n, err = storeio.BuildTermPosting(
					component[:], 41, &test.posting, &test.live,
				)
				if err != nil {
					panic(err)
				}
				view, err = storeio.OpenTermPosting(
					record, component[:n:n], &test.live,
				)
				if err != nil {
					panic(err)
				}
				if !view.MasksInto(&expanded) {
					panic("materialize")
				}
				iterator := view.Iterator()
				for {
					_, ok := iterator.Next()
					if !ok {
						break
					}
				}
			}); allocs != 0 {
				t.Fatalf("warmed build/open/iterate allocated %.2f times", allocs)
			}
		})
	}
}

func TestTermPostingTileSpaceKillGates(t *testing.T) {
	gates := map[string]float64{
		"all-live":          0.01,
		"dense-alternating": 0.30,
		"two-runs":          0.05,
		"one-wide-mask":     0.60,
		"one-row-per-chunk": 0.10,
		"inline-singleton":  0.30,
	}
	t.Log("pattern                codec          chunks current-B vnext-B ratio")
	for _, test := range postingTileCases() {
		if test.name == "empty" {
			continue
		}
		record, _, _ := buildAndOpenPostingTile(
			t, 1, &test.posting, &test.live,
		)
		chunks := nonemptyPostingChunks(&test.posting)
		current := chunks * 32
		next := record.ReachableBytes()
		ratio := float64(next) / float64(current)
		t.Logf(
			"%-22s %-14v %6d %9d %7d %.3f",
			test.name, record.Codec, chunks, current, next, ratio,
		)
		if gate := gates[test.name]; ratio > gate {
			t.Fatalf(
				"%s space ratio %.3f exceeds kill gate %.3f",
				test.name, ratio, gate,
			)
		}
	}
}

func FuzzTermPostingTileRoundTrip(f *testing.F) {
	for _, seed := range [][]byte{
		{},
		{0},
		{0xff},
		{0x55, 0xaa},
		{1, 2, 3, 4, 5, 6, 7, 8},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed []byte) {
		var live, posting [storeio.TermPostingTileChunks]uint64
		for row := 0; row < storeio.TermPostingTileRows; row++ {
			if len(seed) == 0 {
				continue
			}
			value := seed[row%len(seed)]
			if value&(1<<uint(row&7)) != 0 {
				live[row>>6] |= uint64(1) << (row & 63)
			}
			if value&(1<<uint((row+3)&7)) != 0 {
				posting[row>>6] |= uint64(1) << (row & 63)
			}
		}
		for chunk := range posting {
			posting[chunk] &= live[chunk]
		}
		var component [storeio.TermPostingMaxPayloadBytes]byte
		record, n, err := storeio.BuildTermPosting(
			component[:], 123, &posting, &live,
		)
		if err != nil {
			t.Fatal(err)
		}
		view, err := storeio.OpenTermPosting(record, component[:n:n], &live)
		if err != nil {
			t.Fatal(err)
		}
		assertPostingTileView(t, view, &posting)
	})
}

func buildAndOpenPostingTile(
	t testing.TB,
	tileID uint32,
	posting, live *[storeio.TermPostingTileChunks]uint64,
) (storeio.TermPosting, int, storeio.TermPostingView) {
	t.Helper()
	var component [storeio.TermPostingMaxPayloadBytes]byte
	record, n, err := storeio.BuildTermPosting(
		component[:], tileID, posting, live,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := storeio.OpenTermPosting(record, component[:n:n], live)
	if err != nil {
		t.Fatal(err)
	}
	return record, n, view
}

func assertPostingTileView(
	t testing.TB,
	view storeio.TermPostingView,
	want *[storeio.TermPostingTileChunks]uint64,
) {
	t.Helper()
	var got [storeio.TermPostingTileChunks]uint64
	rows := 0
	previous := -1
	iterator := view.Iterator()
	for {
		mask, ok := iterator.Next()
		if !ok {
			break
		}
		if int(mask.Chunk) <= previous || mask.Bits == 0 {
			t.Fatalf("non-canonical iterator mask after chunk %d: %+v", previous, mask)
		}
		got[mask.Chunk] = mask.Bits
		rows += bits.OnesCount64(mask.Bits)
		previous = int(mask.Chunk)
	}
	if got != *want || rows != int(view.Rows()) {
		t.Fatalf(
			"posting mismatch codec %v rows %d/%d\ngot  %x\nwant %x",
			view.Codec(), rows, view.Rows(), got, *want,
		)
	}
}

func setPostingRows(
	posting *[storeio.TermPostingTileChunks]uint64,
	start, end int,
) {
	for row := start; row < end; row++ {
		posting[row>>6] |= uint64(1) << (row & 63)
	}
}

func nonemptyPostingChunks(
	posting *[storeio.TermPostingTileChunks]uint64,
) int {
	count := 0
	for _, mask := range posting {
		if mask != 0 {
			count++
		}
	}
	return count
}

func littleEndianMask(mask uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], mask)
	return encoded[:]
}

func denseSingletonPayload() []byte {
	payload := make([]byte, storeio.TermPostingDenseBytes)
	payload[0] = 1
	return payload
}

func inlineTermPosting(
	tileID uint32,
	codec storeio.TermPostingCodec,
	rows uint16,
	payload []byte,
) storeio.TermPosting {
	record := storeio.TermPosting{
		TileID: tileID, Rows: rows, Codec: codec,
		Placement:    storeio.TermPostingInline,
		EncodedBytes: uint16(len(payload)),
	}
	if len(payload) <= len(record.Inline) {
		copy(record.Inline[:], payload)
	}
	return record
}
