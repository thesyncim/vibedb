package storeio

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestCutIndexTermLeavesRejectsUnsplitableKey(t *testing.T) {
	liveTile, _ := cutterTestLive()
	key := make([]byte, IndexTermMaxKeyBytes)
	key[0] = byte(IndexTermString)
	record := IndexTermKeyRecord{
		Canonical: key,
		RouteHash: IndexTermRouteHash(testStoreID, key),
	}
	term := IndexTermLeafTerm{
		Key: record,
		Postings: []IndexTermLeafPosting{{
			Posting:    TermPosting{TileID: 1, Rows: 1},
			Live:       liveTile,
			Chunk0Bits: 1, Chunk0Only: true,
		}},
	}
	err := CutIndexTermLeaves(
		[]IndexTermLeafTerm{term}, IndexTermLeafCutBudget(4096),
		func([]IndexTermLeafTerm, bool) error {
			t.Fatal("unsplittable term was emitted")
			return nil
		},
	)
	if !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("cut error = %v, want %v", err, ErrInvalidWrite)
	}
}

// cutterTestTerm builds one canonical chunk-0 term with the given posting
// tiles, in the exact input shape the durable fold and bulk build feed the
// cutter.
func cutterTestTerm(
	t testing.TB, value string, tiles []uint32, rowsPerTile uint16,
	live *[TermPostingTileChunks]uint64,
) IndexTermLeafTerm {
	t.Helper()
	key, ok := AppendIndexTermKey(nil, []IndexTermComponent{{
		Kind: IndexTermString, Direction: IndexTermAscending,
		JSON: []byte(fmt.Sprintf("%q", value)),
	}})
	if !ok {
		t.Fatalf("canonical key for %q", value)
	}
	record, ok := OpenIndexTermKeyRecord(testStoreID, key)
	if !ok {
		t.Fatalf("key record for %q", value)
	}
	sorted := append([]uint32(nil), tiles...)
	slices.Sort(sorted)
	postings := make([]IndexTermLeafPosting, len(sorted))
	var bits uint64
	for i := 0; i < int(rowsPerTile); i++ {
		bits |= uint64(1) << i
	}
	for i, tileID := range sorted {
		postings[i] = IndexTermLeafPosting{
			Posting:    TermPosting{TileID: tileID, Rows: rowsPerTile},
			Live:       live,
			Chunk0Bits: bits, Chunk0Only: true,
		}
	}
	return IndexTermLeafTerm{Key: record, Postings: postings}
}

type cutterTestLeaf struct {
	piece   bool
	encoded []byte
}

// cutterTestCut runs the cutter and encodes every emitted leaf, proving each
// one actually fits the budget and admits.
func cutterTestCut(
	t testing.TB, terms []IndexTermLeafTerm, budget int,
	live IndexTermLeafLiveLookup,
) []cutterTestLeaf {
	t.Helper()
	var out []cutterTestLeaf
	err := CutIndexTermLeaves(
		terms, budget,
		func(leaf []IndexTermLeafTerm, piece bool) error {
			encoded, encErr := AppendIndexTermLeaf(nil, testStoreID, leaf)
			if encErr != nil {
				return encErr
			}
			if len(encoded) > budget {
				t.Fatalf("emitted leaf holds %d bytes over budget %d",
					len(encoded), budget)
			}
			if _, viewErr := OpenIndexTermLeaf(
				encoded, testStoreID, live,
			); viewErr != nil {
				return viewErr
			}
			out = append(out, cutterTestLeaf{piece: piece, encoded: encoded})
			return nil
		},
	)
	if err != nil {
		t.Fatalf("cut: %v", err)
	}
	return out
}

func cutterTestLive() (
	*[TermPostingTileChunks]uint64, IndexTermLeafLiveLookup,
) {
	live := &[TermPostingTileChunks]uint64{}
	live[0] = ^uint64(0)
	return live, func(uint32) *[TermPostingTileChunks]uint64 { return live }
}

// TestCutIndexTermLeavesComposition pins the locality property the dirty-leaf
// fold depends on: cutting the whole term sequence equals concatenating
// independent cuts of its rule-1 runs (and of each giant term), byte for
// byte. Without this, a fold that re-cuts only dirty runs could not splice
// its output into carried leaves and keep the bulk-build identity.
func TestCutIndexTermLeavesComposition(t *testing.T) {
	liveTile, live := cutterTestLive()
	budget := IndexTermLeafCutBudget(64 << 10)
	var terms []IndexTermLeafTerm
	for i := 0; i < 400; i++ {
		tiles := make([]uint32, 0, 8)
		for j := 0; j < 1+i%7; j++ {
			tiles = append(tiles, uint32(i*13+j*97)%4096)
		}
		terms = append(terms, cutterTestTerm(
			t, fmt.Sprintf("value-%05d", i), tiles, 3, liveTile,
		))
	}
	// One giant term in the middle: enough postings that its estimate
	// overruns the budget and rule 2 must stripe it.
	giantTiles := make([]uint32, 0, 6000)
	for tile := uint32(0); tile < 12000; tile += 2 {
		giantTiles = append(giantTiles, tile)
	}
	giant := cutterTestTerm(t, "value-00250-giant", giantTiles, 5, liveTile)
	slices.SortFunc(terms, func(a, b IndexTermLeafTerm) int {
		return slices.Compare(a.Key.Canonical, b.Key.Canonical)
	})
	at, _ := slices.BinarySearchFunc(
		terms, giant, func(a, b IndexTermLeafTerm) int {
			return slices.Compare(a.Key.Canonical, b.Key.Canonical)
		},
	)
	terms = slices.Insert(terms, at, giant)

	whole := cutterTestCut(t, terms, budget, live)

	// Independent per-segment cuts: split at every rule-1 cut term and
	// around the giant term, cut each segment alone, concatenate.
	var spliced []cutterTestLeaf
	segStart := 0
	flush := func(end int) {
		if end == segStart {
			return
		}
		spliced = append(
			spliced, cutterTestCut(t, terms[segStart:end], budget, live)...,
		)
		segStart = end
	}
	for i := range terms {
		if IndexTermLeafRunCut(terms[i].Key.RouteHash) {
			flush(i)
		}
		if IndexTermLeafGiant(
			IndexTermLeafEstimateTermBytes(&terms[i]), budget,
		) {
			flush(i)
			flush(i + 1)
		}
	}
	flush(len(terms))

	if len(whole) != len(spliced) {
		t.Fatalf("whole cut emits %d leaves, spliced %d",
			len(whole), len(spliced))
	}
	pieces := 0
	for i := range whole {
		if whole[i].piece != spliced[i].piece ||
			!slices.Equal(whole[i].encoded, spliced[i].encoded) {
			t.Fatalf("leaf %d differs: piece %v/%v, %d vs %d bytes",
				i, whole[i].piece, spliced[i].piece,
				len(whole[i].encoded), len(spliced[i].encoded))
		}
		if whole[i].piece {
			pieces++
		}
	}
	if pieces == 0 {
		t.Fatal("giant term emitted no stripe pieces; rule 2 went unexercised")
	}
	if len(whole) < 3 {
		t.Fatalf("corpus cut into only %d leaves; composition unexercised", len(whole))
	}
}

// TestCutGiantIndexTermHardCapAndLocality is the adversarial term-size gate:
// one term holding more than 65,535 rows inside a single stripe (2,048 tiles
// at 64 rows each = 131,072 rows) must emit through the rule-3 hard-cap path
// under a small budget — every piece fitting and admitting — and a content
// change inside one stripe must leave every other stripe's pieces
// byte-identical (the locality claim the stripe-patch fold relies on).
func TestCutGiantIndexTermHardCapAndLocality(t *testing.T) {
	liveTile, live := cutterTestLive()
	// Three full stripes: tiles [0, 2048), [2048, 4096), [4096, 6144), all
	// 64 rows per tile. Stripe 0 alone holds 2048*64 = 131,072 rows.
	tiles := make([]uint32, 0, 3*IndexTermLeafStripeTiles)
	for tile := uint32(0); tile < 3*IndexTermLeafStripeTiles; tile++ {
		tiles = append(tiles, tile)
	}
	term := cutterTestTerm(t, "adversarial-giant", tiles, 64, liveTile)
	rows := 0
	for i := range term.Postings {
		rows += int(term.Postings[i].Posting.Rows)
	}
	if rows/3 <= 65535 {
		t.Fatalf("stripe holds %d rows, want > 65535", rows/3)
	}

	// A small budget forces rule-3 sub-cuts inside every stripe: a full
	// 2048-posting stripe estimates ~32 KiB, so an 8 KiB budget needs
	// several pieces per stripe.
	const budget = 8 << 10
	var pieceStripes []uint32
	var pieces [][]byte
	if err := CutGiantIndexTerm(
		&term, budget,
		func(leaf []IndexTermLeafTerm, piece bool) error {
			if !piece || len(leaf) != 1 {
				t.Fatalf("giant term emitted a non-piece leaf")
			}
			encoded, err := AppendIndexTermLeaf(nil, testStoreID, leaf)
			if err != nil {
				return err
			}
			if len(encoded) > budget {
				t.Fatalf("piece holds %d bytes over budget %d",
					len(encoded), budget)
			}
			if _, err := OpenIndexTermLeaf(encoded, testStoreID, live); err != nil {
				return err
			}
			first := leaf[0].Postings[0].Posting.TileID
			last := leaf[0].Postings[len(leaf[0].Postings)-1].Posting.TileID
			if IndexTermLeafStripe(first) != IndexTermLeafStripe(last) {
				t.Fatalf("piece spans stripes %d and %d",
					IndexTermLeafStripe(first), IndexTermLeafStripe(last))
			}
			pieceStripes = append(pieceStripes, IndexTermLeafStripe(first))
			pieces = append(pieces, encoded)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	perStripe := map[uint32]int{}
	for _, stripe := range pieceStripes {
		perStripe[stripe]++
	}
	for stripe, count := range perStripe {
		if count < 2 {
			t.Fatalf("stripe %d emitted %d piece(s); hard-cap sub-cut unexercised",
				stripe, count)
		}
	}
	if len(perStripe) != 3 {
		t.Fatalf("pieces cover %d stripes, want 3", len(perStripe))
	}

	// Locality: drop half of stripe 1's postings and re-cut. Stripe 0 and
	// stripe 2 pieces must be byte-identical to the first cut.
	mutated := term
	kept := make([]IndexTermLeafPosting, 0, len(term.Postings))
	for i := range term.Postings {
		tile := term.Postings[i].Posting.TileID
		if IndexTermLeafStripe(tile) == 1 && tile%2 == 0 {
			continue
		}
		kept = append(kept, term.Postings[i])
	}
	mutated.Postings = kept
	var mutatedStripes []uint32
	var mutatedPieces [][]byte
	if err := CutGiantIndexTerm(
		&mutated, budget,
		func(leaf []IndexTermLeafTerm, _ bool) error {
			encoded, err := AppendIndexTermLeaf(nil, testStoreID, leaf)
			if err != nil {
				return err
			}
			mutatedStripes = append(
				mutatedStripes,
				IndexTermLeafStripe(leaf[0].Postings[0].Posting.TileID),
			)
			mutatedPieces = append(mutatedPieces, encoded)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	filterPieces := func(
		stripes []uint32, encoded [][]byte, stripe uint32,
	) [][]byte {
		var out [][]byte
		for i := range stripes {
			if stripes[i] == stripe {
				out = append(out, encoded[i])
			}
		}
		return out
	}
	for _, stripe := range []uint32{0, 2} {
		before := filterPieces(pieceStripes, pieces, stripe)
		after := filterPieces(mutatedStripes, mutatedPieces, stripe)
		if len(before) != len(after) {
			t.Fatalf("stripe %d piece count changed: %d vs %d",
				stripe, len(before), len(after))
		}
		for i := range before {
			if !slices.Equal(before[i], after[i]) {
				t.Fatalf("stripe %d piece %d changed under a stripe-1 mutation",
					stripe, i)
			}
		}
	}
	if slices.Equal(
		filterPieces(pieceStripes, pieces, 1)[0],
		filterPieces(mutatedStripes, mutatedPieces, 1)[0],
	) {
		t.Fatal("stripe 1 mutation did not change its own pieces")
	}
}

// TestCutIndexTermLeavesDeterminism pins that two cuts of the same content
// are identical — the cutter carries no hidden state between calls.
func TestCutIndexTermLeavesDeterminism(t *testing.T) {
	liveTile, live := cutterTestLive()
	budget := IndexTermLeafCutBudget(64 << 10)
	var terms []IndexTermLeafTerm
	for i := 0; i < 900; i++ {
		terms = append(terms, cutterTestTerm(
			t, fmt.Sprintf("d-%06d", i),
			[]uint32{uint32(i % 512), uint32(1000 + i%512)}, 7, liveTile,
		))
	}
	first := cutterTestCut(t, terms, budget, live)
	second := cutterTestCut(t, terms, budget, live)
	if len(first) != len(second) {
		t.Fatalf("leaf counts differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if !slices.Equal(first[i].encoded, second[i].encoded) {
			t.Fatalf("leaf %d not deterministic", i)
		}
	}
}
