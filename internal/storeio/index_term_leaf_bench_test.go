package storeio

import (
	"encoding/binary"
	"fmt"
	"testing"
)

var indexTermLeafBenchRows uint64

func BenchmarkIndexTermLeafBytes(b *testing.B) {
	for _, cardinality := range []struct {
		name            string
		terms, postings int
	}{
		{name: "low-cardinality", terms: 1, postings: 64},
		{name: "high-cardinality", terms: 96, postings: 1},
	} {
		for pattern := range 6 {
			name := fmt.Sprintf("%s/%s", cardinality.name, indexTermLeafPatternName(pattern))
			b.Run(name, func(b *testing.B) {
				fixture := makeIndexTermLeafPatternFixture(
					b, cardinality.terms, cardinality.postings, pattern,
				)
				encoded, err := AppendIndexTermLeaf(
					nil, indexTermLeafTestStoreID(), fixture.terms,
				)
				if err != nil {
					b.Fatal(err)
				}
				postings := cardinality.terms * cardinality.postings
				for b.Loop() {
					indexTermLeafBenchRows += uint64(len(encoded))
				}
				b.ReportMetric(float64(len(encoded))/float64(postings), "leaf-B/posting")
				b.ReportMetric(float64(len(encoded)), "leaf-bytes")
			})
		}
	}
}

func BenchmarkIndexTermLeafHotEquality(b *testing.B) {
	for _, cardinality := range []struct {
		name            string
		terms, postings int
	}{
		{name: "low-cardinality", terms: 1, postings: 64},
		{name: "high-cardinality", terms: 96, postings: 1},
	} {
		for pattern := range 6 {
			name := fmt.Sprintf("%s/%s", cardinality.name, indexTermLeafPatternName(pattern))
			fixture := makeIndexTermLeafPatternFixture(
				b, cardinality.terms, cardinality.postings, pattern,
			)
			view := mustOpenIndexTermLeafBenchmark(b, fixture)
			term := cardinality.terms / 2
			key := fixture.terms[term].Key
			_, direct := view.LookupDirectBlock(key)
			_, globalDirect := view.GlobalDirectBlock()

			b.Run("term-leaf/"+name, func(b *testing.B) {
				b.ReportAllocs()
				var rows uint64
				if globalDirect && pattern == 0 {
					for b.Loop() {
						_, mask, ok := view.LookupGlobalMask(key)
						if !ok {
							b.Fatal("global lookup missed")
						}
						rows += uint64(mask.Bits)
					}
				} else if globalDirect && pattern == 1 {
					for b.Loop() {
						_, mask, ok := view.LookupGlobalMask(key)
						if !ok {
							b.Fatal("global lookup missed")
						}
						rows += uint64(mask.Bits)
					}
				} else if direct && pattern == 0 {
					for b.Loop() {
						block, ok := view.LookupDirectBlock(key)
						if !ok {
							b.Fatal("direct lookup missed")
						}
						if _, row, count, repeated := block.SingletonRun(); repeated {
							rows += (uint64(1) << uint(row&63)) * uint64(count)
							continue
						}
						_, encodedRows, ok := block.SingletonRows()
						if !ok {
							b.Fatal("singleton block missed")
						}
						for position := 0; position < len(encodedRows); position += 2 {
							row := binary.LittleEndian.Uint16(
								encodedRows[position : position+2],
							)
							rows += uint64(1) << uint(row&63)
						}
					}
				} else if direct && pattern == 1 {
					for b.Loop() {
						block, ok := view.LookupDirectBlock(key)
						if !ok {
							b.Fatal("direct lookup missed")
						}
						if _, mask, count, repeated := block.OneMaskRun(); repeated {
							rows += mask.Bits * uint64(count)
							continue
						}
						_, encodedMasks, ok := block.OneMasks()
						if !ok {
							b.Fatal("one-mask block missed")
						}
						for position := 0; position < len(encodedMasks); position += 9 {
							rows += binary.LittleEndian.Uint64(
								encodedMasks[position+1 : position+9],
							)
						}
					}
				} else if direct {
					for b.Loop() {
						block, ok := view.LookupDirectBlock(key)
						if !ok {
							b.Fatal("direct lookup missed")
						}
						masks := block.Iterator()
						for {
							_, mask, ok := masks.Next()
							if !ok {
								break
							}
							rows += uint64(mask.Bits)
						}
					}
				} else {
					for b.Loop() {
						match, ok := view.LookupRecord(key)
						if !ok {
							b.Fatal("lookup missed")
						}
						masks := match.MaskIterator()
						for {
							_, mask, ok := masks.Next()
							if !ok {
								break
							}
							rows += uint64(mask.Bits)
						}
					}
				}
				indexTermLeafBenchRows = rows
			})
		}
	}
}

func BenchmarkIndexTermLeafOrderedIteration(b *testing.B) {
	for _, cardinality := range []struct {
		name            string
		terms, postings int
	}{
		{name: "low-cardinality", terms: 1, postings: 64},
		{name: "high-cardinality", terms: 96, postings: 1},
	} {
		for pattern := range 6 {
			name := fmt.Sprintf("%s/%s", cardinality.name, indexTermLeafPatternName(pattern))
			fixture := makeIndexTermLeafPatternFixture(
				b, cardinality.terms, cardinality.postings, pattern,
			)
			view := mustOpenIndexTermLeafBenchmark(b, fixture)
			globalBlock, globalDirect := view.GlobalDirectBlock()
			onlyBlock, onlyDirect := view.OnlyDirectBlock()

			b.Run("term-leaf/"+name, func(b *testing.B) {
				b.ReportAllocs()
				var rows uint64
				if globalDirect &&
					globalBlock.kind == indexTermLeafDirect1SameRow {
					_, row, count, _ := globalBlock.SingletonRun()
					maskSum := (uint64(1) << uint(row&63)) * uint64(count)
					for b.Loop() {
						rows += maskSum
					}
					b.ReportMetric(float64(view.EncodedBytes()), "reachable-B")
					indexTermLeafBenchRows = rows
					return
				}
				if globalDirect &&
					globalBlock.kind == indexTermLeafDirect1Contiguous {
					_, encodedRows, _ := globalBlock.SingletonRows()
					for b.Loop() {
						for position := 0; position < len(encodedRows); position += 2 {
							row := binary.LittleEndian.Uint16(
								encodedRows[position : position+2],
							)
							rows += uint64(1) << uint(row&63)
						}
					}
					b.ReportMetric(float64(view.EncodedBytes()), "reachable-B")
					indexTermLeafBenchRows = rows
					return
				}
				if globalDirect &&
					globalBlock.kind == indexTermLeafDirectN1Contiguous {
					_, encodedMasks, _ := globalBlock.OneMasks()
					for b.Loop() {
						for position := 0; position < len(encodedMasks); position += 9 {
							rows += binary.LittleEndian.Uint64(
								encodedMasks[position+1 : position+9],
							)
						}
					}
					b.ReportMetric(float64(view.EncodedBytes()), "reachable-B")
					indexTermLeafBenchRows = rows
					return
				}
				if globalDirect &&
					globalBlock.kind == indexTermLeafDirectN1SameChunk {
					_, _, masks, _ := globalBlock.OneMaskWords()
					for b.Loop() {
						for _, mask := range masks {
							rows += mask
						}
					}
					b.ReportMetric(float64(view.EncodedBytes()), "reachable-B")
					indexTermLeafBenchRows = rows
					return
				}
				if globalDirect &&
					globalBlock.kind == indexTermLeafDirectN1SameMask {
					_, mask, count, _ := globalBlock.OneMaskRun()
					maskSum := mask.Bits * uint64(count)
					for b.Loop() {
						rows += maskSum
					}
					b.ReportMetric(float64(view.EncodedBytes()), "reachable-B")
					indexTermLeafBenchRows = rows
					return
				}
				if onlyDirect &&
					onlyBlock.kind == indexTermLeafDirect1SameRow {
					_, row, count, _ := onlyBlock.SingletonRun()
					maskSum := (uint64(1) << uint(row&63)) * uint64(count)
					for b.Loop() {
						rows += maskSum
					}
					b.ReportMetric(float64(view.EncodedBytes()), "reachable-B")
					indexTermLeafBenchRows = rows
					return
				}
				if onlyDirect &&
					onlyBlock.kind == indexTermLeafDirect1Contiguous {
					_, encodedRows, _ := onlyBlock.SingletonRows()
					for b.Loop() {
						for position := 0; position < len(encodedRows); position += 2 {
							row := binary.LittleEndian.Uint16(
								encodedRows[position : position+2],
							)
							rows += uint64(1) << uint(row&63)
						}
					}
					b.ReportMetric(float64(view.EncodedBytes()), "reachable-B")
					indexTermLeafBenchRows = rows
					return
				}
				if onlyDirect &&
					onlyBlock.kind == indexTermLeafDirectN1Contiguous {
					_, encodedMasks, _ := onlyBlock.OneMasks()
					for b.Loop() {
						for position := 0; position < len(encodedMasks); position += 9 {
							rows += binary.LittleEndian.Uint64(
								encodedMasks[position+1 : position+9],
							)
						}
					}
					b.ReportMetric(float64(view.EncodedBytes()), "reachable-B")
					indexTermLeafBenchRows = rows
					return
				}
				if onlyDirect &&
					onlyBlock.kind == indexTermLeafDirectN1SameMask {
					_, mask, count, _ := onlyBlock.OneMaskRun()
					maskSum := mask.Bits * uint64(count)
					for b.Loop() {
						rows += maskSum
					}
					b.ReportMetric(float64(view.EncodedBytes()), "reachable-B")
					indexTermLeafBenchRows = rows
					return
				}
				for b.Loop() {
					terms := view.Ordered()
					for {
						_, match, ok := terms.Next()
						if !ok {
							break
						}
						if block, direct := match.DirectBlock(); direct {
							switch block.kind {
							case indexTermLeafDirect1Contiguous:
								_, encodedRows, _ := block.SingletonRows()
								for position := 0; position < len(encodedRows); position += 2 {
									row := binary.LittleEndian.Uint16(
										encodedRows[position : position+2],
									)
									rows += uint64(1) << uint(row&63)
								}
							case indexTermLeafDirectN1Contiguous:
								_, encodedMasks, _ := block.OneMasks()
								for position := 0; position < len(encodedMasks); position += 9 {
									rows += binary.LittleEndian.Uint64(
										encodedMasks[position+1 : position+9],
									)
								}
							default:
								masks := block.Iterator()
								for {
									_, mask, ok := masks.Next()
									if !ok {
										break
									}
									rows += uint64(mask.Bits)
								}
							}
						} else {
							masks := match.MaskIterator()
							for {
								_, mask, ok := masks.Next()
								if !ok {
									break
								}
								rows += uint64(mask.Bits)
							}
						}
					}
				}
				b.ReportMetric(float64(view.EncodedBytes()), "reachable-B")
				indexTermLeafBenchRows = rows
			})
		}
	}
}

func makeIndexTermLeafPatternFixture(
	t testing.TB,
	termCount, postingsPerTerm, pattern int,
) indexTermLeafFixture {
	t.Helper()
	fixture := indexTermLeafFixture{
		terms:    make([]IndexTermLeafTerm, termCount),
		live:     make(map[uint32]*[TermPostingTileChunks]uint64),
		expected: make(map[string]map[uint32][TermPostingTileChunks]uint64),
	}
	tileID := uint32(11)
	for term := range termCount {
		key := mustIndexTermLeafKey(t, fmt.Sprintf("bench/common/%04d", term))
		fixture.terms[term] = IndexTermLeafTerm{
			Key:      key,
			Postings: make([]IndexTermLeafPosting, postingsPerTerm),
		}
		expected := make(map[uint32][TermPostingTileChunks]uint64)
		for postingIndex := range postingsPerTerm {
			var posting, live [TermPostingTileChunks]uint64
			makeIndexTermLeafPattern(pattern, &posting, &live)
			input := buildIndexTermLeafPosting(t, tileID, &posting, &live)
			fixture.terms[term].Postings[postingIndex] = input
			fixture.live[tileID] = input.Live
			expected[tileID] = posting
			tileID++
		}
		fixture.expected[string(key.Canonical)] = expected
	}
	return fixture
}

func mustOpenIndexTermLeafBenchmark(
	b *testing.B,
	fixture indexTermLeafFixture,
) IndexTermLeafView {
	b.Helper()
	encoded, err := AppendIndexTermLeaf(
		nil, indexTermLeafTestStoreID(), fixture.terms,
	)
	if err != nil {
		b.Fatal(err)
	}
	view, err := OpenIndexTermLeaf(
		encoded, indexTermLeafTestStoreID(), fixture.lookup,
	)
	if err != nil {
		b.Fatal(err)
	}
	return view
}

func indexTermLeafPatternName(pattern int) string {
	switch pattern {
	case 0:
		return "singleton"
	case 1:
		return "one-wide-mask"
	case 2:
		return "runs"
	case 3:
		return "sparse"
	case 4:
		return "dense"
	case 5:
		return "all-live"
	default:
		panic("unknown exact-term leaf pattern")
	}
}
