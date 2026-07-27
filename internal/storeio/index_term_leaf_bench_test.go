package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

var (
	indexTermLeafBenchRows uint64
	indexTermLeafBenchOK   bool
)

func BenchmarkIndexTermLeafBytes(b *testing.B) {
	for _, cardinality := range []struct {
		name            string
		terms, postings int
	}{
		{name: "low-cardinality", terms: 1, postings: 64},
		{name: "high-cardinality", terms: 96, postings: 1},
	} {
		for pattern := 0; pattern < 6; pattern++ {
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
				currentRecords, currentCertificateBytes :=
					indexTermLeafCurrentRecordCounts(fixture)
				postings := cardinality.terms * cardinality.postings
				for b.Loop() {
					indexTermLeafBenchRows += uint64(len(encoded))
				}
				b.ReportMetric(float64(len(encoded))/float64(postings), "leaf-B/posting")
				b.ReportMetric(float64(currentRecords*IndexDirectoryLeafRecordSize)/
					float64(postings), "current32-B/posting")
				b.ReportMetric(float64(currentRecords*IndexDirectoryLeafRecordSize+
					currentCertificateBytes)/float64(postings), "current+exact-B/posting")
				b.ReportMetric(float64(len(encoded))/
					float64(currentRecords*IndexDirectoryLeafRecordSize+
						currentCertificateBytes), "leaf/current")
				b.ReportMetric(float64(len(encoded)), "leaf-bytes")
				b.ReportMetric(float64(currentRecords), "current-records")
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
		for pattern := 0; pattern < 6; pattern++ {
			name := fmt.Sprintf("%s/%s", cardinality.name, indexTermLeafPatternName(pattern))
			fixture := makeIndexTermLeafPatternFixture(
				b, cardinality.terms, cardinality.postings, pattern,
			)
			view := mustOpenIndexTermLeafBenchmark(b, fixture)
			current := makeIndexTermLeafCurrent32(fixture)
			term := cardinality.terms / 2
			currentCertificate := append(
				[]byte(nil), fixture.terms[term].Key.Canonical...,
			)
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
			b.Run("packed-current32-route-only-lower-bound/"+name, func(b *testing.B) {
				b.ReportAllocs()
				var rows uint64
				for b.Loop() {
					var ok bool
					var sum uint64
					sum, ok = lookupIndexTermLeafCurrent32(current, uint32(term))
					if !ok {
						b.Fatal("lookup missed")
					}
					rows += sum
				}
				indexTermLeafBenchRows = rows
			})
			b.Run("packed-current32-exact/"+name, func(b *testing.B) {
				b.ReportAllocs()
				var rows uint64
				for b.Loop() {
					var ok bool
					var sum uint64
					sum, ok = lookupIndexTermLeafCurrent32Exact(
						current, uint32(term), currentCertificate, key.Canonical,
					)
					if !ok {
						b.Fatal("exact lookup missed")
					}
					rows += sum
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
		for pattern := 0; pattern < 6; pattern++ {
			name := fmt.Sprintf("%s/%s", cardinality.name, indexTermLeafPatternName(pattern))
			fixture := makeIndexTermLeafPatternFixture(
				b, cardinality.terms, cardinality.postings, pattern,
			)
			view := mustOpenIndexTermLeafBenchmark(b, fixture)
			current := makeIndexTermLeafCurrent32(fixture)
			_, currentCertificateBytes := indexTermLeafCurrentRecordCounts(fixture)
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
			b.Run("packed-current32/"+name, func(b *testing.B) {
				b.ReportAllocs()
				var rows uint64
				for b.Loop() {
					for position := 0; position < len(current); position +=
						IndexDirectoryLeafRecordSize {
						rows += binary.LittleEndian.Uint64(current[position+16 : position+24])
					}
				}
				b.ReportMetric(
					float64(len(current)+currentCertificateBytes), "reachable-B",
				)
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
	for term := 0; term < termCount; term++ {
		key := mustIndexTermLeafKey(t, fmt.Sprintf("bench/common/%04d", term))
		fixture.terms[term] = IndexTermLeafTerm{
			Key:      key,
			Postings: make([]IndexTermLeafPosting, postingsPerTerm),
		}
		expected := make(map[uint32][TermPostingTileChunks]uint64)
		for postingIndex := 0; postingIndex < postingsPerTerm; postingIndex++ {
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

func indexTermLeafCurrentRecordCounts(
	fixture indexTermLeafFixture,
) (records, certificateBytes int) {
	for i := range fixture.terms {
		certificateBytes += len(fixture.terms[i].Key.Canonical)
		for _, masks := range fixture.expected[string(fixture.terms[i].Key.Canonical)] {
			for _, mask := range masks {
				if mask != 0 {
					records++
				}
			}
		}
	}
	return records, certificateBytes
}

// makeIndexTermLeafCurrent32 models the current leaf's dominant physical and
// iteration cost: one packed 32-byte record per non-empty chunk mask. Exact
// certificate bytes are reported separately in the byte benchmark.
func makeIndexTermLeafCurrent32(fixture indexTermLeafFixture) []byte {
	records, _ := indexTermLeafCurrentRecordCounts(fixture)
	encoded := make([]byte, records*IndexDirectoryLeafRecordSize)
	position := 0
	for term := range fixture.terms {
		for _, input := range fixture.terms[term].Postings {
			masks := fixture.expected[string(fixture.terms[term].Key.Canonical)][input.Posting.TileID]
			for chunk, mask := range masks {
				if mask == 0 {
					continue
				}
				record := encoded[position:]
				binary.LittleEndian.PutUint32(record[0:4], uint32(term))
				binary.LittleEndian.PutUint32(record[4:8], input.Posting.TileID)
				binary.LittleEndian.PutUint32(record[8:12], uint32(chunk))
				binary.LittleEndian.PutUint64(record[16:24], mask)
				position += IndexDirectoryLeafRecordSize
			}
		}
	}
	return encoded
}

func lookupIndexTermLeafCurrent32(encoded []byte, term uint32) (uint64, bool) {
	count := len(encoded) / IndexDirectoryLeafRecordSize
	low, high := 0, count
	for low < high {
		middle := int(uint(low+high) >> 1)
		record := encoded[middle*IndexDirectoryLeafRecordSize:]
		if binary.LittleEndian.Uint32(record[0:4]) < term {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low == count ||
		binary.LittleEndian.Uint32(encoded[low*IndexDirectoryLeafRecordSize:]) != term {
		return 0, false
	}
	var rows uint64
	for low < count {
		record := encoded[low*IndexDirectoryLeafRecordSize:]
		if binary.LittleEndian.Uint32(record[0:4]) != term {
			break
		}
		rows += binary.LittleEndian.Uint64(record[16:24])
		low++
	}
	return rows, true
}

func lookupIndexTermLeafCurrent32Exact(
	encoded []byte,
	term uint32,
	certificate, canonical []byte,
) (uint64, bool) {
	rows, ok := lookupIndexTermLeafCurrent32(encoded, term)
	return rows, ok && bytes.Equal(certificate, canonical)
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
