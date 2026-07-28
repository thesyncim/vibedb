package store

import (
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibejson"
)

// storeBulkDoc is one row of a corpus shaped like the one the cross-engine
// footprint benchmark loads: a flat scalar prefix, one nested object, and one
// array, so no document conforms to a shape tape and every row is stored with a
// full classic tape. That is the case the bulk path's arena reservations have to
// be right for, because it is the one where the tape is the largest single
// thing the loader holds.
func storeBulkDoc(i int) []byte {
	return []byte(`{"id":` + strconv.Itoa(i) +
		`,"name":"user-` + strconv.Itoa(i) +
		`","country":"PT","score":` + strconv.Itoa(i%1000) +
		`,"active":true,"profile":{"tier":"pro","region":"eu-west-1",` +
		`"joined":"2020-03-04"},"tags":["alpha","delta","eta"],` +
		`"note":"steady state, no anomalies observed in the reporting window"}`)
}

// storeBulkTapeArenas counts the distinct backing arrays the page's document
// tapes live in. One is the whole point of reserving the arena up front: every
// additional array is a growth generation that buildDoc reached through the
// ErrIndexFull spill path, and every one of them stays live for the builder's
// entire run because the tapes already built into it still alias it.
//
// Rows are committed in ordinal order into the arena's free tail, so tapes that
// share an arena are exactly the tapes that abut. Comparing addresses rather
// than identifying the array is the only way to ask the question at all: a
// slice header names its first element, which differs for every row whether or
// not the rows share storage.
func storeBulkTapeArenas(docs *Segment) int {
	const stride = unsafe.Sizeof(vibejson.IndexEntry{})
	arenas, previous := 0, uintptr(0)
	for i := range docs.docs {
		entries := docs.docs[i].Entries
		if len(entries) == 0 {
			continue
		}
		start := uintptr(unsafe.Pointer(unsafe.SliceData(entries)))
		if start != previous {
			arenas++
		}
		previous = start + uintptr(len(entries))*stride
	}
	return arenas
}

// storeBulkAllocated totals what fn allocates. ReadMemStats stops the world and
// TotalAlloc and Mallocs are exact running counters, so this measures the
// allocator rather than sampling it — which matters here because the quantity
// under test is transient garbage that HeapAlloc never shows.
func storeBulkAllocated(fn func()) (bytes, allocs uint64) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc, after.Mallocs - before.Mallocs
}

// TestStoreBuilderPageArenaAllocation pins the bulk loader's per-page
// allocation behavior. A Builder page takes ChunkDocuments rows and then never
// takes another, so both of its arenas and its row table have a knowable final
// size before the first row is appended; discovering them by geometric growth
// instead is not amortized the way ordinary append growth is, because each
// superseded entry arena still backs the tapes of the rows built into it and so
// stays live rather than becoming collectable.
func TestStoreBuilderPageArenaAllocation(t *testing.T) {
	const pages = 64
	builder, err := NewBuilder(Options{})
	if err != nil {
		t.Fatal(err)
	}
	rows := pages * MaxChunkDocuments
	for i := range rows {
		if err := builder.Append(fmt.Sprintf("doc:%08d", i), storeBulkDoc(i)); err != nil {
			t.Fatal(err)
		}
	}

	// The first page has no preceding page to measure, so it keeps the growth
	// path; every page after it must be reserved exactly once.
	for id := uint32(1); id < builder.chunks.Count; id++ {
		chunk := builder.chunks.Get(id)
		if chunk == nil {
			continue
		}
		docs := &chunk.Docs
		if arenas := storeBulkTapeArenas(docs); arenas != 1 {
			t.Fatalf("page %d tapes span %d entry arenas, want 1: the page grew "+
				"its arena and every superseded generation is still live", id, arenas)
		}
		if got := cap(docs.docs); got != MaxChunkDocuments {
			t.Fatalf("page %d row table cap = %d, want the exact page size %d",
				id, got, MaxChunkDocuments)
		}
		committed := docs.committedEntries()
		if committed == 0 {
			t.Fatalf("page %d committed no entries; the corpus is not exercising "+
				"the classic tape this test is about", id)
		}
		// Reserved from the preceding page's measured entries per byte, so the
		// surplus is the source reservation's headroom and nothing more. A page
		// that reserved several times what it filled would be retaining that
		// surplus for the whole load just as surely as a growth generation.
		if slack := cap(docs.entryChunk) - committed; slack < 0 || slack > committed/4 {
			t.Fatalf("page %d reserved %d entries for %d committed: slack %d is "+
				"not a bounded reservation", id, cap(docs.entryChunk), committed, slack)
		}
	}

	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if got := collection.Len(); got != uint64(rows) {
		t.Fatalf("Len = %d, want %d", got, rows)
	}
}

// TestStoreBuilderBulkLoadAllocation bounds what the whole bulk load allocates
// per row. Bytes per row rather than a total keeps the bound meaningful as the
// corpus changes, and the allocation count is the sharper of the two: reserving
// each page's source arena, entry arena, and row table up front is what turns a
// page's dozen allocations into three, and no per-row allocation exists at all.
func TestStoreBuilderBulkLoadAllocation(t *testing.T) {
	const rows = 64 * MaxChunkDocuments
	keys := make([]string, rows)
	bodies := make([][]byte, rows)
	for i := range keys {
		keys[i] = fmt.Sprintf("doc:%08d", i)
		bodies[i] = storeBulkDoc(i)
	}
	var collection *Collection
	bytes, allocs := storeBulkAllocated(func() {
		builder, err := NewBuilder(Options{})
		if err != nil {
			panic(err)
		}
		for i := range keys {
			if err := builder.Append(keys[i], bodies[i]); err != nil {
				panic(err)
			}
		}
		built, err := builder.Build()
		if err != nil {
			panic(err)
		}
		collection = built
	})
	perRow := float64(bytes) / rows
	// The rows themselves are ~250 B of source and ~420 B of tape, both of
	// which the load has to hold until Build compacts them off the Go heap.
	// Anything much above their sum is arena surplus and spill scratch.
	if perRow > 900 {
		t.Fatalf("bulk load allocated %.0f B per row, want at most 900", perRow)
	}
	// One source arena, one entry arena, one row table, one key table, and a
	// handful of fixed objects per page of MaxChunkDocuments rows.
	if perAlloc := float64(allocs) / rows; perAlloc > 0.25 {
		t.Fatalf("bulk load made %.3f allocations per row, want at most 0.25", perAlloc)
	}
	if got := collection.Len(); got != rows {
		t.Fatalf("Len = %d, want %d", got, rows)
	}
}

// TestStoreCollectionReplaceAllocation bounds one replacement into a full
// chunk. Every rebuild parses exactly one document, and without a reservation
// that one parse always ran twice over: the 16-entry arena minimum fits no real
// document, so the build failed with ErrIndexFull and reran into a len(src)+2
// scratch tape an order of magnitude larger than the document's own, which
// sealIngest then discarded inside the same rebuild.
func TestStoreCollectionReplaceAllocation(t *testing.T) {
	collection := &Collection{}
	for i := range MaxChunkDocuments {
		if _, err := collection.Put(fmt.Sprintf("k%02d", i), storeBulkDoc(i)); err != nil {
			t.Fatal(err)
		}
	}
	const replacements = 256
	bodies := make([][]byte, replacements)
	for i := range bodies {
		bodies[i] = storeBulkDoc(1_000 + i)
	}
	bytes, _ := storeBulkAllocated(func() {
		for i := range bodies {
			if _, err := collection.Put("k07", bodies[i]); err != nil {
				panic(err)
			}
		}
	})
	// A rebuild is O(chunk) by design — it copies the row table, the key table,
	// and the new document's source — so this is a bound on the rebuild, not a
	// claim that it is cheap. The scratch tape alone was ~3.9 kB of it.
	if perPut := float64(bytes) / replacements; perPut > 9000 {
		t.Fatalf("replacement into a full chunk allocated %.0f B, want at most 9000", perPut)
	}
}

// TestStoreCollectionReplaceReservationKeepsExactTapes is the correctness half
// of the reservation: prepareStoreSegment now hands buildDoc an arena the
// document is expected to fit in, but a published chunk must still retain the
// exact tape and no arena, and must still answer byte-identically. It carries no
// allocation assertion so the race and checkptr jobs, which skip Alloc tests,
// keep running it.
func TestStoreCollectionReplaceReservationKeepsExactTapes(t *testing.T) {
	for _, name := range []string{"same-size", "shorter", "longer", "degenerate"} {
		t.Run(name, func(t *testing.T) {
			collection := &Collection{}
			want := make(map[string]string)
			for i := range MaxChunkDocuments {
				key := fmt.Sprintf("k%02d", i)
				body := storeBulkDoc(i)
				if _, err := collection.Put(key, body); err != nil {
					t.Fatal(err)
				}
				want[key] = string(body)
			}
			// Each variant crosses the reservation from a different side: an
			// equal-length body should land exactly, a much shorter one must not
			// be under-reserved just because the estimate scales with length, a
			// much longer one must reserve proportionally more, and an empty
			// object must not reserve from the row it replaces at all.
			var replacement []byte
			switch name {
			case "same-size":
				replacement = storeBulkDoc(7)
			case "shorter":
				replacement = []byte(`{"id":7,"name":"u","country":"PT","score":1,` +
					`"active":true,"profile":{"tier":"p","region":"e","joined":"j"},` +
					`"tags":["a","b","c"],"note":"n"}`)
			case "longer":
				body := `{"id":7,"tags":[`
				for i := range 400 {
					if i > 0 {
						body += ","
					}
					body += strconv.Itoa(i)
				}
				replacement = []byte(body + `]}`)
			case "degenerate":
				replacement = []byte(`{}`)
			}
			created, err := collection.Put("k07", replacement)
			if err != nil || created {
				t.Fatalf("Put replacement = (%v,%v), want (false,nil)", created, err)
			}
			want["k07"] = string(replacement)

			state := collection.state.Load()
			state.Chunks.Each(func(id uint32, chunk *Chunk) bool {
				docs := &chunk.Docs
				if cap(docs.entryChunk) != 0 {
					t.Fatalf("chunk %d retained a %d-entry arena over %d committed "+
						"entries; the reservation must not outlive the rebuild",
						id, cap(docs.entryChunk), len(docs.entryChunk))
				}
				for i := range docs.docs {
					entries := docs.docs[i].Entries
					if cap(entries) != len(entries) {
						t.Fatalf("chunk %d ordinal %d retained %d unwritten tape entries",
							id, i, cap(entries)-len(entries))
					}
				}
				return true
			})

			snapshot, err := collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			for key, body := range want {
				value, ok := snapshot.GetRaw(key)
				if !ok {
					t.Fatalf("GetRaw(%q) missing", key)
				}
				if got := string(value.Bytes()); got != body {
					t.Fatalf("GetRaw(%q) = %s, want %s", key, got, body)
				}
			}
			if got := collection.Len(); got != MaxChunkDocuments {
				t.Fatalf("Len = %d, want %d", got, MaxChunkDocuments)
			}
		})
	}
}

// TestStoreBuilderPageReservationRoundTrips proves the Builder's page
// reservations change only how much is allocated, never what is stored: a
// corpus whose row sizes swing across pages exercises both the reservation that
// carries over and the resample that replaces it, and every row must read back
// byte-identically through the compacted collection.
func TestStoreBuilderPageReservationRoundTrips(t *testing.T) {
	for _, options := range []Options{
		{},
		{ChunkDocuments: 3},
		{ShapeTapes: true},
		{Postings: true},
	} {
		t.Run(fmt.Sprintf("%d/%v/%v", options.ChunkDocuments, options.ShapeTapes, options.Postings), func(t *testing.T) {
			builder, err := NewBuilder(options)
			if err != nil {
				t.Fatal(err)
			}
			const rows = 200
			want := make(map[string]string, rows)
			for i := range rows {
				key := fmt.Sprintf("doc:%04d", i)
				var body []byte
				switch {
				case i%37 == 0:
					// A flat object with no nesting, which is the only shape a
					// shape tape can compact, so the entry hint collapses.
					body = []byte(`{"a":1,"b":2}`)
				case i%11 == 0:
					body = []byte(`{"id":` + strconv.Itoa(i) + `,"pad":"` +
						storeBulkRepeat("x", 900) + `"}`)
				default:
					body = storeBulkDoc(i)
				}
				if err := builder.Append(key, body); err != nil {
					t.Fatal(err)
				}
				want[key] = string(body)
			}
			collection, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if got := collection.Len(); got != rows {
				t.Fatalf("Len = %d, want %d", got, rows)
			}
			for key, body := range want {
				value, ok := snapshot.GetRaw(key)
				if !ok {
					t.Fatalf("GetRaw(%q) missing", key)
				}
				if got := string(value.Bytes()); got != body {
					t.Fatalf("GetRaw(%q) = %s, want %s", key, got, body)
				}
			}
		})
	}
}

func storeBulkRepeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for range n {
		out = append(out, s...)
	}
	return string(out)
}
