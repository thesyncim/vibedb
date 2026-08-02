package durable

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// unifiedEquivalenceCorpus is the competitive corpus with non-canonically
// spelled documents (unsorted keys, whitespace, escapes) mixed in, which the
// admission pass must rewrite; their unique shapes also degrade to trivial
// rows. Keys keep the corpus's sorted order. Overflow documents cannot enter
// through CreateFromPrimary (the bulk path requires inline documents, a
// pre-existing constraint for every format), so the equivalence test injects
// them through Put after the build.
func unifiedEquivalenceCorpus(n int, high bool) ([]string, [][]byte) {
	keys, docs := unifiedCompetitiveCorpus(n, high)
	for i := range docs {
		if i%89 == 0 {
			docs[i] = fmt.Appendf(nil,
				"{ \"zz\" : \"\\u0041-%d\" ,\n\t\"aa\" : [ 1 , -2 , 3e2 ] , \"only-%d\" : true }",
				i, i)
		}
	}
	return keys, docs
}

// unifiedOverflowDoc builds a ~1.5 KiB canonical-spelling document for key i,
// large enough to ride an overflow chain (InlineValueBytes default 512).
func unifiedOverflowDoc(i int) []byte {
	long := bytes.Repeat([]byte("overflow payload segment "), 60)
	return fmt.Appendf(nil,
		`{"chain":%d,"payload":"%s","tail":{"j":[%d,null,{"deep":"%s"}]}}`,
		i, long, i, long[:200])
}

// TestUnifiedStoreEquivalence is an end-to-end functional test at store
// level: the canonical class-5 store must agree with a plain map oracle of
// canonical spellings on point reads (present and absent keys), full ordered
// scans, the token filter lane, and the field probe — over both competitive
// cardinalities with overflow, trivial, and rewrite rows mixed in.
func TestUnifiedStoreEquivalence(t *testing.T) {
	const n = 20_000
	for _, high := range []bool{false, true} {
		name := "low"
		if high {
			name = "high"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			keys, docs := unifiedEquivalenceCorpus(n, high)
			want := canonicalDocs(t, docs)
			oracle := make(map[string][]byte, n)
			for i := range keys {
				oracle[keys[i]] = want[i]
			}
			collection := createUnifiedPrimary(t, filepath.Join(dir, "u.vibe"), keys, docs)
			// Inject overflow documents through the mutation path: the touched
			// class-5 leaves route through the
			// structural rewrite and the big documents ride overflow chains. The
			// Put spelling is written canonically so the map oracle stays one
			// canonical spelling per key.
			for i := 0; i < n; i += 97 {
				doc := unifiedOverflowDoc(i)
				if _, err := collection.Put([]byte(keys[i]), doc); err != nil {
					t.Fatalf("Put overflow %q: %v", keys[i], err)
				}
				oracle[keys[i]] = doc
				want[i] = doc
			}
			snapshot, err := collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()

			// Point reads: every present key returns exactly the oracle's
			// canonical spelling; absent keys read as missing.
			dst := make([]byte, 0, 4096)
			for i := range keys {
				var found bool
				dst, found, err = snapshot.AppendRaw(dst[:0], []byte(keys[i]))
				if err != nil || !found {
					t.Fatalf("AppendRaw(%q) = (%v,%v)", keys[i], found, err)
				}
				if !bytes.Equal(dst, oracle[keys[i]]) {
					t.Fatalf("point %q:\n got %q\nwant %q", keys[i], dst, oracle[keys[i]])
				}
			}
			for _, missing := range []string{"doc:99999999", "absent", "doc:"} {
				if _, found, err := snapshot.AppendRaw(nil, []byte(missing)); err != nil || found {
					t.Fatalf("missing key %q = (%v,%v)", missing, found, err)
				}
			}

			// Ordered scan: same rows, same order, same bytes.
			rank := 0
			if err := snapshot.RangeRaw(func(k, v []byte) error {
				if string(k) != keys[rank] {
					return fmt.Errorf("scan rank %d key %q want %q", rank, k, keys[rank])
				}
				if !bytes.Equal(v, oracle[string(k)]) {
					return fmt.Errorf("scan value mismatch at %q", k)
				}
				rank++
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if rank != n {
				t.Fatalf("scanned %d rows want %d", rank, n)
			}

			// Token filter lane vs the oracle: count and per-row agreement for
			// needles across the compare classes.
			var pathOracle storeio.UnifiedHoleResolver
			filterCases := []struct{ path, needle string }{
				{"/country", `"PT"`},
				{"/active", `true`},
				{"/profile/tier", `"pro"`},
				{"/tags/1", `"beta"`},
				{"/chain", `97`},
				{"/profile", `{"x":true}`},
				{"/nope", `"x"`},
			}
			for _, tc := range filterCases {
				filter, err := NewEqFilter(tc.path, []byte(tc.needle))
				if err != nil {
					t.Fatal(err)
				}
				if err := pathOracle.SetPath([]byte(tc.path)); err != nil {
					t.Fatal(err)
				}
				wantCount := 0
				for i := range keys {
					start, end, found, err := pathOracle.PathSpanOf(want[i])
					if err != nil {
						t.Fatal(err)
					}
					if found && bytes.Equal(want[i][start:end], filter.inner.Needle()) {
						wantCount++
					}
				}
				result, err := snapshot.FilterEqCount(filter)
				if err != nil {
					t.Fatalf("FilterEqCount(%q,%s): %v", tc.path, tc.needle, err)
				}
				if result.Scanned != n {
					t.Fatalf("filter scanned %d want %d", result.Scanned, n)
				}
				if result.Matched != wantCount {
					t.Fatalf("filter (%q == %s): lane %d oracle %d (fallback %d)",
						tc.path, tc.needle, result.Matched, wantCount, result.Fallback)
				}
			}

			// Field probe vs the oracle at every key, across token, trivial,
			// container, overflow, and absent targets.
			for _, path := range []string{
				"/country", "/id", "/active", "/profile/joined", "/profile",
				"/tags/0", "/payload", "/tail/j/2/deep", "/absent",
			} {
				probe, err := NewFieldProbe(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := pathOracle.SetPath([]byte(path)); err != nil {
					t.Fatal(err)
				}
				field := make([]byte, 0, 2048)
				for i := range keys {
					start, end, wantFound, err := pathOracle.PathSpanOf(want[i])
					if err != nil {
						t.Fatal(err)
					}
					var found bool
					field, found, err = snapshot.AppendField(field[:0], probe, []byte(keys[i]))
					if err != nil {
						t.Fatalf("AppendField(%q,%q): %v", path, keys[i], err)
					}
					if found != wantFound {
						t.Fatalf("probe %q at %q: found=%v want %v", path, keys[i], found, wantFound)
					}
					if found && !bytes.Equal(field, want[i][start:end]) {
						t.Fatalf("probe %q at %q:\n got %q\nwant %q",
							path, keys[i], field, want[i][start:end])
					}
				}
				if _, found, err := snapshot.AppendField(nil, probe, []byte("no-such-key")); err != nil || found {
					t.Fatalf("probe of missing key = (%v,%v)", found, err)
				}
			}
		})
	}
}

// TestUnifiedReadZeroAlloc pins the zero-allocation contract on the durable
// read surface: warmed point reads, ordered scans, field probes, and token
// filter scans allocate nothing per operation.
func TestUnifiedReadZeroAlloc(t *testing.T) {
	dir := t.TempDir()
	keys, docs := unifiedCompetitiveCorpus(5000, false)
	collection := createUnifiedPrimary(t, filepath.Join(dir, "u.vibe"), keys, docs)
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	key := []byte(keys[1234])
	dst := make([]byte, 0, 4096)
	if n := testing.AllocsPerRun(200, func() {
		out, found, err := snapshot.AppendRaw(dst[:0], key)
		if err != nil || !found {
			t.Fatal("AppendRaw")
		}
		dst = out[:0]
	}); n != 0 {
		t.Fatalf("point read allocates %v/op", n)
	}

	probe, err := NewFieldProbe("/country")
	if err != nil {
		t.Fatal(err)
	}
	// Warm the per-(leaf, template) resolution cache over every leaf first so
	// the pin measures the steady state, not cache population (which is a
	// boundary cost by design).
	for i := range keys {
		if _, _, err := snapshot.AppendField(dst[:0], probe, []byte(keys[i])); err != nil {
			t.Fatal(err)
		}
	}
	if n := testing.AllocsPerRun(200, func() {
		out, found, err := snapshot.AppendField(dst[:0], probe, key)
		if err != nil || !found {
			t.Fatal("AppendField")
		}
		dst = out[:0]
	}); n != 0 {
		t.Fatalf("field probe allocates %v/op", n)
	}

	if _, err := snapshot.RangeRawBuffer(nil, func(k, v []byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(3, func() {
		if _, err := snapshot.RangeRawBuffer(nil, func(k, v []byte) error { return nil }); err != nil {
			t.Fatal("RangeRawBuffer")
		}
	}); n != 0 {
		t.Fatalf("ordered scan allocates %v/scan", n)
	}

	filter, err := NewEqFilter("/country", []byte(`"PT"`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.FilterEqCount(filter); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(3, func() {
		result, err := snapshot.FilterEqCount(filter)
		if err != nil || result.Matched == 0 {
			t.Fatal("FilterEqCount")
		}
	}); n != 0 {
		t.Fatalf("token filter scan allocates %v/scan", n)
	}

	numberFilter, err := NewScalarEqFilter("/score", []byte(`500.0`))
	if err != nil {
		t.Fatal(err)
	}
	wantNumbers := 0
	for _, doc := range docs {
		matched, err := numberFilter.inner.EvalRendered(doc)
		if err != nil {
			t.Fatal(err)
		}
		if matched {
			wantNumbers++
		}
	}
	result, err := snapshot.FilterEqCount(numberFilter)
	if err != nil || result.Matched != wantNumbers || result.Fallback != 0 {
		t.Fatalf("scalar number filter = (%+v,%v), want matched=%d fallback=0",
			result, err, wantNumbers)
	}
	if n := testing.AllocsPerRun(3, func() {
		result, err := snapshot.FilterEqCount(numberFilter)
		if err != nil || result.Matched != wantNumbers || result.Fallback != 0 {
			t.Fatal("numeric FilterEqCount")
		}
	}); n != 0 {
		t.Fatalf("numeric filter scan allocates %v/scan", n)
	}

	fractionFilter, err := NewScalarEqFilter("/score", []byte(`500.5`))
	if err != nil {
		t.Fatal(err)
	}
	result, err = snapshot.FilterEqCount(fractionFilter)
	if err != nil || result.Matched != 0 || result.Fallback != 0 {
		t.Fatalf("fractional number filter = (%+v,%v), want no match/fallback", result, err)
	}
	if n := testing.AllocsPerRun(3, func() {
		result, err := snapshot.FilterEqCount(fractionFilter)
		if err != nil || result.Matched != 0 || result.Fallback != 0 {
			t.Fatal("fractional FilterEqCount")
		}
	}); n != 0 {
		t.Fatalf("fractional filter scan allocates %v/scan", n)
	}
}

// TestUnifiedProbeCanonicalIntSpellings pins canonical-integer losslessness on
// the probe surface: integer spellings inside and outside the canonical-int
// admission class round-trip byte-identically through the token view.
func TestUnifiedProbeCanonicalIntSpellings(t *testing.T) {
	dir := t.TempDir()
	spellings := []string{
		"0", "-1", "7", "999", "123456789012345678", "-123456789012345678",
		"1e3", "1.5", "-0.25", "3e-2", "10.0",
	}
	n := 600
	keys := make([]string, n)
	docs := make([][]byte, n)
	for i := range n {
		keys[i] = fmt.Sprintf("doc:%08d", i)
		docs[i] = fmt.Appendf(nil, `{"pad":"row-%04d","v":%s}`, i%7, spellings[i%len(spellings)])
	}
	collection := createUnifiedPrimary(t, filepath.Join(dir, "ints.vibe"), keys, docs)
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	probe, err := NewFieldProbe("/v")
	if err != nil {
		t.Fatal(err)
	}
	for i := range keys {
		got, found, err := snapshot.AppendField(nil, probe, []byte(keys[i]))
		if err != nil || !found {
			t.Fatalf("AppendField(%q) = (%v,%v)", keys[i], found, err)
		}
		if string(got) != spellings[i%len(spellings)] {
			t.Fatalf("spelling %q read back as %q", spellings[i%len(spellings)], got)
		}
	}
}
