// Package competitive holds a cross-engine benchmark harness that measures
// VibeDB's store and store/durable against real embedded key/value and SQL
// engines on one shared JSON corpus.
//
// It lives in its own Go module on purpose. The root VibeDB module has no
// third-party dependencies and that property is load-bearing; every competitor
// dependency is confined to bench/competitive/go.mod, which replaces VibeDB
// with the parent directory.
package competitive

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"math/rand/v2"
	"strconv"

	"github.com/thesyncim/vibejson"
)

// Doc is one corpus record: a key and its JSON body.
type Doc struct {
	Key  string
	JSON []byte
}

// AppendExpectedStoredJSON appends the representation an engine promises to
// return for src. The durable class-5 format is canonical by construction;
// byte-preserving competitors return the submitted spelling. Correctness
// oracles use this outside timed regions so canonicalization is never charged
// to, or hidden inside, a benchmarked read.
func AppendExpectedStoredJSON(dst []byte, engineName string, src []byte) ([]byte, error) {
	if engineName == "vibedb" {
		return vibejson.AppendCanonicalize(dst, src)
	}
	return append(dst, src...), nil
}

// Cardinality selects which corpus variant is generated. It exists because the
// shipped corpus is highly redundant and store/durable's unified grammar can
// exploit that redundancy automatically. Reporting only the redundant corpus
// could therefore mistake corpus entropy for format efficiency. Both variants
// are measured and published through the same durable representation.
type Cardinality int

const (
	// LowCardinality is the shipped corpus: note drawn from 4 fixed sentences,
	// tier from 4, region from 5, tags from a pool of 8. gzip -9 compresses it
	// to about 8% of its size, i.e. it is ~92% redundant.
	LowCardinality Cardinality = iota
	// HighCardinality keeps every document's shape and byte length identical to
	// its LowCardinality counterpart but replaces every pool-drawn string field
	// — note, tier, region, tags — with a per-document random value of that
	// field's exact length, so no dictionary can pay for them. It is the
	// opposite extreme, not a realistic corpus: real data sits between the two,
	// and the pair brackets the range a dictionary-based writer can occupy.
	//
	// Two fields are deliberately left alone. country stays drawn from the same
	// hundred-value alphabet in both variants, because every filter workload
	// depends on its 1% selectivity. The joined date stays a date, drawn from
	// its 7x12x28 space, because a repeating date is what a real corpus has and
	// the variant is meant to isolate value redundancy, not to manufacture the
	// most incompressible input that will still parse.
	HighCardinality
)

func (c Cardinality) String() string {
	if c == HighCardinality {
		return "high"
	}
	return "low"
}

// ParseCardinality maps the command-line spelling onto the variant.
func ParseCardinality(s string) (Cardinality, error) {
	switch s {
	case "low", "":
		return LowCardinality, nil
	case "high":
		return HighCardinality, nil
	}
	return 0, fmt.Errorf("unknown corpus cardinality %q (want low or high)", s)
}

// CorpusSize is the default document count. It is large enough that a full
// scan is dominated by storage and parse work rather than by fixed per-call
// overhead, and small enough that every engine's working set is a realistic
// few tens of megabytes.
const CorpusSize = 100_000

// FilterPath is the RFC 6901 pointer of the scalar field every filtered and
// indexed workload predicates on.
const FilterPath = "/country"

// FilterField is FilterPath as a bare top-level field name, for the engines
// whose query surface takes names rather than pointers.
const FilterField = "country"

// FilterValue is the needle. Countries are drawn uniformly from a
// hundred-element alphabet, so this predicate selects ~1% of the corpus.
const FilterValue = "PT"

// countries is the hundred-value alphabet of the indexed column. Uniform draws
// from it give FilterValue a 1% selectivity, which is the interesting regime:
// selective enough that an index should win by a wide margin, unselective
// enough that a scan is not answering a near-empty question.
var countries = buildCountries()

func buildCountries() []string {
	const letters = "ABCDEFGHIJ"
	out := make([]string, 0, 100)
	for i := range len(letters) {
		for j := range len(letters) {
			out = append(out, string(letters[i])+string(letters[j]))
		}
	}
	// Place the needle at a known slot so selectivity is exactly 1-in-100.
	out[26] = FilterValue
	return out
}

var (
	tiers   = []string{"free", "pro", "team", "enterprise"}
	regions = []string{"eu-west-1", "eu-central-1", "us-east-1", "us-west-2", "ap-south-1"}
	tagPool = []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	notes   = []string{
		"steady state, no anomalies observed in the last reporting window",
		"processed by the current pipeline during the maintenance window",
		"flagged for review after a threshold breach on the ingest path",
		"nominal; retention policy applied and checkpoint acknowledged",
	}
)

// Key returns the storage key for document i. Keys are fixed-width and
// lexicographically ordered so that every ordered engine (bbolt, pebble,
// badger, SQLite WITHOUT ROWID) sees the same sequential insert pattern and
// none is penalised by a key distribution the others do not share.
func Key(i int) string { return fmt.Sprintf("doc:%08d", i) }

// Corpus builds n documents of the shipped low-cardinality variant.
func Corpus(n int) []Doc { return CorpusOf(n, LowCardinality) }

// CorpusOf builds n documents deterministically in the requested variant. The
// same n and variant always yield byte-identical documents, so every engine in
// every process stores exactly the same bytes under exactly the same keys.
//
// The two variants are shape-identical and length-identical document for
// document: HighCardinality draws the same pool entry LowCardinality would
// have, then substitutes a random string of that entry's exact length. Any
// difference in a disk column between the two runs is therefore attributable to
// value redundancy alone and not to a different corpus size.
func CorpusOf(n int, card Cardinality) []Doc {
	rng := rand.New(rand.NewPCG(0x5DEECE66D, 0xB16B00B5))
	// A second, independent stream for the high-cardinality substitutions, so
	// that the pool draws above stay identical between the two variants and the
	// documents stay length-for-length comparable.
	fill := rand.New(rand.NewPCG(0xC0FFEE, 0x1234567))
	uniq := func(sample string) string {
		if card == LowCardinality {
			return sample
		}
		return randomLower(fill, len(sample))
	}
	docs := make([]Doc, n)
	buf := make([]byte, 0, 512)
	for i := range n {
		country := countries[rng.IntN(len(countries))]
		tier := uniq(tiers[rng.IntN(len(tiers))])
		region := uniq(regions[rng.IntN(len(regions))])
		note := uniq(notes[rng.IntN(len(notes))])
		score := rng.IntN(1000)
		joinedY := 2018 + rng.IntN(7)
		joinedM := 1 + rng.IntN(12)
		joinedD := 1 + rng.IntN(28)
		nTags := 2 + rng.IntN(3)

		buf = buf[:0]
		buf = append(buf, `{"id":`...)
		buf = strconv.AppendInt(buf, int64(i), 10)
		buf = append(buf, `,"name":"user-`...)
		buf = strconv.AppendInt(buf, int64(i), 10)
		buf = append(buf, `","country":"`...)
		buf = append(buf, country...)
		buf = append(buf, `","score":`...)
		buf = strconv.AppendInt(buf, int64(score), 10)
		buf = append(buf, `,"active":`...)
		if i%3 == 0 {
			buf = append(buf, "false"...)
		} else {
			buf = append(buf, "true"...)
		}
		buf = append(buf, `,"profile":{"tier":"`...)
		buf = append(buf, tier...)
		buf = append(buf, `","region":"`...)
		buf = append(buf, region...)
		buf = append(buf, `","joined":"`...)
		buf = append(buf, fmt.Sprintf("%04d-%02d-%02d", joinedY, joinedM, joinedD)...)
		buf = append(buf, `"},"tags":[`...)
		for t := range nTags {
			if t > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, '"')
			buf = append(buf, uniq(tagPool[(i+t*3)%len(tagPool)])...)
			buf = append(buf, '"')
		}
		buf = append(buf, `],"note":"`...)
		buf = append(buf, note...)
		buf = append(buf, `"}`...)

		docs[i] = Doc{Key: Key(i), JSON: append([]byte(nil), buf...)}
	}
	return docs
}

// UpdatedJSON returns a same-shape replacement body for document i, used by
// the point-write workload. It keeps the country field unchanged so that
// replacing a document does not perturb index selectivity, and differs from
// the original so no engine can elide the write.
func UpdatedJSON(docs []Doc, i int) []byte {
	src := docs[i%len(docs)].JSON
	out := make([]byte, 0, len(src)+4)
	// Rewrite "score":N to "score":999 without reparsing: append a trailing
	// field instead, which every engine stores verbatim and every JSON reader
	// accepts.
	out = append(out, src[:len(src)-1]...)
	out = append(out, `,"rev":1}`...)
	return out
}

// SameSizeUpdatedJSON changes one scalar digit without changing the document
// length or indexed country. It isolates engines' fixed-size replacement path
// from the allocation and page-shape work required by a growing value.
func SameSizeUpdatedJSON(docs []Doc, i int) []byte {
	return AppendSameSizeUpdatedJSON(nil, docs, i)
}

// AppendSameSizeUpdatedJSON is SameSizeUpdatedJSON with caller-owned scratch.
func AppendSameSizeUpdatedJSON(dst []byte, docs []Doc, i int) []byte {
	src := docs[i%len(docs)].JSON
	out := append(dst, src...)
	needle := []byte(`"score":`)
	at := bytes.Index(out, needle)
	if at < 0 {
		panic("competitive corpus score field missing")
	}
	at += len(needle)
	if at >= len(out) || out[at] < '0' || out[at] > '9' {
		panic("competitive corpus score value malformed")
	}
	if out[at] == '9' {
		out[at] = '8'
	} else {
		out[at]++
	}
	return out
}

// randomLower renders n random lowercase letters. Length is preserved exactly
// so the high-cardinality corpus is byte-for-byte the same size as the low one.
func randomLower(rng *rand.Rand, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte('a' + rng.IntN(26))
	}
	return string(out)
}

// CorpusByteCounts separates the bytes applications ask an engine to store
// from the engine's own framing. LogicalBytes is key plus JSON bytes exactly
// once per record; it excludes indexes, page headers, journals, allocators, and
// filesystem rounding, all of which belong in the physical footprint columns.
type CorpusByteCounts struct {
	KeyBytes     int
	JSONBytes    int
	LogicalBytes int
}

// CorpusBytes returns the exact key, JSON-document, and key-inclusive logical
// byte counts for docs.
func CorpusBytes(docs []Doc) CorpusByteCounts {
	var counts CorpusByteCounts
	for i := range docs {
		counts.KeyBytes += len(docs[i].Key)
		counts.JSONBytes += len(docs[i].JSON)
	}
	counts.LogicalBytes = counts.KeyBytes + counts.JSONBytes
	return counts
}

// CorpusRedundancy reports JSON-document bytes only and what gzip -9
// compresses their concatenation to. Keys are deliberately excluded from this
// entropy control and are reported separately by CorpusBytes. Together they
// distinguish corpus redundancy from the key-inclusive logical payload used
// as the denominator for physical-footprint ratios.
func CorpusRedundancy(docs []Doc) (total, gzipped int, err error) {
	var out bytes.Buffer
	w, err := gzip.NewWriterLevel(&out, gzip.BestCompression)
	if err != nil {
		return 0, 0, err
	}
	for i := range docs {
		total += len(docs[i].JSON)
		if _, err := w.Write(docs[i].JSON); err != nil {
			return 0, 0, err
		}
	}
	if err := w.Close(); err != nil {
		return 0, 0, err
	}
	return total, out.Len(), nil
}

// CorpusStats reports the aggregate shape of a corpus, for the README and the
// report header.
func CorpusStats(docs []Doc) (totalBytes int, minBytes, maxBytes int, matches int) {
	minBytes = 1 << 30
	for i := range docs {
		n := len(docs[i].JSON)
		totalBytes += n
		if n < minBytes {
			minBytes = n
		}
		if n > maxBytes {
			maxBytes = n
		}
		if hasCountry(docs[i].JSON, FilterValue) {
			matches++
		}
	}
	return totalBytes, minBytes, maxBytes, matches
}

func hasCountry(src []byte, want string) bool {
	needle := `"country":"` + want + `"`
	return indexOf(src, needle) >= 0
}

func indexOf(src []byte, needle string) int {
	n := len(needle)
	if n == 0 || len(src) < n {
		return -1
	}
	for i := 0; i+n <= len(src); i++ {
		if string(src[i:i+n]) == needle {
			return i
		}
	}
	return -1
}
