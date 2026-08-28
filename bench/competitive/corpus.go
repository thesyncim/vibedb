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

	"github.com/thesyncim/vibedb/internal/benchcorpus"
	"github.com/thesyncim/vibejson"
)

// Doc is one corpus record: a key and its JSON body.
type Doc = benchcorpus.Document

// AppendExpectedStoredJSON appends the representation an engine promises to
// return for src. The durable compact-stripe format is canonical by
// construction; byte-preserving competitors return the submitted spelling.
// Correctness oracles use this outside timed regions so canonicalization is
// never charged to, or hidden inside, a benchmarked read.
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

// DocumentShape selects the value-size distribution independently from value
// cardinality. The benchmark adapter retains the durable store's 512-byte
// inline threshold, so the two
// overflow shapes use exact, bounded target sizes on opposite sides of that
// boundary rather than relying on incidental corpus growth.
type DocumentShape uint8

const (
	InlineDocuments DocumentShape = iota
	MixedDocuments
	OverflowHeavyDocuments

	mixedOverflowDocumentBytes = 4 << 10
	heavyOverflowDocumentBytes = 16 << 10
	maxShapedDocumentBytes     = heavyOverflowDocumentBytes
)

func (s DocumentShape) String() string {
	switch s {
	case InlineDocuments:
		return "inline"
	case MixedDocuments:
		return "mixed"
	case OverflowHeavyDocuments:
		return "overflow-heavy"
	default:
		return "invalid"
	}
}

// ParseDocumentShape parses the stable command-line corpus-shape spelling.
func ParseDocumentShape(value string) (DocumentShape, error) {
	switch value {
	case "inline", "":
		return InlineDocuments, nil
	case "mixed":
		return MixedDocuments, nil
	case "overflow-heavy":
		return OverflowHeavyDocuments, nil
	default:
		return 0, fmt.Errorf("unknown document shape %q (want inline, mixed, or overflow-heavy)", value)
	}
}

// MaxDocumentBytes is the exact upper bound needed by an engine admission
// configuration for this shape.
func (s DocumentShape) MaxDocumentBytes() int {
	switch s {
	case InlineDocuments:
		return 1 << 10
	case MixedDocuments:
		return mixedOverflowDocumentBytes
	case OverflowHeavyDocuments:
		return heavyOverflowDocumentBytes
	default:
		return 0
	}
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

// Key returns the storage key for document i. Keys are fixed-width and
// lexicographically ordered so that every ordered engine (bbolt, pebble,
// badger, SQLite WITHOUT ROWID) sees the same sequential insert pattern and
// none is penalised by a key distribution the others do not share.
func Key(i int) string { return benchcorpus.Key(i) }

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
	return CorpusOfShape(n, card, InlineDocuments)
}

// CorpusOfShape builds a deterministic corpus with a bounded value-size
// distribution. Mixed alternates inline and 4 KiB values exactly; overflow
// heavy makes seven of every eight values exactly 16 KiB. The appended payload
// alphabet contains no JSON escapes and is generated directly into bytes.
// vibejson validates every constructed value, keeping stdlib JSON entirely out
// of corpus construction.
func CorpusOfShape(n int, card Cardinality, shape DocumentShape) []Doc {
	docs := make([]Doc, 0, n)
	if err := GenerateCorpus(n, card, shape, func(doc Doc) error {
		docs = append(docs, Doc{Key: doc.Key, JSON: append([]byte(nil), doc.JSON...)})
		return nil
	}); err != nil {
		panic(err)
	}
	return docs
}

// GenerateCorpus visits the exact canonical competitive corpus one document at
// a time. JSON is borrowed until visit returns. This is the bounded-memory
// source used by the out-of-RAM lane; CorpusOfShape is implemented through it
// so the resident and streaming paths remain byte-identical.
func GenerateCorpus(n int, card Cardinality, shape DocumentShape, visit func(Doc) error) error {
	ordinal := 0
	return benchcorpus.Generate(n, card == HighCardinality, func(doc benchcorpus.Document) error {
		target := shapedDocumentBytes(shape, ordinal)
		if target != 0 && len(doc.JSON) < target {
			doc.JSON = appendDeterministicPayload(doc.JSON, target, uint64(ordinal)+1)
		}
		if len(doc.JSON) > maxShapedDocumentBytes || !vibejson.Valid(doc.JSON) {
			return fmt.Errorf("competitive shaped corpus invariant violated at document %d", ordinal)
		}
		ordinal++
		return visit(doc)
	})
}

func shapedDocumentBytes(shape DocumentShape, ordinal int) int {
	switch shape {
	case InlineDocuments:
		return 0
	case MixedDocuments:
		if ordinal&1 != 0 {
			return mixedOverflowDocumentBytes
		}
		return 0
	case OverflowHeavyDocuments:
		if ordinal&7 != 0 {
			return heavyOverflowDocumentBytes
		}
		return 0
	default:
		panic("invalid competitive document shape")
	}
}

func appendDeterministicPayload(src []byte, target int, state uint64) []byte {
	const prefix = `,"payload":"`
	const suffix = `"}`
	payloadBytes := target - (len(src) - 1) - len(prefix) - len(suffix)
	if payloadBytes < 0 || len(src) == 0 || src[len(src)-1] != '}' {
		panic("competitive corpus cannot fit shaped payload")
	}
	out := make([]byte, 0, target)
	out = append(out, src[:len(src)-1]...)
	out = append(out, prefix...)
	for range payloadBytes {
		// xorshift64* yields deterministic high-entropy bytes. Mapping onto 64
		// JSON-safe ASCII symbols avoids escaping and fixes the exact length.
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		out = append(out, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"[(state*2685821657736338717)>>58])
	}
	out = append(out, suffix...)
	return out
}

// UpdatedJSON returns a longer replacement body for document i, used by the
// point-write workload. It keeps the country field unchanged so that replacing
// a document does not perturb index selectivity, and differs from the original
// so no engine can elide the write.
func UpdatedJSON(docs []Doc, i int) []byte {
	return AppendUpdatedJSON(nil, docs, i)
}

// AppendUpdatedJSON is UpdatedJSON with caller-owned scratch. Benchmarks use
// it so JSON fixture construction is not charged to any storage engine.
func AppendUpdatedJSON(dst []byte, docs []Doc, i int) []byte {
	src := docs[i%len(docs)].JSON
	out := dst
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
