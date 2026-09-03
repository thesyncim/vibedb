package planner

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
)

// AnalyzeOptions bounds retained samples and distinct sketches independently of
// table size. A coordinator and its shards must use identical options and paths.
// These are explicit maintenance operations, never work on the query hot path.
type AnalyzeOptions struct {
	SampleRows      int
	DistinctEntries int
	MostCommon      int
	MaxSampleBytes  int
}

func (o AnalyzeOptions) defaults() (AnalyzeOptions, error) {
	if o.SampleRows == 0 {
		o.SampleRows = 2048
	}
	if o.DistinctEntries == 0 {
		o.DistinctEntries = 2048
	}
	if o.MostCommon == 0 {
		o.MostCommon = 64
	}
	if o.MaxSampleBytes == 0 {
		o.MaxSampleBytes = 64 << 20
	}
	if o.SampleRows < 16 || o.SampleRows > 1<<20 || o.DistinctEntries < 16 || o.DistinctEntries > 1<<20 || o.MostCommon < 1 || o.MostCommon > o.SampleRows || o.MaxSampleBytes < 1 {
		return o, fmt.Errorf("%w: invalid analyze bounds", ErrInvalidStatistics)
	}
	return o, nil
}

// StatisticsRow is borrowed for the duration of an iterator yield. Values are
// JSON scalars in the requested paths' order; missing SQL values use "null".
// Bytes is the physical row width, including columns not being analyzed.
type StatisticsRow struct {
	Values []string
	Bytes  uint64
}

type distinctSample struct {
	hashes   []uint64
	capacity int
	full     bool
}

func (s *distinctSample) add(h uint64) {
	// Once the sketch fills, almost every high-cardinality input is above the
	// threshold. Reject it before searching or moving the retained sorted run.
	if len(s.hashes) == s.capacity && s.capacity != 0 && h >= s.hashes[len(s.hashes)-1] {
		s.full = s.full || h > s.hashes[len(s.hashes)-1]
		return
	}
	i, exists := slices.BinarySearch(s.hashes, h)
	if exists {
		return
	}
	if len(s.hashes) == s.capacity {
		s.full = true
		if i == len(s.hashes) {
			return
		}
		s.hashes = s.hashes[:len(s.hashes)-1]
	}
	s.hashes = append(s.hashes, 0)
	copy(s.hashes[i+1:], s.hashes[i:])
	s.hashes[i] = h
}

// merge performs a sequential union of two sorted bottom-k sketches. scratch
// is recycled between groups/shards; no input synopsis is modified or borrowed.
func (s *distinctSample) merge(other distinctSample, scratch []uint64) []uint64 {
	if cap(scratch) < s.capacity {
		scratch = make([]uint64, 0, s.capacity)
	}
	out := scratch[:0]
	i, j := 0, 0
	full := s.full || other.full
	for i < len(s.hashes) || j < len(other.hashes) {
		var h uint64
		switch {
		case j == len(other.hashes) || i < len(s.hashes) && s.hashes[i] < other.hashes[j]:
			h = s.hashes[i]
			i++
		case i == len(s.hashes) || other.hashes[j] < s.hashes[i]:
			h = other.hashes[j]
			j++
		default:
			h = s.hashes[i]
			i++
			j++
		}
		if len(out) == s.capacity {
			full = true
			break
		}
		out = append(out, h)
	}
	old := s.hashes
	s.hashes, s.full = out, full
	return old[:0]
}
func (s distinctSample) estimate(rows float64) Estimate {
	if !s.full {
		return ExactEstimate(float64(len(s.hashes)))
	}
	k := float64(len(s.hashes))
	threshold := (float64(s.hashes[len(s.hashes)-1]) + 1) / math.Exp2(64)
	value := min(rows, max(k, (k-1)/threshold))
	relative := 3 / math.Sqrt(k-2)
	// Three standard errors are an approximate statistical interval, not a
	// correctness proof. Estimates are never used to remove rows or skip shards.
	return Estimate{Value: value, Lower: max(k, value/(1+relative)), Upper: min(rows, value/max(.01, 1-relative)), Confidence: .99}
}

type analyzedSampleRow struct {
	priority  uint64
	partition string
	ordinal   uint64
	values    []string
	bytes     int
}

func compareSampleRow(a, b analyzedSampleRow) int {
	if a.priority < b.priority {
		return -1
	}
	if a.priority > b.priority {
		return 1
	}
	if c := strings.Compare(a.partition, b.partition); c != 0 {
		return c
	}
	if a.ordinal < b.ordinal {
		return -1
	}
	if a.ordinal > b.ordinal {
		return 1
	}
	return 0
}

// PartitionAnalysis is an immutable synopsis for one disjoint shard at a pinned
// catalog generation. Its bounded row sample can be merged without bias toward
// small shards, and its distinct sketches can be unioned without double counting.
// Callers must scan one consistent source snapshot and use a common generation.
type PartitionAnalysis struct {
	generation        uint64
	table, partition  string
	paths             []string
	groups            [][]string
	ordinals          [][]int
	options           AnalyzeOptions
	rows, bytes       uint64
	nulls, valueBytes []uint64
	distinct          []distinctSample
	sample            []analyzedSampleRow
	sampleBytes       int
}

// AnalyzePartition scans rows once and retains bounded bottom-k samples and
// mergeable KMV distinct sketches. groups are workload-selected multicolumn path
// sets; single-column synopses are always collected. The iterator must emit each
// live row once. Source errors and cancellation discard the partial analysis.
func AnalyzePartition(ctx context.Context, generation uint64, table, partition string, paths []string, groups [][]string, rows iter.Seq2[StatisticsRow, error], options AnalyzeOptions) (*PartitionAnalysis, error) {
	options, err := options.defaults()
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if table == "" || partition == "" || !utf8.ValidString(table) || !utf8.ValidString(partition) || len(paths) == 0 || len(paths) > 256 || len(groups) > 256 || rows == nil {
		return nil, fmt.Errorf("%w: invalid analysis source", ErrInvalidStatistics)
	}
	a := &PartitionAnalysis{generation: generation, table: strings.Clone(table), partition: strings.Clone(partition), options: options, paths: make([]string, len(paths)), nulls: make([]uint64, len(paths)), valueBytes: make([]uint64, len(paths))}
	for i, p := range paths {
		if p == "" || !utf8.ValidString(p) || slices.Contains(paths[:i], p) {
			return nil, statisticsError(table, "invalid analysis paths")
		}
		a.paths[i] = strings.Clone(p)
		a.groups = append(a.groups, []string{a.paths[i]})
		a.ordinals = append(a.ordinals, []int{i})
	}
	for _, g := range groups {
		normalized, err := normalizeGroup(table, GroupStatistics{Paths: g, Distinct: ExactEstimate(0)})
		if err != nil {
			return nil, err
		}
		if len(g) < 2 {
			return nil, statisticsError(table, "explicit analysis groups must be multicolumn")
		}
		for _, previous := range a.groups {
			if slices.Equal(previous, normalized.Paths) {
				return nil, statisticsError(table, "duplicate analysis group")
			}
		}
		ordinals := make([]int, len(g))
		for i, p := range normalized.Paths {
			ordinals[i] = slices.Index(a.paths, p)
			if ordinals[i] < 0 {
				return nil, statisticsError(table, "group path absent from analysis")
			}
			normalized.Paths[i] = strings.Clone(p)
		}
		a.groups = append(a.groups, normalized.Paths)
		a.ordinals = append(a.ordinals, ordinals)
	}
	a.distinct = make([]distinctSample, len(a.groups))
	for i := range a.distinct {
		a.distinct[i].capacity = options.DistinctEntries
	}
	var key []byte
	values := make([]string, len(paths))
	for row, sourceErr := range rows {
		if sourceErr != nil {
			return nil, sourceErr
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(row.Values) != len(paths) || row.Bytes > math.MaxUint64-a.bytes || a.rows == math.MaxUint64 {
			return nil, statisticsError(table, "invalid analysis row")
		}
		rowBytes := 0
		for i, v := range row.Values {
			if len(v) > options.MaxSampleBytes-rowBytes {
				return nil, statisticsError(table, "analysis row exceeds sample byte budget")
			}
			canonical, err := CanonicalScalarJSON(v)
			if err != nil {
				return nil, err
			}
			values[i] = canonical
			rowBytes += len(canonical)
			if rowBytes > options.MaxSampleBytes {
				return nil, statisticsError(table, "canonical row exceeds sample byte budget")
			}
			if canonical == "null" {
				a.nulls[i]++
			} else {
				if uint64(len(canonical)) > math.MaxUint64-a.valueBytes[i] {
					return nil, statisticsError(table, "analysis width overflow")
				}
				a.valueBytes[i] += uint64(len(canonical))
			}
		}
		a.rows++
		a.bytes += row.Bytes
		for i, ordinals := range a.ordinals {
			key = key[:0]
			for _, ordinal := range ordinals {
				key = binary.AppendUvarint(key, uint64(len(values[ordinal])))
				key = append(key, values[ordinal]...)
			}
			a.distinct[i].add(xxhash.Sum64(key))
		}
		key = append(key[:0], partition...)
		key = append(key, 0)
		key = binary.LittleEndian.AppendUint64(key, a.rows)
		candidate := analyzedSampleRow{priority: xxhash.Sum64(key), partition: a.partition, ordinal: a.rows, bytes: rowBytes}
		if len(a.sample) == options.SampleRows && compareSampleRow(candidate, a.sample[len(a.sample)-1]) >= 0 {
			continue
		}
		candidate.values = slices.Clone(values)
		if err := a.addSample(candidate); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *PartitionAnalysis) addSample(row analyzedSampleRow) error {
	i, _ := slices.BinarySearchFunc(a.sample, row, compareSampleRow)
	if len(a.sample) == a.options.SampleRows {
		if i == len(a.sample) {
			return nil
		}
		a.sampleBytes -= a.sample[len(a.sample)-1].bytes
		a.sample = a.sample[:len(a.sample)-1]
	}
	if row.bytes > a.options.MaxSampleBytes-a.sampleBytes {
		return statisticsError(a.table, "analysis sample byte budget exhausted")
	}
	a.sample = append(a.sample, analyzedSampleRow{})
	copy(a.sample[i+1:], a.sample[i:])
	a.sample[i] = row
	a.sampleBytes += row.bytes
	return nil
}

// MergePartitionStatistics produces one publication from disjoint shard
// synopses. Input order does not affect the result. Duplicate shards, different
// generations or incompatible schemas/bounds are rejected. No shard NDVs or
// local top-N percentages are ever naively summed or averaged.
func MergePartitionStatistics(ctx context.Context, partitions ...*PartitionAnalysis) (TableStatistics, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(partitions) == 0 {
		return TableStatistics{}, fmt.Errorf("%w: no analysis partitions", ErrInvalidStatistics)
	}
	ordered := slices.Clone(partitions)
	for _, p := range ordered {
		if p == nil {
			return TableStatistics{}, fmt.Errorf("%w: nil analysis partition", ErrInvalidStatistics)
		}
	}
	slices.SortFunc(ordered, func(a, b *PartitionAnalysis) int { return strings.Compare(a.partition, b.partition) })
	first := ordered[0]
	a := &PartitionAnalysis{table: first.table, options: first.options, paths: first.paths, groups: first.groups, ordinals: first.ordinals, nulls: make([]uint64, len(first.paths)), valueBytes: make([]uint64, len(first.paths)), distinct: make([]distinctSample, len(first.distinct))}
	for i := range a.distinct {
		a.distinct[i].capacity = a.options.DistinctEntries
	}
	result := TableStatistics{Table: first.table}
	var sketchScratch []uint64
	for pi, p := range ordered {
		if err := ctx.Err(); err != nil {
			return TableStatistics{}, err
		}
		if pi > 0 && p.partition == ordered[pi-1].partition || p.table != first.table || p.generation != first.generation || p.options != first.options || !slices.Equal(p.paths, first.paths) || !slices.EqualFunc(p.groups, first.groups, slices.Equal[[]string]) {
			return TableStatistics{}, statisticsError(first.table, "incompatible or duplicate analysis partitions")
		}
		if p.rows > math.MaxUint64-a.rows || p.bytes > math.MaxUint64-a.bytes {
			return TableStatistics{}, statisticsError(first.table, "merged analysis overflow")
		}
		a.rows += p.rows
		a.bytes += p.bytes
		for i := range a.paths {
			a.nulls[i] += p.nulls[i]
			if p.valueBytes[i] > math.MaxUint64-a.valueBytes[i] {
				return TableStatistics{}, statisticsError(a.table, "merged width overflow")
			}
			a.valueBytes[i] += p.valueBytes[i]
		}
		for i, s := range p.distinct {
			sketchScratch = a.distinct[i].merge(s, sketchScratch)
		}
		for _, r := range p.sample {
			if err := a.addSample(r); err != nil {
				return TableStatistics{}, err
			}
		}
		local := PartitionStatistics{Partition: p.partition, Rows: ExactEstimate(float64(p.rows))}
		for i, paths := range p.groups {
			local.Groups = append(local.Groups, GroupStatistics{Paths: slices.Clone(paths), Distinct: p.distinct[i].estimate(float64(p.rows))})
		}
		result.Partitions = append(result.Partitions, local)
	}
	result.Rows = ExactEstimate(float64(a.rows))
	width := 0.0
	if a.rows != 0 {
		width = float64(a.bytes) / float64(a.rows)
	}
	result.RowBytes = ExactEstimate(width)
	for i, p := range a.paths {
		distinct := a.distinct[i].estimate(float64(a.rows))
		null := 0.0
		valueWidth := 0.0
		if a.rows != 0 {
			null = float64(a.nulls[i]) / float64(a.rows)
		}
		if a.nulls[i] != 0 {
			distinct.Value = max(0, distinct.Value-1)
			distinct.Lower = max(0, distinct.Lower-1)
			distinct.Upper = max(0, distinct.Upper-1)
		}
		if a.rows > a.nulls[i] {
			valueWidth = float64(a.valueBytes[i]) / float64(a.rows-a.nulls[i])
		}
		result.Columns = append(result.Columns, ColumnStatistics{Path: p, Distinct: distinct, NullFraction: null, AvgValueBytes: valueWidth})
	}
	for gi, paths := range a.groups {
		if err := ctx.Err(); err != nil {
			return TableStatistics{}, err
		}
		g := GroupStatistics{Paths: slices.Clone(paths), Distinct: a.distinct[gi].estimate(float64(a.rows))}
		type frequency struct {
			key    string
			values []string
			count  int
		}
		counts := make(map[string]*frequency)
		for _, row := range a.sample {
			values := make([]string, len(paths))
			for i, ordinal := range a.ordinals[gi] {
				values[i] = row.values[ordinal]
			}
			key := strings.Join(values, "\x00")
			if f := counts[key]; f != nil {
				f.count++
			} else {
				counts[key] = &frequency{key: key, values: values, count: 1}
			}
		}
		ranked := make([]*frequency, 0, len(counts))
		for _, f := range counts {
			ranked = append(ranked, f)
		}
		slices.SortFunc(ranked, func(a, b *frequency) int {
			if a.count != b.count {
				return b.count - a.count
			}
			return strings.Compare(a.key, b.key)
		})
		// DKW gives a uniform empirical CDF bound. A category is a difference of
		// two CDF endpoints, so twice epsilon bounds every selected category even
		// though heavy hitters are selected after sampling. Census samples are exact.
		delta := 0.0
		if uint64(len(a.sample)) < a.rows {
			delta = 2 * math.Sqrt(math.Log(200)/(2*float64(len(a.sample))))
		}
		for _, f := range ranked[:min(len(ranked), a.options.MostCommon)] {
			value := float64(f.count) / float64(len(a.sample))
			e := ExactEstimate(value)
			if delta > 0 {
				e = Estimate{Value: value, Lower: max(0, value-delta), Upper: min(1, value+delta), Confidence: .99}
			}
			g.MostCommon = append(g.MostCommon, TupleFrequency{Values: f.values, Frequency: e})
		}
		// Existing single-column MCV descriptors have no frequency interval. Publish
		// them only for census samples; sampled frequencies stay in the joint format.
		if gi < len(a.paths) && delta == 0 {
			for _, f := range g.MostCommon {
				if f.Values[0] != "null" {
					result.Columns[gi].MostCommon = append(result.Columns[gi].MostCommon, ValueFrequency{Value: f.Values[0], Frequency: f.Frequency.Value})
				}
			}
		}
		result.Groups = append(result.Groups, g)
	}
	return result, nil
}
