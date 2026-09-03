package planner

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"slices"

	"github.com/cespare/xxhash/v2"
)

// MaxAnalysisWireBytes bounds a complete partition synopsis. Transport layers
// may impose a smaller limit. Decoding also bounds retained allocations.
const MaxAnalysisWireBytes = 16 << 20

const analysisMagic = "VDBSTAT1"

var analysisCRC = crc32.MakeTable(crc32.Castagnoli)

// MarshalBinary owns its output and preserves the exact unionable sketches and
// priority sample. TableStatistics alone cannot represent this merge state.
func (a *PartitionAnalysis) MarshalBinary() ([]byte, error) {
	if a == nil || a.table == "" || a.partition == "" || len(a.paths) == 0 || len(a.groups) != len(a.distinct) {
		return nil, statisticsError("", "invalid analysis synopsis")
	}
	w := analysisEncoder{}
	a.encode(&w)
	if w.n > MaxAnalysisWireBytes-4 {
		return nil, statisticsError(a.table, "analysis synopsis exceeds wire bound")
	}
	w = analysisEncoder{data: make([]byte, 0, w.n+4)}
	a.encode(&w)
	w.data = binary.LittleEndian.AppendUint32(w.data, crc32.Checksum(w.data, analysisCRC))
	return w.data, nil
}

type analysisEncoder struct {
	data []byte
	n    int
}

func (w *analysisEncoder) raw(b []byte) {
	if w.n > MaxAnalysisWireBytes || len(b) > MaxAnalysisWireBytes-w.n {
		w.n = MaxAnalysisWireBytes + 1
		return
	}
	w.n += len(b)
	if w.data != nil {
		w.data = append(w.data, b...)
	}
}
func (w *analysisEncoder) u64(n uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], n)
	w.raw(b[:])
}
func (w *analysisEncoder) u32(n uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], n)
	w.raw(b[:])
}
func (w *analysisEncoder) str(s string) { w.u32(uint32(len(s))); w.raw([]byte(s)) }

func (a *PartitionAnalysis) encode(w *analysisEncoder) {
	w.raw([]byte(analysisMagic))
	w.u64(a.generation)
	w.str(a.table)
	w.str(a.partition)
	w.u32(uint32(a.options.SampleRows))
	w.u32(uint32(a.options.DistinctEntries))
	w.u32(uint32(a.options.MostCommon))
	w.u64(uint64(a.options.MaxSampleBytes))
	w.u64(a.rows)
	w.u64(a.bytes)
	w.u32(uint32(len(a.paths)))
	for _, p := range a.paths {
		w.str(p)
	}
	w.u32(uint32(len(a.groups) - len(a.paths)))
	for _, ordinals := range a.ordinals[len(a.paths):] {
		w.u32(uint32(len(ordinals)))
		for _, ordinal := range ordinals {
			w.u32(uint32(ordinal))
		}
	}
	for i := range a.paths {
		w.u64(a.nulls[i])
		w.u64(a.valueBytes[i])
	}
	for _, s := range a.distinct {
		flag := uint32(0)
		if s.full {
			flag = 1
		}
		w.u32(flag)
		w.u32(uint32(len(s.hashes)))
		for _, h := range s.hashes {
			w.u64(h)
		}
	}
	w.u32(uint32(len(a.sample)))
	for _, row := range a.sample {
		w.u64(row.priority)
		w.u64(row.ordinal)
		for _, v := range row.values {
			w.str(v)
		}
	}
}

type analysisDecoder struct {
	data     []byte
	retained uint64
	bad      bool
}

func (d *analysisDecoder) take(n int) []byte {
	if d.bad || n < 0 || n > len(d.data) {
		d.bad = true
		return nil
	}
	b := d.data[:n]
	d.data = d.data[n:]
	return b
}
func (d *analysisDecoder) u64() uint64 {
	b := d.take(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}
func (d *analysisDecoder) u32() uint32 {
	b := d.take(4)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}
func (d *analysisDecoder) alloc(n uint64) bool {
	if d.bad || n > 4*MaxAnalysisWireBytes-d.retained {
		d.bad = true
		return false
	}
	d.retained += n
	return true
}
func (d *analysisDecoder) count(maximum, minimumBytes int) int {
	n := uint64(d.u32())
	if d.bad || n > uint64(maximum) || n*uint64(minimumBytes) > uint64(len(d.data)) {
		d.bad = true
		return 0
	}
	return int(n)
}
func (d *analysisDecoder) str() string {
	n := d.count(MaxAnalysisWireBytes, 1)
	if !d.alloc(uint64(n)) {
		return ""
	}
	return string(d.take(n))
}

// UnmarshalPartitionAnalysis rejects truncated, over-budget, noncanonical, or
// internally inconsistent synopses before they can enter a distributed union.
// The result owns every retained byte; callers may immediately reuse data.
func UnmarshalPartitionAnalysis(data []byte) (*PartitionAnalysis, error) {
	invalid := func() (*PartitionAnalysis, error) {
		return nil, fmt.Errorf("%w: malformed analysis synopsis", ErrInvalidStatistics)
	}
	if len(data) < len(analysisMagic)+4 || len(data) > MaxAnalysisWireBytes || string(data[:len(analysisMagic)]) != analysisMagic {
		return invalid()
	}
	body := data[:len(data)-4]
	if crc32.Checksum(body, analysisCRC) != binary.LittleEndian.Uint32(data[len(data)-4:]) {
		return invalid()
	}
	d := analysisDecoder{data: body[len(analysisMagic):]}
	generation, table, partition := d.u64(), d.str(), d.str()
	if !d.alloc(uint64(len(table)) + 3*uint64(len(partition)) + 9) {
		return invalid()
	}
	opts := AnalyzeOptions{SampleRows: int(d.u32()), DistinctEntries: int(d.u32()), MostCommon: int(d.u32())}
	maxSampleBytes := d.u64()
	if maxSampleBytes > uint64(^uint(0)>>1) {
		return invalid()
	}
	opts.MaxSampleBytes = int(maxSampleBytes)
	canonical, err := opts.defaults()
	if d.bad || err != nil || opts != canonical {
		return invalid()
	}
	rows, rowBytes := d.u64(), d.u64()
	n := d.count(256, 4)
	if n == 0 || !d.alloc(uint64(n)*256) {
		return invalid()
	}
	paths := make([]string, n)
	for i := range paths {
		paths[i] = d.str()
		if !d.alloc(uint64(len(paths[i]))) {
			return invalid()
		}
	}
	ng := d.count(256, 4)
	if !d.alloc(uint64(ng) * 512) {
		return invalid()
	}
	groups := make([][]string, ng)
	for i := range groups {
		arity := d.count(MaxStatisticsGroupColumns, 4)
		if arity < 2 {
			return invalid()
		}
		groups[i] = make([]string, arity)
		for j := range groups[i] {
			ordinal := d.u32()
			if d.bad || uint64(ordinal) >= uint64(len(paths)) {
				return invalid()
			}
			groups[i][j] = paths[ordinal]
			if !d.alloc(uint64(len(groups[i][j]))) {
				return invalid()
			}
		}
		if !slices.IsSorted(groups[i]) {
			return invalid()
		}
	}
	if d.bad {
		return invalid()
	}
	a, err := AnalyzePartition(context.Background(), generation, table, partition, paths, groups,
		func(func(StatisticsRow, error) bool) {}, opts)
	if err != nil {
		return invalid()
	}
	a.rows, a.bytes = rows, rowBytes
	for i := range paths {
		a.nulls[i], a.valueBytes[i] = d.u64(), d.u64()
		if a.nulls[i] > rows || a.nulls[i] == rows && a.valueBytes[i] != 0 {
			return invalid()
		}
	}
	for i := range a.distinct {
		flag := d.u32()
		count := d.count(opts.DistinctEntries, 8)
		if flag > 1 || uint64(count) > rows || rows != 0 && count == 0 || flag == 1 && (count != opts.DistinctEntries || rows <= uint64(count)) || !d.alloc(uint64(count)*8) {
			return invalid()
		}
		s := &a.distinct[i]
		s.full = flag == 1
		s.hashes = make([]uint64, count)
		for j := range s.hashes {
			s.hashes[j] = d.u64()
			if j > 0 && s.hashes[j] <= s.hashes[j-1] {
				return invalid()
			}
		}
	}
	count := d.count(opts.SampleRows, 16+4*len(paths))
	if uint64(count) != min(rows, uint64(opts.SampleRows)) || !d.alloc(uint64(count)*(128+uint64(len(paths))*16)) {
		return invalid()
	}
	a.sample = make([]analyzedSampleRow, count)
	key := make([]byte, 0, len(partition)+9)
	for i := range a.sample {
		r := &a.sample[i]
		r.priority, r.ordinal, r.partition = d.u64(), d.u64(), a.partition
		if r.ordinal == 0 || r.ordinal > rows {
			return invalid()
		}
		key = append(key[:0], partition...)
		key = append(key, 0)
		key = binary.LittleEndian.AppendUint64(key, r.ordinal)
		if xxhash.Sum64(key) != r.priority || i > 0 && compareSampleRow(a.sample[i-1], *r) >= 0 {
			return invalid()
		}
		r.values = make([]string, len(paths))
		for j := range r.values {
			v := d.str()
			if d.bad || len(v) > opts.MaxSampleBytes-a.sampleBytes {
				return invalid()
			}
			canonical, err := CanonicalScalarJSON(v)
			if err != nil || canonical != v {
				return invalid()
			}
			r.values[j] = v
			r.bytes += len(v)
			a.sampleBytes += len(v)
		}
	}
	if d.bad || len(d.data) != 0 {
		return invalid()
	}
	return a, nil
}

// MatchesDefinition lets a coordinator refuse a valid synopsis for the wrong
// shard, generation, schema, or collection bounds before merging it.
func (a *PartitionAnalysis) MatchesDefinition(generation uint64, table, partition string, paths []string, groups [][]string, opts AnalyzeOptions) bool {
	if a == nil {
		return false
	}
	want, err := AnalyzePartition(context.Background(), generation, table, partition, paths, groups,
		func(func(StatisticsRow, error) bool) {}, opts)
	return err == nil && a.generation == want.generation && a.table == want.table && a.partition == want.partition && a.options == want.options && slices.Equal(a.paths, want.paths) && slices.EqualFunc(a.groups, want.groups, slices.Equal[[]string])
}
