package planner

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"unicode/utf8"
	"unsafe"
)

var ErrInvalidStatistics = errors.New("planner: invalid statistics")

// TableStatistics is the cold input for one ANALYZE/statistics publication.
// Rows and RowBytes include uncertainty; the optimizer may price their Upper
// bound for latency-sensitive queries. Columns are sparse: unobserved columns
// consume no directory entry.
type TableStatistics struct {
	Table    string             `json:"table"`
	Rows     Estimate           `json:"rows"`
	RowBytes Estimate           `json:"row_bytes"`
	Columns  []ColumnStatistics `json:"columns,omitempty"`
}

// ColumnStatistics contains the selectivity facts with the highest practical
// value per retained byte. MostCommon handles skewed equality predicates;
// Histogram is an equi-depth cumulative distribution for range predicates.
// Values are canonical JSON scalars supplied by the collector.
type ColumnStatistics struct {
	Path          string            `json:"path"`
	Distinct      Estimate          `json:"distinct"`
	NullFraction  float64           `json:"null_fraction"`
	AvgValueBytes float64           `json:"avg_value_bytes"`
	MostCommon    []ValueFrequency  `json:"most_common,omitempty"`
	Histogram     []HistogramBucket `json:"histogram,omitempty"`
}

// ValueFrequency is one heavy hitter and its fraction of all table rows.
type ValueFrequency struct {
	Value     string  `json:"value"`
	Frequency float64 `json:"frequency"`
}

// HistogramBucket is one increasing upper bound in an equi-depth histogram.
// Frequency is cumulative and therefore increases through (0, 1]. Distinct is
// the estimated number of distinct non-null values at or below Upper.
type HistogramBucket struct {
	Upper     string  `json:"upper"`
	Frequency float64 `json:"frequency"`
	Distinct  float64 `json:"distinct"`
}

type statisticStringRef struct {
	offset uint32
	length uint32
}

type compactEstimate struct {
	value      float64
	upper      float64
	lower      float32
	confidence float32
}

func newCompactEstimate(estimate Estimate, fallback float64) compactEstimate {
	e := estimate.Normalize(fallback)
	return compactEstimate{
		value: e.Value, upper: e.Upper, lower: float32(e.Lower), confidence: float32(e.Confidence),
	}
}

func (e compactEstimate) public() Estimate {
	lower := min(float64(e.lower), e.value)
	return Estimate{
		Value: e.value, Lower: lower, Upper: e.upper, Confidence: float64(e.confidence),
	}
}

// compactTableStatistic is exactly 64 bytes on supported targets: two compact
// estimates, one interned name, and one column run.
type compactTableStatistic struct {
	name        statisticStringRef
	rows        compactEstimate
	rowBytes    compactEstimate
	columnBase  uint32
	columnCount uint32
}

// compactColumnStatistic is a fixed 64-byte sparse record. Variable skew and
// histogram data live in flat shared runs, so a column with no such data pays
// no per-column slice headers or allocations.
type compactColumnStatistic struct {
	path          statisticStringRef
	distinct      compactEstimate
	nullFraction  float32
	avgValueBytes float32
	commonBase    uint32
	commonCount   uint32
	histBase      uint32
	histCount     uint32
	_             [8]byte
}

type compactValueFrequency struct {
	value     statisticStringRef
	frequency float64
}

type compactHistogramBucket struct {
	upper     statisticStringRef
	frequency float64
	distinct  float64
}

// StatisticsCatalog is an immutable, compact statistics snapshot. Lookups are
// binary searches over sorted flat directories and allocate nothing. It can be
// pinned to the same generation as routing metadata without putting mutable
// feedback in the query path.
type StatisticsCatalog struct {
	generation uint64
	tables     []compactTableStatistic
	columns    []compactColumnStatistic
	common     []compactValueFrequency
	histogram  []compactHistogramBucket
	arena      string
}

// NewStatisticsCatalog validates and compacts one generation. A nil descriptor
// slice returns a usable empty catalog.
func NewStatisticsCatalog(generation uint64, descriptors []TableStatistics) (*StatisticsCatalog, error) {
	ordered := slices.Clone(descriptors)
	slices.SortFunc(ordered, func(a, b TableStatistics) int { return strings.Compare(a.Table, b.Table) })
	catalog := &StatisticsCatalog{generation: generation}
	if len(ordered) == 0 {
		return catalog, nil
	}
	stringBytes := uint64(0)
	columnCount, commonCount, histogramCount := uint64(0), uint64(0), uint64(0)
	for i := range ordered {
		table := &ordered[i]
		if table.Table == "" || !utf8.ValidString(table.Table) {
			return nil, statisticsError(table.Table, "table name is empty or invalid UTF-8")
		}
		if i != 0 && ordered[i-1].Table == table.Table {
			return nil, statisticsError(table.Table, "duplicate table")
		}
		if err := validateEstimate(table.Rows, "row count"); err != nil {
			return nil, statisticsError(table.Table, err.Error())
		}
		if err := validateEstimate(table.RowBytes, "row width"); err != nil {
			return nil, statisticsError(table.Table, err.Error())
		}
		stringBytes += uint64(len(table.Table))
		columnCount += uint64(len(table.Columns))
		for j := range table.Columns {
			column := &table.Columns[j]
			stringBytes += uint64(len(column.Path))
			commonCount += uint64(len(column.MostCommon))
			histogramCount += uint64(len(column.Histogram))
			for _, value := range column.MostCommon {
				stringBytes += uint64(len(value.Value))
			}
			for _, bucket := range column.Histogram {
				stringBytes += uint64(len(bucket.Upper))
			}
		}
	}
	maxUint32 := uint64(^uint32(0))
	if columnCount > maxUint32 || commonCount > maxUint32 || histogramCount > maxUint32 || stringBytes > maxUint32 {
		return nil, fmt.Errorf("%w: compact statistics capacity exceeded", ErrInvalidStatistics)
	}
	catalog.tables = make([]compactTableStatistic, 0, len(ordered))
	catalog.columns = make([]compactColumnStatistic, 0, int(columnCount))
	catalog.common = make([]compactValueFrequency, 0, int(commonCount))
	catalog.histogram = make([]compactHistogramBucket, 0, int(histogramCount))
	var arena strings.Builder
	arena.Grow(int(stringBytes))
	interned := make(map[string]statisticStringRef)
	intern := func(value string) statisticStringRef {
		if ref, ok := interned[value]; ok {
			return ref
		}
		ref := statisticStringRef{offset: uint32(arena.Len()), length: uint32(len(value))}
		arena.WriteString(value)
		interned[value] = ref
		return ref
	}
	for i := range ordered {
		table := &ordered[i]
		columns := slices.Clone(table.Columns)
		slices.SortFunc(columns, func(a, b ColumnStatistics) int { return strings.Compare(a.Path, b.Path) })
		tableEntry := compactTableStatistic{
			name: intern(table.Table), rows: newCompactEstimate(table.Rows, 1000),
			rowBytes:   newCompactEstimate(table.RowBytes, 128),
			columnBase: uint32(len(catalog.columns)), columnCount: uint32(len(columns)),
		}
		for columnIndex := range columns {
			column := &columns[columnIndex]
			if err := validateColumnStatistic(table.Table, columns, columnIndex); err != nil {
				return nil, err
			}
			entry := compactColumnStatistic{
				path: intern(column.Path), distinct: newCompactEstimate(column.Distinct, 100),
				nullFraction: float32(column.NullFraction), avgValueBytes: float32(column.AvgValueBytes),
				commonBase: uint32(len(catalog.common)), commonCount: uint32(len(column.MostCommon)),
				histBase: uint32(len(catalog.histogram)), histCount: uint32(len(column.Histogram)),
			}
			for _, value := range column.MostCommon {
				catalog.common = append(catalog.common, compactValueFrequency{
					value: intern(value.Value), frequency: value.Frequency,
				})
			}
			for _, bucket := range column.Histogram {
				catalog.histogram = append(catalog.histogram, compactHistogramBucket{
					upper: intern(bucket.Upper), frequency: bucket.Frequency, distinct: bucket.Distinct,
				})
			}
			catalog.columns = append(catalog.columns, entry)
		}
		catalog.tables = append(catalog.tables, tableEntry)
	}
	catalog.arena = arena.String()
	return catalog, nil
}

func validateColumnStatistic(table string, columns []ColumnStatistics, index int) error {
	column := &columns[index]
	if column.Path == "" || !utf8.ValidString(column.Path) {
		return statisticsError(table, "column path is empty or invalid UTF-8")
	}
	if index != 0 && columns[index-1].Path == column.Path {
		return statisticsError(table, "duplicate column path "+column.Path)
	}
	if err := validateEstimate(column.Distinct, "distinct count"); err != nil {
		return statisticsError(table, column.Path+": "+err.Error())
	}
	if !finiteFraction(column.NullFraction) {
		return statisticsError(table, column.Path+": null fraction is outside [0,1]")
	}
	if !finiteNonNegative(column.AvgValueBytes) {
		return statisticsError(table, column.Path+": average value width is invalid")
	}
	commonTotal := 0.0
	seen := make(map[string]struct{}, len(column.MostCommon))
	for _, value := range column.MostCommon {
		if err := validateStatisticScalar(value.Value); err != nil {
			return statisticsError(table, column.Path+": heavy hitter: "+err.Error())
		}
		if _, duplicate := seen[value.Value]; duplicate {
			return statisticsError(table, column.Path+": duplicate heavy hitter")
		}
		seen[value.Value] = struct{}{}
		if !finiteFraction(value.Frequency) || value.Frequency == 0 {
			return statisticsError(table, column.Path+": heavy-hitter frequency is outside (0,1]")
		}
		commonTotal += value.Frequency
	}
	if commonTotal > 1-column.NullFraction+1e-6 {
		return statisticsError(table, column.Path+": heavy hitters exceed non-null frequency")
	}
	previousFrequency, previousDistinct := 0.0, 0.0
	for _, bucket := range column.Histogram {
		if err := validateStatisticScalar(bucket.Upper); err != nil {
			return statisticsError(table, column.Path+": histogram: "+err.Error())
		}
		if !finiteFraction(bucket.Frequency) || bucket.Frequency <= previousFrequency {
			return statisticsError(table, column.Path+": histogram frequencies are not increasing")
		}
		if !finiteNonNegative(bucket.Distinct) || bucket.Distinct < previousDistinct {
			return statisticsError(table, column.Path+": histogram distinct counts regress")
		}
		previousFrequency, previousDistinct = bucket.Frequency, bucket.Distinct
	}
	if previousFrequency > 1-column.NullFraction+1e-6 {
		return statisticsError(table, column.Path+": histogram exceeds non-null frequency")
	}
	return nil
}

func validateEstimate(estimate Estimate, label string) error {
	values := [...]float64{estimate.Value, estimate.Lower, estimate.Upper, estimate.Confidence}
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return fmt.Errorf("%s estimate is invalid", label)
		}
	}
	if estimate.Lower > estimate.Value || estimate.Upper < estimate.Value || estimate.Confidence > 1 {
		return fmt.Errorf("%s estimate interval is invalid", label)
	}
	return nil
}

func finiteFraction(value float64) bool {
	return finiteNonNegative(value) && value <= 1
}

func validateStatisticScalar(value string) error {
	if value == "" || !utf8.ValidString(value) {
		return errors.New("value is empty or invalid UTF-8")
	}
	var decoded any
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("value %q is not JSON: %w", value, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("value %q has trailing JSON", value)
	}
	switch decoded.(type) {
	case nil, bool, json.Number, string:
		return nil
	default:
		return fmt.Errorf("value %q is not a scalar", value)
	}
}

func statisticsError(table, reason string) error {
	if table == "" {
		return fmt.Errorf("%w: %s", ErrInvalidStatistics, reason)
	}
	return fmt.Errorf("%w: table %q: %s", ErrInvalidStatistics, table, reason)
}

func (c *StatisticsCatalog) Generation() uint64 {
	if c == nil {
		return 0
	}
	return c.generation
}

// Descriptors reconstructs an independently owned cold representation for
// persistence or publication. Query planning uses the compact views instead.
func (c *StatisticsCatalog) Descriptors() []TableStatistics {
	if c == nil || len(c.tables) == 0 {
		return nil
	}
	out := make([]TableStatistics, len(c.tables))
	for tableIndex := range c.tables {
		table := c.tables[tableIndex]
		descriptor := &out[tableIndex]
		descriptor.Table = strings.Clone(c.string(table.name))
		descriptor.Rows = table.rows.public()
		descriptor.RowBytes = table.rowBytes.public()
		descriptor.Columns = make([]ColumnStatistics, table.columnCount)
		for columnOffset := uint32(0); columnOffset < table.columnCount; columnOffset++ {
			entry := c.columns[table.columnBase+columnOffset]
			column := &descriptor.Columns[columnOffset]
			column.Path = strings.Clone(c.string(entry.path))
			column.Distinct = entry.distinct.public()
			column.NullFraction = float64(entry.nullFraction)
			column.AvgValueBytes = float64(entry.avgValueBytes)
			column.MostCommon = make([]ValueFrequency, entry.commonCount)
			for i := uint32(0); i < entry.commonCount; i++ {
				value := c.common[entry.commonBase+i]
				column.MostCommon[i] = ValueFrequency{
					Value: strings.Clone(c.string(value.value)), Frequency: value.frequency,
				}
			}
			column.Histogram = make([]HistogramBucket, entry.histCount)
			for i := uint32(0); i < entry.histCount; i++ {
				bucket := c.histogram[entry.histBase+i]
				column.Histogram[i] = HistogramBucket{
					Upper:     strings.Clone(c.string(bucket.upper)),
					Frequency: bucket.frequency, Distinct: bucket.distinct,
				}
			}
		}
	}
	return out
}

// RetainedBytes reports the exact owned flat-directory capacities plus the
// interned string arena. It excludes one StatisticsCatalog header.
func (c *StatisticsCatalog) RetainedBytes() uint64 {
	if c == nil {
		return 0
	}
	return uint64(cap(c.tables))*uint64(unsafe.Sizeof(compactTableStatistic{})) +
		uint64(cap(c.columns))*uint64(unsafe.Sizeof(compactColumnStatistic{})) +
		uint64(cap(c.common))*uint64(unsafe.Sizeof(compactValueFrequency{})) +
		uint64(cap(c.histogram))*uint64(unsafe.Sizeof(compactHistogramBucket{})) +
		uint64(len(c.arena))
}

// TableStatistic is an immutable allocation-free view into one catalog.
type TableStatistic struct {
	catalog *StatisticsCatalog
	index   uint32
}

func (c *StatisticsCatalog) Table(name string) (TableStatistic, bool) {
	if c == nil {
		return TableStatistic{}, false
	}
	lo, hi := 0, len(c.tables)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if c.string(c.tables[mid].name) < name {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(c.tables) || c.string(c.tables[lo].name) != name {
		return TableStatistic{}, false
	}
	return TableStatistic{catalog: c, index: uint32(lo)}, true
}

func (s TableStatistic) Name() string {
	if s.catalog == nil || int(s.index) >= len(s.catalog.tables) {
		return ""
	}
	return s.catalog.string(s.catalog.tables[s.index].name)
}

func (s TableStatistic) Rows() Estimate {
	if s.catalog == nil || int(s.index) >= len(s.catalog.tables) {
		return Estimate{}
	}
	return s.catalog.tables[s.index].rows.public()
}

func (s TableStatistic) RowBytes() Estimate {
	if s.catalog == nil || int(s.index) >= len(s.catalog.tables) {
		return Estimate{}
	}
	return s.catalog.tables[s.index].rowBytes.public()
}

func (s TableStatistic) Column(path string) (ColumnStatistic, bool) {
	if s.catalog == nil || int(s.index) >= len(s.catalog.tables) {
		return ColumnStatistic{}, false
	}
	table := s.catalog.tables[s.index]
	lo, hi := uint32(0), table.columnCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry := s.catalog.columns[table.columnBase+mid]
		if s.catalog.string(entry.path) < path {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == table.columnCount {
		return ColumnStatistic{}, false
	}
	index := table.columnBase + lo
	if s.catalog.string(s.catalog.columns[index].path) != path {
		return ColumnStatistic{}, false
	}
	return ColumnStatistic{catalog: s.catalog, index: index}, true
}

type ColumnStatistic struct {
	catalog *StatisticsCatalog
	index   uint32
}

func (s ColumnStatistic) Path() string {
	if s.catalog == nil || int(s.index) >= len(s.catalog.columns) {
		return ""
	}
	return s.catalog.string(s.catalog.columns[s.index].path)
}

func (s ColumnStatistic) Distinct() Estimate {
	if s.catalog == nil || int(s.index) >= len(s.catalog.columns) {
		return Estimate{}
	}
	return s.catalog.columns[s.index].distinct.public()
}

func (s ColumnStatistic) NullFraction() float64 {
	if s.catalog == nil || int(s.index) >= len(s.catalog.columns) {
		return 0
	}
	return float64(s.catalog.columns[s.index].nullFraction)
}

func (s ColumnStatistic) AvgValueBytes() float64 {
	if s.catalog == nil || int(s.index) >= len(s.catalog.columns) {
		return 0
	}
	return float64(s.catalog.columns[s.index].avgValueBytes)
}

// EqualitySelectivity estimates path = canonicalScalar. It uses a heavy hitter
// exactly when present and distributes the remaining non-null mass over the
// remaining distinct values otherwise.
func (s ColumnStatistic) EqualitySelectivity(canonicalScalar string) float64 {
	if s.catalog == nil || int(s.index) >= len(s.catalog.columns) {
		return 0.1
	}
	entry := s.catalog.columns[s.index]
	commonTotal := 0.0
	for i := uint32(0); i < entry.commonCount; i++ {
		value := s.catalog.common[entry.commonBase+i]
		if s.catalog.string(value.value) == canonicalScalar {
			return value.frequency
		}
		commonTotal += value.frequency
	}
	distinct := max(1, entry.distinct.value-float64(entry.commonCount))
	return max(0, 1-float64(entry.nullFraction)-commonTotal) / distinct
}

func (c *StatisticsCatalog) string(ref statisticStringRef) string {
	end := ref.offset + ref.length
	if end < ref.offset || int(end) > len(c.arena) {
		return ""
	}
	return c.arena[ref.offset:end]
}
