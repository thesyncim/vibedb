package planner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
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
	Table      string                `json:"table"`
	Rows       Estimate              `json:"rows"`
	RowBytes   Estimate              `json:"row_bytes"`
	Columns    []ColumnStatistics    `json:"columns,omitempty"`
	Partitions []PartitionStatistics `json:"partitions,omitempty"`
}

// PartitionStatistics is an optional topology-pinned row estimate for one
// physical partition/shard. A distributed cost model can sum selected
// partitions without assuming table rows are uniformly distributed.
type PartitionStatistics struct {
	Partition string   `json:"partition"`
	Rows      Estimate `json:"rows"`
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
	lowerCode  float32
	confidence float32
}

func newCompactEstimate(estimate Estimate, fallback float64) compactEstimate {
	e := estimate.Normalize(fallback)
	lowerCode := conservativeFloat32(e.Lower)
	if e.Lower > math.MaxFloat32 && e.Value > 0 {
		// Negative codes store a scale-free ratio. Ordinary magnitudes retain
		// the previous absolute representation and its useful exact values. Both
		// encodings round down so a compact lower bound never becomes optimistic.
		lowerCode = -conservativeFloat32(min(1, e.Lower/e.Value))
	}
	return compactEstimate{
		value: e.Value, upper: e.Upper, lowerCode: lowerCode, confidence: conservativeFloat32(e.Confidence),
	}
}

func conservativeFloat32(value float64) float32 {
	encoded := float32(value)
	if float64(encoded) > value {
		encoded = math.Nextafter32(encoded, 0)
	}
	return encoded
}

func conservativeUpperFloat32(value float64) float32 {
	encoded := float32(value)
	if float64(encoded) < value {
		encoded = math.Nextafter32(encoded, float32(math.Inf(1)))
	}
	return encoded
}

func (e compactEstimate) lower() float64 {
	if e.lowerCode < 0 {
		return min(e.value, e.value*float64(-e.lowerCode))
	}
	return min(e.value, float64(e.lowerCode))
}

func (e compactEstimate) public() Estimate {
	return Estimate{
		Value: e.value, Lower: e.lower(), Upper: e.upper, Confidence: float64(e.confidence),
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
	commonTotal   float64
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

// compactPartitionStatistic is a 40-byte composite-keyed row estimate. It is
// global rather than linked from the 64-byte table record, preserving that hot
// directory shape for catalogs that do not publish per-partition statistics.
type compactPartitionStatistic struct {
	table     statisticStringRef
	partition statisticStringRef
	rows      compactEstimate
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
	partitions []compactPartitionStatistic
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
	columnCount, commonCount, histogramCount, partitionCount := uint64(0), uint64(0), uint64(0), uint64(0)
	for i := range ordered {
		table := &ordered[i]
		table.Columns = slices.Clone(table.Columns)
		table.Partitions = slices.Clone(table.Partitions)
		slices.SortFunc(table.Columns, func(a, b ColumnStatistics) int {
			return strings.Compare(a.Path, b.Path)
		})
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
			column.MostCommon = slices.Clone(column.MostCommon)
			column.Histogram = slices.Clone(column.Histogram)
			for valueIndex := range column.MostCommon {
				canonical, err := CanonicalScalarJSON(column.MostCommon[valueIndex].Value)
				if err != nil {
					return nil, statisticsError(table.Table, column.Path+": heavy hitter: "+err.Error())
				}
				column.MostCommon[valueIndex].Value = canonical
			}
			slices.SortFunc(column.MostCommon, func(a, b ValueFrequency) int {
				return strings.Compare(a.Value, b.Value)
			})
			for bucketIndex := range column.Histogram {
				canonical, err := CanonicalScalarJSON(column.Histogram[bucketIndex].Upper)
				if err != nil {
					return nil, statisticsError(table.Table, column.Path+": histogram: "+err.Error())
				}
				column.Histogram[bucketIndex].Upper = canonical
			}
			if err := validateColumnStatistic(table.Table, table.Columns, j); err != nil {
				return nil, err
			}
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
		slices.SortFunc(table.Partitions, func(a, b PartitionStatistics) int {
			return strings.Compare(a.Partition, b.Partition)
		})
		partitionCount += uint64(len(table.Partitions))
		for partitionIndex := range table.Partitions {
			partition := &table.Partitions[partitionIndex]
			if partition.Partition == "" || !utf8.ValidString(partition.Partition) {
				return nil, statisticsError(table.Table, "partition name is empty or invalid UTF-8")
			}
			if partitionIndex != 0 && table.Partitions[partitionIndex-1].Partition == partition.Partition {
				return nil, statisticsError(table.Table, "duplicate partition "+partition.Partition)
			}
			if err := validateEstimate(partition.Rows, "partition row count"); err != nil {
				return nil, statisticsError(table.Table, partition.Partition+": "+err.Error())
			}
			stringBytes += uint64(len(partition.Partition))
		}
	}
	maxUint32 := uint64(^uint32(0))
	if columnCount > maxUint32 || commonCount > maxUint32 || histogramCount > maxUint32 ||
		partitionCount > maxUint32 || stringBytes > maxUint32 {
		return nil, fmt.Errorf("%w: compact statistics capacity exceeded", ErrInvalidStatistics)
	}
	catalog.tables = make([]compactTableStatistic, 0, len(ordered))
	catalog.columns = make([]compactColumnStatistic, 0, int(columnCount))
	catalog.common = make([]compactValueFrequency, 0, int(commonCount))
	catalog.histogram = make([]compactHistogramBucket, 0, int(histogramCount))
	catalog.partitions = make([]compactPartitionStatistic, 0, int(partitionCount))
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
		tableEntry := compactTableStatistic{
			name: intern(table.Table), rows: newCompactEstimate(table.Rows, 1000),
			rowBytes:   newCompactEstimate(table.RowBytes, 128),
			columnBase: uint32(len(catalog.columns)), columnCount: uint32(len(table.Columns)),
		}
		for partitionIndex := range table.Partitions {
			partition := &table.Partitions[partitionIndex]
			catalog.partitions = append(catalog.partitions, compactPartitionStatistic{
				table: tableEntry.name, partition: intern(partition.Partition),
				rows: newCompactEstimate(partition.Rows, 0),
			})
		}
		for columnIndex := range table.Columns {
			column := &table.Columns[columnIndex]
			entry := compactColumnStatistic{
				path: intern(column.Path), distinct: newCompactEstimate(column.Distinct, 100),
				nullFraction:  conservativeFloat32(column.NullFraction),
				avgValueBytes: conservativeUpperFloat32(column.AvgValueBytes),
				commonBase:    uint32(len(catalog.common)), commonCount: uint32(len(column.MostCommon)),
				histBase: uint32(len(catalog.histogram)), histCount: uint32(len(column.Histogram)),
				commonTotal: commonFrequencyTotal(column.MostCommon),
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
	if !finiteNonNegative(column.AvgValueBytes) || column.AvgValueBytes > math.MaxFloat32 {
		return statisticsError(table, column.Path+": average value width is invalid")
	}
	commonTotal := 0.0
	for valueIndex, value := range column.MostCommon {
		if valueIndex != 0 && column.MostCommon[valueIndex-1].Value == value.Value {
			return statisticsError(table, column.Path+": duplicate heavy hitter")
		}
		if !finiteFraction(value.Frequency) || value.Frequency == 0 {
			return statisticsError(table, column.Path+": heavy-hitter frequency is outside (0,1]")
		}
		commonTotal += value.Frequency
	}
	if commonTotal > 1-column.NullFraction+1e-6 {
		return statisticsError(table, column.Path+": heavy hitters exceed non-null frequency")
	}
	previousFrequency, previousDistinct := 0.0, 0.0
	previousUpper := ""
	for _, bucket := range column.Histogram {
		if !finiteFraction(bucket.Frequency) || bucket.Frequency <= previousFrequency {
			return statisticsError(table, column.Path+": histogram frequencies are not increasing")
		}
		if !finiteNonNegative(bucket.Distinct) || bucket.Distinct < previousDistinct {
			return statisticsError(table, column.Path+": histogram distinct counts regress")
		}
		if previousUpper != "" {
			comparison, err := CompareCanonicalScalarJSON(previousUpper, bucket.Upper)
			if err != nil || comparison >= 0 {
				return statisticsError(table, column.Path+": histogram upper bounds are not increasing")
			}
		}
		previousFrequency, previousDistinct = bucket.Frequency, bucket.Distinct
		previousUpper = bucket.Upper
	}
	if previousFrequency > 1-column.NullFraction+1e-6 {
		return statisticsError(table, column.Path+": histogram exceeds non-null frequency")
	}
	return nil
}

func commonFrequencyTotal(values []ValueFrequency) float64 {
	total := 0.0
	for _, value := range values {
		total += value.Frequency
	}
	return total
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

// CanonicalScalarJSON validates one JSON scalar and returns a stable spelling.
// Equal numbers share one normalized scientific representation and strings use
// encoding/json's canonical escaping. Catalog publication uses this before
// duplicate detection, so spelling variants cannot create duplicate skew keys.
func CanonicalScalarJSON(value string) (string, error) {
	if value == "" || !utf8.ValidString(value) {
		return "", errors.New("value is empty or invalid UTF-8")
	}
	value = trimJSONSpace(value)
	if value == "" {
		return "", errors.New("value is empty JSON")
	}
	switch value[0] {
	case 'n':
		if value != "null" {
			return "", fmt.Errorf("value %q is not JSON null", value)
		}
		return "null", nil
	case 't', 'f':
		if value != "true" && value != "false" {
			return "", fmt.Errorf("value %q is not a JSON boolean", value)
		}
		return value, nil
	case '"':
		var scalar string
		if err := json.Unmarshal([]byte(value), &scalar); err != nil {
			return "", fmt.Errorf("value %q is not a JSON string: %w", value, err)
		}
		return CanonicalStatisticString(scalar)
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return CanonicalStatisticNumber(value)
	default:
		return "", fmt.Errorf("value %q is not a scalar", value)
	}
}

// CanonicalStatisticString returns the canonical JSON spelling of one raw
// UTF-8 string without reparsing an intermediate quoted representation.
func CanonicalStatisticString(value string) (string, error) {
	encoded, err := AppendCanonicalStatisticString(nil, value)
	return string(encoded), err
}

// AppendCanonicalStatisticString appends the canonical JSON spelling of raw to
// dst. Callers with request-local scratch can perform a skew lookup without a
// transient heap string.
func AppendCanonicalStatisticString(dst []byte, value string) ([]byte, error) {
	if !utf8.ValidString(value) {
		return dst, errors.New("string is invalid UTF-8")
	}
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	for index := 0; index < len(value); {
		char := value[index]
		if char < utf8.RuneSelf {
			index++
			switch char {
			case '\\', '"':
				dst = append(dst, '\\', char)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			case '<', '>', '&':
				dst = append(dst, '\\', 'u', '0', '0', hex[char>>4], hex[char&0xf])
			default:
				if char < 0x20 {
					dst = append(dst, '\\', 'u', '0', '0', hex[char>>4], hex[char&0xf])
				} else {
					dst = append(dst, char)
				}
			}
			continue
		}
		runeValue, width := utf8.DecodeRuneInString(value[index:])
		if runeValue == '\u2028' || runeValue == '\u2029' {
			dst = append(dst, '\\', 'u', '2', '0', '2', hex[byte(runeValue)&0xf])
		} else {
			dst = append(dst, value[index:index+width]...)
		}
		index += width
	}
	return append(dst, '"'), nil
}

// CanonicalStatisticNumber validates and canonicalizes one JSON number. It is
// the direct path for already typed routing numbers and avoids interface-based
// generic JSON decoding.
func CanonicalStatisticNumber(value string) (string, error) {
	if value == "" || !utf8.ValidString(value) {
		return "", errors.New("number is empty or invalid UTF-8")
	}
	if err := validateJSONNumber(value); err != nil {
		return "", err
	}
	return canonicalJSONNumber(value)
}

func trimJSONSpace(value string) string {
	start, end := 0, len(value)
	for start < end && isJSONSpace(value[start]) {
		start++
	}
	for end > start && isJSONSpace(value[end-1]) {
		end--
	}
	return value[start:end]
}

func isJSONSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}

func validateJSONNumber(value string) error {
	index := 0
	if value[index] == '-' {
		index++
		if index == len(value) {
			return errors.New("number has no integer digits")
		}
	}
	if value[index] == '0' {
		index++
		if index < len(value) && value[index] >= '0' && value[index] <= '9' {
			return errors.New("number has a leading zero")
		}
	} else {
		if value[index] < '1' || value[index] > '9' {
			return errors.New("number has an invalid integer part")
		}
		for index < len(value) && value[index] >= '0' && value[index] <= '9' {
			index++
		}
	}
	if index < len(value) && value[index] == '.' {
		index++
		start := index
		for index < len(value) && value[index] >= '0' && value[index] <= '9' {
			index++
		}
		if index == start {
			return errors.New("number has no fractional digits")
		}
	}
	if index < len(value) && (value[index] == 'e' || value[index] == 'E') {
		index++
		if index < len(value) && (value[index] == '+' || value[index] == '-') {
			index++
		}
		start := index
		for index < len(value) && value[index] >= '0' && value[index] <= '9' {
			index++
		}
		if index == start {
			return errors.New("number has no exponent digits")
		}
	}
	if index != len(value) {
		return errors.New("number has trailing syntax")
	}
	return nil
}

func canonicalJSONNumber(value string) (string, error) {
	negative := len(value) != 0 && value[0] == '-'
	if negative {
		value = value[1:]
	}
	mantissa, exponentText := value, "0"
	if index := strings.IndexAny(value, "eE"); index >= 0 {
		mantissa, exponentText = value[:index], value[index+1:]
	}
	var exponent big.Int
	if _, ok := exponent.SetString(exponentText, 10); !ok {
		return "", errors.New("number has an invalid exponent")
	}
	fractionDigits := 0
	if point := strings.IndexByte(mantissa, '.'); point >= 0 {
		fractionDigits = len(mantissa) - point - 1
		mantissa = mantissa[:point] + mantissa[point+1:]
	}
	digits := strings.TrimLeft(mantissa, "0")
	if digits == "" {
		return "0", nil
	}
	exponent.Sub(&exponent, big.NewInt(int64(fractionDigits)))
	trimmed := len(digits) - len(strings.TrimRight(digits, "0"))
	if trimmed != 0 {
		digits = digits[:len(digits)-trimmed]
		exponent.Add(&exponent, big.NewInt(int64(trimmed)))
	}
	exponent.Add(&exponent, big.NewInt(int64(len(digits)-1)))
	var out strings.Builder
	out.Grow(len(digits) + len(exponent.String()) + 3)
	if negative {
		out.WriteByte('-')
	}
	out.WriteByte(digits[0])
	if len(digits) > 1 {
		out.WriteByte('.')
		out.WriteString(digits[1:])
	}
	if exponent.Sign() != 0 {
		out.WriteByte('e')
		out.WriteString(exponent.String())
	}
	return out.String(), nil
}

type canonicalStatisticNumber struct {
	negative    bool
	coefficient string
	digitCount  int
	dot         int
	exponent    string
}

func parseCanonicalStatisticNumber(value string) (canonicalStatisticNumber, error) {
	result := canonicalStatisticNumber{}
	if len(value) != 0 && value[0] == '-' {
		result.negative = true
		value = value[1:]
	}
	result.exponent = "0"
	if index := strings.IndexByte(value, 'e'); index >= 0 {
		result.exponent = value[index+1:]
		value = value[:index]
	}
	if err := validateCanonicalInteger(result.exponent); err != nil {
		return canonicalStatisticNumber{}, fmt.Errorf("canonical number exponent: %w", err)
	}
	result.coefficient = value
	result.dot = strings.IndexByte(value, '.')
	result.digitCount = len(value)
	if result.dot >= 0 {
		result.digitCount--
		if result.dot != 1 || len(value) < 3 {
			return canonicalStatisticNumber{}, errors.New("canonical number has an invalid decimal point")
		}
	}
	if result.digitCount == 0 {
		return canonicalStatisticNumber{}, errors.New("canonical number has no digits")
	}
	for index, digit := range value {
		if index == result.dot {
			continue
		}
		if digit < '0' || digit > '9' {
			return canonicalStatisticNumber{}, errors.New("canonical number has a non-digit coefficient")
		}
	}
	if result.digitCount > 1 && (value[0] == '0' || value[len(value)-1] == '0') {
		return canonicalStatisticNumber{}, errors.New("canonical number has a non-normalized coefficient")
	}
	if result.digitCount == 1 && value[0] == '0' {
		result.negative = false
	}
	return result, nil
}

func validateCanonicalInteger(value string) error {
	if value == "" {
		return errors.New("integer is empty")
	}
	start := 0
	if value[0] == '-' {
		start = 1
		if start == len(value) {
			return errors.New("integer has no digits")
		}
	}
	if len(value)-start > 1 && value[start] == '0' {
		return errors.New("integer has a leading zero")
	}
	for index := start; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return errors.New("integer has a non-digit")
		}
	}
	return nil
}

func (n canonicalStatisticNumber) digit(index int) byte {
	if n.dot >= 0 && index >= n.dot {
		index++
	}
	return n.coefficient[index]
}

// CompareCanonicalScalarJSON compares two outputs of [CanonicalScalarJSON]
// under null < bool < number < string. Exact number comparison is proportional
// to the spelling length and never expands a decimal exponent.
func CompareCanonicalScalarJSON(left, right string) (int, error) {
	leftKind, err := canonicalStatisticKind(left)
	if err != nil {
		return 0, err
	}
	rightKind, err := canonicalStatisticKind(right)
	if err != nil {
		return 0, err
	}
	if leftKind != rightKind {
		if leftKind < rightKind {
			return -1, nil
		}
		return 1, nil
	}
	switch leftKind {
	case ckStatisticNull:
		return 0, nil
	case ckStatisticBool:
		return strings.Compare(left, right), nil
	case ckStatisticNumber:
		return compareCanonicalStatisticNumbers(left, right)
	case ckStatisticString:
		return compareCanonicalStatisticStrings(left, right)
	default:
		return 0, errors.New("unknown canonical statistic scalar kind")
	}
}

type canonicalStringCursor struct {
	value string
	index int
}

func compareCanonicalStatisticStrings(left, right string) (int, error) {
	if len(left) < 2 || left[0] != '"' || left[len(left)-1] != '"' ||
		len(right) < 2 || right[0] != '"' || right[len(right)-1] != '"' {
		return 0, errors.New("canonical string is not quoted")
	}
	a := canonicalStringCursor{value: left, index: 1}
	b := canonicalStringCursor{value: right, index: 1}
	for {
		leftRune, leftOK, err := a.next()
		if err != nil {
			return 0, err
		}
		rightRune, rightOK, err := b.next()
		if err != nil {
			return 0, err
		}
		if !leftOK || !rightOK {
			if leftOK {
				return 1, nil
			}
			if rightOK {
				return -1, nil
			}
			return 0, nil
		}
		if leftRune < rightRune {
			return -1, nil
		}
		if leftRune > rightRune {
			return 1, nil
		}
	}
}

func (c *canonicalStringCursor) next() (rune, bool, error) {
	end := len(c.value) - 1
	if c.index >= end {
		return 0, false, nil
	}
	char := c.value[c.index]
	if char != '\\' {
		if char < 0x20 || char == '"' {
			return 0, false, errors.New("canonical string contains an unescaped character")
		}
		runeValue, width := utf8.DecodeRuneInString(c.value[c.index:end])
		if runeValue == utf8.RuneError && width == 1 {
			return 0, false, errors.New("canonical string contains invalid UTF-8")
		}
		c.index += width
		return runeValue, true, nil
	}
	if c.index+1 >= end {
		return 0, false, errors.New("canonical string has a truncated escape")
	}
	escape := c.value[c.index+1]
	c.index += 2
	switch escape {
	case '"', '\\', '/':
		return rune(escape), true, nil
	case 'b':
		return '\b', true, nil
	case 'f':
		return '\f', true, nil
	case 'n':
		return '\n', true, nil
	case 'r':
		return '\r', true, nil
	case 't':
		return '\t', true, nil
	case 'u':
		if c.index+4 > end {
			return 0, false, errors.New("canonical string has a truncated Unicode escape")
		}
		var runeValue rune
		for offset := 0; offset < 4; offset++ {
			nibble, ok := hexNibble(c.value[c.index+offset])
			if !ok {
				return 0, false, errors.New("canonical string has an invalid Unicode escape")
			}
			runeValue = runeValue<<4 | rune(nibble)
		}
		c.index += 4
		if runeValue >= 0xd800 && runeValue <= 0xdfff {
			return 0, false, errors.New("canonical string contains a surrogate escape")
		}
		return runeValue, true, nil
	default:
		return 0, false, errors.New("canonical string has an invalid escape")
	}
}

func hexNibble(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}

const (
	ckStatisticNull = iota
	ckStatisticBool
	ckStatisticNumber
	ckStatisticString
)

func canonicalStatisticKind(value string) (int, error) {
	switch {
	case value == "null":
		return ckStatisticNull, nil
	case value == "false" || value == "true":
		return ckStatisticBool, nil
	case len(value) != 0 && value[0] == '"':
		return ckStatisticString, nil
	case len(value) != 0:
		return ckStatisticNumber, nil
	default:
		return 0, errors.New("empty canonical statistic scalar")
	}
}

func compareCanonicalStatisticNumbers(left, right string) (int, error) {
	a, err := parseCanonicalStatisticNumber(left)
	if err != nil {
		return 0, err
	}
	b, err := parseCanonicalStatisticNumber(right)
	if err != nil {
		return 0, err
	}
	aZero := a.digitCount == 1 && a.coefficient[0] == '0'
	bZero := b.digitCount == 1 && b.coefficient[0] == '0'
	if aZero || bZero {
		if aZero && bZero {
			return 0, nil
		}
		if aZero {
			if b.negative {
				return 1, nil
			}
			return -1, nil
		}
		if a.negative {
			return -1, nil
		}
		return 1, nil
	}
	if a.negative != b.negative {
		if a.negative {
			return -1, nil
		}
		return 1, nil
	}
	comparison := compareCanonicalInteger(a.exponent, b.exponent)
	if comparison == 0 {
		width := max(a.digitCount, b.digitCount)
		for i := 0; i < width; i++ {
			leftDigit, rightDigit := byte('0'), byte('0')
			if i < a.digitCount {
				leftDigit = a.digit(i)
			}
			if i < b.digitCount {
				rightDigit = b.digit(i)
			}
			if leftDigit < rightDigit {
				comparison = -1
				break
			}
			if leftDigit > rightDigit {
				comparison = 1
				break
			}
		}
	}
	if a.negative {
		comparison = -comparison
	}
	return comparison, nil
}

func compareCanonicalInteger(left, right string) int {
	leftNegative := left[0] == '-'
	rightNegative := right[0] == '-'
	if leftNegative != rightNegative {
		if leftNegative {
			return -1
		}
		return 1
	}
	leftDigits, rightDigits := left, right
	if leftNegative {
		leftDigits, rightDigits = left[1:], right[1:]
	}
	comparison := 0
	if len(leftDigits) < len(rightDigits) {
		comparison = -1
	} else if len(leftDigits) > len(rightDigits) {
		comparison = 1
	} else {
		comparison = strings.Compare(leftDigits, rightDigits)
	}
	if leftNegative {
		return -comparison
	}
	return comparison
}

// CanonicalNumberFitsDecimalBytes reports whether materializing canonical as
// a plain finite decimal is conservatively bounded by maxBytes. It rejects a
// huge positive or negative exponent using only exponent-sized arithmetic.
func CanonicalNumberFitsDecimalBytes(canonical string, maxBytes uint64) bool {
	if maxBytes == 0 {
		return true
	}
	number, err := parseCanonicalStatisticNumber(canonical)
	if err != nil {
		return false
	}
	digits := big.NewInt(int64(number.digitCount))
	var exponent big.Int
	if _, ok := exponent.SetString(number.exponent, 10); !ok {
		return false
	}
	var needed big.Int
	if exponent.Sign() >= 0 {
		digitsMinusOne := new(big.Int).Sub(new(big.Int).Set(digits), big.NewInt(1))
		if exponent.Cmp(digitsMinusOne) >= 0 {
			needed.Add(&exponent, big.NewInt(1))
		} else {
			needed.Add(digits, big.NewInt(1)) // decimal point inside the coefficient
		}
	} else {
		needed.Neg(&exponent)
		needed.Add(&needed, digits)
		needed.Add(&needed, big.NewInt(1)) // leading "0."
	}
	if number.negative {
		needed.Add(&needed, big.NewInt(1))
	}
	return needed.Cmp(new(big.Int).SetUint64(maxBytes)) <= 0
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
	partitionCursor := 0
	for tableIndex := range c.tables {
		table := c.tables[tableIndex]
		descriptor := &out[tableIndex]
		descriptor.Table = strings.Clone(c.string(table.name))
		descriptor.Rows = table.rows.public()
		descriptor.RowBytes = table.rowBytes.public()
		partitionStart := partitionCursor
		for partitionCursor < len(c.partitions) && c.partitions[partitionCursor].table == table.name {
			partitionCursor++
		}
		descriptor.Partitions = make([]PartitionStatistics, partitionCursor-partitionStart)
		for i := partitionStart; i < partitionCursor; i++ {
			partition := c.partitions[i]
			descriptor.Partitions[i-partitionStart] = PartitionStatistics{
				Partition: strings.Clone(c.string(partition.partition)), Rows: partition.rows.public(),
			}
		}
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
		uint64(cap(c.partitions))*uint64(unsafe.Sizeof(compactPartitionStatistic{})) +
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

// PartitionRows returns the immutable row estimate for one physical partition.
// The composite table/partition binary search allocates nothing.
func (s TableStatistic) PartitionRows(partition string) (Estimate, bool) {
	if s.catalog == nil || int(s.index) >= len(s.catalog.tables) {
		return Estimate{}, false
	}
	tableName := s.catalog.string(s.catalog.tables[s.index].name)
	lo, hi := 0, len(s.catalog.partitions)
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry := s.catalog.partitions[mid]
		entryTable := s.catalog.string(entry.table)
		if entryTable < tableName ||
			(entryTable == tableName && s.catalog.string(entry.partition) < partition) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(s.catalog.partitions) {
		return Estimate{}, false
	}
	entry := s.catalog.partitions[lo]
	if s.catalog.string(entry.table) != tableName || s.catalog.string(entry.partition) != partition {
		return Estimate{}, false
	}
	return entry.rows.public(), true
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
	return s.EqualitySelectivityEstimate(canonicalScalar).Value
}

// EqualitySelectivityEstimate returns a risk-aware selectivity interval. Tail
// bounds use the NDV interval inversely: a smaller possible NDV means a larger
// possible equality result. canonicalScalar must be the output of
// [CanonicalScalarJSON]; keeping canonicalization off this hot lookup preserves
// its zero-allocation contract.
func (s ColumnStatistic) EqualitySelectivityEstimate(canonicalScalar string) Estimate {
	if s.catalog == nil || int(s.index) >= len(s.catalog.columns) {
		return unknownEqualitySelectivity()
	}
	entry := s.catalog.columns[s.index]
	commonIndex := entry.commonCount
	if entry.commonCount <= 8 {
		for i := uint32(0); i < entry.commonCount; i++ {
			value := s.catalog.common[entry.commonBase+i]
			if s.catalog.string(value.value) == canonicalScalar {
				commonIndex = i
				break
			}
		}
	} else {
		lo, hi := uint32(0), entry.commonCount
		for lo < hi {
			mid := lo + (hi-lo)/2
			value := s.catalog.common[entry.commonBase+mid]
			if s.catalog.string(value.value) < canonicalScalar {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo < entry.commonCount &&
			s.catalog.string(s.catalog.common[entry.commonBase+lo].value) == canonicalScalar {
			commonIndex = lo
		}
	}
	return s.equalitySelectivityAt(entry, commonIndex)
}

// EqualitySelectivityEstimateBytes is the scratch-buffer form of
// [ColumnStatistic.EqualitySelectivityEstimate]. canonicalScalar must contain
// one canonical JSON scalar and is borrowed only for this call.
func (s ColumnStatistic) EqualitySelectivityEstimateBytes(canonicalScalar []byte) Estimate {
	if s.catalog == nil || int(s.index) >= len(s.catalog.columns) {
		return unknownEqualitySelectivity()
	}
	entry := s.catalog.columns[s.index]
	commonIndex := entry.commonCount
	if entry.commonCount <= 8 {
		for i := uint32(0); i < entry.commonCount; i++ {
			value := s.catalog.common[entry.commonBase+i]
			if bytes.Equal([]byte(s.catalog.string(value.value)), canonicalScalar) {
				commonIndex = i
				break
			}
		}
	} else {
		lo, hi := uint32(0), entry.commonCount
		for lo < hi {
			mid := lo + (hi-lo)/2
			value := s.catalog.common[entry.commonBase+mid]
			if bytes.Compare([]byte(s.catalog.string(value.value)), canonicalScalar) < 0 {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo < entry.commonCount &&
			bytes.Equal([]byte(s.catalog.string(s.catalog.common[entry.commonBase+lo].value)), canonicalScalar) {
			commonIndex = lo
		}
	}
	return s.equalitySelectivityAt(entry, commonIndex)
}

func unknownEqualitySelectivity() Estimate {
	return Estimate{Value: 0.1, Lower: 0, Upper: 1, Confidence: 0}
}

func (s ColumnStatistic) equalitySelectivityAt(entry compactColumnStatistic, commonIndex uint32) Estimate {
	if commonIndex < entry.commonCount {
		value := s.catalog.common[entry.commonBase+commonIndex]
		return Estimate{
			Value: value.frequency, Lower: value.frequency, Upper: value.frequency,
			Confidence: float64(entry.distinct.confidence),
		}
	}
	tail := max(0, 1-float64(entry.nullFraction)-entry.commonTotal)
	common := float64(entry.commonCount)
	valueDistinct := max(1, entry.distinct.value-common)
	upperDistinct := max(1, entry.distinct.upper-common)
	lowerDistinct := entry.distinct.lower() - common
	upper := tail
	if lowerDistinct > 1 {
		upper = tail / lowerDistinct
	}
	return Estimate{
		Value:      min(1, tail/valueDistinct),
		Lower:      min(1, tail/upperDistinct),
		Upper:      min(1, upper),
		Confidence: float64(entry.distinct.confidence),
	}.Normalize(0.1)
}

// LessThanSelectivityEstimate estimates path < upper (or <= upper when
// inclusive) from cumulative equi-depth buckets. The returned interval spans
// the containing bucket because no within-bucket distribution is invented.
func (s ColumnStatistic) LessThanSelectivityEstimate(canonicalUpper string, inclusive bool) Estimate {
	if s.catalog == nil || int(s.index) >= len(s.catalog.columns) {
		return Estimate{Value: 1.0 / 3, Lower: 0, Upper: 1, Confidence: 0}
	}
	entry := s.catalog.columns[s.index]
	if entry.histCount == 0 {
		return Estimate{Value: 1.0 / 3, Lower: 0, Upper: 1, Confidence: 0}
	}
	lo, hi := uint32(0), entry.histCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		bucket := s.catalog.histogram[entry.histBase+mid]
		comparison, err := CompareCanonicalScalarJSON(s.catalog.string(bucket.upper), canonicalUpper)
		if err != nil {
			return Estimate{Value: 1.0 / 3, Lower: 0, Upper: 1, Confidence: 0}
		}
		if comparison < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	previous := 0.0
	if lo != 0 {
		previous = s.catalog.histogram[entry.histBase+lo-1].frequency
	}
	if lo < entry.histCount {
		bucket := s.catalog.histogram[entry.histBase+lo]
		comparison, err := CompareCanonicalScalarJSON(s.catalog.string(bucket.upper), canonicalUpper)
		if err != nil {
			return Estimate{Value: 1.0 / 3, Lower: 0, Upper: 1, Confidence: 0}
		}
		value := (previous + bucket.frequency) / 2
		if comparison == 0 && inclusive {
			value = bucket.frequency
		}
		return Estimate{
			Value: value, Lower: previous, Upper: bucket.frequency,
			Confidence: float64(entry.distinct.confidence),
		}.Normalize(value)
	}
	nonNull := max(previous, 1-float64(entry.nullFraction))
	return Estimate{
		Value: (previous + nonNull) / 2, Lower: previous, Upper: nonNull,
		Confidence: float64(entry.distinct.confidence),
	}.Normalize(nonNull)
}

func (c *StatisticsCatalog) string(ref statisticStringRef) string {
	end := ref.offset + ref.length
	if end < ref.offset || int(end) > len(c.arena) {
		return ""
	}
	return c.arena[ref.offset:end]
}
