package planner

import (
	"fmt"
	"math"
	"math/bits"
	"slices"
	"strings"
	"unicode/utf8"
	"unsafe"
)

// GroupStatistics describes a joint distribution, including tuples containing
// NULL. Paths identify a set, not an ordering; publication canonicalizes both
// paths and tuple coordinates. Partition groups describe local distinct counts,
// which must not be added to estimate global distinct counts.
type GroupStatistics struct {
	Paths      []string         `json:"paths"`
	Distinct   Estimate         `json:"distinct"`
	MostCommon []TupleFrequency `json:"most_common,omitempty"`
}

// TupleFrequency is a joint frequency in all rows, with explicit uncertainty.
// Values are JSON scalars in the same order as GroupStatistics.Paths.
type TupleFrequency struct {
	Values    []string `json:"values"`
	Frequency Estimate `json:"frequency"`
}

const MaxStatisticsGroupColumns = 8

type compactGroupStatistic struct {
	table                 uint32
	partition             statisticStringRef
	pathBase, pathCount   uint32
	tupleBase, tupleCount uint32
	distinct              compactEstimate
}
type compactTupleFrequency struct {
	valueBase uint32
	frequency compactEstimate
}

func (c *StatisticsCatalog) buildGroups(tables []TableStatistics) error {
	var arena strings.Builder
	intern := func(value string) (statisticStringRef, error) {
		if uint64(arena.Len())+uint64(len(value)) > math.MaxUint32 {
			return statisticStringRef{}, fmt.Errorf("%w: group arena capacity", ErrInvalidStatistics)
		}
		ref := statisticStringRef{uint32(arena.Len()), uint32(len(value))}
		arena.WriteString(value)
		return ref, nil
	}
	for tableID, table := range tables {
		add := func(partition string, groups []GroupStatistics) error {
			normalized := make([]GroupStatistics, len(groups))
			for i, group := range groups {
				var err error
				normalized[i], err = normalizeGroup(table.Table, group)
				if err != nil {
					return err
				}
			}
			slices.SortFunc(normalized, func(a, b GroupStatistics) int { return slices.Compare(a.Paths, b.Paths) })
			for i, g := range normalized {
				if i > 0 && slices.Equal(g.Paths, normalized[i-1].Paths) {
					return statisticsError(table.Table, "duplicate statistics group")
				}
				if uint64(len(c.groupPaths))+uint64(len(g.Paths)) > math.MaxUint32 || uint64(len(c.tuples))+uint64(len(g.MostCommon)) > math.MaxUint32 || uint64(len(c.tupleValues))+uint64(len(g.MostCommon))*uint64(len(g.Paths)) > math.MaxUint32 {
					return statisticsError(table.Table, "group directory capacity")
				}
				part, err := intern(partition)
				if err != nil {
					return err
				}
				entry := compactGroupStatistic{table: uint32(tableID), partition: part, pathBase: uint32(len(c.groupPaths)), pathCount: uint32(len(g.Paths)), tupleBase: uint32(len(c.tuples)), tupleCount: uint32(len(g.MostCommon)), distinct: newCompactEstimate(g.Distinct, 100)}
				for _, p := range g.Paths {
					ref, err := intern(p)
					if err != nil {
						return err
					}
					c.groupPaths = append(c.groupPaths, ref)
				}
				for _, tuple := range g.MostCommon {
					c.tuples = append(c.tuples, compactTupleFrequency{valueBase: uint32(len(c.tupleValues)), frequency: newCompactEstimate(tuple.Frequency, 0)})
					for _, v := range tuple.Values {
						ref, err := intern(v)
						if err != nil {
							return err
						}
						c.tupleValues = append(c.tupleValues, ref)
					}
				}
				c.groups = append(c.groups, entry)
			}
			return nil
		}
		if err := add("", table.Groups); err != nil {
			return err
		}
		for _, partition := range table.Partitions {
			if err := add(partition.Partition, partition.Groups); err != nil {
				return err
			}
		}
	}
	c.groupArena = arena.String()
	return nil
}

func normalizeGroup(table string, group GroupStatistics) (GroupStatistics, error) {
	n := len(group.Paths)
	if n == 0 || n > MaxStatisticsGroupColumns {
		return GroupStatistics{}, statisticsError(table, "statistics group must have 1..8 paths")
	}
	if err := validateEstimate(group.Distinct, "group distinct count"); err != nil {
		return GroupStatistics{}, statisticsError(table, err.Error())
	}
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return strings.Compare(group.Paths[a], group.Paths[b]) })
	result := GroupStatistics{Paths: make([]string, n), Distinct: group.Distinct, MostCommon: make([]TupleFrequency, len(group.MostCommon))}
	for i, j := range order {
		p := group.Paths[j]
		if p == "" || !utf8.ValidString(p) || i > 0 && p == result.Paths[i-1] {
			return GroupStatistics{}, statisticsError(table, "invalid or repeated group path")
		}
		result.Paths[i] = p
	}
	total, lower := 0.0, 0.0
	for i, tuple := range group.MostCommon {
		if len(tuple.Values) != n {
			return GroupStatistics{}, statisticsError(table, "tuple arity mismatch")
		}
		if err := validateEstimate(tuple.Frequency, "tuple frequency"); err != nil {
			return GroupStatistics{}, statisticsError(table, err.Error())
		}
		if tuple.Frequency.Upper > 1 || tuple.Frequency.Value == 0 {
			return GroupStatistics{}, statisticsError(table, "tuple frequency outside (0,1]")
		}
		result.MostCommon[i] = TupleFrequency{Values: make([]string, n), Frequency: tuple.Frequency}
		for k, j := range order {
			v, err := CanonicalScalarJSON(tuple.Values[j])
			if err != nil {
				return GroupStatistics{}, statisticsError(table, err.Error())
			}
			result.MostCommon[i].Values[k] = v
		}
		total += tuple.Frequency.Value
		lower += tuple.Frequency.Lower
	}
	if total > 1+1e-12 || lower > 1+1e-12 || float64(len(group.MostCommon)) > group.Distinct.Upper {
		return GroupStatistics{}, statisticsError(table, "inconsistent tuple frequency or distinct count")
	}
	slices.SortFunc(result.MostCommon, func(a, b TupleFrequency) int { return slices.Compare(a.Values, b.Values) })
	for i := 1; i < len(result.MostCommon); i++ {
		if slices.Equal(result.MostCommon[i-1].Values, result.MostCommon[i].Values) {
			return GroupStatistics{}, statisticsError(table, "duplicate tuple")
		}
	}
	return result, nil
}

func (c *StatisticsCatalog) groupString(ref statisticStringRef) string {
	return c.groupArena[ref.offset : uint64(ref.offset)+uint64(ref.length)]
}
func (c *StatisticsCatalog) groupRetainedBytes() uint64 {
	return uint64(cap(c.groups))*uint64(unsafe.Sizeof(compactGroupStatistic{})) + uint64(cap(c.groupPaths))*uint64(unsafe.Sizeof(statisticStringRef{})) + uint64(cap(c.tuples))*uint64(unsafe.Sizeof(compactTupleFrequency{})) + uint64(cap(c.tupleValues))*uint64(unsafe.Sizeof(statisticStringRef{})) + uint64(len(c.groupArena))
}
func (c *StatisticsCatalog) groupDescriptors(table uint32, partition string) []GroupStatistics {
	var out []GroupStatistics
	for _, entry := range c.groupEntries(table, partition) {
		if entry.table != table || c.groupString(entry.partition) != partition {
			continue
		}
		g := GroupStatistics{Paths: make([]string, entry.pathCount), Distinct: entry.distinct.public(), MostCommon: make([]TupleFrequency, entry.tupleCount)}
		for i := range g.Paths {
			g.Paths[i] = strings.Clone(c.groupString(c.groupPaths[entry.pathBase+uint32(i)]))
		}
		for i := range g.MostCommon {
			t := c.tuples[entry.tupleBase+uint32(i)]
			g.MostCommon[i] = TupleFrequency{Values: make([]string, entry.pathCount), Frequency: t.frequency.public()}
			for j := range g.Paths {
				g.MostCommon[i].Values[j] = strings.Clone(c.groupString(c.tupleValues[t.valueBase+uint32(j)]))
			}
		}
		out = append(out, g)
	}
	return out
}

// AppendColumnPaths enumerates published columns without allocating if dst has
// capacity. This lets SQL compile predicate statistics independently of routing.
func (s TableStatistic) AppendColumnPaths(dst []string) []string {
	if s.catalog == nil {
		return dst
	}
	t := s.catalog.tables[s.index]
	for i := uint32(0); i < t.columnCount; i++ {
		dst = append(dst, s.catalog.string(s.catalog.columns[t.columnBase+i].path))
	}
	for _, g := range s.catalog.groupEntries(s.index, "") {
		if g.table != s.index || g.partition.length != 0 {
			continue
		}
		for i := uint32(0); i < g.pathCount; i++ {
			path := s.catalog.groupString(s.catalog.groupPaths[g.pathBase+i])
			if !slices.Contains(dst, path) {
				dst = append(dst, path)
			}
		}
	}
	return dst
}

// GroupDistinct returns joint NDV, including NULL groups, for the exact set of
// paths. A nonempty partition requests local statistics, never a global fallback.
// The lookup is allocation-free and accepts paths in any order.
func (s TableStatistic) GroupDistinct(partition string, paths []string) (Estimate, bool) {
	if s.catalog == nil || len(paths) == 0 {
		return Estimate{}, false
	}
	for _, g := range s.catalog.groupEntries(s.index, partition) {
		if g.table != s.index || int(g.pathCount) != len(paths) || s.catalog.groupString(g.partition) != partition {
			continue
		}
		matched := true
		for i := uint32(0); i < g.pathCount; i++ {
			p := s.catalog.groupString(s.catalog.groupPaths[g.pathBase+i])
			count := 0
			for _, path := range paths {
				if p == path {
					count++
				}
			}
			if count != 1 {
				matched = false
				break
			}
		}
		if matched {
			return g.distinct.public(), true
		}
	}
	if partition == "" && len(paths) == 1 {
		if col, ok := s.Column(paths[0]); ok {
			e := col.Distinct()
			if col.NullFraction() > 0 {
				e.Value++
				e.Lower++
				e.Upper++
			}
			return e, true
		}
	}
	return Estimate{}, false
}

// EqualityConstraint denotes a finite domain of canonical, non-NULL JSON
// scalars. Values must be distinct. SQL = NULL is not represented here.
type EqualityConstraint struct {
	Path   string
	Values []string
}

// JointSelectivity estimates a fully constrained published column group. It
// returns false rather than inventing correlations when the group is absent.
// MCV matches are counted once even for IN predicates; residual mass is spread
// over the remaining joint NDV with the inverse NDV uncertainty bounds.
func (s TableStatistic) JointSelectivity(constraints []EqualityConstraint) (Estimate, bool) {
	if s.catalog == nil || len(constraints) < 1 || len(constraints) > MaxStatisticsGroupColumns {
		return Estimate{}, false
	}
	c := s.catalog
	for _, g := range c.groupEntries(s.index, "") {
		if g.table != s.index || g.partition.length != 0 || int(g.pathCount) != len(constraints) {
			continue
		}
		var slots [MaxStatisticsGroupColumns]int
		complete := true
		combinations := 1.0
		for i := uint32(0); i < g.pathCount; i++ {
			slots[i] = -1
			p := c.groupString(c.groupPaths[g.pathBase+i])
			for j, q := range constraints {
				if p == q.Path {
					if slots[i] != -1 {
						complete = false
					}
					slots[i] = j
				}
			}
			if slots[i] < 0 {
				complete = false
				break
			}
			n := len(constraints[slots[i]].Values)
			if n == 0 {
				return ExactEstimate(0), true
			}
			combinations = min(math.MaxFloat64, combinations*float64(n))
		}
		if !complete {
			continue
		}
		result := Estimate{Confidence: float64(g.distinct.confidence)}
		total, totalLower, totalUpper, matches := 0.0, 0.0, 0.0, 0.0
		for i := uint32(0); i < g.tupleCount; i++ {
			t := c.tuples[g.tupleBase+i]
			f := t.frequency.public()
			total += f.Value
			totalLower += f.Lower
			totalUpper += f.Upper
			match := true
			for j := uint32(0); j < g.pathCount; j++ {
				v := c.groupString(c.tupleValues[t.valueBase+j])
				if v == "null" || !slices.Contains(constraints[slots[j]].Values, v) {
					match = false
					break
				}
			}
			result.Confidence = min(result.Confidence, f.Confidence)
			if match {
				matches++
				result.Value += f.Value
				result.Lower += f.Lower
				result.Upper += f.Upper
			}
		}
		residual := max(0, combinations-matches)
		n := float64(g.tupleCount)
		result.Value += max(0, 1-total) * min(1, residual/max(1, g.distinct.value-n))
		result.Lower += max(0, 1-totalUpper) * min(1, residual/max(1, g.distinct.upper-n))
		result.Upper += max(0, 1-totalLower) * min(1, residual/max(1, g.distinct.lower()-n))
		result.Value = min(1, result.Value)
		result.Lower = min(result.Value, result.Lower)
		result.Upper = min(1, max(result.Value, result.Upper))
		return result, true
	}
	return Estimate{}, false
}

// HasGroups reports whether this table publishes a global joint distribution.
func (s TableStatistic) HasGroups() bool {
	if s.catalog == nil {
		return false
	}
	for _, g := range s.catalog.groupEntries(s.index, "") {
		if g.table == s.index && g.partition.length == 0 {
			return true
		}
	}
	return false
}

// ConjunctionSelectivity uses the largest available non-overlapping groups and
// applies exponential backoff only between independent statistics objects.
// Each predicate is consumed once; an overlapping group cannot double count it.
// Unsupported or oversized inputs fall back to the caller's existing estimator.
func (s TableStatistic) ConjunctionSelectivity(constraints []EqualityConstraint) (Estimate, bool) {
	if !s.HasGroups() || len(constraints) == 0 || len(constraints) > MaxStatisticsGroupColumns {
		return Estimate{}, false
	}
	for i, q := range constraints {
		if len(q.Values) == 0 {
			return ExactEstimate(0), true
		}
		for _, previous := range constraints[:i] {
			if q.Path == previous.Path {
				return Estimate{}, false
			}
		}
	}
	var factors [MaxStatisticsGroupColumns]Estimate
	var subset [MaxStatisticsGroupColumns]EqualityConstraint
	used, count := uint16(0), 0
	for {
		bestMask := uint16(0)
		bestCount := 0
		for _, g := range s.catalog.groupEntries(s.index, "") {
			if g.table != s.index || g.partition.length != 0 || int(g.pathCount) <= bestCount {
				continue
			}
			mask := uint16(0)
			for i := uint32(0); i < g.pathCount; i++ {
				path := s.catalog.groupString(s.catalog.groupPaths[g.pathBase+i])
				for j, q := range constraints {
					if path == q.Path {
						mask |= 1 << j
						break
					}
				}
			}
			if mask&used != 0 || bits.OnesCount16(mask) != int(g.pathCount) {
				continue
			}
			bestMask, bestCount = mask, int(g.pathCount)
		}
		if bestMask == 0 {
			break
		}
		n := 0
		for i, q := range constraints {
			if bestMask&(1<<i) != 0 {
				subset[n] = q
				n++
			}
		}
		e, ok := s.JointSelectivity(subset[:n])
		if !ok {
			return Estimate{}, false
		}
		factors[count] = e
		count++
		used |= bestMask
	}
	if used == 0 {
		return Estimate{}, false
	}
	for i, q := range constraints {
		if used&(1<<i) != 0 {
			continue
		}
		e := ExactEstimate(0)
		column, ok := s.Column(q.Path)
		if !ok {
			e = unknownEqualitySelectivity()
		} else {
			for _, v := range q.Values {
				x := column.EqualitySelectivityEstimate(v)
				e.Value += x.Value
				e.Lower += x.Lower
				e.Upper += x.Upper
				e.Confidence = min(e.Confidence, x.Confidence)
			}
		}
		e.Value = min(1, e.Value)
		e.Lower = min(e.Value, e.Lower)
		e.Upper = min(1, max(e.Value, e.Upper))
		factors[count] = e
		count++
	}
	slices.SortFunc(factors[:count], func(a, b Estimate) int {
		if a.Upper < b.Upper {
			return -1
		}
		if a.Upper > b.Upper {
			return 1
		}
		return 0
	})
	result, exponent := ExactEstimate(1), 1.0
	for _, e := range factors[:count] {
		result.Value *= math.Pow(e.Value, exponent)
		result.Lower *= math.Pow(e.Lower, exponent)
		result.Upper *= math.Pow(e.Upper, exponent)
		result.Confidence = min(result.Confidence, e.Confidence)
		exponent *= .5
	}
	return result, true
}

// Directories are ordered by table, partition, then path set. A hot lookup
// searches only one publication's group run, never every shard in the catalog.
func (c *StatisticsCatalog) groupEntries(table uint32, partition string) []compactGroupStatistic {
	compare := func(g compactGroupStatistic) int {
		if g.table < table {
			return -1
		}
		if g.table > table {
			return 1
		}
		return strings.Compare(c.groupString(g.partition), partition)
	}
	lo, hi := 0, len(c.groups)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if compare(c.groups[mid]) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	start := lo
	hi = len(c.groups)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if compare(c.groups[mid]) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return c.groups[start:lo]
}
