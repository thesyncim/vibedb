package query

// A Column is one entry in a SELECT list: either a projection of a JSON path
// or an aggregate over one. Columns are plain values built by the constructors
// below and handed to Select; they carry no compiled state until the query
// compiles. The zero Column is invalid.
type Column struct {
	agg    aggKind
	spec   string // path spec; empty for COUNT(*)
	header string
}

// aggKind names a column's reduction, or aggNone for a plain projection.
type aggKind uint8

const (
	aggNone aggKind = iota
	aggCount
	aggSum
	aggAvg
	aggMin
	aggMax
)

// Path projects the value at spec from each row. spec is a dotted path
// ("user.name") or an RFC 6901 JSON Pointer ("/user/name"); the empty spec
// projects the whole document. Absent and null paths project null.
func Path(spec string) Column {
	return Column{agg: aggNone, spec: spec, header: spec}
}

// Count is the COUNT aggregate. With no argument it counts result rows
// (COUNT(*)); with one path it counts the rows whose path is present and
// non-null (COUNT(path)). More than one argument is rejected at compile time.
func Count(path ...string) Column {
	switch len(path) {
	case 0:
		return Column{agg: aggCount, spec: "", header: "count(*)"}
	default:
		return Column{agg: aggCount, spec: path[0], header: aggHeader("count", path[0])}
	}
}

// Sum totals the numeric values at spec, skipping rows whose value is null or
// not a number. The result is null when no row contributes.
func Sum(spec string) Column { return aggColumn(aggSum, "sum", spec) }

// Avg averages the numeric values at spec, skipping null and non-numeric rows.
// The result is null when no row contributes.
func Avg(spec string) Column { return aggColumn(aggAvg, "avg", spec) }

// Min returns the least numeric value at spec, skipping null and non-numeric
// rows. The result is null when no row contributes.
func Min(spec string) Column { return aggColumn(aggMin, "min", spec) }

// Max returns the greatest numeric value at spec, skipping null and
// non-numeric rows. The result is null when no row contributes.
func Max(spec string) Column { return aggColumn(aggMax, "max", spec) }

func aggColumn(kind aggKind, name, spec string) Column {
	return Column{agg: kind, spec: spec, header: aggHeader(name, spec)}
}

func aggHeader(name, spec string) string {
	return name + "(" + spec + ")"
}

// isAggregate reports whether the column reduces rows rather than projecting
// one value per row.
func (c Column) isAggregate() bool { return c.agg != aggNone }
