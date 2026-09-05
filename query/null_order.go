package query

import sqlast "github.com/thesyncim/vibedb/sql"

func sqlOrderDirection(desc bool, nulls sqlast.WindowNullOrder) Direction {
	if desc {
		if nulls == sqlast.WindowNullsFirst {
			return DescNullsFirst
		}
		return Desc
	}
	if nulls == sqlast.WindowNullsLast {
		return AscNullsLast
	}
	return Asc
}

func compareOrderedScalar(a, b scalar, direction Direction) int {
	if direction >= AscNullsLast && (a.kind == kindNull || b.kind == kindNull) {
		if a.kind == b.kind {
			return 0
		}
		if (a.kind == kindNull) == (direction == DescNullsFirst) {
			return -1
		}
		return 1
	}
	cmp := compareScalar(a, b)
	if direction == Desc || direction == DescNullsFirst {
		return -cmp
	}
	return cmp
}
