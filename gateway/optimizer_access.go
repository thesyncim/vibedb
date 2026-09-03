package gateway

import (
	"math"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
)

// globalIndexAccessCost compares complete alternatives before any remote probe.
// A probe fetches locators and then visits base owners, so a low-NDV global
// index can be much more expensive than another eligible index. Unique indexes
// bound candidates by key count, but a huge unique IN list is not automatically
// cheaper than one selective nonunique probe.
func globalIndexAccessCost(program GlobalIndexProgram, domains distribution.BoundConstraints) float64 {
	keys := 1.0
	for _, domain := range domains {
		keys = boundedProduct(keys, float64(len(domain.Values)))
	}
	rows, width := 1000.0, 128.0
	stats, hasStats := program.snapshot.Statistics(program.metadata.Table)
	if hasStats {
		rows = stats.Rows().Normalize(rows).Upper
		width = max(1, stats.RowBytes().Normalize(width).Upper)
	}
	if program.metadata.Flags&IndexUnique != 0 {
		rows = min(rows, keys)
	} else if hasStats {
		paths := program.metadata.Paths[:program.metadata.PathCount]
		if joint, ok := boundJointSelectivity(stats, paths, domains); ok {
			rows = boundedProduct(rows, joint.Upper)
		} else {
			var selectivities [4]float64
			count := 0
			var scratch [256]byte
			for i, domain := range domains {
				column, ok := stats.Column(paths[i])
				if !ok {
					continue
				}
				sel := 0.0
				complete := true
				for _, value := range domain.Values {
					canonical, valid := appendBoundStatisticScalar(scratch[:0], value)
					if !valid {
						complete = false
						break
					}
					sel += column.EqualitySelectivityEstimateBytes(canonical).Upper
				}
				if complete {
					selectivities[count] = min(1, sel)
					count++
				}
			}
			slices.Sort(selectivities[:count])
			combined, exponent := 1.0, 1.0
			for _, sel := range selectivities[:count] {
				combined *= math.Pow(sel, exponent)
				exponent *= .5
			}
			rows = boundedProduct(rows, combined)
		}
	}
	// The locator and base row both cross the network; key probes also have
	// a nonzero fixed cost even if the estimated result is empty.
	locatorWidth := float64(program.metadata.LocatorCount) * 16
	return boundedAdd(boundedProduct(keys, 64), boundedProduct(rows, boundedAdd(width, locatorWidth)))
}
