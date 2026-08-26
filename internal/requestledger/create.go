package requestledger

import "bytes"

// MaterializeCreate converts the sole canonical Create template into the
// operational head persisted by replicated apply. The lease is measured in
// applied entries, so no wall clock or caller-observed absolute index enters
// the immutable execution contract.
func MaterializeCreate(template HeadRecord, appliedIndex uint64) (HeadRecord, error) {
	if err := validateHead(template); err != nil ||
		template.PlanningLeaseExpiryIndex != UnmaterializedPlanningLeaseExpiryIndex ||
		appliedIndex == 0 || template.PlanningLeaseSpan == 0 ||
		template.PlanningLeaseSpan > MaxPlanningLeaseSpan ||
		appliedIndex > ^uint64(0)-template.PlanningLeaseSpan {
		return HeadRecord{}, ErrInvalidState
	}
	template.PlanningLeaseExpiryIndex = appliedIndex + template.PlanningLeaseSpan
	return template, validateHead(template)
}

// MaterializeCreateAtExpiry reconstructs the exact persisted Create head from
// a replicated settlement that carries the original absolute expiry.
func MaterializeCreateAtExpiry(template HeadRecord, expiry uint64) (HeadRecord, error) {
	if err := validateHead(template); err != nil ||
		template.PlanningLeaseExpiryIndex != UnmaterializedPlanningLeaseExpiryIndex ||
		expiry <= template.PlanningLeaseSpan {
		return HeadRecord{}, ErrInvalidState
	}
	template.PlanningLeaseExpiryIndex = expiry
	return template, validateHead(template)
}

// SameCreateBytes reports whether persistedRaw is the exact materialization
// of templateRaw. Both records are independently authenticated first. The
// comparison skips only the operational expiry lane and the checksum which
// necessarily covers it; all other bytes, including inline plan and span, are
// compared in place without allocation.
func SameCreateBytes(persistedRaw, templateRaw []byte) bool {
	if len(persistedRaw) != len(templateRaw) || len(persistedRaw) < headHeaderBytes+checksumBytes {
		return false
	}
	persisted, persistedErr := OpenHead(persistedRaw)
	template, templateErr := OpenHead(templateRaw)
	return persistedErr == nil && templateErr == nil &&
		persisted.PlanningLeaseExpiryIndex > persisted.PlanningLeaseSpan &&
		template.PlanningLeaseExpiryIndex == UnmaterializedPlanningLeaseExpiryIndex &&
		bytes.Equal(persistedRaw[:640], templateRaw[:640]) &&
		bytes.Equal(persistedRaw[648:len(persistedRaw)-checksumBytes],
			templateRaw[648:len(templateRaw)-checksumBytes])
}
