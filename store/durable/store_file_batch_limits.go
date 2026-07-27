package durable

// MaxBatchDocuments reports how many distinct keys one [Collection.Update] may
// mutate before the batch is refused with [ErrBatchTooLarge].
//
// The bound is set when the collection is created and cannot change, so a
// caller that assembles a batch incrementally — a SQL transaction, say — can
// read it once and refuse an over-large statement where it was written, rather
// than discovering the limit at commit with a batch it has to throw away. That
// is the whole reason this exists: Update already reports the violation, but it
// reports it at the point where the caller has the least useful context.
func (c *Collection) MaxBatchDocuments() int {
	if c == nil {
		return 0
	}
	return c.options.MaxBatchDocuments
}

// MaxBatchBytes reports the maximum copied key and current-value bytes one
// [Collection.Update] may retain. Superseded values do not consume this budget.
func (c *Collection) MaxBatchBytes() int {
	if c == nil {
		return 0
	}
	return c.options.MaxBatchBytes
}
