package durable

// NormalizeOptions applies every zero-value default and validates the result.
// The returned Options are the exact logical bounds and modes a new collection
// will freeze into its catalog. Callers that stage work before a Collection
// exists use this to enforce the same limits before allocating from
// peer-controlled input.
func NormalizeOptions(options Options) (Options, error) {
	normalized, err := options.normalized()
	if err != nil {
		return Options{}, err
	}
	return normalized.Options, nil
}

// MaxKeyBytes reports the maximum encoded primary-key size.
func (c *Collection) MaxKeyBytes() int {
	if c == nil {
		return 0
	}
	return c.options.MaxKeyBytes
}

// MaxDocumentBytes reports the maximum JSON document size.
func (c *Collection) MaxDocumentBytes() int {
	if c == nil {
		return 0
	}
	return c.options.MaxDocumentBytes
}

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
