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

// SealedRecoveryJournalBytes returns the immutable strictly allocated record
// region of the paired recovery journal. Zero denotes the ordinary elastic
// journal geometry.
func (c *Collection) SealedRecoveryJournalBytes() uint64 {
	if c == nil {
		return 0
	}
	return c.options.SealedRecoveryJournalBytes
}

// HasSchema reports whether the collection's sealed logical definition
// enforces a document schema. The result is immutable for the collection
// lifetime. Integration layers that accept an explicitly schema-free command
// grammar use this at construction instead of trusting a caller assertion that
// could make committed input fail only at apply time.
func (c *Collection) HasSchema() bool {
	return c != nil && c.options.Collection.Schema != nil
}

// HasIndexes reports whether the collection's current durable logical catalog
// contains any exact index definitions. The result is captured under the same
// publication gate as snapshots and online index changes.
func (c *Collection) HasIndexes() bool {
	if c == nil {
		return false
	}
	c.snapshotGate.RLock()
	defer c.snapshotGate.RUnlock()
	return len(c.options.Indexes) != 0
}

// HasSynchronousDurability reports whether every acknowledged mutation is
// fenced before it becomes visible. Replicated state-machine construction uses
// this to refuse volatile or deferred-acknowledgement handles before a
// committed entry can reach apply.
func (c *Collection) HasSynchronousDurability() bool {
	return c != nil && c.options.Durability == DurabilitySync
}
