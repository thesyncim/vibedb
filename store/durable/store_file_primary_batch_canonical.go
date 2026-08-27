package durable

// canonicalizePrimaryBatchValues runs under the collection writer before any
// journal, exact-index, or primary plan borrows the batch arena. Already
// canonical values stay in place. Changed values reuse the single-document
// indexed renderer and copy back into their owned slot; shrinking leaves
// bounded holes rather than moving unrelated values or relying on entry order.
func (c *Collection) canonicalizePrimaryBatchValues(batch *WriteBatch) error {
	if batch.canonical {
		return nil
	}
	for at := range batch.entries {
		entry := &batch.entries[at]
		if entry.remove {
			continue
		}
		value := batch.value(*entry)
		if len(value) == 0 || len(value) > c.options.MaxDocumentBytes {
			return ErrDocumentTooLarge
		}
		if c.options.OpaqueValues {
			continue
		}
		if err := c.validatePrimarySchema(value); err != nil {
			return err
		}
		canonical, err := c.canonicalPrimaryMutationValue(value)
		if err != nil {
			return err
		}
		if len(canonical) > c.options.MaxDocumentBytes {
			return ErrDocumentTooLarge
		}
		if len(canonical) > len(value) {
			// Raw UTF-8 U+2028/U+2029 expand from three bytes to six-byte
			// escapes. replaceValue repairs all offsets and may grow the arena;
			// no plan has borrowed it yet. The current grammar can at most
			// double source bytes, so even expansion remains input-bounded.
			batch.replaceValue(at, canonical)
		} else if &canonical[0] != &value[0] {
			copy(value, canonical)
			clear(value[len(canonical):])
			entry.valueLength = len(canonical)
		}
	}
	if !batch.recovery && batch.logicalBytes() > int64(c.options.MaxBatchBytes) {
		return ErrBatchTooLarge
	}
	batch.canonical = true
	return nil
}

// logicalBytes excludes holes left by canonical shrinking. The physical arena
// remains bounded by admitted input plus Unicode escape expansion; transaction
// admission charges the exact final keys and values, not superseded spellings.
func (batch *WriteBatch) logicalBytes() int64 {
	var total int64
	for _, entry := range batch.entries {
		total += int64(entry.keyLength) + int64(entry.valueLength)
	}
	return total
}

// All participant writers must be held. Complete normalization and final-byte
// admission for every member before any staged plan can borrow an arena. Raw
// Unicode line separators can expand even when the original input fit limits.
func canonicalizePrimaryTransactionBatches(members []NamedCollection, byName map[string]*WriteBatch, limits TxnLimits) error {
	var totalBytes int64
	totalDocs := 0
	for _, member := range members {
		batch := byName[member.Name]
		if err := member.Collection.canonicalizePrimaryBatchValues(batch); err != nil {
			return err
		}
		totalDocs += batch.Len()
		totalBytes += batch.logicalBytes()
	}
	return checkTxnLimits(limits, len(members), totalDocs, totalBytes)
}
