package driver

import (
	"context"
	"errors"
	"fmt"
	"math/bits"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
)

// compiledUniqueIndex is the mutation-side view of one logical unique exact
// index. Uniqueness is intentionally SQL-catalog metadata: the physical exact
// index may be shared with a non-unique alias over the same path vector.
type compiledUniqueIndex struct {
	name  string
	paths []vibejson.CompiledPointer
}

func compileUniqueIndexes(indexes []indexMeta) ([]compiledUniqueIndex, error) {
	count := 0
	for i := range indexes {
		if indexes[i].Unique {
			count++
		}
	}
	if count == 0 {
		return nil, nil
	}
	compiled := make([]compiledUniqueIndex, 0, count)
	for i := range indexes {
		meta := &indexes[i]
		if !meta.Unique {
			continue
		}
		definition := compiledUniqueIndex{
			name: meta.Name, paths: make([]vibejson.CompiledPointer, len(meta.Paths)),
		}
		for column := range meta.Paths {
			pointer, err := vibejson.CompilePointer(meta.Paths[column])
			if err != nil {
				return nil, fmt.Errorf(
					"vibedb: compile unique index %q path %q: %w",
					meta.Name, meta.Paths[column], err,
				)
			}
			definition.paths[column] = pointer
		}
		compiled = append(compiled, definition)
	}
	return compiled, nil
}

// appendUniqueTupleKey appends the same exact scalar identity used by the
// physical index. Each ordered-key component is canonical and prefix-free, so
// concatenation is an unambiguous compound tuple. PostgreSQL's default UNIQUE
// semantics make every tuple containing NULL distinct. Missing paths do not
// participate. Present arrays and objects fail closed: silently omitting a
// non-NULL value would make an accepted uniqueness constraint dishonest.
func appendUniqueTupleKey(
	dst []byte,
	document []byte,
	index compiledUniqueIndex,
) ([]byte, bool, error) {
	start := len(dst)
	participates := true
	for _, pointer := range index.paths {
		value, found, err := pointer.GetRaw(document)
		if err != nil {
			return dst[:start], false, fmt.Errorf(
				"vibedb: unique index %q could not read a JSON document: %w",
				index.name, err,
			)
		}
		if !found || value.IsNull() {
			participates = false
			continue
		}
		var ok bool
		switch value.Kind() {
		case jsondoc.Bool:
			boolean, _ := value.Bool()
			dst, ok = orderedkey.AppendBool(dst, boolean, orderedkey.Ascending)
		case jsondoc.Number:
			number, _ := value.NumberBytes()
			dst, ok = orderedkey.AppendNumber(dst, number, orderedkey.Ascending)
		case jsondoc.String:
			dst, ok = orderedkey.AppendJSONString(dst, value.Bytes(), orderedkey.Ascending)
		default:
			return dst[:start], false, fmt.Errorf(
				"vibedb: unique index %q found a non-scalar value: %w",
				index.name, store.ErrIndexScalar,
			)
		}
		if !ok {
			return dst[:start], false, fmt.Errorf(
				"vibedb: unique index %q found an invalid scalar value",
				index.name,
			)
		}
	}
	if !participates {
		return dst[:start], false, nil
	}
	return dst, true, nil
}

func uniqueProbeValues(
	document []byte,
	index compiledUniqueIndex,
	values *[store.MaxIndexColumns]vibejson.Index,
	entries *[store.MaxIndexColumns]vibejson.IndexEntry,
) ([]vibejson.Index, bool, error) {
	participates := true
	for column, pointer := range index.paths {
		value, found, err := pointer.GetRaw(document)
		if err != nil {
			return nil, false, err
		}
		if !found || value.IsNull() {
			participates = false
			continue
		}
		switch value.Kind() {
		case jsondoc.Bool, jsondoc.Number, jsondoc.String:
		default:
			return nil, false, fmt.Errorf(
				"vibedb: unique index %q found a non-scalar value: %w",
				index.name, store.ErrIndexScalar,
			)
		}
		built, err := vibejson.BuildIndex(
			value.Bytes(), entries[column:column+1:column+1],
		)
		if err != nil {
			return nil, false, fmt.Errorf(
				"vibedb: build unique index %q probe: %w", index.name, err,
			)
		}
		values[column] = built
	}
	if !participates {
		return nil, false, nil
	}
	return values[:len(index.paths)], true, nil
}

func uniqueConstraintError(index, key string) error {
	return fmt.Errorf(
		"%w: index %q exact scalar tuple is already claimed (primary key %q)",
		ErrUniqueConstraint, index, key,
	)
}

// mapDurableUniqueConstraintError preserves the durable sentinel for native
// callers while adding the SQL-driver class consumed by database/sql and
// pgwire. SQL normally detects the violation during final-image validation;
// this translation is the fail-closed boundary if durable admission catches a
// missed or externally raced mutation first.
func mapDurableUniqueConstraintError(err error) error {
	if err == nil || !errors.Is(err, store.ErrUniqueIndexViolation) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrUniqueConstraint, err)
}

// validateUniquePostimages proves that replacing every affected key with the
// supplied final documents leaves each logical unique index injective. It
// checks statement-local siblings first, then probes the immutable pre-write
// snapshot and subtracts matching old images of keys the same atomic mutation
// replaces or removes. That subtraction permits a final-image swap while an
// unaffected owner still causes a violation.
func validateUniquePostimages(
	ctx context.Context,
	indexes []indexMeta,
	snapshot *durable.Snapshot,
	postimages []seedDocument,
	affectedKeys []string,
) error {
	compiled, err := compileUniqueIndexes(indexes)
	if err != nil || len(compiled) == 0 {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	finalByKey := make(map[string][]byte, len(postimages))
	for i := range postimages {
		finalByKey[postimages[i].key] = postimages[i].document
	}
	if len(affectedKeys) == 0 {
		affectedKeys = make([]string, 0, len(finalByKey))
		for key := range finalByKey {
			affectedKeys = append(affectedKeys, key)
		}
	}
	affected := make(map[string]struct{}, len(affectedKeys))
	for _, key := range affectedKeys {
		affected[key] = struct{}{}
	}

	var workspace durable.IndexWorkspace
	defer workspace.Release()
	var masks []store.Mask
	var raw []byte
	for _, index := range compiled {
		if err := contextCheckpoint(ctx); err != nil {
			return err
		}
		owners := make(map[string]string, len(finalByKey))
		representatives := make(map[string]seedDocument, len(finalByKey))
		for key, document := range finalByKey {
			tuple, present, err := appendUniqueTupleKey(nil, document, index)
			if err != nil {
				return err
			}
			if !present {
				continue
			}
			identity := string(tuple)
			if owner, duplicate := owners[identity]; duplicate && owner != key {
				return uniqueConstraintError(index.name, key)
			}
			owners[identity] = key
			representatives[identity] = seedDocument{key: key, document: document}
		}
		if snapshot == nil || len(representatives) == 0 {
			continue
		}

		oldCounts := make(map[string]int, len(affected))
		for key := range affected {
			if err := contextCheckpoint(ctx); err != nil {
				return err
			}
			var found bool
			raw, found, err = snapshot.AppendRaw(raw[:0], []byte(key))
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			tuple, present, tupleErr := appendUniqueTupleKey(nil, raw, index)
			if tupleErr != nil {
				return tupleErr
			}
			if present {
				oldCounts[string(tuple)]++
			}
		}

		for identity, representative := range representatives {
			if err := contextCheckpoint(ctx); err != nil {
				return err
			}
			var values [store.MaxIndexColumns]vibejson.Index
			var entries [store.MaxIndexColumns]vibejson.IndexEntry
			probe, present, probeErr := uniqueProbeValues(
				representative.document, index, &values, &entries,
			)
			if probeErr != nil {
				return probeErr
			}
			if !present {
				continue
			}
			masks, err = snapshot.AppendIndexMasksInto(
				masks[:0], &workspace, index.name, probe...,
			)
			if err != nil {
				return fmt.Errorf(
					"vibedb: probe unique index %q: %w", index.name, err,
				)
			}
			matches := 0
			for _, mask := range masks {
				matches += bits.OnesCount64(mask.Bits)
			}
			if matches > oldCounts[identity] {
				return uniqueConstraintError(index.name, representative.key)
			}
		}
	}
	return nil
}

func validateTableUniquePostimages(
	ctx context.Context,
	table *table,
	postimages []seedDocument,
	affectedKeys []string,
) error {
	if len(postimages) == 0 || !tableHasUniqueIndexes(table) {
		return nil
	}
	var snapshot *durable.Snapshot
	if table.collection != nil {
		var err error
		snapshot, err = table.collection.Snapshot()
		if err != nil {
			return err
		}
		defer snapshot.Close()
	}
	return validateUniquePostimages(
		ctx, table.meta.Indexes, snapshot, postimages, affectedKeys,
	)
}

func tableHasUniqueIndexes(table *table) bool {
	if table == nil || table.meta == nil {
		return false
	}
	for i := range table.meta.Indexes {
		if table.meta.Indexes[i].Unique {
			return true
		}
	}
	return false
}

func transactionUniqueImage(
	state *txTable,
	staged []stagedTxMutation,
) ([]seedDocument, []string) {
	final := make(map[string][]byte, len(state.pending)+len(staged))
	affected := make(map[string]struct{}, len(state.order)+len(staged))
	for _, key := range state.order {
		affected[key] = struct{}{}
		mutation := state.pending[key]
		if mutation != nil && !mutation.remove {
			final[key] = mutation.document
		}
	}
	for i := range staged {
		mutation := &staged[i]
		affected[mutation.key] = struct{}{}
		if mutation.remove {
			delete(final, mutation.key)
		} else {
			final[mutation.key] = mutation.document
		}
	}
	postimages := make([]seedDocument, 0, len(final))
	for key, document := range final {
		postimages = append(postimages, seedDocument{key: key, document: document})
	}
	keys := make([]string, 0, len(affected))
	for key := range affected {
		keys = append(keys, key)
	}
	return postimages, keys
}

func validateTransactionUniqueStatement(
	ctx context.Context,
	state *txTable,
	staged []stagedTxMutation,
) error {
	if state == nil || len(state.uniqueIndexes) == 0 {
		return nil
	}
	postimages, affected := transactionUniqueImage(state, staged)
	if len(postimages) == 0 {
		return nil
	}
	return validateUniquePostimages(
		ctx, state.uniqueIndexes, state.snapshot, postimages, affected,
	)
}

func validateTransactionUniqueCommit(
	table *table,
	state *txTable,
) error {
	if state == nil || !tableHasUniqueIndexes(table) {
		return nil
	}
	postimages, affected := transactionUniqueImage(state, nil)
	if len(postimages) == 0 {
		return nil
	}
	snapshot, err := table.collection.Snapshot()
	if err != nil {
		return err
	}
	defer snapshot.Close()
	return validateUniquePostimages(
		context.Background(), table.meta.Indexes, snapshot, postimages, affected,
	)
}
