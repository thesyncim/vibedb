package durable

import (
	"math"

	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibedb/store"
)

// fileMaterializationProjectionsEqual reports whether replacing an existing
// document can preserve every configured durable secondary projection
// byte-for-byte.
//
// Exact indexes omit a document when any tuple component is missing or is not
// a scalar. Two omitted tuples are therefore equal even when scalar components
// elsewhere in the tuple differ. Present tuples are compared by their exact
// scalar relation rather than by their (potentially colliding) directory hash.
//
// Float64 covering columns omit missing, non-numeric, and non-finite values.
// Present values compare by the IEEE bits persisted by the column encoding,
// which deliberately preserves signed zero.
func (c *Collection) fileMaterializationProjectionsEqual(
	existing, replacement vibejson.Index,
) (bool, error) {
	for _, exact := range c.options.indexes {
		equal, err := fileExactIndexProjectionsEqual(
			exact, existing, replacement,
		)
		if err != nil || !equal {
			return false, err
		}
	}
	for _, column := range c.options.float64Columns {
		existingValue, existingPresent, err := fileFloat64ProjectionValue(
			existing, column.pointer,
		)
		if err != nil {
			return false, err
		}
		replacementValue, replacementPresent, err :=
			fileFloat64ProjectionValue(replacement, column.pointer)
		if err != nil {
			return false, err
		}
		if existingPresent != replacementPresent ||
			existingPresent &&
				math.Float64bits(existingValue) !=
					math.Float64bits(replacementValue) {
			return false, nil
		}
	}
	return true, nil
}

func fileExactIndexProjectionsEqual(
	exact *store.ExactIndex,
	existing, replacement vibejson.Index,
) (bool, error) {
	var existingValues, replacementValues [store.MaxIndexColumns]vibejson.RawValue
	existingPresent, err := fileExactIndexProjection(
		exact, existing, &existingValues,
	)
	if err != nil {
		return false, err
	}
	replacementPresent, err := fileExactIndexProjection(
		exact, replacement, &replacementValues,
	)
	if err != nil {
		return false, err
	}
	if existingPresent != replacementPresent {
		return false, nil
	}
	if !existingPresent {
		return true, nil
	}
	for column := range int(exact.N) {
		if !fileIndexRawValuesEqual(
			existingValues[column], replacementValues[column],
		) {
			return false, nil
		}
	}
	return true, nil
}

func fileExactIndexProjection(
	exact *store.ExactIndex,
	index vibejson.Index,
	values *[store.MaxIndexColumns]vibejson.RawValue,
) (bool, error) {
	if exact == nil || exact.N == 0 ||
		int(exact.N) > len(values) {
		return false, nil
	}
	for column := range int(exact.N) {
		node, found, err := index.PointerCompiled(exact.Paths[column])
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		raw := node.Raw()
		switch raw.Kind() {
		case document.Null, document.Bool, document.Number, document.String:
			values[column] = raw
		default:
			return false, nil
		}
	}
	return true, nil
}
