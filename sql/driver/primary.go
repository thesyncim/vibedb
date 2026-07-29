package driver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

func primaryPredicateKeys(
	where *sqlast.Expr,
	primaryKey string,
	args []any,
	dst []string,
	maxKeyBytes int,
) ([]string, bool, error) {
	if !isPrimaryPredicate(where, primaryKey) {
		clear(dst)
		return dst[:0], false, nil
	}
	keys, err := bindPrimaryPredicateKeys(where, args, dst, maxKeyBytes)
	return keys, true, err
}

// isPrimaryPredicate performs the immutable shape check for a primary-key
// point predicate. Prepared SELECT statements cache this result, so their hot
// path only binds operands and never renders the JSON path again.
func isPrimaryPredicate(where *sqlast.Expr, primaryKey string) bool {
	if where == nil || where.Path == nil ||
		string(where.Path.AppendPointer(nil)) != primaryKey {
		return false
	}
	switch where.Kind {
	case sqlast.ExprCompare:
		return where.Op == sqlast.OpEq && where.Subquery == nil
	case sqlast.ExprIn:
		return !where.Negated && where.Subquery == nil
	default:
		return false
	}
}

// bindPrimaryPredicateKeys binds a predicate already proven by
// isPrimaryPredicate to be either primary-key equality or a positive IN list.
func bindPrimaryPredicateKeys(
	where *sqlast.Expr,
	args []any,
	dst []string,
	maxKeyBytes int,
) ([]string, error) {
	clear(dst)
	keys := dst[:0]
	var operands []sqlast.Operand
	switch where.Kind {
	case sqlast.ExprCompare:
		operands = []sqlast.Operand{where.Value}
	case sqlast.ExprIn:
		operands = where.List
	}
	for _, operand := range operands {
		value, err := operandValue(operand, args)
		if err != nil {
			return keys, err
		}
		if value == nil {
			// SQL equality with NULL is UNKNOWN. In an IN list that alternative
			// contributes no match; as the sole operand it yields the empty
			// point set.
			continue
		}
		key, matchable, err := boundedPrimaryScalarKey(value, maxKeyBytes)
		if err != nil {
			return keys, fmt.Errorf("vibedb: primary key predicate: %w", err)
		}
		if !matchable {
			if operand.Kind == sqlast.OperandParam {
				args[operand.Ordinal] = nil
			}
			continue
		}
		duplicate := false
		for _, prior := range keys {
			if prior == key {
				duplicate = true
				break
			}
		}
		if !duplicate {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// bindPointPredicateKeys is the query-only, borrowed counterpart to
// bindPrimaryPredicateKeys. It builds all encoded keys in one connection-owned
// arena, then creates zero-copy string views only after the arena is stable.
//
// Point reads consume the keys synchronously and never retain them. The
// owning-string binder remains mandatory for DML because transactional maps
// can retain those keys beyond the call.
func (c *conn) bindPointPredicateKeys(
	where *sqlast.Expr,
	args []any,
	maxKeyBytes int,
) ([]string, error) {
	// Borrowed strings from the prior execution must stop being reachable
	// before their backing arena is reused.
	clear(c.pointKeys)
	c.pointKeys = c.pointKeys[:0]
	c.pointKeyRaw = c.pointKeyRaw[:0]
	c.pointKeyEnds = c.pointKeyEnds[:0]

	var operands []sqlast.Operand
	switch where.Kind {
	case sqlast.ExprCompare:
		operands = []sqlast.Operand{where.Value}
	case sqlast.ExprIn:
		operands = where.List
	}
	for _, operand := range operands {
		start := len(c.pointKeyRaw)
		var (
			matchable bool
			err       error
		)
		if operand.Kind == sqlast.OperandParam &&
			operand.Ordinal < len(args) {
			if value, ok := args[operand.Ordinal].([]byte); ok {
				c.pointKeyRaw, matchable, err =
					appendBoundedPrimaryString(
						c.pointKeyRaw, value, maxKeyBytes,
					)
			} else {
				var scalar any
				scalar, err = operandValue(operand, args)
				if err == nil && scalar != nil {
					c.pointKeyRaw, matchable, err =
						appendBoundedPrimaryScalarKey(
							c.pointKeyRaw, scalar, maxKeyBytes,
						)
				}
			}
		} else {
			var scalar any
			scalar, err = operandValue(operand, args)
			if err == nil && scalar != nil {
				c.pointKeyRaw, matchable, err =
					appendBoundedPrimaryScalarKey(
						c.pointKeyRaw, scalar, maxKeyBytes,
					)
			}
		}
		if err != nil {
			return c.pointKeys, fmt.Errorf(
				"vibedb: primary key predicate: %w", err)
		}
		if !matchable {
			c.pointKeyRaw = c.pointKeyRaw[:start]
			if operand.Kind == sqlast.OperandParam {
				args[operand.Ordinal] = nil
			}
			continue
		}
		candidate := c.pointKeyRaw[start:]
		duplicate := false
		priorStart := 0
		for _, priorEnd := range c.pointKeyEnds {
			if bytes.Equal(c.pointKeyRaw[priorStart:priorEnd], candidate) {
				duplicate = true
				break
			}
			priorStart = priorEnd
		}
		if duplicate {
			c.pointKeyRaw = c.pointKeyRaw[:start]
			continue
		}
		c.pointKeyEnds = append(c.pointKeyEnds, len(c.pointKeyRaw))
	}

	start := 0
	for _, end := range c.pointKeyEnds {
		c.pointKeys = append(c.pointKeys,
			byteview.String(c.pointKeyRaw[start:end:end]))
		start = end
	}
	return c.pointKeys, nil
}

// primaryScalarKey is the one storage-key encoding used by both document
// derivation and point predicates. orderedkey's type tags keep strings, bools,
// and numbers disjoint, and its exact-decimal encoding makes 1, 1.0, and 1e0
// identify one row without a float64 conversion or a decode-after-encode pass.
func primaryScalarKey(value any) (string, error) {
	key, err := appendPrimaryScalarKey(nil, value)
	if err != nil {
		return "", err
	}
	return string(key), nil
}

func boundedPrimaryScalarKey(
	value any,
	maxKeyBytes int,
) (string, bool, error) {
	key, matchable, err := appendBoundedPrimaryScalarKey(
		nil, value, maxKeyBytes)
	if err != nil || !matchable {
		return "", matchable, err
	}
	return string(key), true, nil
}

// appendBoundedPrimaryScalarKey refuses a valid but unstoreable key before
// appending any bytes. Point predicates treat that value as an exact
// non-match; invalid values remain errors. The distinction prevents one legal
// multi-megabyte SQL parameter from permanently growing a pooled connection's
// point-key arena even though SQL tables admit only a tiny encoded key.
func appendBoundedPrimaryScalarKey(
	dst []byte,
	value any,
	maxKeyBytes int,
) ([]byte, bool, error) {
	switch value := value.(type) {
	case string:
		return appendBoundedPrimaryString(
			dst, byteview.Bytes(value), maxKeyBytes,
		)
	case []byte:
		return appendBoundedPrimaryString(dst, value, maxKeyBytes)
	case bool:
		if maxKeyBytes < 1 {
			return dst, false, nil
		}
		key, _ := orderedkey.AppendBool(dst, value, orderedkey.Ascending)
		return key, true, nil
	case *bool:
		if value == nil {
			return dst, false, errors.New("a primary key cannot be NULL")
		}
		if maxKeyBytes < 1 {
			return dst, false, nil
		}
		key, _ := orderedkey.AppendBool(dst, *value, orderedkey.Ascending)
		return key, true, nil
	case int:
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendInt(text[:0], int64(value), 10), maxKeyBytes)
	case int8:
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendInt(text[:0], int64(value), 10), maxKeyBytes)
	case int16:
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendInt(text[:0], int64(value), 10), maxKeyBytes)
	case int32:
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendInt(text[:0], int64(value), 10), maxKeyBytes)
	case int64:
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendInt(text[:0], value, 10), maxKeyBytes)
	case *int64:
		if value == nil {
			return dst, false, errors.New("a primary key cannot be NULL")
		}
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendInt(text[:0], *value, 10), maxKeyBytes)
	case uint:
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendUint(text[:0], uint64(value), 10), maxKeyBytes)
	case uint8:
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendUint(text[:0], uint64(value), 10), maxKeyBytes)
	case uint16:
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendUint(text[:0], uint64(value), 10), maxKeyBytes)
	case uint32:
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendUint(text[:0], uint64(value), 10), maxKeyBytes)
	case uint64:
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendUint(text[:0], value, 10), maxKeyBytes)
	case float32:
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendFloat(
				text[:0], float64(value), 'g', -1, 64,
			), maxKeyBytes)
	case float64:
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendFloat(text[:0], value, 'g', -1, 64),
			maxKeyBytes)
	case *float64:
		if value == nil {
			return dst, false, errors.New("a primary key cannot be NULL")
		}
		var text [32]byte
		return appendBoundedPrimaryNumber(
			dst, strconv.AppendFloat(text[:0], *value, 'g', -1, 64),
			maxKeyBytes)
	case json.Number:
		return appendBoundedPrimaryNumber(
			dst, byteview.Bytes(string(value)), maxKeyBytes)
	case query.Number:
		return appendBoundedPrimaryNumber(
			dst, byteview.Bytes(string(value)), maxKeyBytes)
	case *query.Number:
		if value == nil {
			return dst, false, errors.New("a primary key cannot be NULL")
		}
		return appendBoundedPrimaryNumber(
			dst, byteview.Bytes(string(*value)), maxKeyBytes)
	case *string:
		if value == nil {
			return dst, false, errors.New("a primary key cannot be NULL")
		}
		return appendBoundedPrimaryString(
			dst, byteview.Bytes(*value), maxKeyBytes)
	case nil:
		return dst, false, errors.New("a primary key cannot be NULL")
	default:
		return dst, false, fmt.Errorf(
			"%T is not a JSON string, number, or bool", value)
	}
}

func appendBoundedPrimaryString(
	dst, value []byte,
	maxKeyBytes int,
) ([]byte, bool, error) {
	size, ok := orderedkey.StringEncodedSize(value)
	if !ok {
		return dst, false, errors.New(
			"string primary key is not valid UTF-8")
	}
	if size > maxKeyBytes {
		return dst, false, nil
	}
	key, _ := orderedkey.AppendString(
		dst, value, orderedkey.Ascending)
	return key, true, nil
}

func appendBoundedPrimaryNumber(
	dst, number []byte,
	maxKeyBytes int,
) ([]byte, bool, error) {
	size, ok := orderedkey.NumberEncodedSize(number)
	if !ok {
		return dst, false, errors.New(
			"numeric primary key is not a valid JSON number")
	}
	if size > maxKeyBytes {
		return dst, false, nil
	}
	key, _ := orderedkey.AppendNumber(dst, number, orderedkey.Ascending)
	return key, true, nil
}

func appendPrimaryScalarKey(dst []byte, value any) ([]byte, error) {
	key, matchable, err := appendBoundedPrimaryScalarKey(
		dst, value, int(^uint(0)>>1))
	if err != nil {
		return dst, err
	}
	if !matchable {
		return dst, errors.New("primary key exceeds the platform size bound")
	}
	return key, nil
}
