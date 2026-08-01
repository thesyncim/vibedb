package driver

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// mutationWindow is the deliberately small part of SQL's mutation window
// grammar the driver can execute without changing query.Filter's meaning.
// query.Filter selects rows in bounded batches, so ORDER BY and LIMIT cannot
// be left on its synthetic SELECT: doing that would reset LIMIT at each batch
// and would sort only within each batch. The driver instead keeps a bounded
// key frontier while the complete predicate stream is scanned.
type mutationWindow struct {
	limited bool
	limit   int
	ordered bool
	desc    bool
}

func newMutationWindow(
	statement *query.DMLStatement,
	args []any,
	primaryKey string,
	maxDocuments int,
) (mutationWindow, error) {
	var order []sqlast.OrderTerm
	var limitOperand *sqlast.Operand
	switch statement.Tree().Kind {
	case sqlast.KindUpdate:
		order = statement.Tree().Update.OrderBy
		limitOperand = statement.Tree().Update.Limit
	case sqlast.KindDelete:
		order = statement.Tree().Delete.OrderBy
		limitOperand = statement.Tree().Delete.Limit
	default:
		return mutationWindow{}, fmt.Errorf(
			"vibedb: %s does not have a mutation window", statement.Kind())
	}
	if len(order) > 1 {
		return mutationWindow{}, errorsMutationWindow(
			"only one ORDER BY key is supported")
	}
	window := mutationWindow{ordered: len(order) != 0}
	if window.ordered {
		if limitOperand == nil {
			return mutationWindow{}, errorsMutationWindow(
				"ORDER BY requires LIMIT")
		}
		path := order[0].Path
		if path == nil || string(path.AppendPointer(nil)) != primaryKey {
			return mutationWindow{}, errorsMutationWindow(
				"ORDER BY is supported only on the declared primary-key path")
		}
		window.desc = order[0].Desc
	}
	if limitOperand == nil {
		return window, nil
	}
	limit, err := mutationRowCount(*limitOperand, args)
	if err != nil {
		return mutationWindow{}, err
	}
	window.limited, window.limit = true, limit
	if maxDocuments < 0 {
		return mutationWindow{}, errorsMutationWindow(
			"the table has an invalid mutation document bound")
	}
	return window, nil
}

func errorsMutationWindow(detail string) error {
	return fmt.Errorf("vibedb: mutation %s", detail)
}

// selectionLimit is one extra slot when LIMIT exceeds the durable batch
// bound. That extra match lets the caller report ErrBatchTooLarge instead of
// silently treating a too-large LIMIT as the table's batch size.
func (w mutationWindow) selectionLimit(maxDocuments int) int {
	if !w.limited {
		return 0
	}
	if w.limit <= maxDocuments {
		return w.limit
	}
	if maxDocuments == int(^uint(0)>>1) {
		return maxDocuments
	}
	return maxDocuments + 1
}

// mutationKeySelector keeps the best keys in semantic ORDER BY order. Keys
// are orderedkey encodings, whose byte order is the query engine's scalar
// order (null, bool, number, string); a declared primary key cannot be null,
// so byte comparison is exact for this supported ORDER BY form.
type mutationKeySelector struct {
	window mutationWindow
	limit  int
	keys   []string
}

func newMutationKeySelector(window mutationWindow, maxDocuments int) mutationKeySelector {
	return mutationKeySelector{
		window: window,
		limit:  window.selectionLimit(maxDocuments),
	}
}

func (s *mutationKeySelector) add(key string) {
	if !s.window.limited {
		s.keys = append(s.keys, key)
		return
	}
	if s.limit == 0 {
		return
	}
	if !s.window.ordered {
		if len(s.keys) < s.limit {
			s.keys = append(s.keys, key)
		}
		return
	}

	// keys is maintained best-to-worst. Inserting before the first key that
	// compares worse keeps the final mutation and RETURNING order identical.
	at := len(s.keys)
	for i, prior := range s.keys {
		if s.better(key, prior) {
			at = i
			break
		}
	}
	if at == len(s.keys) && len(s.keys) >= s.limit {
		return
	}
	s.keys = append(s.keys, "")
	copy(s.keys[at+1:], s.keys[at:])
	s.keys[at] = key
	if len(s.keys) > s.limit {
		s.keys = s.keys[:s.limit]
	}
}

func (s mutationKeySelector) better(a, b string) bool {
	cmp := bytes.Compare([]byte(a), []byte(b))
	if s.window.desc {
		return cmp > 0
	}
	return cmp < 0
}

func mutationRowCount(operand sqlast.Operand, args []any) (int, error) {
	if operand.Kind == sqlast.OperandNumber {
		n, err := strconv.ParseInt(operand.Text, 10, 64)
		if err != nil || n < 0 || int64(int(n)) != n {
			return 0, fmt.Errorf(
				"vibedb: LIMIT %s is not a non-negative count", operand.Text)
		}
		return int(n), nil
	}
	if operand.Kind != sqlast.OperandParam || operand.Ordinal >= len(args) {
		return 0, errorsMutationWindow("LIMIT must be an integer or a placeholder")
	}
	switch value := args[operand.Ordinal].(type) {
	case int:
		if value >= 0 {
			return value, nil
		}
	case int64:
		if value >= 0 && int64(int(value)) == value {
			return int(value), nil
		}
	case *int64:
		if value != nil && *value >= 0 && int64(int(*value)) == *value {
			return int(*value), nil
		}
	case query.Number:
		if n, err := strconv.ParseInt(string(value), 10, 64); err == nil && n >= 0 && int64(int(n)) == n {
			return int(n), nil
		}
	case *query.Number:
		if value != nil {
			if n, err := strconv.ParseInt(string(*value), 10, 64); err == nil && n >= 0 && int64(int(n)) == n {
				return int(n), nil
			}
		}
	}
	return 0, fmt.Errorf(
		"vibedb: LIMIT was bound to %T; a row count must be a non-negative integer",
		args[operand.Ordinal])
}
