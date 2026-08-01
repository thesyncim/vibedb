package query

import (
	"errors"
	"fmt"
	"strconv"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	// ErrUndefinedColumn classifies a reference that names no output column of
	// a derived relation.
	ErrUndefinedColumn = errors.New("query: undefined column")
	// ErrAmbiguousColumn classifies a reference that matches more than one
	// output column of a derived relation.
	ErrAmbiguousColumn = errors.New("query: ambiguous column")
)

// RelationColumnError reports name resolution against a derived relation.
// Matches is zero for an undefined name and greater than one for ambiguity.
type RelationColumnError struct {
	Relation string
	Column   string
	Matches  int
}

func (e *RelationColumnError) Error() string {
	if e.Matches > 1 {
		return fmt.Sprintf(
			"query: column %q is ambiguous in derived relation %q (%d outputs have that name): %v",
			e.Column, e.Relation, e.Matches, ErrAmbiguousColumn,
		)
	}
	return fmt.Sprintf(
		"query: derived relation %q has no column %q: %v",
		e.Relation, e.Column, ErrUndefinedColumn,
	)
}

func (e *RelationColumnError) Unwrap() error {
	if e.Matches > 1 {
		return ErrAmbiguousColumn
	}
	return ErrUndefinedColumn
}

// statementDerived is one uncorrelated, relation-valued child plan. Its spool
// is ordinal and columnar: SQL output names remain display metadata, so
// duplicate names never collapse relation identity.
type statementDerived struct {
	tree *sqlast.SelectStmt
	stmt *Statement
	exec Exec

	spool relationSpool

	names       []string
	ordinalSpec []string
	specData    []byte
	activeBytes int64
}

func (s *Statement) prepareDerived() error {
	if len(s.tree.From) == 0 || s.tree.From[0].Kind != sqlast.RelationDerived {
		return nil
	}
	if len(s.tree.From) != 1 || s.tree.From[0].Query == nil {
		return fmt.Errorf("query: a derived relation must be the sole FROM source")
	}
	child, err := prepareTree(s.text, s.tree.From[0].Query)
	if err != nil {
		return err
	}
	if s.nested == nil {
		s.nested = new(nestedStatements)
	}
	d := &statementDerived{
		tree: s.tree.From[0].Query,
		stmt: child,
	}
	d.names = append(d.names, child.Columns()...)
	d.ordinalSpec = make([]string, len(d.names))
	for i := range d.ordinalSpec {
		start := len(d.specData)
		d.specData = append(d.specData, '/')
		d.specData = strconv.AppendInt(d.specData, int64(i), 10)
		d.ordinalSpec[i] = byteview.String(
			d.specData[start:len(d.specData):len(d.specData)],
		)
	}
	s.nested.derived = d
	return s.validateDerivedReferences()
}

func (s *Statement) derived() *statementDerived {
	if s.nested == nil {
		return nil
	}
	return s.nested.derived
}

func (s *Statement) validateDerivedReferences() error {
	d := s.derived()
	if d == nil {
		return nil
	}
	check := func(path *sqlast.PathExpr) error {
		if path == nil || len(path.Segments) == 0 || path.Source != 0 {
			return nil
		}
		_, err := d.resolve(path.Segments[0].Key, s.tree.From[0].Alias)
		return err
	}
	for i := range s.tree.Columns {
		if err := check(s.tree.Columns[i].Path); err != nil {
			return err
		}
	}
	var walkExpr func(*sqlast.Expr) error
	walkExpr = func(expr *sqlast.Expr) error {
		if expr == nil {
			return nil
		}
		if err := check(expr.Path); err != nil {
			return err
		}
		for _, kid := range expr.Kids {
			if err := walkExpr(kid); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walkExpr(s.tree.Where); err != nil {
		return err
	}
	if err := walkExpr(s.tree.Having); err != nil {
		return err
	}
	for _, path := range s.tree.GroupBy {
		if err := check(path); err != nil {
			return err
		}
	}
	for i := range s.tree.OrderBy {
		if err := check(s.tree.OrderBy[i].Path); err != nil {
			return err
		}
	}
	return nil
}

func (d *statementDerived) resolve(name, relation string) (int, error) {
	found, matches := -1, 0
	for i := range d.names {
		if d.names[i] == name {
			found = i
			matches++
		}
	}
	if matches != 1 {
		return -1, &RelationColumnError{
			Relation: relation,
			Column:   name,
			Matches:  matches,
		}
	}
	return found, nil
}

func (s *Statement) renderDerived(path *sqlast.PathExpr, local bool) string {
	d := s.derived()
	if d == nil || path == nil || path.Source != 0 || len(path.Segments) == 0 {
		return ""
	}
	for i := range s.specs {
		if s.specs[i].path == path && s.specs[i].local == local {
			return s.specs[i].text
		}
	}
	ordinal, err := d.resolve(path.Segments[0].Key, s.tree.From[0].Alias)
	if err != nil {
		// Every reference is validated during prepare. Reaching this branch
		// would require mutation of the caller-owned AST after preparation.
		return ""
	}
	start := len(s.specBuf)
	s.specBuf = append(s.specBuf, d.ordinalSpec[ordinal]...)
	for _, segment := range path.Segments[1:] {
		s.specBuf = append(s.specBuf, '/')
		if segment.IsIndex {
			s.specBuf = strconv.AppendInt(s.specBuf, int64(segment.Index), 10)
			continue
		}
		for i := 0; i < len(segment.Key); i++ {
			switch segment.Key[i] {
			case '~':
				s.specBuf = append(s.specBuf, '~', '0')
			case '/':
				s.specBuf = append(s.specBuf, '~', '1')
			default:
				s.specBuf = append(s.specBuf, segment.Key[i])
			}
		}
	}
	text := byteview.String(s.specBuf[start:len(s.specBuf):len(s.specBuf)])
	s.specs = append(s.specs, pathSpec{path: path, local: local, text: text})
	return text
}

func (s *Statement) derivedDisplaySpec(path *sqlast.PathExpr) string {
	if path == nil {
		return ""
	}
	start := len(s.specBuf)
	s.specBuf = path.AppendSpec(s.specBuf)
	return byteview.String(s.specBuf[start:len(s.specBuf):len(s.specBuf)])
}

func (s *Statement) runDerived(
	parent *Exec,
	src Source,
	args []any,
	frame *statementFrame,
) (Source, error) {
	d := s.derived()
	if d == nil {
		return src, nil
	}
	// The previous successful result was invalidated by runIntoFrame before
	// this reset, so no surviving cursor can still borrow the old relation.
	d.spool.reset()
	d.activeBytes = 0

	n := d.stmt.NumParams()
	base := d.tree.ParamBase
	if base < 0 || base+n > len(args) {
		return Source{}, fmt.Errorf("query: invalid derived placeholder range")
	}
	nestedSource, err := src.subquerySource(s.Collection(), d.stmt.Collection())
	if err != nil {
		return Source{}, err
	}
	d.exec.Options = parent.Options
	cursor, err := d.stmt.runIntoFrame(
		&d.exec, nestedSource, args[base:base+n], frame,
		"derived query result",
	)
	if err != nil {
		return Source{}, err
	}
	resultBytes := d.exec.Result.resultBytesUsed
	if err := frame.intermediate.reserve("derived query result", resultBytes); err != nil {
		return Source{}, err
	}
	defer frame.intermediate.release(resultBytes)

	charge, err := d.spool.materialize(
		cursor, len(d.names), frame, parent.Options.Cancel,
		"derived relation spool",
	)
	if err != nil {
		s.releaseDerived(frame)
		return Source{}, err
	}
	d.activeBytes = charge
	// The spool owns every cell now. The child result and any child relation it
	// borrowed may be invalidated before the outer plan starts.
	clearExecBorrowedViews(&d.exec)
	d.stmt.releaseDerived(frame)
	return fromRelationSpool(&d.spool), nil
}

func (s *Statement) releaseDerived(frame *statementFrame) {
	d := s.derived()
	if d == nil {
		return
	}
	clearExecBorrowedViews(&d.exec)
	d.stmt.releaseDerived(frame)
	d.spool.reset()
	frame.intermediate.release(d.activeBytes)
	d.activeBytes = 0
}

// discardDerived invalidates a relation retained by a previous completed
// execution when the next top-level attempt fails before it can install a new
// statement frame. No account survives between RunInto calls, so this path
// resets ownership without debiting a frame that no longer exists.
func (s *Statement) discardDerived() {
	d := s.derived()
	if d == nil {
		return
	}
	clearExecBorrowedViews(&d.exec)
	d.stmt.discardDerived()
	d.spool.reset()
	d.activeBytes = 0
}

// encodedCellJSONBytes measures a Cell without copying a borrowed payload.
// Computed machine numbers are the sole representation without raw bytes and
// fit the fixed formatting scratch; projected strings, containers, decimals,
// booleans, and nulls report their exact retained source length directly.
func encodedCellJSONBytes(cell Cell) int {
	if cell.raw != nil {
		return len(cell.raw)
	}
	if cell.kind != TypeNumber {
		return 0
	}
	var scratch [32]byte
	return len(cell.AppendJSON(scratch[:0]))
}
