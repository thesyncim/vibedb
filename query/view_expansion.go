package query

import (
	"errors"
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
)

const (
	maxSQLViewExpansionDepth = 32
	maxSQLViewExpansionRefs  = 1024
	maxSQLViewExpansionBytes = 16 << 20
)

var (
	// ErrSQLViewCycle classifies a durable-view dependency cycle. Ordinary
	// views are expanded, never iterated, so every cycle is a definition error.
	ErrSQLViewCycle = errors.New("query: SQL view dependency cycle")
	// ErrSQLViewExpansionLimit classifies a definition graph that exceeds the
	// finite preparation bounds.
	ErrSQLViewExpansionLimit = errors.New("query: SQL view expansion limit")
)

// SQLViewDefinition is the immutable catalog payload needed by the query
// expander. Query is normalized SQL containing one parameter-free SELECT.
// Columns is the optional exact, ordinal output-name replacement from CREATE
// VIEW's column list.
type SQLViewDefinition struct {
	Name    string
	Query   string
	Columns []string
}

// SQLViewResolver resolves one catalog relation name without changing its
// lifetime. Implementations must keep a returned definition immutable until
// ExpandSQLViews returns.
type SQLViewResolver interface {
	ResolveSQLView(name string) (SQLViewDefinition, bool, error)
}

// SQLViewDependency identifies one distinct expanded view in deterministic
// first-reference order. Pos is the byte offset of the authored reference in
// the outer statement when available.
type SQLViewDependency struct {
	Name string
	Pos  int
}

// SQLViewExpansionOptions controls cold-path cancellation and cycle detection.
// RootName is non-empty while validating CREATE VIEW and makes a reference
// back to the definition being created a cycle even before it is published.
type SQLViewExpansionOptions struct {
	RootName string
	Check    func() error
}

// SQLViewExpansion is the immutable result metadata of one successful pass.
type SQLViewExpansion struct {
	Dependencies []SQLViewDependency
}

// ExpandSQLViewDefinition parses and expands one unpublished or cataloged view
// definition into an owned AST. It applies the exact CREATE VIEW output alias
// list before expansion and treats a reference to definition.Name as a cycle.
// The returned tree and dependency metadata are published only on success.
func ExpandSQLViewDefinition(
	definition SQLViewDefinition,
	resolver SQLViewResolver,
	options SQLViewExpansionOptions,
) (*sqlast.SelectStmt, SQLViewExpansion, error) {
	if definition.Name == "" {
		return nil, SQLViewExpansion{}, errors.New(
			"query: SQL view definition has an empty name",
		)
	}
	parser := new(sqlast.Parser)
	parser.SetCancellationCheck(options.Check)
	tree := new(sqlast.SelectStmt)
	if err := parser.Parse(tree, definition.Query); err != nil {
		return nil, SQLViewExpansion{}, err
	}
	if tree.Params != 0 {
		return nil, SQLViewExpansion{}, sqlast.NewFeatureNotSupportedError(
			definition.Query, 0,
			"stored view definitions cannot contain parameters",
		)
	}
	if err := applySQLViewColumns(
		definition.Query, tree, definition.Name, definition.Columns, 0,
	); err != nil {
		return nil, SQLViewExpansion{}, err
	}
	options.RootName = definition.Name
	// Count the unpublished definition itself as the first view in the
	// dependency chain. This matches expansion of the same definition through
	// an authored top-level reference, so CREATE cannot durably publish a view
	// that immediately exceeds the execution-time depth bound.
	expansion, err := expandSQLViews(
		definition.Query, tree, resolver, options, 1,
	)
	if err != nil {
		return nil, SQLViewExpansion{}, err
	}
	return tree, expansion, nil
}

// SQLViewCycleError reports the authored reference that closes a cycle.
type SQLViewCycleError struct {
	Name string
	Path []string
	Pos  int
}

func (e *SQLViewCycleError) Error() string {
	if e == nil {
		return ErrSQLViewCycle.Error()
	}
	return fmt.Sprintf("query: SQL view %q closes dependency cycle %v: %v",
		e.Name, e.Path, ErrSQLViewCycle)
}

func (e *SQLViewCycleError) Unwrap() error { return ErrSQLViewCycle }

func (e *SQLViewCycleError) Position() int {
	if e == nil {
		return 0
	}
	return e.Pos
}

// SQLViewExpansionLimitError describes which finite admission bound failed.
type SQLViewExpansionLimitError struct {
	Name  string
	Kind  string
	Limit int
	Pos   int
}

func (e *SQLViewExpansionLimitError) Error() string {
	if e == nil {
		return ErrSQLViewExpansionLimit.Error()
	}
	return fmt.Sprintf("query: SQL view %q exceeds %s limit %d: %v",
		e.Name, e.Kind, e.Limit, ErrSQLViewExpansionLimit)
}

func (e *SQLViewExpansionLimitError) Unwrap() error { return ErrSQLViewExpansionLimit }

func (e *SQLViewExpansionLimitError) Position() int {
	if e == nil {
		return 0
	}
	return e.Pos
}

type sqlViewExpansionEntry struct {
	name  string
	tree  *sqlast.SelectStmt
	state uint8
}

type sqlViewExpansionPatch struct {
	target      *sqlast.TableRef
	replacement sqlast.TableRef
}

type sqlViewExpander struct {
	source   string
	resolver SQLViewResolver
	options  SQLViewExpansionOptions

	entries      []sqlViewExpansionEntry
	entryByName  map[string]int
	dependencies []SQLViewDependency
	patches      []sqlViewExpansionPatch
	stack        []string
	totalBytes   int
	references   int
}

// ExpandSQLViews expands every durable view reference reachable from root.
// Physical collections and lexical CTE references are unchanged. Publication
// is atomic with respect to root: relation replacements are recorded and only
// installed after every definition parses, validates, and expands. On error,
// root remains byte-for-byte structurally unchanged.
func ExpandSQLViews(
	source string,
	root *sqlast.SelectStmt,
	resolver SQLViewResolver,
	options SQLViewExpansionOptions,
) (SQLViewExpansion, error) {
	return expandSQLViews(source, root, resolver, options, 0)
}

func expandSQLViews(
	source string,
	root *sqlast.SelectStmt,
	resolver SQLViewResolver,
	options SQLViewExpansionOptions,
	initialDepth int,
) (SQLViewExpansion, error) {
	if root == nil {
		return SQLViewExpansion{}, errors.New("query: SQL view expansion received a nil SELECT")
	}
	if resolver == nil {
		return SQLViewExpansion{}, nil
	}
	expander := sqlViewExpander{
		source:      source,
		resolver:    resolver,
		options:     options,
		entryByName: make(map[string]int),
	}
	if options.RootName != "" {
		expander.stack = append(expander.stack, options.RootName)
	}
	if err := expander.check(); err != nil {
		return SQLViewExpansion{}, err
	}
	if err := expander.expandSelect(root, initialDepth, -1); err != nil {
		return SQLViewExpansion{}, err
	}
	if err := expander.check(); err != nil {
		return SQLViewExpansion{}, err
	}
	for i := range expander.patches {
		patch := &expander.patches[i]
		*patch.target = patch.replacement
	}
	return SQLViewExpansion{Dependencies: expander.dependencies}, nil
}

func (x *sqlViewExpander) check() error {
	if x.options.Check == nil {
		return nil
	}
	return x.options.Check()
}

func (x *sqlViewExpander) expandSelect(
	tree *sqlast.SelectStmt,
	depth int,
	origin int,
) error {
	if tree == nil {
		return nil
	}
	if depth > maxSQLViewExpansionDepth {
		return &SQLViewExpansionLimitError{
			Name: x.currentName(), Kind: "dependency depth",
			Limit: maxSQLViewExpansionDepth, Pos: origin,
		}
	}
	if err := x.check(); err != nil {
		return err
	}
	if tree.Set != nil {
		return x.expandSet(tree.Set.Root, depth, origin)
	}
	if tree.With != nil {
		for i := range tree.With.CTEs {
			if err := x.expandSelect(tree.With.CTEs[i].Query, depth, origin); err != nil {
				return err
			}
		}
	}
	for i := range tree.From {
		ref := &tree.From[i]
		switch ref.Kind {
		case sqlast.RelationCollection:
			if err := x.expandReference(ref, depth, origin); err != nil {
				return err
			}
		case sqlast.RelationDerived:
			if err := x.expandSelect(ref.Query, depth, origin); err != nil {
				return err
			}
		case sqlast.RelationCTE:
			// The owning definition is visited once above. Its reference is
			// lexical identity, not a catalog lookup.
		}
	}
	if err := x.expandExpr(tree.Where, depth, origin); err != nil {
		return err
	}
	return x.expandExpr(tree.Having, depth, origin)
}

func (x *sqlViewExpander) expandSet(
	expression *sqlast.SetExpr,
	depth int,
	origin int,
) error {
	if expression == nil {
		return nil
	}
	if err := x.check(); err != nil {
		return err
	}
	switch expression.Kind {
	case sqlast.SetSelectExpr, sqlast.SetTableExpr:
		return x.expandSelect(expression.Select, depth, origin)
	case sqlast.SetValuesExpr:
		return nil
	case sqlast.SetBinaryExpr:
		if err := x.expandSet(expression.Left, depth, origin); err != nil {
			return err
		}
		return x.expandSet(expression.Right, depth, origin)
	case sqlast.SetGroupExpr:
		return x.expandSet(expression.Child, depth, origin)
	default:
		return nil
	}
}

func (x *sqlViewExpander) expandExpr(
	expression *sqlast.Expr,
	depth int,
	origin int,
) error {
	if expression == nil {
		return nil
	}
	if expression.Subquery != nil {
		if err := x.expandSelect(expression.Subquery, depth, origin); err != nil {
			return err
		}
	}
	for i := range expression.Kids {
		if err := x.expandExpr(expression.Kids[i], depth, origin); err != nil {
			return err
		}
	}
	return nil
}

func (x *sqlViewExpander) expandReference(
	ref *sqlast.TableRef,
	depth int,
	origin int,
) error {
	if ref == nil || ref.Name == "" {
		return nil
	}
	if err := x.check(); err != nil {
		return err
	}
	position := origin
	if position < 0 {
		position = ref.Pos
	}
	for i := range x.stack {
		if x.stack[i] == ref.Name {
			path := append([]string(nil), x.stack[i:]...)
			path = append(path, ref.Name)
			return &SQLViewCycleError{Name: ref.Name, Path: path, Pos: position}
		}
	}
	definition, exists, err := x.resolver.ResolveSQLView(ref.Name)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if definition.Name != ref.Name {
		return fmt.Errorf(
			"query: SQL view resolver associated reference %q with definition %q",
			ref.Name, definition.Name,
		)
	}
	x.references++
	if x.references > maxSQLViewExpansionRefs {
		return &SQLViewExpansionLimitError{
			Name: definition.Name, Kind: "reference count",
			Limit: maxSQLViewExpansionRefs, Pos: position,
		}
	}
	if !x.hasDependency(definition.Name) {
		x.dependencies = append(x.dependencies, SQLViewDependency{
			Name: definition.Name, Pos: position,
		})
	}
	entry, err := x.expandDefinition(definition, depth+1, position)
	if err != nil {
		return err
	}
	replacement := *ref
	replacement.Kind = sqlast.RelationDerived
	replacement.Name = ""
	replacement.Query = entry.tree
	replacement.UnresolvedCTE = sqlast.CTEReferenceMetadata{}
	x.patches = append(x.patches, sqlViewExpansionPatch{
		target: ref, replacement: replacement,
	})
	return nil
}

func (x *sqlViewExpander) expandDefinition(
	definition SQLViewDefinition,
	depth int,
	origin int,
) (*sqlViewExpansionEntry, error) {
	if definition.Name == "" {
		return nil, errors.New("query: SQL view resolver returned an empty definition name")
	}
	if existing, ok := x.entryByName[definition.Name]; ok {
		entry := &x.entries[existing]
		if entry.state == 2 {
			return entry, nil
		}
	}
	if depth > maxSQLViewExpansionDepth {
		return nil, &SQLViewExpansionLimitError{
			Name: definition.Name, Kind: "dependency depth",
			Limit: maxSQLViewExpansionDepth, Pos: origin,
		}
	}
	if len(definition.Query) > maxSQLViewExpansionBytes-x.totalBytes {
		return nil, &SQLViewExpansionLimitError{
			Name: definition.Name, Kind: "definition bytes",
			Limit: maxSQLViewExpansionBytes, Pos: origin,
		}
	}
	x.totalBytes += len(definition.Query)
	if err := x.check(); err != nil {
		return nil, err
	}

	parser := new(sqlast.Parser)
	parser.SetCancellationCheck(x.options.Check)
	tree := new(sqlast.SelectStmt)
	if err := parser.Parse(tree, definition.Query); err != nil {
		return nil, fmt.Errorf("query: stored SQL view %q is invalid: %w", definition.Name, err)
	}
	if tree.Params != 0 {
		return nil, sqlast.NewFeatureNotSupportedError(
			x.source, origin,
			"stored view definitions cannot contain parameters",
		)
	}
	if err := applySQLViewColumns(
		x.source, tree, definition.Name, definition.Columns, origin,
	); err != nil {
		return nil, err
	}
	index := len(x.entries)
	x.entries = append(x.entries, sqlViewExpansionEntry{
		name: definition.Name, tree: tree, state: 1,
	})
	x.entryByName[definition.Name] = index
	x.stack = append(x.stack, definition.Name)
	if err := x.expandSelect(tree, depth, origin); err != nil {
		x.stack = x.stack[:len(x.stack)-1]
		return nil, err
	}
	x.stack = x.stack[:len(x.stack)-1]
	x.entries[index].state = 2
	return &x.entries[index], nil
}

func applySQLViewColumns(
	source string,
	tree *sqlast.SelectStmt,
	name string,
	aliases []string,
	position int,
) error {
	columns, deferred := sqlViewStaticOutputs(tree)
	if deferred {
		return sqlast.NewFeatureNotSupportedError(
			source, position,
			fmt.Sprintf("view %q has a wildcard-dependent output schema; list explicit projected columns so its durable schema cannot drift", name),
		)
	}
	if len(aliases) == 0 {
		return nil
	}
	if len(aliases) != columns {
		return sqlast.NewFeatureNotSupportedError(
			source, position,
			fmt.Sprintf("view %q declares %d output names for %d query columns", name, len(aliases), columns),
		)
	}
	if tree.Set == nil {
		for i := range aliases {
			tree.Columns[i].Alias = aliases[i]
		}
		return nil
	}
	first := tree.Set.First
	if first == nil || len(first.Columns) != len(aliases) {
		return errors.New("query: SQL view set expression lost first-operand output metadata")
	}
	for i := range aliases {
		first.Columns[i].Alias = aliases[i]
		if i < len(tree.Set.Outputs) {
			tree.Set.Outputs[i].Name = aliases[i]
			tree.Set.Outputs[i].Deferred = false
		}
	}
	return nil
}

func sqlViewStaticOutputs(tree *sqlast.SelectStmt) (int, bool) {
	if tree == nil {
		return 0, true
	}
	if tree.Set != nil {
		return len(tree.Set.Outputs), tree.Set.ArityDeferred
	}
	for i := range tree.Columns {
		column := &tree.Columns[i]
		if column.Agg == sqlast.AggNone && column.Path != nil &&
			len(column.Path.Segments) == 0 {
			return 0, true
		}
	}
	return len(tree.Columns), false
}

func (x *sqlViewExpander) hasDependency(name string) bool {
	for i := range x.dependencies {
		if x.dependencies[i].Name == name {
			return true
		}
	}
	return false
}

func (x *sqlViewExpander) currentName() string {
	if len(x.stack) == 0 {
		return ""
	}
	return x.stack[len(x.stack)-1]
}
