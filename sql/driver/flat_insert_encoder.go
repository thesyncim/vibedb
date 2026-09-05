package driver

import (
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// FlatInsertEncoder shares the local SQL runtime's canonical document encoder
// with distributed mutation lowering. It borrows the validated INSERT AST.
type FlatInsertEncoder struct {
	insert   *sqlast.InsertStmt
	ordinals []uint32
	keyBytes uint64
}

func PrepareFlatInsertEncoder(insert *sqlast.InsertStmt) (*FlatInsertEncoder, error) {
	if insert == nil || len(insert.Columns) == 0 || insert.Source != nil {
		return nil, errors.New("vibedb: expected flat INSERT VALUES")
	}
	// Field layout does not execute the conflict action. Its planner owns
	// expression validation, so avoid compiling that projection a second time.
	layout := *insert
	layout.OnConflictUpdate = nil
	dml, err := query.PrepareParsedDML("", &sqlast.Statement{Kind: sqlast.KindInsert, Insert: &layout})
	if err != nil {
		return nil, err
	}
	defer dml.Release()
	return &FlatInsertEncoder{insert: insert, ordinals: slices.Clone(dml.InsertFlatFieldOrdinals()), keyBytes: dml.InsertFlatKeyJSONBytes()}, nil
}

func (e *FlatInsertEncoder) Encode(row *sqlast.InsertRow, args []any, maxBytes int) ([]byte, error) {
	if e == nil || row == nil {
		return nil, errors.New("vibedb: invalid flat INSERT encoder")
	}
	return encodeFlatInsertDocument(e.ordinals, e.keyBytes, e.insert, row, args, maxBytes)
}
