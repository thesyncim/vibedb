package gateway

import (
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/sql/driver"
)

// ReplicatedTableDeclaration is cold discovery metadata, supplied by the
// authenticated schema provisioner. It is never retained in the hot routing
// directory or resolved on a data request. Empty declarations need no sidecar.
type ReplicatedTableDeclaration struct {
	Table       string
	CreateTable string
}

type replicatedTableDeclaration struct {
	declaration ReplicatedTableDeclaration
	info        driver.TableInfo
}

func (s *Snapshot) attachReplicatedTableDeclarations(inputs []ReplicatedTableDeclaration) error {
	if len(inputs) > len(s.replicatedTables) {
		return &CatalogError{Reason: "too many replicated table declarations"}
	}
	for _, input := range inputs {
		if len(input.CreateTable) == 0 || len(input.CreateTable) > driver.ReplicatedChildSchemaMaxBytes {
			return &CatalogError{Reason: "replicated table declaration exceeds bound"}
		}
		tree, err := sqlast.ParseStatement(input.CreateTable)
		if err != nil || tree.CreateTable == nil || tree.CreateTable.Table != input.Table || tree.CreateTable.IfNotExists {
			return &CatalogError{Reason: "invalid replicated table declaration"}
		}
		prepared, err := query.PrepareParsedDML(input.CreateTable, tree)
		if err != nil {
			return err
		}
		_, err = prepared.LowerTable()
		prepared.Release()
		if err != nil {
			return err
		}
		var profile ReplicatedTableProfile
		found := false
		for _, candidate := range s.replicatedTables {
			p, ok := s.replicatedTableProfileAt(candidate)
			if ok && p.Table == input.Table {
				profile, found = p, true
				break
			}
		}
		if !found || len(tree.CreateTable.PrimaryKey) != 1 ||
			string(tree.CreateTable.PrimaryKey[0].AppendPointer(nil)) != profile.PrimaryKey {
			return &CatalogError{Reason: "replicated table declaration has no matching routing profile"}
		}
		info := driver.TableInfo{Name: strings.Clone(input.Table), PrimaryKey: profile.PrimaryKey}
		for _, column := range tree.CreateTable.Columns {
			info.Columns = append(info.Columns, driver.ColumnInfo{Path: string(column.Path.AppendPointer(nil)), Types: column.Type, Required: column.Required})
		}
		s.replicatedTableDeclarations = append(s.replicatedTableDeclarations, replicatedTableDeclaration{
			declaration: ReplicatedTableDeclaration{Table: info.Name, CreateTable: strings.Clone(input.CreateTable)}, info: info,
		})
	}
	slices.SortFunc(s.replicatedTableDeclarations, func(a, b replicatedTableDeclaration) int {
		return strings.Compare(a.declaration.Table, b.declaration.Table)
	})
	for i := 1; i < len(s.replicatedTableDeclarations); i++ {
		if s.replicatedTableDeclarations[i-1].declaration.Table == s.replicatedTableDeclarations[i].declaration.Table {
			return &CatalogError{Reason: "duplicate replicated table declaration"}
		}
	}
	return nil
}

func (s *Snapshot) ReplicatedTableDeclarations() []ReplicatedTableDeclaration {
	if s == nil || len(s.replicatedTableDeclarations) == 0 {
		return nil
	}
	result := make([]ReplicatedTableDeclaration, len(s.replicatedTableDeclarations))
	for i := range result {
		result[i] = s.replicatedTableDeclarations[i].declaration
	}
	return result
}

func (s *Snapshot) declaredTableInfo(table string) (driver.TableInfo, bool) {
	i, found := slices.BinarySearchFunc(s.replicatedTableDeclarations, table, func(a replicatedTableDeclaration, b string) int { return strings.Compare(a.declaration.Table, b) })
	if !found {
		return driver.TableInfo{}, false
	}
	info := s.replicatedTableDeclarations[i].info
	info.Columns = slices.Clone(info.Columns)
	return info, true
}
