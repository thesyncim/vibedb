package main

import (
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

func parseRF3ChildSchemaStatements(node vibejson.Node, used int) ([]string, error) {
	count, ok := node.ArrayLen()
	if !ok || count < 1 || count > 4096+replication.MaxRelationsPerBundle-1 {
		return nil, errInvalidRF3Manifest
	}
	statements := make([]string, 0, count)
	iter, _ := node.ArrayIter()
	for range count {
		value, present := iter.Next()
		if !present || used >= sqldriver.ReplicatedChildSchemaMaxBytes {
			return nil, errInvalidRF3Manifest
		}
		statement, err := rf3ManifestString(value, sqldriver.ReplicatedChildSchemaMaxBytes-used)
		if err != nil {
			return nil, err
		}
		used += len(statement)
		statements = append(statements, statement)
	}
	return statements, nil
}

func parseRF3ChildGlobalIndexes(node vibejson.Node) ([]sqldriver.ReplicatedGlobalIndexRelation, error) {
	count, ok := node.ArrayLen()
	if !ok || count < 1 || count >= replication.MaxRelationsPerBundle {
		return nil, errInvalidRF3Manifest
	}
	result := make([]sqldriver.ReplicatedGlobalIndexRelation, count)
	iter, _ := node.ArrayIter()
	for i := range result {
		value, present := iter.Next()
		if !present {
			return nil, errInvalidRF3Manifest
		}
		fields, ok := value.ObjectIter()
		if !ok {
			return nil, errInvalidRF3Manifest
		}
		value, err := nextRF3Field(&fields, `"relation"`)
		relation, valid := value.Uint64()
		if err != nil || !valid || relation != uint64(i+2) {
			return nil, errInvalidRF3Manifest
		}
		result[i].Relation = uint16(relation)
		value, err = nextRF3Field(&fields, `"table"`)
		if err != nil {
			return nil, err
		}
		if result[i].Table, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
			return nil, err
		}
		for _, field := range []struct {
			name   string
			target *uint64
		}{
			{`"index_id"`, &result[i].IndexID}, {`"incarnation"`, &result[i].Incarnation},
		} {
			value, err = nextRF3Field(&fields, field.name)
			if err != nil {
				return nil, err
			}
			if *field.target, err = rf3ManifestPositiveUint64(value); err != nil {
				return nil, err
			}
		}
		value, err = nextRF3Field(&fields, `"locator_count"`)
		locator, valid := value.Uint64()
		if err != nil || !valid || locator < 1 || locator > 8 {
			return nil, errInvalidRF3Manifest
		}
		result[i].LocatorCount = uint8(locator)
		value, err = nextRF3Field(&fields, `"unique"`)
		if result[i].Unique, ok = value.Bool(); err != nil || !ok {
			return nil, errInvalidRF3Manifest
		}
		var numbers [5]uint8
		for j, key := range []string{`"key_encoding"`, `"key_arity"`, `"tuple_version"`, `"mapper_version"`, `"bucket_bits"`} {
			value, err = nextRF3Field(&fields, key)
			number, valid := value.Uint64()
			if err != nil || !valid || number < 1 || number > 255 {
				return nil, errInvalidRF3Manifest
			}
			numbers[j] = uint8(number)
		}
		result[i].KeyEncoding, result[i].KeyArity = sqldriver.ReplicatedRelationKeyEncoding(numbers[0]), numbers[1]
		result[i].TupleVersion, result[i].MapperVersion = distribution.TupleVersion(numbers[2]), distribution.MapperVersion(numbers[3])
		result[i].BucketBits = numbers[4]
		if _, _, extra := fields.Next(); extra {
			return nil, errInvalidRF3Manifest
		}
	}
	return result, nil
}

func rf3SplitChildSchemaMatchesRetained(registry rf3ManifestSplitChildRegistry, base sqldriver.ReplicatedShardStoreIdentity) bool {
	return registry.Table == base.UserTable && sqldriver.ValidateReplicatedChildSchema(base,
		registry.CreateTable, registry.SchemaStatements, registry.GlobalIndexes) == nil
}
