package driver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// The catalog is durable metadata, not an extensible request payload. Unknown
// members and duplicate names therefore indicate a corrupt or incompatible
// image and fail closed. The ordinary encoding/json behavior (ignore unknown
// fields, keep the last duplicate) would otherwise let two readers assign
// different identities to the same bytes.
//
// These methods affect decoding only; encoding remains the catalog structs'
// ordinary stable JSON representation.

func (c *catalogFile) UnmarshalJSON(data []byte) error {
	var decoded catalogFile
	var versionPresent bool
	var tablesPresent bool
	err := decodeCatalogObject(data, "root", func(
		name string,
		decoder *json.Decoder,
	) error {
		switch name {
		case "version":
			versionPresent = true
			return decoder.Decode(&decoded.Version)
		case "tables":
			tablesPresent = true
			var tables catalogTableMap
			if err := decoder.Decode(&tables); err != nil {
				return err
			}
			decoded.Tables = map[string]*tableMeta(tables)
			return nil
		default:
			return unknownCatalogMember("root", name)
		}
	})
	if err != nil {
		return err
	}
	if !versionPresent {
		return fmt.Errorf("vibedb: SQL catalog root is missing member %q", "version")
	}
	if !tablesPresent {
		return fmt.Errorf("vibedb: SQL catalog root is missing member %q", "tables")
	}
	*c = decoded
	return nil
}

type catalogTableMap map[string]*tableMeta

func (m *catalogTableMap) UnmarshalJSON(data []byte) error {
	decoded := make(catalogTableMap)
	err := decodeCatalogObject(data, "tables", func(
		name string,
		decoder *json.Decoder,
	) error {
		if err := checkCatalogTableCount(len(decoded) + 1); err != nil {
			return err
		}
		var meta *tableMeta
		if err := decoder.Decode(&meta); err != nil {
			return fmt.Errorf("table %q: %w", name, err)
		}
		decoded[name] = meta
		return nil
	})
	if err != nil {
		return err
	}
	*m = decoded
	return nil
}

func (m *tableMeta) UnmarshalJSON(data []byte) error {
	var decoded tableMeta
	err := decodeCatalogObject(data, "table metadata", func(
		name string,
		decoder *json.Decoder,
	) error {
		switch name {
		case "primary_key":
			return decoder.Decode(&decoded.PrimaryKey)
		case "schema":
			return decoder.Decode(&decoded.Schema)
		case "indexes":
			var indexes catalogIndexList
			if err := decoder.Decode(&indexes); err != nil {
				return err
			}
			decoded.Indexes = []indexMeta(indexes)
			return nil
		case "materialized":
			return decoder.Decode(&decoded.Materialized)
		default:
			return unknownCatalogMember("table metadata", name)
		}
	})
	if err != nil {
		return err
	}
	*m = decoded
	return nil
}

func (m *schemaMeta) UnmarshalJSON(data []byte) error {
	var decoded schemaMeta
	err := decodeCatalogObject(data, "schema", func(
		name string,
		decoder *json.Decoder,
	) error {
		switch name {
		case "root":
			return decoder.Decode(&decoded.Root)
		case "fields":
			var fields catalogSchemaFieldList
			if err := decoder.Decode(&fields); err != nil {
				return err
			}
			decoded.Fields = []schemaFieldMeta(fields)
			return nil
		default:
			return unknownCatalogMember("schema", name)
		}
	})
	if err != nil {
		return err
	}
	*m = decoded
	return nil
}

func (m *schemaFieldMeta) UnmarshalJSON(data []byte) error {
	var decoded schemaFieldMeta
	err := decodeCatalogObject(data, "schema field", func(
		name string,
		decoder *json.Decoder,
	) error {
		switch name {
		case "path":
			return decoder.Decode(&decoded.Path)
		case "types":
			return decoder.Decode(&decoded.Types)
		case "required":
			return decoder.Decode(&decoded.Required)
		default:
			return unknownCatalogMember("schema field", name)
		}
	})
	if err != nil {
		return err
	}
	*m = decoded
	return nil
}

func (m *indexMeta) UnmarshalJSON(data []byte) error {
	var decoded indexMeta
	err := decodeCatalogObject(data, "index", func(
		name string,
		decoder *json.Decoder,
	) error {
		switch name {
		case "name":
			return decoder.Decode(&decoded.Name)
		case "paths":
			var paths catalogIndexPathList
			if err := decoder.Decode(&paths); err != nil {
				return err
			}
			decoded.Paths = []string(paths)
			return nil
		default:
			return unknownCatalogMember("index", name)
		}
	})
	if err != nil {
		return err
	}
	*m = decoded
	return nil
}

type catalogSchemaFieldList []schemaFieldMeta

func (l *catalogSchemaFieldList) UnmarshalJSON(data []byte) error {
	var decoded catalogSchemaFieldList
	err := decodeCatalogArray(
		data, "schema fields", storeio.PageCatalogMaxSchemaFields,
		func(decoder *json.Decoder) error {
			var field schemaFieldMeta
			if err := decoder.Decode(&field); err != nil {
				return err
			}
			decoded = append(decoded, field)
			return nil
		},
	)
	if err != nil {
		return err
	}
	*l = decoded
	return nil
}

type catalogIndexList []indexMeta

func (l *catalogIndexList) UnmarshalJSON(data []byte) error {
	var decoded catalogIndexList
	err := decodeCatalogArray(
		data, "indexes", storeio.PageCatalogMaxLogicalIndexes,
		func(decoder *json.Decoder) error {
			var index indexMeta
			if err := decoder.Decode(&index); err != nil {
				return err
			}
			decoded = append(decoded, index)
			return nil
		},
	)
	if err != nil {
		return err
	}
	*l = decoded
	return nil
}

type catalogIndexPathList []string

func (l *catalogIndexPathList) UnmarshalJSON(data []byte) error {
	var decoded catalogIndexPathList
	err := decodeCatalogArray(
		data, "index paths", storeio.PageCatalogMaxIndexColumns,
		func(decoder *json.Decoder) error {
			var path string
			if err := decoder.Decode(&path); err != nil {
				return err
			}
			decoded = append(decoded, path)
			return nil
		},
	)
	if err != nil {
		return err
	}
	*l = decoded
	return nil
}

func decodeCatalogObject(
	data []byte,
	kind string,
	consume func(name string, decoder *json.Decoder) error,
) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return fmt.Errorf(
			"vibedb: SQL catalog %s must be a JSON object", kind,
		)
	}
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		name, ok := token.(string)
		if !ok {
			return fmt.Errorf(
				"vibedb: SQL catalog %s member name is not a string", kind,
			)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf(
				"vibedb: SQL catalog %s has duplicate member %q",
				kind, name,
			)
		}
		seen[name] = struct{}{}
		if err := consume(name, decoder); err != nil {
			return err
		}
	}
	token, err = decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '}' {
		return fmt.Errorf(
			"vibedb: SQL catalog %s has an invalid object terminator", kind,
		)
	}
	return catalogDecoderEOF(decoder, kind)
}

func decodeCatalogArray(
	data []byte,
	kind string,
	limit int,
	consume func(decoder *json.Decoder) error,
) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '[' {
		return fmt.Errorf(
			"vibedb: SQL catalog %s must be a JSON array", kind,
		)
	}
	count := 0
	for decoder.More() {
		if count >= limit {
			return fmt.Errorf(
				"vibedb: SQL catalog %s exceeds the format limit of %d",
				kind, limit,
			)
		}
		if err := consume(decoder); err != nil {
			return err
		}
		count++
	}
	token, err = decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != ']' {
		return fmt.Errorf(
			"vibedb: SQL catalog %s has an invalid array terminator", kind,
		)
	}
	return catalogDecoderEOF(decoder, kind)
}

func catalogDecoderEOF(decoder *json.Decoder, kind string) error {
	if token, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf(
			"vibedb: SQL catalog %s has trailing token %v", kind, token,
		)
	}
	return nil
}

func unknownCatalogMember(kind, name string) error {
	return fmt.Errorf(
		"vibedb: SQL catalog %s has unknown member %q", kind, name,
	)
}
