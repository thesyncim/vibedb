package driver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCatalogDecoderRejectsUnknownAndDuplicateMembers(t *testing.T) {
	validTable := `{
		"primary_key": "/id",
		"schema": {
			"root": 64,
			"fields": [{"path": "/id", "types": 16, "required": true}]
		}
	}`
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "missing version zero",
			raw:  `{"tables":{}}`,
			want: `missing member "version"`,
		},
		{
			name: "missing tables",
			raw:  `{"version":0}`,
			want: `missing member "tables"`,
		},
		{
			name: "unknown root",
			raw:  `{"version":0,"tables":{},"future":true}`,
			want: `unknown member "future"`,
		},
		{
			name: "duplicate root",
			raw:  `{"version":0,"version":0,"tables":{}}`,
			want: `duplicate member "version"`,
		},
		{
			name: "duplicate table name",
			raw: `{"version":0,"tables":{"docs":` + validTable +
				`,"docs":` + validTable + `}}`,
			want: `duplicate member "docs"`,
		},
		{
			name: "unknown table metadata",
			raw: `{"version":0,"tables":{"docs":{
				"primary_key":"/id",
				"future":true,
				"schema":{"root":64,"fields":[
					{"path":"/id","types":16,"required":true}
				]}
			}}}`,
			want: `unknown member "future"`,
		},
		{
			name: "duplicate primary key",
			raw: `{"version":0,"tables":{"docs":{
				"primary_key":"/id",
				"primary_key":"/other",
				"schema":{"root":64,"fields":[
					{"path":"/id","types":16,"required":true}
				]}
			}}}`,
			want: `duplicate member "primary_key"`,
		},
		{
			name: "unknown schema field metadata",
			raw: `{"version":0,"tables":{"docs":{
				"primary_key":"/id",
				"schema":{"root":64,"fields":[
					{"path":"/id","types":16,"required":true,"future":0}
				]}
			}}}`,
			want: `unknown member "future"`,
		},
		{
			name: "duplicate index metadata",
			raw: `{"version":0,"tables":{"docs":{
				"primary_key":"/id",
				"schema":{"root":64,"fields":[
					{"path":"/id","types":16,"required":true}
				]},
				"indexes":[{"name":"by_id","name":"other","paths":["/id"]}]
			}}}`,
			want: `duplicate member "name"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var catalog catalogFile
			err := json.Unmarshal([]byte(test.raw), &catalog)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"catalog decode = %v, want error containing %q",
					err, test.want,
				)
			}
		})
	}
}

func TestCatalogDecoderAcceptsCanonicalEncoding(t *testing.T) {
	input := catalogFile{
		Version: catalogVersion,
		Tables: map[string]*tableMeta{
			"docs": boundedCatalogTableMeta(false),
		},
	}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var decoded catalogFile
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode canonical catalog: %v", err)
	}
	if decoded.Version != input.Version ||
		len(decoded.Tables) != 1 ||
		decoded.Tables["docs"] == nil ||
		decoded.Tables["docs"].PrimaryKey != "/id" {
		t.Fatalf("decoded canonical catalog = %#v", decoded)
	}
}

func TestCatalogOpenRejectsInvalidUTF8BeforeJSONNormalization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	raw := []byte(`{"version":0,"tables":{"bad`)
	raw = append(raw, 0xff)
	raw = append(raw, []byte(`":{
		"primary_key":"/id",
		"schema":{
			"root":64,
			"fields":[{"path":"/id","types":16,"required":true}]
		}
	}}}`)...)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	database, err := openDatabase(path)
	if database != nil {
		_ = database.close()
		t.Fatal("invalid UTF-8 catalog opened")
	}
	if err == nil || !strings.Contains(err.Error(), "not valid UTF-8") {
		t.Fatalf("open invalid UTF-8 catalog = %v, want UTF-8 rejection", err)
	}
}
