package driver

import (
	"errors"
	"strings"
	"testing"
)

func TestDropViewTableIsWrongObjectTypeEvenWithIfExists(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		`DROP VIEW docs`,
		`DROP VIEW IF EXISTS docs`,
	} {
		_, err := db.Exec(source)
		if !errors.Is(err, ErrWrongObjectType) {
			t.Fatalf("%s = %v, want ErrWrongObjectType", source, err)
		}
		var positioned interface{ Position() int }
		if !errors.As(err, &positioned) ||
			positioned.Position() != strings.LastIndex(source, "docs") {
			t.Fatalf("%s position = %v, want table name", source, err)
		}
	}
}
