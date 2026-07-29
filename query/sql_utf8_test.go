package query

import (
	"errors"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestSQLPreparationRejectsInvalidUTF8AtTheParserBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		src     string
		prepare func(string) error
	}{
		{
			name: "SELECT",
			src:  "SELECT name FROM docs -- \xff",
			prepare: func(src string) error {
				_, err := PrepareStatement(src)
				return err
			},
		},
		{
			name: "DML",
			src:  "DELETE FROM docs -- \xff",
			prepare: func(src string) error {
				_, err := PrepareDML(src)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.prepare(tc.src)
			if err == nil {
				t.Fatal("preparation accepted invalid UTF-8")
			}
			var parse *sqlast.ParseError
			if !errors.As(err, &parse) {
				t.Fatalf("preparation returned %T, want *sql.ParseError", err)
			}
			want := strings.IndexByte(tc.src, 0xff)
			if parse.Pos != want || !strings.Contains(parse.Msg, "UTF-8") {
				t.Fatalf("invalid UTF-8 error = %+v, want bad byte %d", parse, want)
			}
		})
	}
}
