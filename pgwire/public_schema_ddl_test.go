package pgwire

import (
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestPublicDDLNamesPreserveOffsets(t *testing.T) {
	for _, input := range []string{
		`CREATE TABLE public.employees (id TEXT PRIMARY KEY, city TEXT)`,
		`CREATE TABLE IF NOT EXISTS "public"."employees" (id TEXT PRIMARY KEY)`,
		`CREATE INDEX public.employees_city ON public.employees (city)`,
		`CREATE INDEX IF NOT EXISTS public.employees_city ON "public".employees (city)`,
		`DROP TABLE IF EXISTS public.employees`,
		`DROP INDEX IF EXISTS public.employees_city ON public.employees`,
		`TRUNCATE public.employees`,
		`TRUNCATE TABLE "public".employees`,
	} {
		output, changed, err := lowerPublicRelations(input, nil)
		if err != nil || !changed || len(output) != len(input) {
			t.Fatalf("%s => %s, %v", input, output, err)
		}
		if strings.Contains(output, "public") {
			t.Fatalf("unresolved public qualifier: %s", output)
		}
		if _, err := sqlast.ParseStatement(output); err != nil {
			t.Fatalf("%s: %v", output, err)
		}
	}
	for _, input := range []string{
		`CREATE TABLE private.employees (id TEXT PRIMARY KEY)`,
		`SELECT public.city FROM documents public`,
		`SELECT '$doc public.employees' FROM documents`,
	} {
		output, changed, err := lowerPublicRelations(input, nil)
		if err != nil || changed || output != input {
			t.Fatalf("rewrote non-public relation: %s => %s", input, output)
		}
	}
}
