package gateway

import (
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestGeneralWriteRewriteHelpers(t *testing.T) {
	// INSERT ... SELECT extraction
	var parser sqlast.Parser
	var stmt sqlast.Statement
	src := `INSERT INTO archive SELECT "$doc" FROM messages WHERE n > 10 ON CONFLICT DO NOTHING RETURNING id`
	if err := parser.ParseStatement(&stmt, src); err != nil {
		t.Fatalf("ParseStatement: %v", err)
	}
	gotSrc, err := extractInsertSelectSource(src, stmt.Insert)
	if err != nil {
		t.Fatalf("extractInsertSelectSource: %v", err)
	}
	if want := `SELECT "$doc" FROM messages WHERE n > 10`; gotSrc != want {
		t.Fatalf("extractInsertSelectSource = %q, want %q", gotSrc, want)
	}

	// DELETE rewrite
	gotDel := buildSelectFromDelete(`DELETE FROM messages WHERE n > ? ORDER BY id LIMIT 5 RETURNING id`, "messages")
	if want := `SELECT "$doc" FROM messages WHERE n > ? ORDER BY id LIMIT 5`; gotDel != want {
		t.Fatalf("buildSelectFromDelete = %q, want %q", gotDel, want)
	}

	// UPDATE split
	setPrefix, whereTail, setParams, ok := splitUpdateForGeneralRewrite(
		`UPDATE messages AS m SET n = m.n + ?, active = TRUE WHERE m.n > ? ORDER BY m.id LIMIT 3 RETURNING id`,
		"messages",
	)
	if !ok {
		t.Fatal("splitUpdateForGeneralRewrite returned ok=false")
	}
	if want := `UPDATE messages AS m SET n = m.n + ?, active = TRUE`; setPrefix != want {
		t.Fatalf("setPrefix = %q, want %q", setPrefix, want)
	}
	if want := `messages AS m WHERE m.n > ? ORDER BY m.id LIMIT 3`; whereTail != want {
		t.Fatalf("whereTail = %q, want %q", whereTail, want)
	}
	if setParams != 1 {
		t.Fatalf("setParams = %d, want 1", setParams)
	}

	// Pointer to SQL path
	pkPath, err := pointerToSQLPath("/profile/user_id")
	if err != nil || pkPath != `"profile"."user_id"` {
		t.Fatalf("pointerToSQLPath = %q, %v", pkPath, err)
	}
}
