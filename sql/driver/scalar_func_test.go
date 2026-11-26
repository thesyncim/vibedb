package driver_test

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestPostgreSQLBuiltinScalarFunctions(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING, score NUMERIC, created_at STRING, tags ARRAY)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO users VALUES ('{"id":1,"name":"  Ada Lovelace  ","score":-12.75,"created_at":"2026-11-26T15:30:45Z","tags":["a","b","c"]}')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var lo, up, tr, rep, sub, cat, uuid, dt, jt string
	var ab float64
	var ln, rd, yr, alen int64
	err = db.QueryRow(`
		SELECT LOWER(TRIM(name)),
		       UPPER(LEFT(TRIM(name), 3)),
		       TRIM(name),
		       REPLACE(TRIM(name), 'Ada', 'Grace'),
		       SUBSTRING(TRIM(name), 5, 4),
		       CONCAT_WS('-', 'u', id, NULL, 'ok'),
		       GEN_RANDOM_UUID(),
		       DATE_TRUNC('day', created_at),
		       JSONB_TYPEOF(tags),
		       LENGTH(TRIM(name)),
		       ABS(score),
		       ROUND(ABS(score)),
		       EXTRACT(YEAR FROM created_at),
		       JSONB_ARRAY_LENGTH(tags)
		FROM users
		WHERE LOWER(TRIM(name)) = 'ada lovelace'`).Scan(
		&lo, &up, &tr, &rep, &sub, &cat, &uuid, &dt, &jt,
		&ln, &ab, &rd, &yr, &alen,
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if lo != "ada lovelace" || up != "ADA" || tr != "Ada Lovelace" || rep != "Grace Lovelace" ||
		sub != "Love" || cat != "u-1-ok" || len(uuid) != 36 || !strings.HasPrefix(dt, "2026-11-26T00:00:00Z") ||
		jt != "array" || ln != 12 || ab != 12.75 || rd != 13 || yr != 2026 || alen != 3 {
		t.Fatalf("unexpected results: lo=%q up=%q tr=%q rep=%q sub=%q cat=%q uuid=%q dt=%q jt=%q ln=%d ab=%v rd=%d yr=%d alen=%d",
			lo, up, tr, rep, sub, cat, uuid, dt, jt, ln, ab, rd, yr, alen)
	}
}
