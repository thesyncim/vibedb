package driver

import (
	"database/sql"
	"testing"
)

func TestScalarConditionalCounterAndUpsert(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE channels (id TEXT PRIMARY KEY, member_count INT, superuser_count INT, channel_group TEXT)`,
		`INSERT INTO channels (id, member_count, superuser_count, channel_group) VALUES ('a', 2, NULL, '')`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatal(err)
		}
	}
	var members, supers int64
	var group sql.NullString
	err := db.QueryRow(`UPDATE channels SET
		member_count = GREATEST(0, member_count + ?),
		superuser_count = GREATEST(0, COALESCE(superuser_count, 0) + ?),
		channel_group = NULLIF(?, '')
		WHERE id = ? RETURNING member_count, superuser_count, channel_group`, -5, 3, "", "a").Scan(&members, &supers, &group)
	if err != nil || members != 0 || supers != 3 || group.Valid {
		t.Fatalf("counter update = %d/%d/%v: %v", members, supers, group, err)
	}
	err = db.QueryRow(`INSERT INTO channels (id, member_count, superuser_count, channel_group)
		VALUES ('a', 8, NULL, 'team') ON CONFLICT DO UPDATE SET
		member_count = GREATEST(channels.member_count, EXCLUDED.member_count),
		superuser_count = COALESCE(EXCLUDED.superuser_count, channels.superuser_count),
		channel_group = EXCLUDED.channel_group
		RETURNING member_count, superuser_count, channel_group`).Scan(&members, &supers, &group)
	if err != nil || members != 8 || supers != 3 || !group.Valid || group.String != "team" {
		t.Fatalf("counter upsert = %d/%d/%v: %v", members, supers, group, err)
	}
	stmt, err := db.Prepare(`SELECT COALESCE(NULLIF(?, ''), 'fallback'), GREATEST(?, 0), LEAST(?, 10)`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var name string
	for _, tc := range []struct {
		input, want     string
		value, min, max int64
	}{
		{"", "fallback", -3, 0, -3}, {"x", "x", 20, 20, 10},
	} {
		if err := stmt.QueryRow(tc.input, tc.value, tc.value).Scan(&name, &members, &supers); err != nil || name != tc.want || members != tc.min || supers != tc.max {
			t.Fatalf("prepared = %q/%d/%d: %v", name, members, supers, err)
		}
	}
}

func TestScalarConditionalNullExactnessLazinessAndGrouping(t *testing.T) {
	db := openTestDB(t)
	var first, greatest, least string
	var nullValue any
	err := db.QueryRow(`SELECT COALESCE(NULL, 'ok', CAST('bad' AS BOOLEAN)::TEXT),
		GREATEST(NULL, 9007199254740993, 9007199254740992)::TEXT,
		LEAST(9007199254740993, NULL, 9007199254740992)::TEXT,
		NULLIF(9007199254740993, 9007199254740993.0)`).Scan(&first, &greatest, &least, &nullValue)
	if err != nil || first != "ok" || greatest != "9007199254740993" || least != "9007199254740992" || nullValue != nil {
		t.Fatalf("conditional exact/lazy = %q/%q/%q/%v: %v", first, greatest, least, nullValue, err)
	}
	for _, source := range []string{
		`CREATE TABLE counters (id TEXT PRIMARY KEY, n INT)`,
		`INSERT INTO counters (id, n) VALUES ('a', 4), ('b', NULL), ('c', -3)`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatal(err)
		}
	}
	var sum int64
	if err := db.QueryRow(`SELECT COALESCE(SUM(n), 0) FROM counters`).Scan(&sum); err != nil || sum != 1 {
		t.Fatalf("aggregate = %d: %v", sum, err)
	}
	if err := db.QueryRow(`SELECT COALESCE(SUM(n), 0) FROM counters WHERE id = 'missing'`).Scan(&sum); err != nil || sum != 0 {
		t.Fatalf("empty aggregate = %d: %v", sum, err)
	}
	if err := db.QueryRow(`SELECT id FROM counters WHERE COALESCE(n, 0) = 0`).Scan(&first); err != nil || first != "b" {
		t.Fatalf("predicate = %q: %v", first, err)
	}
	if err := db.QueryRow(`SELECT id FROM counters WHERE n = COALESCE(?, 0) + 1`, 3).Scan(&first); err != nil || first != "a" {
		t.Fatalf("right-hand predicate = %q: %v", first, err)
	}
}
