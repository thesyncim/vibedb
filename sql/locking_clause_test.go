package sql

import "testing"

func TestSelectForUpdateShareLockingClauses(t *testing.T) {
	cases := []struct {
		src        string
		mode       RowLockMode
		skipLocked bool
		noWait     bool
	}{
		{`SELECT id FROM jobs WHERE state = 'queued' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`, RowLockUpdate, true, false},
		{`SELECT id FROM jobs WHERE id = 1 FOR NO KEY UPDATE NOWAIT`, RowLockNoKeyUpdate, false, true},
		{`SELECT id FROM jobs FOR SHARE OF jobs`, RowLockShare, false, false},
		{`SELECT id FROM jobs FOR KEY SHARE`, RowLockKeyShare, false, false},
	}
	for _, tc := range cases {
		var p Parser
		var stmt Statement
		if err := p.ParseStatement(&stmt, tc.src); err != nil {
			t.Fatalf("ParseStatement(%q): %v", tc.src, err)
		}
		if stmt.Select.LockMode != tc.mode || stmt.Select.SkipLocked != tc.skipLocked || stmt.Select.NoWait != tc.noWait {
			t.Fatalf("ParseStatement(%q): got mode=%d skip=%t nowait=%t", tc.src, stmt.Select.LockMode, stmt.Select.SkipLocked, stmt.Select.NoWait)
		}
	}
}
