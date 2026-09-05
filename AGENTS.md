# Agent workspace guidance

All worktrees and agents in this repository use the standard Go build cache
reported by `go env GOCACHE`. Do not set a task-specific `GOCACHE`, and do not
create a build cache inside a worktree. When sandbox permissions prevent using
the standard cache, request the normal authorized escalation for the Go
command.
