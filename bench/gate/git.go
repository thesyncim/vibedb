package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// gitTopLevel returns the absolute root of the git working tree containing dir.
func gitTopLevel(dir string) (string, error) {
	out, err := gitOutput(dir, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", fmt.Errorf("locating git root: %w", err)
	}
	return out, nil
}

// resolveBase turns the -base flag into a concrete commit SHA and a short
// human description. An empty flag means "the merge-base of HEAD and main",
// which is the fork point the working revision actually diverged from; a
// non-empty flag is resolved as a git revision directly. resolveBase never
// falls back silently: if it cannot resolve a base it returns an error so the
// gate stops rather than comparing against the wrong revision.
func resolveBase(root, flag string) (sha, desc string, err error) {
	flag = strings.TrimSpace(flag)
	if flag == "" {
		sha, err = gitOutput(root, "merge-base", "HEAD", "main")
		if err != nil {
			return "", "", fmt.Errorf("resolving merge-base of HEAD and main "+
				"(pass -base to name a revision explicitly): %w", err)
		}
		return sha, "merge-base of HEAD and main", nil
	}
	sha, err = gitOutput(root, "rev-parse", "--verify", flag+"^{commit}")
	if err != nil {
		return "", "", fmt.Errorf("resolving base revision %q: %w", flag, err)
	}
	return sha, "requested base " + flag, nil
}

// addWorktree checks the base commit out into a throwaway detached worktree and
// returns its path plus a cleanup function. It uses --detach on the SHA rather
// than a branch name so it never conflicts with a branch already checked out in
// the primary tree, and it never touches the primary tree's uncommitted files.
func addWorktree(root, sha string) (dir string, cleanup func(), err error) {
	dir, err = os.MkdirTemp("", "vibedb-bench-gate-base-")
	if err != nil {
		return "", nil, fmt.Errorf("creating base worktree dir: %w", err)
	}
	if _, err = gitOutput(root, "worktree", "add", "--detach", "--force", dir, sha); err != nil {
		_ = os.RemoveAll(dir)
		return "", nil, fmt.Errorf("adding base worktree: %w", err)
	}
	cleanup = func() {
		if _, rmErr := gitOutput(root, "worktree", "remove", "--force", dir); rmErr != nil {
			// Fall back to a filesystem removal and prune so a failed git
			// removal cannot leak the checkout or its administrative entry.
			_ = os.RemoveAll(dir)
			_, _ = gitOutput(root, "worktree", "prune")
		}
	}
	return dir, cleanup, nil
}

// gitOutput runs git in dir and returns trimmed stdout, or an error carrying
// git's stderr.
func gitOutput(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s",
			strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}
