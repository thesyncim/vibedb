package driver

import "errors"

var (
	// ErrAutocommitOnly reports that engine-backed transactions are not
	// available in this build. Autocommit statements continue to work.
	ErrAutocommitOnly = errors.New("vibedb: autocommit only; BEGIN waits for the ordered-primary transactional WriteBatch landing")
	// ErrIndexedTableReadOnly reports a mutation against a table whose exact
	// posting index is currently build-and-read.
	ErrIndexedTableReadOnly = errors.New("vibedb: indexed tables are build-and-read; mutation waits for mutable exact postings")
)
