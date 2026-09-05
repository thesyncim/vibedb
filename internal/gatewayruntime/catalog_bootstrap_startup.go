package gatewayruntime

import (
	"context"
	"errors"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/serviceerrors"
)

// Fresh non-bootstrap frontends may finish their private session startup
// before the designated initializer publishes generation one. Discovery is
// read-only: this seam cannot publish a catalog, reopen a session, or replay an
// uncertain mutation. A retained route seed must never use this path: missing
// durable catalog state on restart is a fault, not a new-bootstrap window.
func waitReplicatedCatalogBootstrap(ctx context.Context, reader gatewayReplicaCatalogReader,
	attempts int, attemptTimeout time.Duration,
) (*gateway.Snapshot, error) {
	if ctx == nil || reader == nil || attempts <= 0 || attempts > gateway.AbsoluteMaxReplicatedAttempts ||
		attemptTimeout <= 0 || attemptTimeout > gateway.AbsoluteMaxReplicatedAttemptTimeout {
		return nil, gateway.ErrReplicatedCatalog
	}
	const poll = 250 * time.Millisecond
	budget := min(time.Duration(attempts)*attemptTimeout, 2*time.Minute)
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	var missing error
	for {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(missing, context.Cause(ctx))
		}
		snapshot, err := reader.Read(ctx)
		if err == nil {
			if snapshot == nil || snapshot.Generation() == 0 {
				return nil, gateway.ErrReplicatedCatalog
			}
			return snapshot, nil
		}
		// Match the whole failure tree. A joined missing marker cannot hide a
		// corruption, authentication failure, cancellation, or unknown outcome.
		if serviceerrors.Without(err, gateway.ErrReplicatedCatalogMissing) != nil {
			return nil, err
		}
		missing = err
		timer := time.NewTimer(poll)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.Join(missing, context.Cause(ctx))
		}
	}
}
