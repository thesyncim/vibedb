package gateway

import (
	"time"

	"github.com/thesyncim/vibedb/distribution"
)

// SlowQueryEvent describes one gateway read or write whose end-to-end latency
// met or exceeded Options.SlowQueryThreshold.
type SlowQueryEvent struct {
	Operation string                 // "read" or "write"
	SQL       string                 // statement text (without parameter values)
	RouteKind distribution.RouteKind // route classification when available
	Shards    int                    // number of shards fanned out to
	Rows      int                    // rows returned (reads) or affected (writes)
	Duration  time.Duration          // end-to-end wall latency
	Err       error                  // non-nil when the slow operation failed or timed out
}

// SlowQueryObserver receives SlowQueryEvent notifications for operations whose
// latency meets or exceeds Options.SlowQueryThreshold. Implementations must be
// safe for concurrent invocation and non-blocking.
type SlowQueryObserver func(SlowQueryEvent)

func (e *Executor) observeSlowQuery(op, sql string, dur time.Duration, res *Result, err error) {
	if e.slowQueryThreshold <= 0 || e.slowQueryObserver == nil || dur < e.slowQueryThreshold {
		return
	}
	ev := SlowQueryEvent{
		Operation: op,
		SQL:       sql,
		Duration:  dur,
		Err:       err,
	}
	if res != nil {
		ev.RouteKind = res.RouteKind
		ev.Shards = res.ShardsFanned
		if op == "read" {
			ev.Rows = len(res.Rows)
		} else {
			ev.Rows = int(res.RowsAffected)
		}
	}
	e.slowQueryObserver(ev)
}
