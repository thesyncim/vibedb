package gateway

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/shardservice"
)

func TestSlowQueryObserverFiresOnlyAtOrAboveThreshold(t *testing.T) {
	var mu sync.Mutex
	var events []SlowQueryEvent

	e := &Executor{
		slowQueryThreshold: 5 * time.Millisecond,
		slowQueryObserver: func(ev SlowQueryEvent) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		},
	}

	// Fast query below threshold must not emit.
	e.observeSlowQuery("read", "SELECT 1", 2*time.Millisecond, &Result{ShardsFanned: 1}, nil)
	if len(events) != 0 {
		t.Fatalf("fast query emitted %d events, want 0", len(events))
	}

	// Slow read at threshold must emit with metadata.
	e.observeSlowQuery("read", "SELECT * FROM t", 12*time.Millisecond, &Result{
		ShardsFanned: 3,
		Rows:         [][]shardservice.Cell{{}, {}},
	}, nil)

	// Slow failed write must also emit with error preserved.
	e.observeSlowQuery("write", "INSERT INTO t VALUES (1)", 20*time.Millisecond, nil, context.DeadlineExceeded)

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	if events[0].Operation != "read" || events[0].Shards != 3 || events[0].Rows != 2 || events[0].Err != nil {
		t.Fatalf("event[0] = %+v", events[0])
	}
	if events[1].Operation != "write" || events[1].Err != context.DeadlineExceeded {
		t.Fatalf("event[1] = %+v", events[1])
	}
}
