//go:build darwin || linux

package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardservice"
)

// roundTripWave prepares authenticated sockets before releasing all native
// requests together. These calls qualify waiter admission, not the listener's
// separate hard limit of sixteen simultaneous TLS handshakes. Its callers run
// with no concurrent fixture probes; the exact maximum is the shipped native
// listener's sixty-four connections, not an additional server allowance.
func (fixture *rf3FaultFixture) roundTripWave(
	ctx context.Context, member int, request *shardservice.ReplicatedRequest, callers int,
) ([]rf3FaultRoundTrip, error) {
	if ctx == nil || request == nil || member < 0 || member >= rf3CommandMembers || callers <= 0 || callers > 64 {
		return nil, fmt.Errorf("invalid RF3 fault wave")
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return nil, fmt.Errorf("RF3 fault wave requires a deadline")
	}
	connections := make([]rafttransport.PeerConnection, 0, callers)
	defer func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}()
	for range callers {
		connection, err := fixture.dialNative(ctx, member)
		if err != nil {
			return nil, fmt.Errorf("authenticate wave connection %d/%d: %w", len(connections)+1, callers, err)
		}
		connections = append(connections, connection)
	}
	start := make(chan struct{})
	results := make([]rf3FaultRoundTrip, callers)
	var ready, done sync.WaitGroup
	ready.Add(callers)
	done.Add(callers)
	for i, connection := range connections {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			results[i].response, results[i].err = shardservice.RoundTripReplicated(ctx, connection, request)
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	return results, nil
}
