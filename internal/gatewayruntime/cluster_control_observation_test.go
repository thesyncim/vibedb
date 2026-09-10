package gatewayruntime

import (
	"context"
	"github.com/thesyncim/vibedb/internal/clustercontrol"
	"testing"
	"time"
)

func TestClusterControlWaitRetainsLastAuthoritativeObservation(t *testing.T) {
	calls := 0
	result := observeClusterControlStatus(t.Context(), 50*time.Millisecond, func(ctx context.Context) clustercontrol.Response {
		calls++
		if calls == 1 {
			return clustercontrol.Response{OK: true, OperationID: "exact-operation", State: "running", DirectoryRevision: 7}
		}
		<-ctx.Done()
		return clustercontrol.Response{Error: ctx.Err().Error()}
	})
	if calls != 2 || !result.OK || result.Error != "" || result.OperationID != "exact-operation" || result.DirectoryRevision != 7 || result.SafeToStop || result.State != "running" {
		t.Fatalf("calls=%d result=%+v", calls, result)
	}
}

func TestClusterControlWaitDoesNotCancelInitialRead(t *testing.T) {
	result := observeClusterControlStatus(t.Context(), time.Nanosecond, func(ctx context.Context) clustercontrol.Response {
		if _, bounded := ctx.Deadline(); bounded {
			t.Fatal("observation budget applied to initial read")
		}
		return clustercontrol.Response{OK: true, State: "running"}
	})
	if !result.OK || result.State != "running" {
		t.Fatalf("result=%+v", result)
	}
}

func TestClusterControlWaitPreservesReadFailures(t *testing.T) {
	for _, failCall := range []int{1, 2} {
		calls := 0
		result := observeClusterControlStatus(t.Context(), time.Second, func(context.Context) clustercontrol.Response {
			calls++
			if calls == failCall {
				return clustercontrol.Response{Error: "invalid durable proof"}
			}
			return clustercontrol.Response{OK: true, State: "running"}
		})
		if result.OK || result.Error != "invalid durable proof" || calls != failCall {
			t.Fatalf("calls=%d result=%+v", calls, result)
		}
	}
}

func TestClusterControlWaitReturnsNewTerminalProof(t *testing.T) {
	calls := 0
	result := observeClusterControlStatus(t.Context(), time.Second, func(context.Context) clustercontrol.Response {
		calls++
		if calls == 1 {
			return clustercontrol.Response{OK: true, State: "running", DirectoryRevision: 7}
		}
		return clustercontrol.Response{OK: true, State: "decommissioned", DirectoryRevision: 8, SafeToStop: true}
	})
	if calls != 2 || !result.OK || !result.SafeToStop || result.DirectoryRevision != 8 || result.State != "decommissioned" {
		t.Fatalf("calls=%d result=%+v", calls, result)
	}
}
